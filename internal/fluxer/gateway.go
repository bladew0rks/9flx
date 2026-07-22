package fluxer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"runtime"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	opDispatch       = 0
	opHeartbeat      = 1
	opIdentify       = 2
	opResume         = 6
	opReconnect      = 7
	opInvalidSession = 9
	opHello          = 10
	opHeartbeatACK   = 11
)

type FatalGatewayError struct{ Err error }

func (e *FatalGatewayError) Error() string { return e.Err.Error() }
func (e *FatalGatewayError) Unwrap() error { return e.Err }

type Gateway struct {
	URL         string
	Token       string
	Dialer      *websocket.Dialer
	OnEvent     func(eventType string, sequence int64, data json.RawMessage) error
	OnState     func(state string, connected bool, sequence int64)
	OnError     func(error)
	OnReconnect func()
	OnGap       func()

	mu           sync.Mutex
	sessionID    string
	sequence     int64
	lastActivity time.Time
	readySince   time.Time
	gapPending   bool
}

func (g *Gateway) Run(ctx context.Context) error {
	if g.Dialer == nil {
		g.Dialer = websocket.DefaultDialer
	}
	backoff := time.Second
	first := true
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		if !first && g.OnReconnect != nil {
			g.OnReconnect()
		}
		first = false
		g.state("connecting", false)
		err := g.connect(ctx)
		g.mu.Lock()
		wasReady := !g.readySince.IsZero()
		g.readySince = time.Time{}
		g.mu.Unlock()
		if wasReady {
			backoff = time.Second
		}
		if err == nil || errors.Is(err, context.Canceled) {
			return nil
		}
		var fatal *FatalGatewayError
		if errors.As(err, &fatal) {
			g.state("fatal", false)
			return fatal
		}
		g.report(err)
		g.state("reconnecting", false)
		jitter := time.Duration(rand.Int63n(int64(backoff/4 + 1)))
		timer := time.NewTimer(backoff + jitter)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
		if backoff < 60*time.Second {
			backoff *= 2
			if backoff > 60*time.Second {
				backoff = 60 * time.Second
			}
		}
	}
}

func (g *Gateway) connect(ctx context.Context) error {
	u, err := url.Parse(g.URL)
	if err != nil {
		return fmt.Errorf("gateway URL: %w", err)
	}
	query := u.Query()
	query.Set("v", "1")
	query.Set("encoding", "json")
	query.Set("compress", "none")
	u.RawQuery = query.Encode()
	conn, response, err := g.Dialer.DialContext(ctx, u.String(), http.Header{"User-Agent": []string{"9flx/0.1"}})
	if err != nil {
		if response != nil {
			return fmt.Errorf("gateway dial: HTTP %d: %w", response.StatusCode, err)
		}
		return fmt.Errorf("gateway dial: %w", err)
	}
	defer conn.Close()

	closed := make(chan struct{})
	var closeOnce sync.Once
	stop := func() { closeOnce.Do(func() { close(closed); _ = conn.Close() }) }
	go func() {
		select {
		case <-ctx.Done():
			stop()
		case <-closed:
		}
	}()

	var writeMu sync.Mutex
	write := func(v any) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		return conn.WriteJSON(v)
	}

	var heartbeatMu sync.Mutex
	var heartbeatStarted bool
	var heartbeatStop chan struct{}
	var awaitingACK bool
	var heartbeatSent time.Time
	var heartbeatGeneration uint64
	markHeartbeatSent := func() {
		heartbeatMu.Lock()
		awaitingACK, heartbeatSent = true, time.Now()
		heartbeatGeneration++
		generation := heartbeatGeneration
		heartbeatMu.Unlock()
		go func() {
			timer := time.NewTimer(15 * time.Second)
			defer timer.Stop()
			select {
			case <-timer.C:
				heartbeatMu.Lock()
				timedOut := awaitingACK && heartbeatGeneration == generation
				heartbeatMu.Unlock()
				if timedOut {
					stop()
				}
			case <-closed:
			}
		}()
	}
	startHeartbeat := func(interval time.Duration) {
		if heartbeatStarted {
			return
		}
		heartbeatStarted = true
		heartbeatStop = make(chan struct{})
		go func() {
			delay := interval * 4 / 5
			if delay < time.Second {
				delay = time.Second
			}
			ticker := time.NewTicker(delay)
			defer ticker.Stop()
			for {
				select {
				case <-heartbeatStop:
					return
				case <-closed:
					return
				case <-ticker.C:
					heartbeatMu.Lock()
					waiting := awaitingACK
					if waiting && time.Since(heartbeatSent) >= 15*time.Second {
						stop()
					}
					heartbeatMu.Unlock()
					if waiting {
						continue
					}
					g.mu.Lock()
					seq := g.sequence
					g.mu.Unlock()
					if err := write(map[string]any{"op": opHeartbeat, "d": seq}); err != nil {
						stop()
						return
					}
					markHeartbeatSent()
				}
			}
		}()
	}
	defer func() {
		if heartbeatStarted {
			close(heartbeatStop)
		}
		stop()
	}()

	for {
		var payload GatewayPayload
		if err := conn.ReadJSON(&payload); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			var closeErr *websocket.CloseError
			if errors.As(err, &closeErr) && closeErr.Code == 4004 {
				return &FatalGatewayError{Err: fmt.Errorf("gateway authentication failed: %s", closeErr.Text)}
			}
			return fmt.Errorf("gateway read: %w", err)
		}
		g.mu.Lock()
		previousActivity := g.lastActivity
		g.lastActivity = time.Now()
		if payload.S != nil && *payload.S > g.sequence {
			g.sequence = *payload.S
		}
		seq := g.sequence
		g.mu.Unlock()
		switch payload.Op {
		case opHello:
			var hello struct {
				HeartbeatInterval int64 `json:"heartbeat_interval"`
			}
			if err := json.Unmarshal(payload.D, &hello); err != nil || hello.HeartbeatInterval <= 0 {
				return errors.New("gateway HELLO has invalid heartbeat interval")
			}
			startHeartbeat(time.Duration(hello.HeartbeatInterval) * time.Millisecond)
			g.mu.Lock()
			hadSession := g.sessionID != ""
			resumable := hadSession && !previousActivity.IsZero() && time.Since(previousActivity) <= 3*time.Minute
			sessionID, currentSeq := g.sessionID, g.sequence
			if hadSession && !resumable {
				g.sessionID, g.sequence, g.gapPending = "", 0, true
				currentSeq = 0
			}
			g.mu.Unlock()
			if resumable {
				err = write(map[string]any{"op": opResume, "d": map[string]any{"token": g.Token, "session_id": sessionID, "seq": currentSeq}})
			} else {
				err = write(map[string]any{"op": opIdentify, "d": map[string]any{
					"token": g.Token, "flags": 0,
					"properties": map[string]string{"os": runtime.GOOS, "browser": "9flx", "device": "9flx", "locale": "en-US", "user_agent": "9flx/0.1", "browser_version": "0.1", "os_version": runtime.GOARCH, "build_version": "0.1"},
				}})
			}
			if err != nil {
				return fmt.Errorf("gateway authenticate: %w", err)
			}
		case opDispatch:
			if payload.T == "READY" {
				var ready struct {
					SessionID string `json:"session_id"`
				}
				if err := json.Unmarshal(payload.D, &ready); err != nil {
					return err
				}
				g.mu.Lock()
				g.sessionID = ready.SessionID
				g.readySince = time.Now()
				gap := g.gapPending
				g.gapPending = false
				g.mu.Unlock()
				if gap && g.OnGap != nil {
					g.OnGap()
				}
				g.state("ready", true)
			} else if payload.T == "RESUMED" {
				g.state("ready", true)
			}
			if g.OnEvent != nil {
				if err := g.OnEvent(payload.T, seq, payload.D); err != nil {
					g.report(fmt.Errorf("gateway %s: %w", payload.T, err))
				}
			}
			if payload.T != "READY" && payload.T != "RESUMED" {
				g.state("ready", true)
			}
		case opHeartbeat:
			g.mu.Lock()
			seq := g.sequence
			g.mu.Unlock()
			if err := write(map[string]any{"op": opHeartbeat, "d": seq}); err != nil {
				return err
			}
			markHeartbeatSent()
		case opHeartbeatACK:
			heartbeatMu.Lock()
			awaitingACK = false
			heartbeatMu.Unlock()
		case opReconnect:
			return errors.New("gateway requested reconnect")
		case opInvalidSession:
			var resumable bool
			_ = json.Unmarshal(payload.D, &resumable)
			if !resumable {
				g.mu.Lock()
				g.sessionID = ""
				g.sequence = 0
				g.gapPending = true
				g.mu.Unlock()
			}
			return fmt.Errorf("gateway invalidated session (resumable=%t)", resumable)
		}
	}
}

func (g *Gateway) state(state string, connected bool) {
	g.mu.Lock()
	seq := g.sequence
	g.mu.Unlock()
	if g.OnState != nil {
		g.OnState(state, connected, seq)
	}
}
func (g *Gateway) report(err error) {
	if g.OnError != nil {
		g.OnError(err)
	}
}
