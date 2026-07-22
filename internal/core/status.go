package core

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

type StatusSnapshot struct {
	StartedAt        time.Time  `json:"started_at"`
	GatewayState     string     `json:"gateway_state"`
	GatewayConnected bool       `json:"gateway_connected"`
	GatewaySequence  int64      `json:"gateway_sequence"`
	ReconnectCount   uint64     `json:"reconnect_count"`
	QueueOverflows   uint64     `json:"queue_overflows"`
	LastRESTSuccess  *time.Time `json:"last_rest_success,omitempty"`
	LastErrorAt      *time.Time `json:"last_error_at,omitempty"`
	LastError        string     `json:"last_error,omitempty"`
}

type Status struct {
	mu sync.RWMutex
	s  StatusSnapshot
}

func NewStatus() *Status {
	return &Status{s: StatusSnapshot{StartedAt: time.Now().UTC(), GatewayState: "starting"}}
}

func (s *Status) ObserveREST(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	if err == nil {
		s.s.LastRESTSuccess = &now
		return
	}
	s.s.LastErrorAt, s.s.LastError = &now, err.Error()
}

func (s *Status) SetGateway(state string, connected bool, sequence int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.s.GatewayState, s.s.GatewayConnected, s.s.GatewaySequence = state, connected, sequence
}

func (s *Status) Reconnected() { s.mu.Lock(); s.s.ReconnectCount++; s.mu.Unlock() }
func (s *Status) Overflow()    { s.mu.Lock(); s.s.QueueOverflows++; s.mu.Unlock() }
func (s *Status) Error(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	s.s.LastErrorAt, s.s.LastError = &now, err.Error()
}

func (s *Status) Snapshot() StatusSnapshot { s.mu.RLock(); defer s.mu.RUnlock(); return s.s }

func (s *Status) JSON() []byte {
	b, _ := json.MarshalIndent(s.Snapshot(), "", "  ")
	return append(b, '\n')
}

func (s *Status) Text() []byte {
	v := s.Snapshot()
	lastREST, lastError := "never", "none"
	if v.LastRESTSuccess != nil {
		lastREST = v.LastRESTSuccess.Format(time.RFC3339)
	}
	if v.LastError != "" {
		lastError = v.LastError
	}
	return []byte(fmt.Sprintf("gateway %s\nconnected %t\nsequence %d\nreconnects %d\noverflows %d\nlast_rest_success %s\nlast_error %s\n",
		v.GatewayState, v.GatewayConnected, v.GatewaySequence, v.ReconnectCount, v.QueueOverflows, lastREST, lastError))
}
