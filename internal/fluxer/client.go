package fluxer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const defaultMaxRateLimitWait = 30 * time.Second

type APIError struct {
	StatusCode int
	Code       string
	Message    string
	RetryAfter time.Duration
}

func (e *APIError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("fluxer: %s (%s, HTTP %d)", e.Message, e.Code, e.StatusCode)
	}
	return fmt.Sprintf("fluxer: %s (HTTP %d)", e.Message, e.StatusCode)
}

type Client struct {
	baseURL     *url.URL
	token       string
	http        *http.Client
	maxRateWait time.Duration
	onRequest   func(error)
	assetMu     sync.RWMutex
	mediaBase   string
	staticBase  string
}

type ClientOption func(*Client)

func WithHTTPClient(h *http.Client) ClientOption        { return func(c *Client) { c.http = h } }
func WithMaxRateLimitWait(d time.Duration) ClientOption { return func(c *Client) { c.maxRateWait = d } }
func WithRequestObserver(fn func(error)) ClientOption   { return func(c *Client) { c.onRequest = fn } }

func NewClient(baseURL, token string, options ...ClientOption) (*Client, error) {
	u, err := url.Parse(strings.TrimRight(baseURL, "/"))
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("invalid Fluxer API base URL %q", baseURL)
	}
	if strings.TrimSpace(token) == "" {
		return nil, errors.New("Fluxer token is empty")
	}
	c := &Client{
		baseURL:     u,
		token:       token,
		http:        &http.Client{Timeout: 35 * time.Second},
		maxRateWait: defaultMaxRateLimitWait,
	}
	for _, option := range options {
		option(c)
	}
	return c, nil
}

func (c *Client) endpoint(path string) string {
	return strings.TrimRight(c.baseURL.String(), "/") + "/" + strings.TrimLeft(path, "/")
}

func (c *Client) do(ctx context.Context, method, path string, body any, out any) (err error) {
	defer func() {
		if c.onRequest != nil {
			c.onRequest(err)
		}
	}()
	var encoded []byte
	if body != nil {
		encoded, err = json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode request: %w", err)
		}
	}
	for {
		var reader io.Reader
		if encoded != nil {
			reader = bytes.NewReader(encoded)
		}
		req, reqErr := http.NewRequestWithContext(ctx, method, c.endpoint(path), reader)
		if reqErr != nil {
			return reqErr
		}
		req.Header.Set("Authorization", c.token)
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "9flx/0.1")
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}

		resp, reqErr := c.http.Do(req)
		if reqErr != nil {
			return fmt.Errorf("Fluxer request: %w", reqErr)
		}
		data, readErr := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
		resp.Body.Close()
		if readErr != nil {
			return fmt.Errorf("read Fluxer response: %w", readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			apiErr := decodeAPIError(resp, data)
			if apiErr.RetryAfter <= 0 || apiErr.RetryAfter > c.maxRateWait {
				return apiErr
			}
			timer := time.NewTimer(apiErr.RetryAfter)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
				continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return decodeAPIError(resp, data)
		}
		if out == nil || resp.StatusCode == http.StatusNoContent || len(data) == 0 {
			return nil
		}
		if err := json.Unmarshal(data, out); err != nil {
			return fmt.Errorf("decode Fluxer response for %s: %w", path, err)
		}
		return nil
	}
}

func decodeAPIError(resp *http.Response, data []byte) *APIError {
	e := &APIError{StatusCode: resp.StatusCode, Message: strings.TrimSpace(string(data))}
	var payload struct {
		Code       string  `json:"code"`
		Message    string  `json:"message"`
		RetryAfter float64 `json:"retry_after"`
	}
	if json.Unmarshal(data, &payload) == nil {
		e.Code, e.Message = payload.Code, payload.Message
		if payload.RetryAfter > 0 {
			e.RetryAfter = time.Duration(payload.RetryAfter * float64(time.Second))
		}
	}
	if e.Message == "" {
		e.Message = resp.Status
	}
	if e.RetryAfter == 0 {
		if seconds, err := strconv.ParseFloat(resp.Header.Get("Retry-After"), 64); err == nil {
			e.RetryAfter = time.Duration(seconds * float64(time.Second))
		}
	}
	return e
}

func (c *Client) Discovery(ctx context.Context) (Discovery, error) {
	var v Discovery
	err := c.do(ctx, http.MethodGet, "/.well-known/fluxer", nil, &v)
	if err == nil {
		c.assetMu.Lock()
		c.mediaBase = strings.TrimRight(v.Endpoints.Media, "/")
		c.staticBase = strings.TrimRight(v.Endpoints.StaticCDN, "/")
		c.assetMu.Unlock()
	}
	return v, err
}

func (c *Client) AvatarURL(user User, size int) (string, error) {
	if user.ID == "" {
		return "", errors.New("user has no ID")
	}
	if size == 0 {
		size = 160
	}
	if size < 16 || size > 4096 {
		return "", errors.New("avatar size must be between 16 and 4096")
	}
	c.assetMu.RLock()
	mediaBase, staticBase := c.mediaBase, c.staticBase
	c.assetMu.RUnlock()
	if user.Avatar == nil || *user.Avatar == "" {
		if staticBase == "" {
			return "", errors.New("instance discovery returned no static CDN endpoint")
		}
		id, ok := new(big.Int).SetString(user.ID, 10)
		if !ok {
			return "", errors.New("user ID is not a decimal snowflake")
		}
		index := new(big.Int).Mod(id, big.NewInt(6)).Int64()
		return fmt.Sprintf("%s/avatars/%d.png", staticBase, index), nil
	}
	if mediaBase == "" {
		return "", errors.New("instance discovery returned no media endpoint")
	}
	hash := strings.TrimPrefix(*user.Avatar, "a_")
	if hash == "" {
		return "", errors.New("user avatar hash is empty")
	}
	return fmt.Sprintf("%s/avatars/%s/%s.webp?size=%d", mediaBase, url.PathEscape(user.ID), url.PathEscape(hash), size), nil
}

func (c *Client) Avatar(ctx context.Context, user User, size int) (data []byte, contentType string, err error) {
	defer func() {
		if c.onRequest != nil {
			c.onRequest(err)
		}
	}()
	avatarURL, err := c.AvatarURL(user, size)
	if err != nil {
		return nil, "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, avatarURL, nil)
	if err != nil {
		return nil, "", fmt.Errorf("create avatar request: %w", err)
	}
	req.Header.Set("Accept", "image/*")
	req.Header.Set("User-Agent", "9flx/0.1")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("fetch avatar: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		return nil, "", fmt.Errorf("avatar fetch failed (HTTP %d)", resp.StatusCode)
	}
	data, err = io.ReadAll(io.LimitReader(resp.Body, 16<<20+1))
	if err != nil {
		return nil, "", fmt.Errorf("read avatar: %w", err)
	}
	if len(data) > 16<<20 {
		return nil, "", errors.New("avatar exceeds 16 MiB limit")
	}
	if len(data) == 0 {
		return nil, "", errors.New("avatar response is empty")
	}
	return data, resp.Header.Get("Content-Type"), nil
}

func (c *Client) Me(ctx context.Context) (User, error) {
	var v User
	err := c.do(ctx, http.MethodGet, "/users/@me", nil, &v)
	return v, err
}

func (c *Client) Relationships(ctx context.Context) ([]Relationship, error) {
	var v []Relationship
	err := c.do(ctx, http.MethodGet, "/users/@me/relationships", nil, &v)
	return v, err
}

func (c *Client) PrivateChannels(ctx context.Context) ([]Channel, error) {
	var v []Channel
	err := c.do(ctx, http.MethodGet, "/users/@me/channels", nil, &v)
	return v, err
}

func (c *Client) Guilds(ctx context.Context) ([]Guild, error) {
	var all []Guild
	var before string
	for {
		path := "/users/@me/guilds?limit=200"
		if before != "" {
			path += "&before=" + url.QueryEscape(before)
		}
		var page []Guild
		if err := c.do(ctx, http.MethodGet, path, nil, &page); err != nil {
			return nil, err
		}
		all = append(all, page...)
		if len(page) < 200 {
			return all, nil
		}
		before = page[len(page)-1].ID
	}
}

func (c *Client) GuildChannels(ctx context.Context, guildID string) ([]Channel, error) {
	var v []Channel
	err := c.do(ctx, http.MethodGet, "/guilds/"+url.PathEscape(guildID)+"/channels", nil, &v)
	for i := range v {
		if v[i].GuildID == "" {
			v[i].GuildID = guildID
		}
	}
	return v, err
}

func (c *Client) Messages(ctx context.Context, channelID string, limit int) ([]Message, error) {
	if limit < 1 {
		limit = 1
	}
	if limit > 100 {
		limit = 100
	}
	var v []Message
	path := fmt.Sprintf("/channels/%s/messages?limit=%d", url.PathEscape(channelID), limit)
	err := c.do(ctx, http.MethodGet, path, nil, &v)
	return v, err
}

func (c *Client) CreateDM(ctx context.Context, recipientID string) (Channel, error) {
	var v Channel
	err := c.do(ctx, http.MethodPost, "/users/@me/channels", map[string]string{"recipient_id": recipientID}, &v)
	return v, err
}

func (c *Client) SendMessage(ctx context.Context, channelID, content, nonce string) (Message, error) {
	var v Message
	body := map[string]any{"content": content, "nonce": nonce, "tts": false}
	err := c.do(ctx, http.MethodPost, "/channels/"+url.PathEscape(channelID)+"/messages", body, &v)
	return v, err
}

type attachmentUploadPlan struct {
	ID             int    `json:"id"`
	Filename       string `json:"filename"`
	UploadFilename string `json:"upload_filename"`
	FileSize       int64  `json:"file_size"`
	ContentType    string `json:"content_type"`
	UploadMode     string `json:"upload_mode"`
	UploadURL      string `json:"upload_url"`
	UploadID       string `json:"upload_id"`
	PartSize       int64  `json:"part_size"`
	Parts          []struct {
		PartNumber int    `json:"part_number"`
		UploadURL  string `json:"upload_url"`
	} `json:"parts"`
}

func (c *Client) SendAttachmentMessage(ctx context.Context, channelID, content, filename, contentType string, data []byte, nonce string) (Message, error) {
	request := map[string]any{"attachments": []map[string]any{{
		"id": 0, "filename": filename, "file_size": len(data), "content_type": contentType,
	}}}
	var planned struct {
		Attachments []attachmentUploadPlan `json:"attachments"`
	}
	path := "/channels/" + url.PathEscape(channelID) + "/attachments"
	if err := c.do(ctx, http.MethodPost, path, request, &planned); err != nil {
		return Message{}, err
	}
	if len(planned.Attachments) != 1 {
		return Message{}, fmt.Errorf("Fluxer returned %d upload plans for one attachment", len(planned.Attachments))
	}
	plan := planned.Attachments[0]
	if err := c.uploadAttachment(ctx, plan, contentType, data); err != nil {
		return Message{}, err
	}
	if plan.UploadMode == "multipart" {
		complete := map[string]any{"uploads": []map[string]string{{
			"upload_filename": plan.UploadFilename,
			"upload_id":       plan.UploadID,
		}}}
		if err := c.do(ctx, http.MethodPost, path+"/complete", complete, nil); err != nil {
			return Message{}, err
		}
	}

	body := map[string]any{
		"nonce": nonce,
		"tts":   false,
		"attachments": []map[string]any{{
			"id":              0,
			"filename":        filename,
			"upload_filename": plan.UploadFilename,
			"file_size":       len(data),
			"content_type":    contentType,
		}},
	}
	if content != "" {
		body["content"] = content
	}
	var message Message
	err := c.do(ctx, http.MethodPost, "/channels/"+url.PathEscape(channelID)+"/messages", body, &message)
	return message, err
}

func (c *Client) uploadAttachment(ctx context.Context, plan attachmentUploadPlan, contentType string, data []byte) error {
	switch plan.UploadMode {
	case "singlepart":
		if plan.UploadURL == "" {
			return errors.New("Fluxer returned an empty attachment upload URL")
		}
		return c.putAttachment(ctx, plan.UploadURL, contentType, data)
	case "multipart":
		if plan.UploadID == "" || plan.PartSize <= 0 || len(plan.Parts) == 0 {
			return errors.New("Fluxer returned an invalid multipart attachment plan")
		}
		for _, part := range plan.Parts {
			if part.PartNumber < 1 || part.UploadURL == "" {
				return errors.New("Fluxer returned an invalid attachment part")
			}
			start := int64(part.PartNumber-1) * plan.PartSize
			if start >= int64(len(data)) {
				return errors.New("Fluxer attachment plan exceeds file size")
			}
			end := start + plan.PartSize
			if end > int64(len(data)) {
				end = int64(len(data))
			}
			if err := c.putAttachment(ctx, part.UploadURL, contentType, data[start:end]); err != nil {
				return fmt.Errorf("upload attachment part %d: %w", part.PartNumber, err)
			}
		}
		return nil
	default:
		return fmt.Errorf("Fluxer returned unknown attachment upload mode %q", plan.UploadMode)
	}
}

func (c *Client) putAttachment(ctx context.Context, uploadURL, contentType string, data []byte) (err error) {
	defer func() {
		if c.onRequest != nil {
			c.onRequest(err)
		}
	}()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, uploadURL, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("create attachment upload: %w", err)
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("User-Agent", "9flx/0.1")
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("upload attachment: %w", err)
	}
	_, readErr := io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()
	if readErr != nil {
		return fmt.Errorf("read attachment upload response: %w", readErr)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("attachment upload failed (HTTP %d)", resp.StatusCode)
	}
	return nil
}

func (c *Client) ReplyMessage(ctx context.Context, channelID, messageID, content, nonce string) (Message, error) {
	var v Message
	body := map[string]any{
		"content": content,
		"nonce":   nonce,
		"tts":     false,
		"message_reference": map[string]any{
			"message_id": messageID,
			"channel_id": channelID,
			"type":       0,
		},
	}
	err := c.do(ctx, http.MethodPost, "/channels/"+url.PathEscape(channelID)+"/messages", body, &v)
	return v, err
}

func (c *Client) EditMessage(ctx context.Context, channelID, messageID, content string) (Message, error) {
	var v Message
	path := "/channels/" + url.PathEscape(channelID) + "/messages/" + url.PathEscape(messageID)
	err := c.do(ctx, http.MethodPatch, path, map[string]any{"content": content}, &v)
	return v, err
}

func (c *Client) DeleteMessage(ctx context.Context, channelID, messageID string) error {
	path := "/channels/" + url.PathEscape(channelID) + "/messages/" + url.PathEscape(messageID)
	return c.do(ctx, http.MethodDelete, path, nil, nil)
}

func (c *Client) AddReaction(ctx context.Context, channelID, messageID, emoji string) error {
	path := "/channels/" + url.PathEscape(channelID) + "/messages/" + url.PathEscape(messageID) +
		"/reactions/" + url.PathEscape(emoji) + "/@me"
	return c.do(ctx, http.MethodPut, path, nil, nil)
}

func (c *Client) RemoveReaction(ctx context.Context, channelID, messageID, emoji string) error {
	path := "/channels/" + url.PathEscape(channelID) + "/messages/" + url.PathEscape(messageID) +
		"/reactions/" + url.PathEscape(emoji) + "/@me"
	return c.do(ctx, http.MethodDelete, path, nil, nil)
}
