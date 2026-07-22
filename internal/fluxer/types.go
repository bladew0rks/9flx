package fluxer

import (
	"encoding/json"
	"time"
)

type User struct {
	ID            string  `json:"id"`
	Username      string  `json:"username"`
	Discriminator string  `json:"discriminator"`
	GlobalName    *string `json:"global_name"`
	Avatar        *string `json:"avatar"`
	Bot           bool    `json:"bot"`
	System        bool    `json:"system"`
}

func (u User) Tag() string {
	if u.Discriminator == "" {
		return u.Username
	}
	return u.Username + "#" + u.Discriminator
}

type Relationship struct {
	ID       string  `json:"id"`
	Type     int     `json:"type"`
	User     User    `json:"user"`
	Since    string  `json:"since,omitempty"`
	Nickname *string `json:"nickname"`
}

const RelationshipFriend = 1

type Channel struct {
	ID            string  `json:"id"`
	GuildID       string  `json:"guild_id,omitempty"`
	Type          int     `json:"type"`
	Name          string  `json:"name,omitempty"`
	Topic         *string `json:"topic,omitempty"`
	ParentID      *string `json:"parent_id,omitempty"`
	OwnerID       *string `json:"owner_id,omitempty"`
	LastMessageID *string `json:"last_message_id,omitempty"`
	Recipients    []User  `json:"recipients,omitempty"`
}

const (
	ChannelText     = 0
	ChannelDM       = 1
	ChannelVoice    = 2
	ChannelGroupDM  = 3
	ChannelCategory = 4
)

type Guild struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	OwnerID     string    `json:"owner_id,omitempty"`
	Channels    []Channel `json:"channels,omitempty"`
	Unavailable bool      `json:"unavailable,omitempty"`
}

type Message struct {
	ID              string          `json:"id"`
	ChannelID       string          `json:"channel_id"`
	GuildID         string          `json:"guild_id,omitempty"`
	Author          User            `json:"author"`
	Content         string          `json:"content"`
	Timestamp       string          `json:"timestamp"`
	EditedTimestamp *string         `json:"edited_timestamp"`
	Type            int             `json:"type"`
	Pinned          bool            `json:"pinned"`
	Attachments     []Attachment    `json:"attachments,omitempty"`
	Raw             json.RawMessage `json:"-"`
}

type Attachment struct {
	ID       string `json:"id"`
	Filename string `json:"filename"`
	URL      string `json:"url"`
	Size     int64  `json:"size"`
}

func (m Message) Time() time.Time {
	t, _ := time.Parse(time.RFC3339Nano, m.Timestamp)
	return t
}

type Discovery struct {
	APICodeVersion int `json:"api_code_version"`
	Endpoints      struct {
		API     string `json:"api"`
		Gateway string `json:"gateway"`
		Media   string `json:"media"`
	} `json:"endpoints"`
	Limits json.RawMessage `json:"limits"`
}

type Ready struct {
	SessionID       string         `json:"session_id"`
	User            User           `json:"user"`
	Users           []User         `json:"users"`
	Guilds          []Guild        `json:"guilds"`
	PrivateChannels []Channel      `json:"private_channels"`
	Relationships   []Relationship `json:"relationships"`
}

type Event struct {
	Type       string          `json:"event"`
	Sequence   int64           `json:"sequence,omitempty"`
	ChannelID  string          `json:"channel_id,omitempty"`
	Message    *Message        `json:"message,omitempty"`
	MessageID  string          `json:"message_id,omitempty"`
	OccurredAt time.Time       `json:"occurred_at"`
	Data       json.RawMessage `json:"data,omitempty"`
	Dropped    uint64          `json:"dropped,omitempty"`
	Reason     string          `json:"reason,omitempty"`
}

type GatewayPayload struct {
	Op int             `json:"op"`
	D  json.RawMessage `json:"d,omitempty"`
	S  *int64          `json:"s,omitempty"`
	T  string          `json:"t,omitempty"`
}
