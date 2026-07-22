package core

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/bladew0rks/9flx/internal/fluxer"
)

type Snapshot struct {
	Me            fluxer.User
	Relationships []fluxer.Relationship
	DMs           []fluxer.Channel
	Guilds        []fluxer.Guild
}

type Store struct {
	mu            sync.RWMutex
	me            fluxer.User
	users         map[string]fluxer.User
	relationships map[string]fluxer.Relationship
	dms           map[string]fluxer.Channel
	guilds        map[string]fluxer.Guild
	hub           *Hub
	onTopology    func()
}

func NewStore(hub *Hub) *Store {
	return &Store{
		users: make(map[string]fluxer.User), relationships: make(map[string]fluxer.Relationship),
		dms: make(map[string]fluxer.Channel), guilds: make(map[string]fluxer.Guild), hub: hub,
	}
}

func (s *Store) SetTopologyObserver(fn func()) { s.mu.Lock(); s.onTopology = fn; s.mu.Unlock() }

func (s *Store) Bootstrap(ctx context.Context, api *fluxer.Client) error {
	me, err := api.Me(ctx)
	if err != nil {
		return fmt.Errorf("load current user: %w", err)
	}
	rels, err := api.Relationships(ctx)
	if err != nil {
		return fmt.Errorf("load relationships: %w", err)
	}
	dms, err := api.PrivateChannels(ctx)
	if err != nil {
		return fmt.Errorf("load private channels: %w", err)
	}
	guilds, err := api.Guilds(ctx)
	if err != nil {
		return fmt.Errorf("load communities: %w", err)
	}
	for i := range guilds {
		channels, channelErr := api.GuildChannels(ctx, guilds[i].ID)
		if channelErr != nil {
			return fmt.Errorf("load channels for %s: %w", guilds[i].ID, channelErr)
		}
		guilds[i].Channels = channels
	}
	s.mu.Lock()
	s.me = me
	s.users[me.ID] = me
	for _, rel := range rels {
		s.relationships[rel.ID] = rel
		if rel.User.ID != "" {
			s.users[rel.User.ID] = rel.User
		}
	}
	for _, dm := range dms {
		s.dms[dm.ID] = dm
		for _, user := range dm.Recipients {
			s.users[user.ID] = user
		}
	}
	for _, guild := range guilds {
		s.guilds[guild.ID] = guild
	}
	observer := s.onTopology
	s.mu.Unlock()
	if observer != nil {
		observer()
	}
	return nil
}

func (s *Store) Snapshot() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v := Snapshot{Me: s.me}
	for _, rel := range s.relationships {
		if rel.User.ID == "" {
			rel.User = s.users[rel.ID]
		}
		v.Relationships = append(v.Relationships, rel)
	}
	for _, dm := range s.dms {
		v.DMs = append(v.DMs, dm)
	}
	for _, guild := range s.guilds {
		v.Guilds = append(v.Guilds, guild)
	}
	return v
}

func (s *Store) FindDMForUser(userID string) (fluxer.Channel, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, dm := range s.dms {
		if dm.Type != fluxer.ChannelDM {
			continue
		}
		for _, user := range dm.Recipients {
			if user.ID == userID {
				return dm, true
			}
		}
	}
	return fluxer.Channel{}, false
}

func (s *Store) UpsertDM(dm fluxer.Channel) {
	s.mu.Lock()
	s.dms[dm.ID] = dm
	observer := s.onTopology
	s.mu.Unlock()
	if observer != nil {
		observer()
	}
}

func (s *Store) ApplyGateway(eventType string, sequence int64, data json.RawMessage) error {
	now := time.Now().UTC()
	messageEvent := func(message *fluxer.Message, id, channelID string) {
		s.hub.Publish(fluxer.Event{Type: eventType, Sequence: sequence, ChannelID: channelID, Message: message, MessageID: id, OccurredAt: now, Data: data})
	}
	switch eventType {
	case "READY":
		var ready fluxer.Ready
		if err := json.Unmarshal(data, &ready); err != nil {
			return err
		}
		s.mu.Lock()
		s.me = ready.User
		if ready.User.ID != "" {
			s.users[ready.User.ID] = ready.User
		}
		for _, user := range ready.Users {
			s.users[user.ID] = user
		}
		for _, rel := range ready.Relationships {
			s.relationships[rel.ID] = rel
		}
		for _, dm := range ready.PrivateChannels {
			s.dms[dm.ID] = dm
		}
		for _, guild := range ready.Guilds {
			if !guild.Unavailable {
				s.guilds[guild.ID] = mergeGuild(s.guilds[guild.ID], guild)
			}
		}
		observer := s.onTopology
		s.mu.Unlock()
		if observer != nil {
			observer()
		}
	case "MESSAGE_CREATE", "MESSAGE_UPDATE":
		var message fluxer.Message
		if err := json.Unmarshal(data, &message); err != nil {
			return err
		}
		message.Raw = append([]byte(nil), data...)
		messageEvent(&message, message.ID, message.ChannelID)
	case "MESSAGE_DELETE":
		var deleted struct {
			ID        string `json:"id"`
			ChannelID string `json:"channel_id"`
		}
		if err := json.Unmarshal(data, &deleted); err != nil {
			return err
		}
		messageEvent(nil, deleted.ID, deleted.ChannelID)
	case "CHANNEL_CREATE", "CHANNEL_UPDATE":
		var channel fluxer.Channel
		if err := json.Unmarshal(data, &channel); err != nil {
			return err
		}
		s.mu.Lock()
		if channel.GuildID == "" {
			s.dms[channel.ID] = channel
		} else if guild, ok := s.guilds[channel.GuildID]; ok {
			found := false
			for i := range guild.Channels {
				if guild.Channels[i].ID == channel.ID {
					guild.Channels[i] = channel
					found = true
				}
			}
			if !found {
				guild.Channels = append(guild.Channels, channel)
			}
			s.guilds[guild.ID] = guild
		}
		observer := s.onTopology
		s.mu.Unlock()
		if observer != nil {
			observer()
		}
	case "CHANNEL_DELETE":
		var channel fluxer.Channel
		if err := json.Unmarshal(data, &channel); err != nil {
			return err
		}
		s.mu.Lock()
		delete(s.dms, channel.ID)
		if guild, ok := s.guilds[channel.GuildID]; ok {
			out := guild.Channels[:0]
			for _, c := range guild.Channels {
				if c.ID != channel.ID {
					out = append(out, c)
				}
			}
			guild.Channels = out
			s.guilds[guild.ID] = guild
		}
		observer := s.onTopology
		s.mu.Unlock()
		if observer != nil {
			observer()
		}
	case "RELATIONSHIP_ADD", "RELATIONSHIP_UPDATE":
		var rel fluxer.Relationship
		if err := json.Unmarshal(data, &rel); err != nil {
			return err
		}
		s.mu.Lock()
		s.relationships[rel.ID] = rel
		if rel.User.ID != "" {
			s.users[rel.User.ID] = rel.User
		}
		observer := s.onTopology
		s.mu.Unlock()
		if observer != nil {
			observer()
		}
	case "RELATIONSHIP_REMOVE":
		var rel struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(data, &rel); err != nil {
			return err
		}
		s.mu.Lock()
		delete(s.relationships, rel.ID)
		observer := s.onTopology
		s.mu.Unlock()
		if observer != nil {
			observer()
		}
	case "GUILD_CREATE", "GUILD_UPDATE":
		var guild fluxer.Guild
		if err := json.Unmarshal(data, &guild); err != nil {
			return err
		}
		s.mu.Lock()
		s.guilds[guild.ID] = mergeGuild(s.guilds[guild.ID], guild)
		observer := s.onTopology
		s.mu.Unlock()
		if observer != nil {
			observer()
		}
	case "GUILD_DELETE":
		var guild fluxer.Guild
		if err := json.Unmarshal(data, &guild); err != nil {
			return err
		}
		s.mu.Lock()
		delete(s.guilds, guild.ID)
		observer := s.onTopology
		s.mu.Unlock()
		if observer != nil {
			observer()
		}
	}
	return nil
}

func mergeGuild(current, update fluxer.Guild) fluxer.Guild {
	if update.ID == "" {
		update.ID = current.ID
	}
	if update.Name == "" {
		update.Name = current.Name
	}
	if update.OwnerID == "" {
		update.OwnerID = current.OwnerID
	}
	if update.Channels == nil {
		update.Channels = current.Channels
	}
	return update
}
