package core

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/bladew0rks/9flx/internal/fluxer"
)

const subscriberBuffer = 1024

type Subscription struct {
	C       <-chan fluxer.Event
	channel chan fluxer.Event
	dropped atomic.Uint64
}

func (s *Subscription) TakeDropped() uint64 { return s.dropped.Swap(0) }

type Hub struct {
	mu         sync.Mutex
	byChannel  map[string]map[*Subscription]struct{}
	global     map[*Subscription]struct{}
	onOverflow func()
}

func NewHub(onOverflow func()) *Hub {
	return &Hub{byChannel: make(map[string]map[*Subscription]struct{}), global: make(map[*Subscription]struct{}), onOverflow: onOverflow}
}

func (h *Hub) SubscribeAll() *Subscription {
	ch := make(chan fluxer.Event, subscriberBuffer)
	s := &Subscription{C: ch, channel: ch}
	h.mu.Lock()
	h.global[s] = struct{}{}
	h.mu.Unlock()
	return s
}

func (h *Hub) Subscribe(channelID string) *Subscription {
	ch := make(chan fluxer.Event, subscriberBuffer)
	s := &Subscription{C: ch, channel: ch}
	h.mu.Lock()
	if h.byChannel[channelID] == nil {
		h.byChannel[channelID] = make(map[*Subscription]struct{})
	}
	h.byChannel[channelID][s] = struct{}{}
	h.mu.Unlock()
	return s
}

func (h *Hub) UnsubscribeAll(s *Subscription) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.global[s]; ok {
		delete(h.global, s)
		close(s.channel)
	}
}

func (h *Hub) Unsubscribe(channelID string, s *Subscription) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if subscribers := h.byChannel[channelID]; subscribers != nil {
		if _, ok := subscribers[s]; ok {
			delete(subscribers, s)
			close(s.channel)
		}
		if len(subscribers) == 0 {
			delete(h.byChannel, channelID)
		}
	}
}

func (h *Hub) Publish(event fluxer.Event) {
	if event.ChannelID == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for subscriber := range h.byChannel[event.ChannelID] {
		h.publish(subscriber, event)
	}
	for subscriber := range h.global {
		h.publish(subscriber, event)
	}
}

func (h *Hub) publish(subscriber *Subscription, event fluxer.Event) {
	select {
	case subscriber.channel <- event:
	default:
		subscriber.dropped.Add(1)
		if h.onOverflow != nil {
			h.onOverflow()
		}
	}
}

func (h *Hub) GapAll(reason string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for channelID, subscribers := range h.byChannel {
		event := fluxer.Event{Type: "GAP", ChannelID: channelID, OccurredAt: time.Now().UTC(), Reason: reason}
		for subscriber := range subscribers {
			h.publish(subscriber, event)
		}
	}
	event := fluxer.Event{Type: "GAP", OccurredAt: time.Now().UTC(), Reason: reason}
	for subscriber := range h.global {
		h.publish(subscriber, event)
	}
}
