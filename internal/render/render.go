package render

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/bladew0rks/9flx/internal/fluxer"
)

func JSON(value any) []byte {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return []byte("{\"error\":\"cannot encode metadata\"}\n")
	}
	return append(b, '\n')
}

func History(messages []fluxer.Message, jsonLines bool) []byte {
	sort.SliceStable(messages, func(i, j int) bool { return messages[i].Time().Before(messages[j].Time()) })
	var out bytes.Buffer
	for _, message := range messages {
		if jsonLines {
			b, _ := json.Marshal(message)
			out.Write(b)
			out.WriteByte('\n')
		} else {
			fmt.Fprintf(&out, "[%s] message=%s %s: %s\n", timestamp(message.Timestamp), message.ID, author(message.Author), escapeContent(message.Content))
		}
	}
	return out.Bytes()
}

func Event(event fluxer.Event, jsonLines bool) []byte {
	return eventRecord(event, jsonLines, false)
}

func InboxEvent(event fluxer.Event, jsonLines bool) []byte {
	return eventRecord(event, jsonLines, true)
}

func eventRecord(event fluxer.Event, jsonLines, includeChannel bool) []byte {
	if jsonLines {
		event.Data = nil
		b, _ := json.Marshal(event)
		return append(b, '\n')
	}
	when := event.OccurredAt.UTC().Format(time.RFC3339)
	if event.Type == "GAP" {
		return []byte(fmt.Sprintf("[%s] GAP dropped=%d reason=%s\n", when, event.Dropped, event.Reason))
	}
	if event.Message != nil {
		if includeChannel {
			return []byte(fmt.Sprintf("[%s] %s channel=%s message=%s %s: %s\n", timestamp(event.Message.Timestamp), event.Type, event.ChannelID, event.Message.ID, author(event.Message.Author), escapeContent(event.Message.Content)))
		}
		return []byte(fmt.Sprintf("[%s] %s message=%s %s: %s\n", timestamp(event.Message.Timestamp), event.Type, event.Message.ID, author(event.Message.Author), escapeContent(event.Message.Content)))
	}
	if includeChannel {
		return []byte(fmt.Sprintf("[%s] %s channel=%s message=%s\n", when, event.Type, event.ChannelID, event.MessageID))
	}
	return []byte(fmt.Sprintf("[%s] %s message=%s\n", when, event.Type, event.MessageID))
}

func Gap(dropped uint64, jsonLines bool) []byte {
	return Event(fluxer.Event{Type: "GAP", OccurredAt: time.Now().UTC(), Dropped: dropped, Reason: "queue_overflow"}, jsonLines)
}

func author(user fluxer.User) string {
	if tag := user.Tag(); tag != "" {
		return tag
	}
	return user.ID
}

func timestamp(value string) string {
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed.UTC().Format(time.RFC3339)
	}
	return value
}

func escapeContent(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "\r", "\\r")
	value = strings.ReplaceAll(value, "\n", "\\n")
	value = strings.ReplaceAll(value, "\t", "\\t")
	return value
}
