package render

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

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
			names := AttachmentNames(message.Attachments)
			for _, attachment := range message.Attachments {
				fmt.Fprintf(&out, "  attachment=%s path=attachments/%s/%s size=%d",
					attachment.ID, message.ID, names[attachment.ID], attachment.Size)
				if attachment.ContentType != nil && *attachment.ContentType != "" {
					fmt.Fprintf(&out, " type=%s", escapeContent(*attachment.ContentType))
				}
				out.WriteByte('\n')
			}
		}
	}
	return out.Bytes()
}

func AttachmentNames(attachments []fluxer.Attachment) map[string]string {
	bases := make(map[string]string, len(attachments))
	counts := make(map[string]int, len(attachments))
	for _, attachment := range attachments {
		base := safePathName(attachment.Filename)
		bases[attachment.ID] = base
		counts[base]++
	}
	names := make(map[string]string, len(attachments))
	for _, attachment := range attachments {
		base := bases[attachment.ID]
		if counts[base] > 1 {
			extension := filepath.Ext(base)
			stem := strings.TrimSuffix(base, extension)
			base = stem + "~" + shortID(attachment.ID) + extension
		}
		names[attachment.ID] = base
	}
	return names
}

func safePathName(value string) string {
	if value == "" {
		return "_"
	}
	var out strings.Builder
	for len(value) > 0 {
		r, size := utf8.DecodeRuneInString(value)
		raw := value[:size]
		value = value[size:]
		if r == utf8.RuneError && size == 1 || r == '/' || r == '%' || r < 0x20 || r == 0x7f {
			for _, b := range []byte(raw) {
				fmt.Fprintf(&out, "%%%02X", b)
			}
		} else {
			out.WriteString(raw)
		}
	}
	name := out.String()
	if name == "." {
		return "%2E"
	}
	if name == ".." {
		return "%2E%2E"
	}
	return name
}

func shortID(id string) string {
	if len(id) > 6 {
		return id[len(id)-6:]
	}
	return id
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
