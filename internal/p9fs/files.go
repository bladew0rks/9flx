package p9fs

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/bladew0rks/9flx/internal/core"
	"github.com/bladew0rks/9flx/internal/fluxer"
	"github.com/bladew0rks/9flx/internal/render"
	"github.com/ronsor/go9p/fs"
	"github.com/ronsor/go9p/proto"
)

const maxSendBuffer = 64 << 20

type snapshotFile struct {
	*fs.BaseFile
	mu       sync.RWMutex
	content  map[uint64][]byte
	generate func() ([]byte, error)
}

func newSnapshotFile(stat *proto.Stat, generate func() ([]byte, error)) *snapshotFile {
	return &snapshotFile{BaseFile: fs.NewBaseFile(stat), content: make(map[uint64][]byte), generate: generate}
}
func (f *snapshotFile) Open(fid uint64, mode proto.Mode) error {
	if mode&0x0f == proto.Owrite || mode&0x0f == proto.Ordwr {
		return errors.New("file is read only")
	}
	data, err := f.generate()
	if err != nil {
		return err
	}
	f.mu.Lock()
	f.content[fid] = data
	f.mu.Unlock()
	return nil
}
func (f *snapshotFile) Read(fid, offset, count uint64) ([]byte, error) {
	f.mu.RLock()
	data, ok := f.content[fid]
	f.mu.RUnlock()
	if !ok {
		return nil, errors.New("file is not open")
	}
	return slice(data, offset, count), nil
}
func (f *snapshotFile) Write(uint64, uint64, []byte) (uint32, error) {
	return 0, errors.New("file is read only")
}
func (f *snapshotFile) Close(fid uint64) error {
	f.mu.Lock()
	delete(f.content, fid)
	f.mu.Unlock()
	return nil
}

type historyFile struct {
	*snapshotFile
}

func newHistoryFile(stat *proto.Stat, api *fluxer.Client, channel func() (string, bool), limit int, jsonLines bool) *historyFile {
	return &historyFile{newSnapshotFile(stat, func() ([]byte, error) {
		id, ok := channel()
		if !ok {
			return []byte{}, nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
		defer cancel()
		messages, err := api.Messages(ctx, id, limit)
		if err != nil {
			return nil, err
		}
		return render.History(messages, jsonLines), nil
	})}
}

func newPinsFile(stat *proto.Stat, api *fluxer.Client, channel func() (string, bool), jsonLines bool) *snapshotFile {
	return newSnapshotFile(stat, func() ([]byte, error) {
		id, ok := channel()
		if !ok {
			return []byte{}, nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
		defer cancel()
		pins, err := api.PinnedMessages(ctx, id, 50)
		if err != nil {
			return nil, err
		}
		messages := make([]fluxer.Message, 0, len(pins.Items))
		for _, pin := range pins.Items {
			messages = append(messages, pin.Message)
		}
		return render.History(messages, jsonLines), nil
	})
}

func newAvatarFile(stat *proto.Stat, api *fluxer.Client, user func() (fluxer.User, bool)) *snapshotFile {
	return newSnapshotFile(stat, func() ([]byte, error) {
		profile, ok := user()
		if !ok {
			return nil, errors.New("conversation has no user avatar")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
		defer cancel()
		data, _, err := api.Avatar(ctx, profile, 160)
		return data, err
	})
}

func newAvatarURLFile(stat *proto.Stat, api *fluxer.Client, user func() (fluxer.User, bool)) *snapshotFile {
	return newSnapshotFile(stat, func() ([]byte, error) {
		profile, ok := user()
		if !ok {
			return nil, errors.New("conversation has no user avatar")
		}
		avatarURL, err := api.AvatarURL(profile, 160)
		if err != nil {
			return nil, err
		}
		return []byte(avatarURL + "\n"), nil
	})
}

type settingFile struct {
	*fs.BaseFile
	mu     sync.Mutex
	reads  map[uint64][]byte
	writes map[uint64][]byte
	limit  uint64
	load   func(context.Context) ([]byte, error)
	store  func(context.Context, []byte) error
}

func newSettingFile(stat *proto.Stat, limit uint64, load func(context.Context) ([]byte, error), store func(context.Context, []byte) error) *settingFile {
	return &settingFile{BaseFile: fs.NewBaseFile(stat), reads: make(map[uint64][]byte), writes: make(map[uint64][]byte), limit: limit, load: load, store: store}
}

func (f *settingFile) Open(fid uint64, mode proto.Mode) error {
	switch mode & 0x0f {
	case proto.Oread:
		ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
		defer cancel()
		data, err := f.load(ctx)
		if err != nil {
			return err
		}
		f.mu.Lock()
		f.reads[fid] = data
		f.mu.Unlock()
		return nil
	case proto.Owrite:
		f.mu.Lock()
		f.writes[fid] = []byte{}
		f.mu.Unlock()
		return nil
	default:
		return errors.New("setting must be opened for either reading or writing")
	}
}

func (f *settingFile) Read(fid, offset, count uint64) ([]byte, error) {
	f.mu.Lock()
	data, ok := f.reads[fid]
	f.mu.Unlock()
	if !ok {
		return nil, errors.New("setting is not open for reading")
	}
	return slice(data, offset, count), nil
}

func (f *settingFile) Write(fid, offset uint64, data []byte) (uint32, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	buffer, ok := f.writes[fid]
	if !ok {
		return 0, errors.New("setting is not open for writing")
	}
	end := offset + uint64(len(data))
	if end > f.limit {
		return 0, errors.New("setting exceeds local buffer limit")
	}
	if end > uint64(len(buffer)) {
		buffer = append(buffer, make([]byte, end-uint64(len(buffer)))...)
	}
	copy(buffer[offset:end], data)
	f.writes[fid] = buffer
	return uint32(len(data)), nil
}

func (f *settingFile) Close(fid uint64) error {
	f.mu.Lock()
	data, writing := f.writes[fid]
	delete(f.writes, fid)
	delete(f.reads, fid)
	f.mu.Unlock()
	if !writing {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	return f.store(ctx, data)
}

func newPresenceFile(stat *proto.Stat, api *fluxer.Client, setLive func(fluxer.PresenceStatus) error) *settingFile {
	return newSettingFile(stat, 64, func(ctx context.Context) ([]byte, error) {
		settings, err := api.Settings(ctx)
		return []byte(settings.Status + "\n"), err
	}, func(ctx context.Context, data []byte) error {
		text, err := commandText(data)
		if err != nil {
			return err
		}
		status := fluxer.PresenceStatus(strings.TrimSpace(text))
		if !status.Valid() {
			return errors.New("status must be online, dnd, idle, or invisible")
		}
		if _, err := api.SetStatus(ctx, status); err != nil {
			return err
		}
		return setLive(status)
	})
}

func newCustomStatusFile(stat *proto.Stat, api *fluxer.Client, setLive func(*fluxer.CustomStatus) error) *settingFile {
	return newSettingFile(stat, 512, func(ctx context.Context) ([]byte, error) {
		settings, err := api.Settings(ctx)
		if err != nil {
			return nil, err
		}
		if settings.CustomStatus == nil || settings.CustomStatus.Text == nil {
			return []byte{}, nil
		}
		return []byte(*settings.CustomStatus.Text + "\n"), nil
	}, func(ctx context.Context, data []byte) error {
		text, err := commandText(data)
		if err != nil {
			return err
		}
		text = strings.TrimSpace(text)
		var value *string
		if text != "!reset" {
			if utf8.RuneCountInString(text) > 128 {
				return errors.New("custom status exceeds Fluxer's 128-character maximum")
			}
			value = &text
		}
		settings, err := api.SetCustomStatus(ctx, value)
		if err != nil {
			return err
		}
		return setLive(settings.CustomStatus)
	})
}

type liveReader struct {
	mu        sync.Mutex
	channelID string
	sub       *core.Subscription
	pending   []byte
}

type liveFile struct {
	*fs.BaseFile
	mu        sync.Mutex
	readers   map[uint64]*liveReader
	hub       *core.Hub
	channel   func() (string, bool)
	jsonLines bool
	global    bool
}

func newLiveFile(stat *proto.Stat, hub *core.Hub, channel func() (string, bool), jsonLines bool) *liveFile {
	return &liveFile{BaseFile: fs.NewBaseFile(stat), readers: make(map[uint64]*liveReader), hub: hub, channel: channel, jsonLines: jsonLines}
}
func newGlobalLiveFile(stat *proto.Stat, hub *core.Hub, jsonLines bool) *liveFile {
	return &liveFile{BaseFile: fs.NewBaseFile(stat), readers: make(map[uint64]*liveReader), hub: hub, jsonLines: jsonLines, global: true}
}
func (f *liveFile) Open(fid uint64, mode proto.Mode) error {
	if mode&0x0f == proto.Owrite || mode&0x0f == proto.Ordwr {
		return errors.New("file is read only")
	}
	id := ""
	var subscription *core.Subscription
	if f.global {
		subscription = f.hub.SubscribeAll()
	} else {
		var ok bool
		id, ok = f.channel()
		if !ok {
			return errors.New("conversation has no channel yet; send a message first")
		}
		subscription = f.hub.Subscribe(id)
	}
	f.mu.Lock()
	f.readers[fid] = &liveReader{channelID: id, sub: subscription}
	f.mu.Unlock()
	return nil
}
func (f *liveFile) Read(fid, _ uint64, count uint64) ([]byte, error) {
	f.mu.Lock()
	reader := f.readers[fid]
	f.mu.Unlock()
	if reader == nil {
		return nil, errors.New("file is not open")
	}
	reader.mu.Lock()
	defer reader.mu.Unlock()
	for {
		if len(reader.pending) > 0 {
			n := int(count)
			if n > len(reader.pending) {
				n = len(reader.pending)
			}
			out := append([]byte(nil), reader.pending[:n]...)
			reader.pending = reader.pending[n:]
			return out, nil
		}
		if dropped := reader.sub.TakeDropped(); dropped > 0 {
			reader.pending = render.Gap(dropped, f.jsonLines)
			continue
		}
		event, ok := <-reader.sub.C
		if !ok {
			return []byte{}, nil
		}
		if f.global {
			reader.pending = render.InboxEvent(event, f.jsonLines)
		} else {
			reader.pending = render.Event(event, f.jsonLines)
		}
	}
}
func (f *liveFile) Write(uint64, uint64, []byte) (uint32, error) {
	return 0, errors.New("file is read only")
}
func (f *liveFile) Close(fid uint64) error {
	f.mu.Lock()
	reader := f.readers[fid]
	delete(f.readers, fid)
	f.mu.Unlock()
	if reader != nil {
		if f.global {
			f.hub.UnsubscribeAll(reader.sub)
		} else {
			f.hub.Unsubscribe(reader.channelID, reader.sub)
		}
	}
	return nil
}

type sendFile struct {
	*fs.BaseFile
	mu      sync.Mutex
	buffers map[uint64][]byte
	resolve func(context.Context) (string, error)
	api     *fluxer.Client
}

type commandFile struct {
	*fs.BaseFile
	mu      sync.Mutex
	buffers map[uint64][]byte
	run     func(context.Context, []byte) error
}

func newCommandFile(stat *proto.Stat, run func(context.Context, []byte) error) *commandFile {
	return &commandFile{BaseFile: fs.NewBaseFile(stat), buffers: make(map[uint64][]byte), run: run}
}

func (f *commandFile) Open(fid uint64, mode proto.Mode) error {
	if mode&0x0f != proto.Owrite && mode&0x0f != proto.Ordwr {
		return errors.New("command file is write only")
	}
	f.mu.Lock()
	f.buffers[fid] = []byte{}
	f.mu.Unlock()
	return nil
}

func (f *commandFile) Read(uint64, uint64, uint64) ([]byte, error) {
	return nil, errors.New("command file is write only")
}

func (f *commandFile) Write(fid, offset uint64, data []byte) (uint32, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	buffer, ok := f.buffers[fid]
	if !ok {
		return 0, errors.New("file is not open")
	}
	end := offset + uint64(len(data))
	if end > 32<<10 {
		return 0, errors.New("command exceeds local buffer limit")
	}
	if end > uint64(len(buffer)) {
		buffer = append(buffer, make([]byte, end-uint64(len(buffer)))...)
	}
	copy(buffer[offset:end], data)
	f.buffers[fid] = buffer
	return uint32(len(data)), nil
}

func (f *commandFile) Close(fid uint64) error {
	f.mu.Lock()
	data, ok := f.buffers[fid]
	delete(f.buffers, fid)
	f.mu.Unlock()
	if !ok {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	return f.run(ctx, data)
}

func newEditFile(stat *proto.Stat, api *fluxer.Client, resolve func(context.Context) (string, error)) *commandFile {
	return newCommandFile(stat, func(ctx context.Context, data []byte) error {
		messageID, content, err := parseMessageCommand(data)
		if err != nil {
			return fmt.Errorf("edit: %w", err)
		}
		channelID, err := resolve(ctx)
		if err != nil {
			return err
		}
		_, err = api.EditMessage(ctx, channelID, messageID, content)
		return err
	})
}

func newReplyFile(stat *proto.Stat, api *fluxer.Client, resolve func(context.Context) (string, error)) *commandFile {
	return newCommandFile(stat, func(ctx context.Context, data []byte) error {
		messageID, content, err := parseMessageCommand(data)
		if err != nil {
			return fmt.Errorf("reply: %w", err)
		}
		channelID, err := resolve(ctx)
		if err != nil {
			return err
		}
		nonce, err := messageNonce()
		if err != nil {
			return err
		}
		_, err = api.ReplyMessage(ctx, channelID, messageID, content, nonce)
		return err
	})
}

func newDeleteFile(stat *proto.Stat, api *fluxer.Client, resolve func(context.Context) (string, error)) *commandFile {
	return newCommandFile(stat, func(ctx context.Context, data []byte) error {
		messageID, err := parseIDCommand(data)
		if err != nil {
			return fmt.Errorf("delete: %w", err)
		}
		channelID, err := resolve(ctx)
		if err != nil {
			return err
		}
		return api.DeleteMessage(ctx, channelID, messageID)
	})
}

func newReactionFile(stat *proto.Stat, api *fluxer.Client, resolve func(context.Context) (string, error), add bool) *commandFile {
	return newCommandFile(stat, func(ctx context.Context, data []byte) error {
		messageID, emoji, err := parseReactionCommand(data)
		if err != nil {
			return err
		}
		channelID, err := resolve(ctx)
		if err != nil {
			return err
		}
		if add {
			return api.AddReaction(ctx, channelID, messageID, emoji)
		}
		return api.RemoveReaction(ctx, channelID, messageID, emoji)
	})
}

func newTypingFile(stat *proto.Stat, api *fluxer.Client, resolve func(context.Context) (string, error)) *commandFile {
	return newCommandFile(stat, func(ctx context.Context, _ []byte) error {
		channelID, err := resolve(ctx)
		if err != nil {
			return err
		}
		return api.IndicateTyping(ctx, channelID)
	})
}

func newPinFile(stat *proto.Stat, api *fluxer.Client, resolve func(context.Context) (string, error), pinned bool) *commandFile {
	return newCommandFile(stat, func(ctx context.Context, data []byte) error {
		messageID, err := parseIDCommand(data)
		if err != nil {
			return err
		}
		channelID, err := resolve(ctx)
		if err != nil {
			return err
		}
		return api.SetPinned(ctx, channelID, messageID, pinned)
	})
}

func newAcknowledgeFile(stat *proto.Stat, api *fluxer.Client, resolve func(context.Context) (string, error)) *commandFile {
	return newCommandFile(stat, func(ctx context.Context, data []byte) error {
		messageID, err := parseIDCommand(data)
		if err != nil {
			return err
		}
		channelID, err := resolve(ctx)
		if err != nil {
			return err
		}
		return api.AcknowledgeMessage(ctx, channelID, messageID)
	})
}

func parseMessageCommand(data []byte) (string, string, error) {
	text, err := commandText(data)
	if err != nil {
		return "", "", err
	}
	separator := strings.IndexFunc(text, unicode.IsSpace)
	if separator < 0 {
		return "", "", errors.New("expected: <message-id> <content>")
	}
	messageID := text[:separator]
	content := strings.TrimLeftFunc(text[separator:], unicode.IsSpace)
	if err := validateMessageID(messageID); err != nil {
		return "", "", err
	}
	if content == "" {
		return "", "", errors.New("message content is empty")
	}
	if utf8.RuneCountInString(content) > 4000 {
		return "", "", errors.New("message exceeds Fluxer's 4000-character maximum")
	}
	return messageID, content, nil
}

func parseReactionCommand(data []byte) (string, string, error) {
	messageID, emoji, err := parseMessageCommand(data)
	if err != nil {
		return "", "", fmt.Errorf("reaction: %w", err)
	}
	if strings.IndexFunc(emoji, unicode.IsSpace) >= 0 {
		return "", "", errors.New("reaction: emoji must not contain whitespace")
	}
	return messageID, emoji, nil
}

func parseIDCommand(data []byte) (string, error) {
	text, err := commandText(data)
	if err != nil {
		return "", err
	}
	messageID := strings.TrimSpace(text)
	if strings.IndexFunc(messageID, unicode.IsSpace) >= 0 {
		return "", errors.New("expected exactly one message ID")
	}
	if err := validateMessageID(messageID); err != nil {
		return "", err
	}
	return messageID, nil
}

func commandText(data []byte) (string, error) {
	if len(data) == 0 {
		return "", errors.New("command is empty")
	}
	if !utf8.Valid(data) {
		return "", errors.New("command is not valid UTF-8")
	}
	text := string(data)
	text = strings.TrimSuffix(text, "\n")
	text = strings.TrimSuffix(text, "\r")
	if text == "" {
		return "", errors.New("command is empty")
	}
	return text, nil
}

func validateMessageID(id string) error {
	if id == "" {
		return errors.New("message ID is empty")
	}
	for _, r := range id {
		if r < '0' || r > '9' {
			return errors.New("message ID must contain only decimal digits")
		}
	}
	return nil
}

func messageNonce() (string, error) {
	var nonceBytes [16]byte
	if _, err := rand.Read(nonceBytes[:]); err != nil {
		return "", fmt.Errorf("generate message nonce: %w", err)
	}
	return hex.EncodeToString(nonceBytes[:]), nil
}

func newSendFile(stat *proto.Stat, api *fluxer.Client, resolve func(context.Context) (string, error)) *sendFile {
	return &sendFile{BaseFile: fs.NewBaseFile(stat), buffers: make(map[uint64][]byte), resolve: resolve, api: api}
}
func (f *sendFile) Open(fid uint64, mode proto.Mode) error {
	if mode&0x0f != proto.Owrite && mode&0x0f != proto.Ordwr {
		return errors.New("send is write only")
	}
	f.mu.Lock()
	f.buffers[fid] = []byte{}
	f.mu.Unlock()
	return nil
}
func (f *sendFile) Read(uint64, uint64, uint64) ([]byte, error) {
	return nil, errors.New("send is write only")
}
func (f *sendFile) Write(fid, offset uint64, data []byte) (uint32, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	buffer, ok := f.buffers[fid]
	if !ok {
		return 0, errors.New("file is not open")
	}
	end := offset + uint64(len(data))
	if end > maxSendBuffer {
		return 0, errors.New("send exceeds 64 MiB local buffer limit")
	}
	if end > uint64(len(buffer)) {
		buffer = append(buffer, make([]byte, end-uint64(len(buffer)))...)
	}
	copy(buffer[offset:end], data)
	f.buffers[fid] = buffer
	return uint32(len(data)), nil
}
func (f *sendFile) Close(fid uint64) error {
	f.mu.Lock()
	data, ok := f.buffers[fid]
	delete(f.buffers, fid)
	f.mu.Unlock()
	if !ok {
		return nil
	}
	payload, err := parseSend(data)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	channelID, err := f.resolve(ctx)
	if err != nil {
		return err
	}
	nonce, err := messageNonce()
	if err != nil {
		return err
	}
	if payload.attachment == nil {
		_, err = f.api.SendMessage(ctx, channelID, payload.content, nonce)
	} else {
		_, err = f.api.SendAttachmentMessage(ctx, channelID, payload.content, payload.attachment.filename, payload.attachment.contentType, payload.attachment.data, nonce)
	}
	return err
}

type sendPayload struct {
	content    string
	attachment *sendAttachment
}

type sendAttachment struct {
	filename, contentType string
	data                  []byte
}

func parseSend(data []byte) (sendPayload, error) {
	if len(data) == 0 {
		return sendPayload{}, errors.New("message is empty")
	}
	if bytes.HasPrefix(data, []byte("!attach ")) {
		return parseFramedAttachment(data)
	}
	contentType := http.DetectContentType(data)
	if strings.HasPrefix(contentType, "text/plain") && utf8.Valid(data) {
		if data[len(data)-1] == '\n' {
			data = data[:len(data)-1]
		}
		if len(data) == 0 {
			return sendPayload{}, errors.New("message is empty")
		}
		if utf8.RuneCount(data) > 4000 {
			return sendPayload{}, errors.New("message exceeds Fluxer's 4000-character maximum")
		}
		return sendPayload{content: string(data)}, nil
	}
	return sendPayload{attachment: &sendAttachment{
		filename: inferAttachmentFilename(contentType), contentType: contentType, data: data,
	}}, nil
}

func parseFramedAttachment(data []byte) (sendPayload, error) {
	lineEnd := bytes.IndexByte(data, '\n')
	if lineEnd < 0 {
		return sendPayload{}, errors.New("attachment header must be followed by a caption and a blank line")
	}
	filename := strings.TrimSpace(string(data[len("!attach "):lineEnd]))
	if err := validateAttachmentFilename(filename); err != nil {
		return sendPayload{}, err
	}
	rest := data[lineEnd+1:]
	var captionBytes, attachmentData []byte
	if bytes.HasPrefix(rest, []byte("\r\n")) {
		attachmentData = rest[2:]
	} else if bytes.HasPrefix(rest, []byte("\n")) {
		attachmentData = rest[1:]
	} else if separator := bytes.Index(rest, []byte("\r\n\r\n")); separator >= 0 {
		captionBytes, attachmentData = rest[:separator], rest[separator+4:]
	} else if separator := bytes.Index(rest, []byte("\n\n")); separator >= 0 {
		captionBytes, attachmentData = rest[:separator], rest[separator+2:]
	} else {
		return sendPayload{}, errors.New("attachment caption must end with a blank line")
	}
	if len(attachmentData) == 0 {
		return sendPayload{}, errors.New("attachment is empty")
	}
	if !utf8.Valid(captionBytes) {
		return sendPayload{}, errors.New("attachment caption is not valid UTF-8")
	}
	if utf8.RuneCount(captionBytes) > 4000 {
		return sendPayload{}, errors.New("attachment caption exceeds Fluxer's 4000-character maximum")
	}
	contentType := http.DetectContentType(attachmentData)
	return sendPayload{
		content: string(captionBytes),
		attachment: &sendAttachment{
			filename: filename, contentType: contentType, data: attachmentData,
		},
	}, nil
}

func validateAttachmentFilename(filename string) error {
	if filename == "" || filename == "." || filename == ".." {
		return errors.New("attachment filename is invalid")
	}
	if len(filename) > 255 || !utf8.ValidString(filename) {
		return errors.New("attachment filename is too long or invalid UTF-8")
	}
	for _, r := range filename {
		if r == '/' || r == '\\' || r < 0x20 || r == 0x7f {
			return errors.New("attachment filename contains a path separator or control character")
		}
	}
	return nil
}

func inferAttachmentFilename(contentType string) string {
	if separator := strings.IndexByte(contentType, ';'); separator >= 0 {
		contentType = contentType[:separator]
	}
	extensions := map[string]string{
		"image/png": ".png", "image/jpeg": ".jpg", "image/gif": ".gif", "image/webp": ".webp",
		"application/pdf": ".pdf", "audio/mpeg": ".mp3", "video/mp4": ".mp4",
		"application/zip": ".zip",
	}
	extension := extensions[contentType]
	if extension == "" {
		extension = ".bin"
	}
	return "attachment" + extension
}
