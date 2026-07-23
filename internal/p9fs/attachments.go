package p9fs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/bladew0rks/9flx/internal/fluxer"
	"github.com/bladew0rks/9flx/internal/render"
	"github.com/ronsor/go9p/fs"
	"github.com/ronsor/go9p/proto"
)

type attachmentSource int

const (
	attachmentHistory attachmentSource = iota
	attachmentPins
)

type attachmentIndex struct {
	mu       sync.Mutex
	fs       *fs.FS
	api      *fluxer.Client
	channel  func() (string, bool)
	dir      *dynamicDir
	sources  [2]map[string]fluxer.Message
	messages map[string]*attachmentMessageDir
}

type attachmentMessageDir struct {
	dir         *dynamicDir
	info        *valueCell
	attachments map[string]*attachmentFile
}

type attachmentInfo struct {
	MessageID   string                 `json:"message_id"`
	Attachments []attachmentInfoRecord `json:"attachments"`
}

type attachmentInfoRecord struct {
	Path       string            `json:"path"`
	Attachment fluxer.Attachment `json:"attachment"`
}

func newAttachmentIndex(filesystem *fs.FS, api *fluxer.Client, channel func() (string, bool)) *attachmentIndex {
	return &attachmentIndex{
		fs: filesystem, api: api, channel: channel,
		dir:      newDynamicDir(filesystem.NewStat("attachments", "9flx", "9flx", 0555)),
		messages: make(map[string]*attachmentMessageDir),
	}
}

func (i *attachmentIndex) update(source attachmentSource, messages []fluxer.Message) {
	current := make(map[string]fluxer.Message)
	for _, message := range messages {
		if message.ID != "" && len(message.Attachments) > 0 {
			current[message.ID] = message
		}
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.sources[source] = current
	combined := make(map[string]fluxer.Message)
	for _, snapshot := range i.sources {
		for id, message := range snapshot {
			combined[id] = message
		}
	}
	children := make(map[string]fs.FSNode, len(combined))
	active := make(map[string]struct{}, len(combined))
	for id, message := range combined {
		node := i.messages[id]
		if node == nil {
			node = &attachmentMessageDir{
				dir:         newDynamicDir(i.fs.NewStat(id, "9flx", "9flx", 0555)),
				info:        &valueCell{},
				attachments: make(map[string]*attachmentFile),
			}
			info := node.info
			_ = node.dir.Add(newSnapshotFile(i.fs.NewStat("info.json", "9flx", "9flx", 0444), func() ([]byte, error) {
				return render.JSON(info.Get()), nil
			}))
			i.messages[id] = node
		}
		i.reconcileMessage(node, message)
		children[id] = node.dir
		active[id] = struct{}{}
	}
	for id := range i.messages {
		if _, ok := active[id]; !ok {
			delete(i.messages, id)
		}
	}
	i.dir.Replace(children)
}

func (i *attachmentIndex) reconcileMessage(node *attachmentMessageDir, message fluxer.Message) {
	names := render.AttachmentNames(message.Attachments)
	children := make(map[string]fs.FSNode, len(message.Attachments)+1)
	if info, err := node.dir.GetChild("info.json"); err == nil && info != nil {
		children["info.json"] = info
	}
	records := make([]attachmentInfoRecord, 0, len(message.Attachments))
	active := make(map[string]struct{}, len(message.Attachments))
	for _, attachment := range message.Attachments {
		name := names[attachment.ID]
		file := node.attachments[attachment.ID]
		if file == nil {
			stat := i.fs.NewStat(name, "9flx", "9flx", 0444)
			stat.Length = attachmentLength(attachment.Size)
			file = newAttachmentFile(stat, i.api, i.channel, message.ID, attachment)
			node.attachments[attachment.ID] = file
		} else {
			file.update(name, attachment)
		}
		children[name] = file
		records = append(records, attachmentInfoRecord{Path: name, Attachment: attachment})
		active[attachment.ID] = struct{}{}
	}
	for id := range node.attachments {
		if _, ok := active[id]; !ok {
			delete(node.attachments, id)
		}
	}
	node.info.Set(attachmentInfo{MessageID: message.ID, Attachments: records})
	node.dir.Replace(children)
}

func attachmentLength(size int64) uint64 {
	if size < 0 {
		return 0
	}
	return uint64(size)
}

type attachmentRead struct {
	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
	body   io.ReadCloser
	offset uint64
}

type attachmentFile struct {
	mu         sync.RWMutex
	stat       proto.Stat
	parent     fs.Dir
	api        *fluxer.Client
	channel    func() (string, bool)
	messageID  string
	attachment fluxer.Attachment
	readers    map[uint64]*attachmentRead
}

func newAttachmentFile(stat *proto.Stat, api *fluxer.Client, channel func() (string, bool), messageID string, attachment fluxer.Attachment) *attachmentFile {
	return &attachmentFile{
		stat: *stat, api: api, channel: channel, messageID: messageID,
		attachment: attachment, readers: make(map[uint64]*attachmentRead),
	}
}

func (f *attachmentFile) Stat() proto.Stat {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.stat
}

func (f *attachmentFile) WriteStat(*proto.Stat) error { return errors.New("attributes are read only") }
func (f *attachmentFile) SetParent(parent fs.Dir)     { f.mu.Lock(); f.parent = parent; f.mu.Unlock() }
func (f *attachmentFile) Parent() fs.Dir              { f.mu.RLock(); defer f.mu.RUnlock(); return f.parent }

func (f *attachmentFile) update(name string, attachment fluxer.Attachment) {
	f.mu.Lock()
	if f.stat.Name != name || f.stat.Length != attachmentLength(attachment.Size) || f.attachment != attachment {
		f.stat.Name = name
		f.stat.Length = attachmentLength(attachment.Size)
		f.stat.Qid.Vers++
	}
	f.attachment = attachment
	f.mu.Unlock()
}

func (f *attachmentFile) Open(fid uint64, mode proto.Mode) error {
	if mode&0x0f != proto.Oread {
		return errors.New("attachment is read only")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	f.mu.Lock()
	f.readers[fid] = &attachmentRead{ctx: ctx, cancel: cancel}
	f.mu.Unlock()
	return nil
}

func (f *attachmentFile) Read(fid, offset, count uint64) ([]byte, error) {
	if count == 0 {
		return []byte{}, nil
	}
	if offset > uint64(^uint64(0)>>1) {
		return nil, errors.New("attachment offset is too large")
	}
	f.mu.RLock()
	reader := f.readers[fid]
	f.mu.RUnlock()
	if reader == nil {
		return nil, errors.New("attachment is not open")
	}
	reader.mu.Lock()
	defer reader.mu.Unlock()
	if reader.body == nil || reader.offset != offset {
		if reader.body != nil {
			_ = reader.body.Close()
		}
		body, err := f.open(reader.ctx, int64(offset), false)
		if err != nil {
			return nil, err
		}
		reader.body = body
		reader.offset = offset
	}
	buffer := make([]byte, count)
	n, err := reader.body.Read(buffer)
	reader.offset += uint64(n)
	if errors.Is(err, io.EOF) {
		err = nil
	}
	return buffer[:n], err
}

func (f *attachmentFile) open(ctx context.Context, offset int64, refreshed bool) (io.ReadCloser, error) {
	f.mu.RLock()
	attachment := f.attachment
	messageID := f.messageID
	f.mu.RUnlock()
	if attachment.URL == nil || *attachment.URL == "" {
		if refreshed {
			return nil, errors.New("attachment has no download URL")
		}
		return f.refreshAndOpen(ctx, offset)
	}
	body, err := f.api.OpenAttachment(ctx, *attachment.URL, offset)
	var apiErr *fluxer.APIError
	if err != nil && !refreshed && errors.As(err, &apiErr) &&
		(apiErr.StatusCode == 401 || apiErr.StatusCode == 403 || apiErr.StatusCode == 404) {
		return f.refreshAndOpen(ctx, offset)
	}
	if err != nil {
		return nil, fmt.Errorf("download attachment %s from message %s: %w", attachment.ID, messageID, err)
	}
	return body, nil
}

func (f *attachmentFile) refreshAndOpen(ctx context.Context, offset int64) (io.ReadCloser, error) {
	channelID, ok := f.channel()
	if !ok {
		return nil, errors.New("conversation has no channel")
	}
	message, err := f.api.Message(ctx, channelID, f.messageID)
	if err != nil {
		return nil, fmt.Errorf("refresh attachment message: %w", err)
	}
	f.mu.RLock()
	attachmentID := f.attachment.ID
	name := f.stat.Name
	f.mu.RUnlock()
	for _, attachment := range message.Attachments {
		if attachment.ID == attachmentID {
			f.update(name, attachment)
			return f.open(ctx, offset, true)
		}
	}
	return nil, errors.New("attachment no longer exists")
}

func (f *attachmentFile) Write(uint64, uint64, []byte) (uint32, error) {
	return 0, errors.New("attachment is read only")
}

func (f *attachmentFile) Close(fid uint64) error {
	f.mu.Lock()
	reader := f.readers[fid]
	delete(f.readers, fid)
	f.mu.Unlock()
	if reader != nil {
		reader.cancel()
		reader.mu.Lock()
		if reader.body != nil {
			_ = reader.body.Close()
		}
		reader.mu.Unlock()
	}
	return nil
}
