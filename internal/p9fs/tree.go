package p9fs

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/bladew0rks/9flx/internal/core"
	"github.com/bladew0rks/9flx/internal/fluxer"
	"github.com/bladew0rks/9flx/internal/render"
	"github.com/ronsor/go9p/fs"
)

type valueCell struct {
	mu    sync.RWMutex
	value any
}

func (c *valueCell) Set(value any) { c.mu.Lock(); c.value = value; c.mu.Unlock() }
func (c *valueCell) Get() any      { c.mu.RLock(); defer c.mu.RUnlock(); return c.value }

type cachedConversation struct {
	dir    *dynamicDir
	info   *valueCell
	avatar *valueCell
}

type cachedCommunity struct {
	dir          *dynamicDir
	info         *valueCell
	channels     *dynamicDir
	channelNodes map[string]*cachedConversation
}

type Tree struct {
	FS                        *fs.FS
	api                       *fluxer.Client
	store                     *core.Store
	hub                       *core.Hub
	status                    *core.Status
	historyLimit              int
	mu                        sync.Mutex
	dmMu                      sync.Mutex
	friends, dms, communities *dynamicDir
	friendNodes               map[string]*cachedConversation
	dmNodes                   map[string]*cachedConversation
	communityNodes            map[string]*cachedCommunity
}

func NewTree(api *fluxer.Client, store *core.Store, hub *core.Hub, status *core.Status, setPresence func(fluxer.PresenceStatus) error, setCustomStatus func(*fluxer.CustomStatus) error, historyLimit int) (*Tree, error) {
	filesystem, root := fs.NewFS("9flx", "9flx", 0555)
	t := &Tree{
		FS: filesystem, api: api, store: store, hub: hub, status: status, historyLimit: historyLimit,
		friendNodes: make(map[string]*cachedConversation), dmNodes: make(map[string]*cachedConversation),
		communityNodes: make(map[string]*cachedCommunity),
	}
	t.friends = newDynamicDir(filesystem.NewStat("friends", "9flx", "9flx", 0555))
	t.dms = newDynamicDir(filesystem.NewStat("dms", "9flx", "9flx", 0555))
	t.communities = newDynamicDir(filesystem.NewStat("communities", "9flx", "9flx", 0555))
	me := newDynamicDir(filesystem.NewStat("me", "9flx", "9flx", 0555))
	resolveMe := func() (fluxer.User, bool) {
		profile := store.Snapshot().Me
		return profile, profile.ID != ""
	}
	_ = me.Add(newSnapshotFile(filesystem.NewStat("info.json", "9flx", "9flx", 0444), func() ([]byte, error) {
		return render.JSON(store.Snapshot().Me), nil
	}))
	_ = me.Add(newAvatarFile(filesystem.NewStat("avatar", "9flx", "9flx", 0444), api, resolveMe))
	_ = me.Add(newAvatarURLFile(filesystem.NewStat("avatar.url", "9flx", "9flx", 0444), api, resolveMe))
	_ = me.Add(newPresenceFile(filesystem.NewStat("status", "9flx", "9flx", 0666), api, setPresence))
	_ = me.Add(newCustomStatusFile(filesystem.NewStat("custom-status", "9flx", "9flx", 0666), api, setCustomStatus))
	for _, node := range []fs.FSNode{
		newSnapshotFile(filesystem.NewStat("status", "9flx", "9flx", 0444), func() ([]byte, error) { return status.Text(), nil }),
		newSnapshotFile(filesystem.NewStat("status.json", "9flx", "9flx", 0444), func() ([]byte, error) { return status.JSON(), nil }),
		newGlobalLiveFile(filesystem.NewStat("inbox", "9flx", "9flx", 0444), hub, false),
		newGlobalLiveFile(filesystem.NewStat("inbox.jsonl", "9flx", "9flx", 0444), hub, true),
		me, t.friends, t.dms, t.communities,
	} {
		if err := root.AddChild(node); err != nil {
			return nil, err
		}
	}
	store.SetTopologyObserver(t.Refresh)
	t.Refresh()
	return t, nil
}

type conversationSpec struct {
	base, id string
	info     any
	avatar   *fluxer.User
	read     func() (string, bool)
	send     func(context.Context) (string, error)
}

func (t *Tree) Refresh() {
	t.mu.Lock()
	defer t.mu.Unlock()
	snapshot := t.store.Snapshot()

	friends := make([]conversationSpec, 0, len(snapshot.Relationships))
	for _, relationship := range snapshot.Relationships {
		if relationship.Type != fluxer.RelationshipFriend || relationship.User.ID == "" {
			continue
		}
		rel, userID := relationship, relationship.User.ID
		avatar := rel.User
		friends = append(friends, conversationSpec{
			base: pathName(rel.User.Tag()), id: userID, info: rel, avatar: &avatar,
			read: func() (string, bool) { dm, ok := t.store.FindDMForUser(userID); return dm.ID, ok },
			send: func(ctx context.Context) (string, error) {
				t.dmMu.Lock()
				defer t.dmMu.Unlock()
				if dm, ok := t.store.FindDMForUser(userID); ok {
					return dm.ID, nil
				}
				dm, err := t.api.CreateDM(ctx, userID)
				if err != nil {
					return "", err
				}
				t.store.UpsertDM(dm)
				return dm.ID, nil
			},
		})
	}
	t.friends.Replace(t.reconcileConversations(friends, t.friendNodes))

	dms := make([]conversationSpec, 0, len(snapshot.DMs))
	for _, channel := range snapshot.DMs {
		ch, base := channel, channel.Name
		if ch.Type == fluxer.ChannelDM && len(ch.Recipients) > 0 {
			base = ch.Recipients[0].Tag()
		}
		if base == "" {
			base = "dm"
		}
		var avatar *fluxer.User
		if ch.Type == fluxer.ChannelDM && len(ch.Recipients) > 0 {
			profile := ch.Recipients[0]
			avatar = &profile
		}
		dms = append(dms, conversationSpec{
			base: pathName(base), id: ch.ID, info: ch, avatar: avatar,
			read: func() (string, bool) { return ch.ID, ch.ID != "" },
			send: func(context.Context) (string, error) { return ch.ID, nil },
		})
	}
	t.dms.Replace(t.reconcileConversations(dms, t.dmNodes))

	communityNames := assignNames(func() []namedID {
		items := make([]namedID, 0, len(snapshot.Guilds))
		for _, guild := range snapshot.Guilds {
			items = append(items, namedID{base: pathName(guild.Name), id: guild.ID})
		}
		return items
	}())
	communityChildren := make(map[string]fs.FSNode, len(snapshot.Guilds))
	activeCommunities := make(map[string]struct{}, len(snapshot.Guilds))
	for _, guild := range snapshot.Guilds {
		name := communityNames[guild.ID]
		community := t.communityNodes[guild.ID]
		if community == nil {
			community = t.newCommunity(name)
			t.communityNodes[guild.ID] = community
		}
		t.reconcileCommunity(community, name, guild)
		communityChildren[name] = community.dir
		activeCommunities[guild.ID] = struct{}{}
	}
	for id := range t.communityNodes {
		if _, ok := activeCommunities[id]; !ok {
			delete(t.communityNodes, id)
		}
	}
	t.communities.Replace(communityChildren)
}

func (t *Tree) reconcileConversations(specs []conversationSpec, cache map[string]*cachedConversation) map[string]fs.FSNode {
	items := make([]namedID, 0, len(specs))
	for _, spec := range specs {
		items = append(items, namedID{base: spec.base, id: spec.id})
	}
	names := assignNames(items)
	children := make(map[string]fs.FSNode, len(specs))
	active := make(map[string]struct{}, len(specs))
	for _, spec := range specs {
		name := names[spec.id]
		conversation := cache[spec.id]
		if conversation == nil {
			conversation = t.newConversation(name, spec.info, spec.avatar, spec.read, spec.send)
			cache[spec.id] = conversation
		} else {
			conversation.dir.SetName(name)
			conversation.info.Set(spec.info)
			if conversation.avatar != nil {
				if spec.avatar == nil {
					conversation.avatar.Set(nil)
				} else {
					conversation.avatar.Set(*spec.avatar)
				}
			}
		}
		children[name] = conversation.dir
		active[spec.id] = struct{}{}
	}
	for id := range cache {
		if _, ok := active[id]; !ok {
			delete(cache, id)
		}
	}
	return children
}

func (t *Tree) newConversation(name string, info any, avatar *fluxer.User, read func() (string, bool), send func(context.Context) (string, error)) *cachedConversation {
	dir := newDynamicDir(t.FS.NewStat(name, "9flx", "9flx", 0555))
	cell := &valueCell{value: info}
	var avatarCell *valueCell
	resolveExisting := func(context.Context) (string, error) {
		id, ok := read()
		if !ok {
			return "", fmt.Errorf("conversation has no channel yet")
		}
		return id, nil
	}
	for _, child := range []fs.FSNode{
		newSnapshotFile(t.FS.NewStat("info.json", "9flx", "9flx", 0444), func() ([]byte, error) { return render.JSON(cell.Get()), nil }),
		newHistoryFile(t.FS.NewStat("history", "9flx", "9flx", 0444), t.api, read, t.historyLimit, false),
		newHistoryFile(t.FS.NewStat("history.jsonl", "9flx", "9flx", 0444), t.api, read, t.historyLimit, true),
		newLiveFile(t.FS.NewStat("events", "9flx", "9flx", 0444), t.hub, read, false),
		newLiveFile(t.FS.NewStat("events.jsonl", "9flx", "9flx", 0444), t.hub, read, true),
		newSendFile(t.FS.NewStat("send", "9flx", "9flx", 0222), t.api, send),
		newEditFile(t.FS.NewStat("edit", "9flx", "9flx", 0222), t.api, resolveExisting),
		newReplyFile(t.FS.NewStat("reply", "9flx", "9flx", 0222), t.api, resolveExisting),
		newDeleteFile(t.FS.NewStat("delete", "9flx", "9flx", 0222), t.api, resolveExisting),
		newReactionFile(t.FS.NewStat("react", "9flx", "9flx", 0222), t.api, resolveExisting, true),
		newReactionFile(t.FS.NewStat("unreact", "9flx", "9flx", 0222), t.api, resolveExisting, false),
	} {
		_ = dir.Add(child)
	}
	if avatar != nil {
		avatarCell = &valueCell{value: *avatar}
		resolveAvatar := func() (fluxer.User, bool) {
			profile, ok := avatarCell.Get().(fluxer.User)
			return profile, ok && profile.ID != ""
		}
		_ = dir.Add(newAvatarFile(t.FS.NewStat("avatar", "9flx", "9flx", 0444), t.api, resolveAvatar))
		_ = dir.Add(newAvatarURLFile(t.FS.NewStat("avatar.url", "9flx", "9flx", 0444), t.api, resolveAvatar))
	}
	return &cachedConversation{dir: dir, info: cell, avatar: avatarCell}
}

func (t *Tree) newCommunity(name string) *cachedCommunity {
	dir := newDynamicDir(t.FS.NewStat(name, "9flx", "9flx", 0555))
	info := &valueCell{}
	channels := newDynamicDir(t.FS.NewStat("channels", "9flx", "9flx", 0555))
	_ = dir.Add(newSnapshotFile(t.FS.NewStat("info.json", "9flx", "9flx", 0444), func() ([]byte, error) { return render.JSON(info.Get()), nil }))
	_ = dir.Add(channels)
	return &cachedCommunity{dir: dir, info: info, channels: channels, channelNodes: make(map[string]*cachedConversation)}
}

func (t *Tree) reconcileCommunity(community *cachedCommunity, name string, guild fluxer.Guild) {
	community.dir.SetName(name)
	metadata := guild
	metadata.Channels = nil
	community.info.Set(metadata)
	specs := make([]conversationSpec, 0, len(guild.Channels))
	for _, channel := range guild.Channels {
		if channel.Type != fluxer.ChannelText {
			continue
		}
		ch := channel
		specs = append(specs, conversationSpec{
			base: pathName(ch.Name), id: ch.ID, info: ch,
			read: func() (string, bool) { return ch.ID, ch.ID != "" },
			send: func(context.Context) (string, error) { return ch.ID, nil },
		})
	}
	community.channels.Replace(t.reconcileConversations(specs, community.channelNodes))
}

type namedID struct{ base, id string }

func assignNames(items []namedID) map[string]string {
	counts := make(map[string]int, len(items))
	for _, item := range items {
		counts[item.base]++
	}
	result := make(map[string]string, len(items))
	for _, item := range items {
		name := item.base
		if counts[name] > 1 {
			name += "~" + shortID(item.id)
		}
		result[item.id] = name
	}
	return result
}

func shortID(id string) string {
	if len(id) > 6 {
		return id[len(id)-6:]
	}
	return id
}

func pathName(value string) string {
	if value == "" {
		return "_"
	}
	var out strings.Builder
	for len(value) > 0 {
		r, size := utf8.DecodeRuneInString(value)
		raw := value[:size]
		value = value[size:]
		if r == utf8.RuneError && size == 1 {
			for _, b := range []byte(raw) {
				fmt.Fprintf(&out, "%%%02X", b)
			}
			continue
		}
		if r == '/' || r == '%' || r < 0x20 || r == 0x7f {
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
