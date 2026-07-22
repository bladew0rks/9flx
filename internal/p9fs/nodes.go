package p9fs

import (
	"fmt"
	"sync"

	"github.com/ronsor/go9p/fs"
	"github.com/ronsor/go9p/proto"
)

type dynamicDir struct {
	mu       sync.RWMutex
	stat     proto.Stat
	parent   fs.Dir
	children map[string]fs.FSNode
}

func newDynamicDir(stat *proto.Stat) *dynamicDir {
	stat.Mode |= proto.DMDIR
	stat.Qid.Qtype = uint8(stat.Mode >> 24)
	return &dynamicDir{stat: *stat, children: make(map[string]fs.FSNode)}
}

func (d *dynamicDir) Stat() proto.Stat            { d.mu.RLock(); defer d.mu.RUnlock(); return d.stat }
func (d *dynamicDir) WriteStat(*proto.Stat) error { return fmt.Errorf("attributes are read only") }
func (d *dynamicDir) SetParent(parent fs.Dir)     { d.mu.Lock(); d.parent = parent; d.mu.Unlock() }
func (d *dynamicDir) Parent() fs.Dir              { d.mu.RLock(); defer d.mu.RUnlock(); return d.parent }
func (d *dynamicDir) SetName(name string) {
	d.mu.Lock()
	if d.stat.Name != name {
		d.stat.Name = name
		d.stat.Qid.Vers++
	}
	d.mu.Unlock()
}
func (d *dynamicDir) Children() map[string]fs.FSNode {
	d.mu.RLock()
	defer d.mu.RUnlock()
	result := make(map[string]fs.FSNode, len(d.children))
	for name, child := range d.children {
		result[name] = child
	}
	return result
}
func (d *dynamicDir) GetChild(name string) (fs.FSNode, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.children[name], nil
}
func (d *dynamicDir) Replace(children map[string]fs.FSNode) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, child := range children {
		child.SetParent(d)
	}
	d.children = children
}
func (d *dynamicDir) Add(child fs.FSNode) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	name := child.Stat().Name
	if _, exists := d.children[name]; exists {
		return fmt.Errorf("%s already exists", name)
	}
	child.SetParent(d)
	d.children[name] = child
	return nil
}

func slice(data []byte, offset, count uint64) []byte {
	if offset >= uint64(len(data)) {
		return []byte{}
	}
	end := offset + count
	if end > uint64(len(data)) {
		end = uint64(len(data))
	}
	return data[offset:end]
}
