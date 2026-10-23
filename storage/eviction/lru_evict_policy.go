package eviction

import (
	"container/list"
	"sync"
)

// LRUEvict ("strict") maintains exact LRU order for one shard using a
// doubly-linked list and a companion map. The back of the list is always the
// least-recently-used key.
//
// Trade-off: every Get takes this policy's mutex to move the key to the
// front, so reads on one shard are serialised on it (the other two policies
// keep the read path fully concurrent).
type LRUEvict struct {
	mu    sync.Mutex
	order *list.List               // front = most recent, back = least recent
	elems map[string]*list.Element // key → its element in order
}

func NewLRUEvict() *LRUEvict {
	return &LRUEvict{
		order: list.New(),
		elems: make(map[string]*list.Element),
	}
}

// BeforeWrite forgets key if it is being overwritten (AfterWrite re-adds it at
// the front), then evicts from the back until the new entry fits.
func (p *LRUEvict) BeforeWrite(s Shard, key string, needed int64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.forget(key)
	if s.Budget() == 0 {
		return true
	}
	for s.Bytes()+needed > s.Budget() && p.order.Len() > 0 {
		tail := p.order.Back()
		victim := tail.Value.(string)
		p.forget(victim)
		s.Evict(victim)
	}
	return true
}

func (p *LRUEvict) AfterWrite(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.elems[key] = p.order.PushFront(key)
}

func (p *LRUEvict) BeforeRead(_ string) {}

func (p *LRUEvict) AfterRead(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if elem, ok := p.elems[key]; ok {
		p.order.MoveToFront(elem)
	}
}

func (p *LRUEvict) BeforeDelete(_ string) {}

func (p *LRUEvict) AfterDelete(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.forget(key)
}

// forget drops key from the LRU order. Caller holds p.mu.
func (p *LRUEvict) forget(key string) {
	if elem, ok := p.elems[key]; ok {
		p.order.Remove(elem)
		delete(p.elems, key)
	}
}
