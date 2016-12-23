package web

import (
	"sync"
	"time"
)

type cache struct {
	ttl          time.Duration
	maxSize      int
	size         int
	oldest       *cacheEntry
	newest       *cacheEntry
	entriesBySQL map[string]*cacheEntry
	mx           sync.RWMutex
}

type cacheEntry struct {
	prior      *cacheEntry
	next       *cacheEntry
	sql        string
	data       []byte
	expiration time.Time
}

func newCache(ttl time.Duration, maxSize int) *cache {
	return &cache{
		ttl:          ttl,
		maxSize:      maxSize,
		entriesBySQL: make(map[string]*cacheEntry),
	}
}

func (c *cache) get(sql string) []byte {
	c.mx.RLock()
	defer c.mx.RUnlock()

	entry := c.entriesBySQL[sql]
	if entry == nil {
		return nil
	}
	if entry.expiration.Before(time.Now()) {
		return nil
	}
	return entry.data
}

func (c *cache) put(sql string, data []byte) {
	c.mx.Lock()
	defer c.mx.Unlock()

	dataSize := len(data)
	if dataSize > c.maxSize {
		// Can't cache
		return
	}

	entry := c.entriesBySQL[sql]
	entryWasNewest := false
	if entry != nil {
		// Update existing entry
		if entry.isOldest() {
			c.oldest = entry.next
			if entry.next != nil {
				entry.next.prior = nil
			}
		} else if entry.isNewest() {
			entryWasNewest = true
		} else {
			entry.prior.next = entry.next
			entry.next.prior = entry.prior
		}
		entry.next = nil
	} else {
		entry = &cacheEntry{sql: sql}
		c.entriesBySQL[sql] = entry
		c.size += dataSize
	}
	entry.data = data
	entry.expiration = time.Now().Add(c.ttl)

	if !entryWasNewest {
		entry.prior = c.newest
		if c.newest != nil {
			c.newest.next = entry
		}
		c.newest = entry
	}
	if entry.isOldest() {
		c.oldest = entry
	}

	// Purge all expired data
	now := time.Now()
	current := c.oldest
	for ; current != nil; current = current.next {
		if current.expiration.Before(now) {
			c.removeEntry(current)
			if current.isOldest() {
				c.oldest = current.next
			}
			if current.isNewest() {
				c.newest = current.prior
			}
		}
	}

	// Purge oldest data to stay below maxSize
	for c.size > c.maxSize {
		c.removeEntry(c.oldest)
		c.oldest = c.oldest.next
		if c.oldest == nil {
			// Oldest is also newest, remove
			c.newest = nil
		}
	}
}

func (c *cache) removeEntry(entry *cacheEntry) {
	delete(c.entriesBySQL, entry.sql)
	c.size -= len(entry.data)
	if entry.next != nil {
		entry.next.prior = entry.prior
	}
	if entry.prior != nil {
		entry.prior.next = entry.next
	}
}

func (e *cacheEntry) isOldest() bool {
	return e.prior == nil
}

func (e *cacheEntry) isNewest() bool {
	return e.next == nil
}
