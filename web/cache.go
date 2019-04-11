package web

import (
	"os"
	"path/filepath"
	"time"

	"github.com/boltdb/bolt"
	"github.com/getlantern/errors"
	"github.com/getlantern/uuid"
	"github.com/getlantern/zenodb/encoding"
)

const (
	statusPending = 0
	statusSuccess = 1
	statusError   = 2

	widthPermalink = 16
	idxStatus      = 0
	idxPermalink   = 1
	idxTime        = idxPermalink + widthPermalink
	idxData        = idxTime + encoding.WidthTime
)

var (
	cacheBucket     = []byte("cache")
	permalinkBucket = []byte("permalink")
)

type cache struct {
	db   *bolt.DB
	ttl  time.Duration
	size int
}

type cacheEntry []byte

func (c *cache) newCacheEntry() cacheEntry {
	ce := make([]byte, widthPermalink+encoding.WidthTime+1)
	permalink := uuid.New()
	copy(ce[idxPermalink:], permalink[:])
	encoding.EncodeTime(ce[idxTime:], time.Now().Add(c.ttl))
	return ce
}

func (ce cacheEntry) permalink() string {
	return ce.uuid().String()
}

func (ce cacheEntry) permalinkBytes() []byte {
	return ce[idxPermalink : idxPermalink+widthPermalink]
}

func (ce cacheEntry) uuid() uuid.UUID {
	var arr [16]byte
	copy(arr[:], ce.permalinkBytes())
	return uuid.UUID(arr)
}

func (ce cacheEntry) expires() time.Time {
	return encoding.TimeFromBytes(ce[idxTime:])
}

func (ce cacheEntry) expired() bool {
	return ce.expires().Before(time.Now())
}

func (ce cacheEntry) status() byte {
	return ce[idxStatus]
}

func (ce cacheEntry) pending() {
	ce[idxStatus] = statusPending
}

func (ce cacheEntry) data() []byte {
	return ce[idxData:]
}

func (ce cacheEntry) error() []byte {
	return ce.data()
}

func (ce cacheEntry) succeed(data []byte) cacheEntry {
	return ce.update(statusSuccess, data)
}

func (ce cacheEntry) fail(err error) cacheEntry {
	return ce.update(statusError, []byte(err.Error()))
}

func (ce cacheEntry) update(status byte, data []byte) cacheEntry {
	result := make(cacheEntry, 0, 1+widthPermalink+encoding.WidthTime+len(data))
	result = append(result, status) // mark as succeeded
	result = append(result, ce[idxPermalink:idxTime]...)
	result = append(result, ce[idxTime:idxData]...)
	result = append(result, data...)
	return result
}

func (ce cacheEntry) copy() cacheEntry {
	if ce == nil {
		return nil
	}
	result := make(cacheEntry, len(ce))
	copy(result, ce)
	return result
}

func newCache(cacheDir string, ttl time.Duration) (*cache, error) {
	err := os.MkdirAll(cacheDir, 0700)
	if err != nil {
		return nil, errors.New("Unable to create cacheDir at %v: %v", cacheDir, err)
	}

	db, err := bolt.Open(filepath.Join(cacheDir, "webcache.db"), 0600, nil)
	if err != nil {
		return nil, errors.New("Unable to open cache database: %v", err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, bucketErr := tx.CreateBucketIfNotExists(cacheBucket)
		if bucketErr != nil {
			return bucketErr
		}
		_, bucketErr = tx.CreateBucketIfNotExists(permalinkBucket)
		return bucketErr
	})
	if err != nil {
		return nil, errors.New("Unable to initialize cache database: %v", err)
	}

	return &cache{
		db:  db,
		ttl: ttl,
	}, nil
}

func (c *cache) getOrBegin(sql string) (ce cacheEntry, created bool, err error) {
	key := []byte(sql)
	err = c.db.Update(func(tx *bolt.Tx) error {
		cb := tx.Bucket(cacheBucket)
		pb := tx.Bucket(permalinkBucket)
		ce = cacheEntry(cb.Get(key)).copy()
		expired := ce != nil && ce.expired()
		if ce == nil || expired {
			created = true
			if expired {
				ce = nil
				err = cb.Delete(key)
				if err != nil {
					return err
				}
			}
			ce = c.newCacheEntry()
			cb.Put(key, ce)
			pb.Put(ce.permalinkBytes(), ce)
			return nil
		}
		return nil
	})
	return
}

func (c *cache) begin(sql string) (ce cacheEntry, err error) {
	key := []byte(sql)
	err = c.db.Update(func(tx *bolt.Tx) error {
		cb := tx.Bucket(cacheBucket)
		pb := tx.Bucket(permalinkBucket)
		ce = c.newCacheEntry()
		cb.Put(key, ce)
		pb.Put(ce.permalinkBytes(), ce)
		return nil
	})
	return
}

func (c *cache) getByPermalink(permalink string) (ce cacheEntry, err error) {
	key := uuid.MustParse(permalink)
	err = c.db.View(func(tx *bolt.Tx) error {
		pb := tx.Bucket(permalinkBucket)
		ce = cacheEntry(pb.Get(key[:])).copy()
		return nil
	})
	return
}

func (c *cache) put(sql string, ce cacheEntry) error {
	key := []byte(sql)

	return c.db.Update(func(tx *bolt.Tx) error {
		cb := tx.Bucket(cacheBucket)
		pb := tx.Bucket(permalinkBucket)
		pb.Put(ce.permalinkBytes(), ce)
		if ce.expired() {
			cb.Delete(key)
			return errors.New("Finished entry after expiration")
		}
		cb.Put(key, ce)
		return nil
	})
}

func (c *cache) Close() error {
	return c.db.Close()
}
