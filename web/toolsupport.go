package web

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/boltdb/bolt"

	"github.com/getlantern/errors"
	"github.com/getlantern/zenodb/encoding"
)

// PermalinkInfo provides info about a stored permalink
type PermalinkInfo struct {
	Permalink string
	SQL       string
	Timestamp time.Time
	NumRows   int
}

type permalinkInfo struct {
	Permalink string
	SQL       string
	TS        int64
	Rows      []*json.RawMessage
}

type PermalinkInfoRow struct {
	TS int64
}

type byTimestamp []*PermalinkInfo

func (a byTimestamp) Len() int           { return len(a) }
func (a byTimestamp) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byTimestamp) Less(i, j int) bool { return a[i].Timestamp.After(a[j].Timestamp) }

// ListPermalinks returns a list of known permalinks and associated info
func ListPermalinks(cacheFile string) ([]*PermalinkInfo, error) {
	c, err := newCacheFromFile(cacheFile, 24*time.Hour, &bolt.Options{
		ReadOnly: true,
		Timeout:  10 * time.Second,
	})
	if err != nil {
		return nil, errors.New("Unable to open cache at %v: %v", cacheFile, err)
	}
	permalinks, err := c.listPermalinks()
	if err != nil {
		return nil, err
	}

	sort.Sort(byTimestamp(permalinks))
	return permalinks, nil
}

func (c *cache) listPermalinks() ([]*PermalinkInfo, error) {
	var permalinks []*PermalinkInfo
	err := c.db.View(func(tx *bolt.Tx) error {
		defer func() {
			fmt.Fprintln(os.Stderr)
		}()

		pb := tx.Bucket(permalinkBucket)
		stats := pb.Stats()
		numKeys := stats.KeyN
		fmt.Fprintf(os.Stderr, "Scanning %d permalinks\n", numKeys)
		reportEvery := numKeys / 100
		i := 0
		return pb.ForEach(func(key []byte, value []byte) error {
			ce := cacheEntry(value)
			if ce.status() != statusSuccess {
				// ignore unsuccessful cache entries
				return nil
			}
			pli := &permalinkInfo{}
			gzr, gzErr := gzip.NewReader(bytes.NewReader(ce.data()))
			if gzErr != nil {
				return errors.New("Unable to decompress cached data: %v", gzErr)
			}
			dec := json.NewDecoder(gzr)
			parseErr := dec.Decode(pli)
			if parseErr != nil {
				return errors.New("Unable to parse cached data: %v", parseErr)
			}
			permalinks = append(permalinks, &PermalinkInfo{
				Permalink: pli.Permalink,
				SQL:       pli.SQL,
				Timestamp: encoding.TimeFromInt(pli.TS * 1000000),
				NumRows:   len(pli.Rows),
			})
			if i > 0 && i%reportEvery == 0 {
				fmt.Fprint(os.Stderr, ".")
			}
			i++
			return nil
		})
	})
	return permalinks, err
}
