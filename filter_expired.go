package tdb

// filterExpired is a gorocksdb.CompactionFilter that removes expired values.
// TODO - make sure that we don't end up with too much old stuff that gets stuck
// in lower levels of the SST because those files aren't being compacted
// anymore.
type filterExpired struct {
	t *table
}

func (f *filterExpired) Filter(level int, key, val []byte) (bool, []byte) {
	if len(val) < width64bits {
		return false, nil
	}
	seq := sequence(val)
	remove := seq.start().Before(f.t.truncateBefore())
	if remove {
		f.t.statsMutex.Lock()
		f.t.stats.ExpiredValues++
		f.t.statsMutex.Unlock()
	}
	return remove, nil
}

func (f *filterExpired) Name() string {
	return "filter_expired"
}
