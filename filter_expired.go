package tdb

// filterExpired is a gorocksdb.CompactionFilter that removes expired values.
type filterExpired struct {
	t *table
}

func (f *filterExpired) Filter(level int, key, val []byte) (remove bool, newVal []byte) {
	if len(val) < width64bits {
		return false, nil
	}
	retainUntil := f.t.clock.Now().Add(-1 * f.t.retentionPeriod)
	seq := sequence(val)
	return seq.start().Before(retainUntil), nil
}

func (f *filterExpired) Name() string {
	return "filter_expired"
}
