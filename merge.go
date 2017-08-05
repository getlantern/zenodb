package zenodb

import (
	"fmt"
	"os"

	"github.com/getlantern/bytemap"
	"github.com/getlantern/errors"
	"github.com/getlantern/goexpr"
	"github.com/getlantern/wal"
	"github.com/getlantern/zenodb/encoding"
	"github.com/getlantern/zenodb/sql"
)

var (
	emptyOffset = make(wal.Offset, wal.OffsetSize)
)

// FilterAndMerge merges the specified inFiles into the given outFile, where
// inFiles and outFiles are all valid filestore files. The schema is based on
// the named table. If whereClause is specified, rows are filtered by comparing
// them to the whereClause. The merge is performed as a disk-based merge in
// order to use minimal memory. If shouldSort is true, the output will be sorted
// by key.
func (db *DB) FilterAndMerge(table string, whereClause string, shouldSort bool, outFile string, inFiles ...string) error {
	t := db.getTable(table)
	if t == nil {
		return errors.New("Table %v not found", table)
	}
	return t.filterAndMerge(whereClause, shouldSort, outFile, inFiles)
}

func (t *table) filterAndMerge(whereClause string, shouldSort bool, outFile string, inFiles []string) error {
	okayToReuseBuffers := false
	rawOkay := false

	filter, err := whereFor(whereClause)
	if err != nil {
		return err
	}

	// Find highest offset amongst all infiles
	var offset wal.Offset
	for _, inFile := range inFiles {
		nextOffset, _, offsetErr := readWALOffset(inFile)
		if offsetErr != nil {
			return errors.New("Unable to read WAL offset from %v: %v", offsetErr)
		}
		if nextOffset.After(offset) {
			offset = nextOffset
		}
	}

	// Create output file
	out, err := os.OpenFile(outFile, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return errors.New("Unable to create outFile at %v: %v", outFile, err)
	}
	defer out.Close()

	fso := &fileStore{
		t:      t,
		fields: t.fields,
	}
	cout, err := fso.createOutWriter(out, t.fields, offset, shouldSort)
	if err != nil {
		return errors.New("Unable to create out writer for %v: %v", outFile, err)
	}
	defer cout.Close()

	truncateBefore := t.truncateBefore()
	for _, inFile := range inFiles {
		fs := &fileStore{
			t:        t,
			fields:   t.fields,
			filename: inFile,
		}
		err = fs.iterate(t.fields, nil, okayToReuseBuffers, rawOkay, func(key bytemap.ByteMap, columns []encoding.Sequence, raw []byte) (bool, error) {
			_, writeErr := fs.doWrite(cout, t.fields, filter, truncateBefore, shouldSort, key, columns, raw)
			return true, writeErr
		})
		if err != nil {
			return errors.New("Error iterating on %v: %v", inFile, err)
		}
	}

	return nil
}

func whereFor(whereClause string) (goexpr.Expr, error) {
	if whereClause == "" {
		return nil, nil
	}

	sqlString := fmt.Sprintf("SELECT * FROM thetable WHERE %v", whereClause)
	query, err := sql.Parse(sqlString)
	if err != nil {
		return nil, fmt.Errorf("Unable to process where clause %v: %v", whereClause, err)
	}

	return query.Where, nil
}
