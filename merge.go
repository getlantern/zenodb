package zenodb

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/golang/snappy"

	"github.com/getlantern/errors"
	"github.com/getlantern/goexpr"
	"github.com/getlantern/wal"

	"github.com/getlantern/zenodb/common"
	"github.com/getlantern/zenodb/core"
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

// FileInfo returns information about the given data file
func FileInfo(inFile string) (offsetsBySource common.OffsetsBySource, fieldsString string, fields core.Fields, err error) {
	fs := &fileStore{
		filename: inFile,
	}
	file, err := os.OpenFile(fs.filename, os.O_RDONLY, 0)
	if err != nil {
		err = errors.New("Unable to open filestore at %v: %v", fs.filename, err)
		return
	}
	defer file.Close()
	r := snappy.NewReader(file)
	return fs.info(r)
}

// Check checks all of the given inFiles for readability and returns errors
// for all files that are in error.
func Check(inFiles ...string) map[string]error {
	errors := make(map[string]error)
	for _, inFile := range inFiles {
		fs := &fileStore{
			filename: inFile,
		}
		file, err := os.OpenFile(fs.filename, os.O_RDONLY, 0)
		if err != nil {
			errors[inFile] = fmt.Errorf("Unable to open filestore at %v: %v", fs.filename, err)
			continue
		}
		defer file.Close()
		r := snappy.NewReader(file)
		_, _, _, err = fs.info(r)
		if err != nil {
			errors[inFile] = err
			continue
		}
		_, err = io.Copy(ioutil.Discard, r)
		if err != nil {
			errors[inFile] = err
		}
	}
	return errors
}

func (t *table) filterAndMerge(whereClause string, shouldSort bool, outFile string, inFiles []string) error {
	// TODO: make this work with multi-source offsets
	// okayToReuseBuffers := false
	// rawOkay := false

	// filter, err := whereFor(whereClause)
	// if err != nil {
	// 	return err
	// }

	// // Find highest offset amongst all infiles
	// var offset wal.Offset
	// for _, inFile := range inFiles {
	// 	nextOffset, _, offsetErr := readWALOffset(inFile)
	// 	if offsetErr != nil {
	// 		return errors.New("Unable to read WAL offset from %v: %v", inFile, offsetErr)
	// 	}
	// 	if nextOffset.After(offset) {
	// 		offset = nextOffset
	// 	}
	// }

	// // Create output file
	// out, err := os.OpenFile(outFile, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	// if err != nil {
	// 	return errors.New("Unable to create outFile at %v: %v", outFile, err)
	// }
	// defer out.Close()

	// fso := &fileStore{
	// 	t:      t,
	// 	fields: t.fields,
	// }
	// cout, err := fso.createOutWriter(out, t.fields, offset, shouldSort)
	// if err != nil {
	// 	return errors.New("Unable to create out writer for %v: %v", outFile, err)
	// }
	// defer cout.Close()

	// truncateBefore := t.truncateBefore()
	// for _, inFile := range inFiles {
	// 	fs := &fileStore{
	// 		t:        t,
	// 		fields:   t.fields,
	// 		filename: inFile,
	// 	}
	// 	_, err = fs.iterate(t.fields, nil, okayToReuseBuffers, rawOkay, func(key bytemap.ByteMap, columns []encoding.Sequence, raw []byte) (bool, error) {
	// 		_, writeErr := fs.doWrite(cout, t.fields, filter, truncateBefore, shouldSort, key, columns, raw)
	// 		return true, writeErr
	// 	})
	// 	if err != nil {
	// 		return errors.New("Error iterating on %v: %v", inFile, err)
	// 	}
	// }

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
