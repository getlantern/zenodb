package zenodb

import (
	"io/ioutil"
	"os"

	"github.com/getlantern/errors"
	"github.com/getlantern/wal"
)

var (
	emptyOffset = make(wal.Offset, wal.OffsetSize)
)

// Merge merges the specified inFiles into the given outFile, where inFiles and
// outFiles are all valid filestore files. The schema is based on the named
// table. If whereClause is specified, rows are filtered by comparing them to
// the whereClause. The merge is performed as a disk-based merge in order to use
// minimal memory.
func (db *DB) Merge(table string, whereClause string, outFile string, inFiles ...string) error {
	t := db.getTable(table)
	if t == nil {
		return errors.New("Table %v not found", table)
	}
	// TODO: add whereClause to table if necessary
	return t.merge(outFile, inFiles)
}

func (t *table) merge(outFile string, inFiles []string) error {
	sortedInFiles := make([]string, 0, len(inFiles))
	defer func() {
		// remove temporary files
		for _, inFile := range sortedInFiles {
			os.Remove(inFile)
		}
	}()

	// Filter and sort
	for _, inFile := range inFiles {
		sortedInFile, err := ioutil.TempFile("", "sortedInFile")
		if err != nil {
			return errors.New("Unable to create temporary sortedInFile: %v", err)
		}
		sortedInFiles = append(sortedInFiles, sortedInFile.Name())
		fs := &fileStore{
			t:        t,
			fields:   t.fields,
			filename: inFile,
		}
		fs.flush(sortedInFile, t.fields, t.Where, emptyOffset, nil, true, true)
		err = sortedInFile.Close()
		if err != nil {
			return errors.New("Unable to finalize sorted in file: %v", err)
		}
	}

	// TODO: merge

	return nil
}
