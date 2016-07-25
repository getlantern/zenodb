package tdb

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/getlantern/tdb/expr"
	"github.com/stretchr/testify/assert"
)

func TestStorage(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "tdbtest")
	if !assert.NoError(t, err, "Unable to create temp directory") {
		return
	}
	defer os.RemoveAll(tmpDir)

	cs, err := openColumnStore(&columnStoreOptions{
		dir:              tmpDir,
		ex:               expr.SUM("i"),
		resolution:       1 * time.Millisecond,
		maxMemStoreBytes: 1,
	})
	if !assert.NoError(t, err) {
		return
	}

	if false {
		cs.insert(&insert{})
	}
}
