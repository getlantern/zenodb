package tdb

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStorage(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "tdbtest")
	if !assert.NoError(t, err, "Unable to create temp directory") {
		return
	}
	defer os.RemoveAll(tmpDir)

	tb := &table{}
	cs, err := tb.openRowStore(&rowStoreOptions{
		dir:              tmpDir,
		maxMemStoreBytes: 1,
	})
	if !assert.NoError(t, err) {
		return
	}

	if false {
		cs.insert(&insert{})
	}
}
