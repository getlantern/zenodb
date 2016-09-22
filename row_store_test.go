package zenodb

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/getlantern/golog"
	"github.com/stretchr/testify/assert"
)

func TestStorage(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "zenodbtest")
	if !assert.NoError(t, err, "Unable to create temp directory") {
		return
	}
	defer os.RemoveAll(tmpDir)

	tb := &table{
		log: golog.LoggerFor("storagetest"),
	}
	cs, _, err := tb.openRowStore(&rowStoreOptions{
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
