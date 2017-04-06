package web

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCache(t *testing.T) {
	ttl := 250 * time.Millisecond

	cacheDir, err := ioutil.TempDir("", "")
	if !assert.NoError(t, err) {
		return
	}
	defer os.RemoveAll(cacheDir)

	cache, err := newCache(cacheDir, 250*time.Millisecond)
	if !assert.NoError(t, err) {
		return
	}
	defer cache.Close()

	ce, created, err := cache.getOrBegin("a")
	if !assert.NoError(t, err) {
		return
	}
	assert.Empty(t, ce.data())
	assert.True(t, created)
	assert.EqualValues(t, statusPending, ce.status())
	assert.NotEmpty(t, ce.permalink())

	permalink := ce.permalink()
	ce, err = cache.getByPermalink(permalink)
	if !assert.NoError(t, err) {
		return
	}
	assert.Empty(t, ce.data())
	assert.EqualValues(t, statusPending, ce.status())
	assert.NotEmpty(t, ce.permalink())

	ce, created, err = cache.getOrBegin("a")
	if !assert.NoError(t, err) {
		return
	}
	assert.False(t, created)
	assert.Empty(t, ce.data())
	assert.EqualValues(t, statusPending, ce.status())
	assert.NotEmpty(t, ce.permalink())

	ce = ce.succeed([]byte("1"))
	err = cache.put("a", ce)
	if !assert.NoError(t, err) {
		return
	}

	ce, created, err = cache.getOrBegin("a")
	if !assert.NoError(t, err) {
		return
	}
	assert.False(t, created)
	assert.EqualValues(t, []byte("1"), ce.data())
	assert.EqualValues(t, statusSuccess, ce.status())

	time.Sleep(ttl)
	ce, created, err = cache.getOrBegin("a")
	if !assert.NoError(t, err) {
		return
	}
	assert.True(t, created)
	assert.Empty(t, ce.data())
	assert.EqualValues(t, statusPending, ce.status())
	assert.NotEqual(t, string(permalink), string(ce.permalink()))

	ce, err = cache.getByPermalink(permalink)
	if !assert.NoError(t, err) {
		return
	}
	assert.EqualValues(t, []byte("1"), ce.data())
	assert.EqualValues(t, statusSuccess, ce.status())
}
