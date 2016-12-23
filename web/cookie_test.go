package web

import (
	"crypto/rand"
	"github.com/gorilla/securecookie"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPendingAuthCookie(t *testing.T) {
	hashKey := make([]byte, 64)
	blockKey := make([]byte, 32)

	log.Debugf("Generating random hash key")
	_, err := rand.Read(hashKey)
	if !assert.NoError(t, err) {
		return
	}

	log.Debugf("Generating random block key")
	_, err = rand.Read(blockKey)
	if !assert.NoError(t, err) {
		return
	}

	sc := securecookie.New(hashKey, blockKey)

	randomKey := make([]byte, randomKeyLength)
	rand.Read(randomKey)

	encoded, err := sc.Encode(authcookie, randomKey)
	if !assert.NoError(t, err) {
		return
	}

	randomKeyRT := make([]byte, randomKeyLength)
	err = sc.Decode(authcookie, encoded, &randomKeyRT)
	if !assert.NoError(t, err) {
		return
	}

	assert.Equal(t, string(randomKey), string(randomKeyRT))
}
