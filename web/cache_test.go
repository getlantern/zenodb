package web

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestCache(t *testing.T) {
	expiration := 100 * time.Millisecond
	c := newCache(expiration, 5)

	assertContents := func(expected map[string]string) {
		for sql, data := range expected {
			assert.Equal(t, data, string(c.get(sql)), "Get did not return expected value")
		}
	}

	assertOrder := func(expected []string) {
		// Iterate forward
		current := c.oldest
		for _, data := range expected {
			if !assert.NotNil(t, current, "Not enough values, stopped at %v", data) {
				break
			}
			assert.Equal(t, data, string(current.data), "Value out of order")
			current = current.next
		}
		assert.Nil(t, current, "More values than expected")

		// Iterate reverse
		current = c.newest
		for i := len(expected) - 1; i >= 0; i-- {
			data := expected[i]
			if !assert.NotNil(t, current, "Not enough values, stopped at %v", data) {
				break
			}
			assert.Equal(t, data, string(current.data), "Value out of order")
			current = current.prior
		}
		assert.Nil(t, current, "More values than expected")
	}

	for _, data := range []string{"a", "b", "c", "d", "e"} {
		c.put(data, []byte(data))
	}

	assertContents(map[string]string{
		"a": "a",
		"b": "b",
		"c": "c",
		"d": "d",
		"e": "e",
	})
	assertOrder([]string{"a", "b", "c", "d", "e"})

	// Now replace a middle one with uppercase
	c.put("c", []byte("C"))

	assertContents(map[string]string{
		"a": "a",
		"b": "b",
		"c": "C",
		"d": "d",
		"e": "e",
	})
	assertOrder([]string{"a", "b", "d", "e", "C"})

	// Now replace the oldest with uppercase
	c.put("a", []byte("A"))

	assertContents(map[string]string{
		"a": "A",
		"b": "b",
		"c": "C",
		"d": "d",
		"e": "e",
	})
	assertOrder([]string{"b", "d", "e", "C", "A"})

	// Now replace the newest with something totally different
	c.put("a", []byte("X"))

	assertContents(map[string]string{
		"a": "X",
		"b": "b",
		"c": "C",
		"d": "d",
		"e": "e",
	})
	assertOrder([]string{"b", "d", "e", "C", "X"})

	// Now add a new key
	c.put("f", []byte("f"))
	// And a value that's too large to cache
	c.put("g", []byte("abcdef"))

	assertContents(map[string]string{
		"a": "X",
		"c": "C",
		"d": "d",
		"e": "e",
		"f": "f",
	})
	assertOrder([]string{"d", "e", "C", "X", "f"})

	// Now add a new value that takes up whole cache
	c.put("g", []byte("abcde"))

	assertContents(map[string]string{
		"g": "abcde",
	})
	assertOrder([]string{"abcde"})

	// Now add a new small value
	c.put("h", []byte("h"))

	assertContents(map[string]string{
		"h": "h",
	})
	assertOrder([]string{"h"})

	// Wait for values to expire
	time.Sleep(expiration * 2)

	assertContents(map[string]string{
		"h": "",
	})
	assertOrder([]string{"h"})

	// Add a new value and make sure that expired get purged
	c.put("i", []byte("i"))
	assertContents(map[string]string{
		"h": "",
		"i": "i",
	})
	assertOrder([]string{"i"})
}
