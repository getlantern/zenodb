package tdb

import (
	"bytes"
	"time"
)

// see https://en.wikipedia.org/wiki/Radix_tree
type tree struct {
	root *node
}

type node struct {
	edges edges
	data  []sequence
}

type edge struct {
	label  []byte
	target *node
}

func newByteTree() *tree {
	return &tree{&node{}}
}

func (bt *tree) update(t *table, truncateBefore time.Time, key []byte, vals tsparams) int {
	return bt.root.update(t, truncateBefore, key, vals)
}

func (n *node) update(t *table, truncateBefore time.Time, key []byte, vals tsparams) int {
	// Try to update on existing edge
	for _, edge := range n.edges {
		labelLength := len(edge.label)
		keyLength := len(key)
		if labelLength < keyLength && bytes.Equal(edge.label, key[:labelLength]) {
			// descend tree
			return edge.target.update(t, truncateBefore, key[labelLength:], vals)
		} else if bytes.Equal(edge.label, key) {
			return edge.target.doUpdate(t, truncateBefore, vals)
		} else {
			i := 0
			for ; i < keyLength; i++ {
				if edge.label[i] != key[i] {
					break
				}
			}
			if i > 0 {
				// common substring, split on that
				return edge.split(t, truncateBefore, i, key, vals)
			}
		}
	}

	// Create new edge
	target := &node{}
	n.edges = append(n.edges, &edge{key, target})
	return target.doUpdate(t, truncateBefore, vals) + len(key)
}

func (n *node) doUpdate(t *table, truncateBefore time.Time, vals tsparams) int {
	bytesAdded := 0
	// Grow sequences to match number of fields in table
	for i := len(n.data); i < len(t.Fields); i++ {
		n.data = append(n.data, nil)
	}
	for i, field := range t.Fields {
		current := n.data[i]
		previousSize := len(current)
		updated := current.update(vals, field, t.Resolution, truncateBefore)
		n.data[i] = updated
		bytesAdded += len(updated) - previousSize
	}
	return bytesAdded
}

func (e *edge) split(t *table, truncateBefore time.Time, splitOn int, key []byte, vals tsparams) int {
	newNode := &node{edges: edges{&edge{e.label[splitOn:], e.target}}}
	newLeaf := newNode
	if splitOn != len(key) {
		newLeaf = &node{}
		newNode.edges = append(newNode.edges, &edge{key[splitOn:], newLeaf})
	}
	e.label = e.label[:splitOn]
	e.target = newNode
	return len(key) - splitOn + newLeaf.doUpdate(t, truncateBefore, vals)
}

type edges []*edge

func (a edges) Len() int           { return len(a) }
func (a edges) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a edges) Less(i, j int) bool { return bytes.Compare(a[i].label, a[j].label) < 0 }
