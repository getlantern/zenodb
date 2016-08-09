package zenodb

import (
	"sync"
	"time"

	"github.com/getlantern/bytemap"
)

// see https://en.wikipedia.org/wiki/Radix_tree
type tree struct {
	root    *node
	_bytes  int
	_length int
	mx      sync.RWMutex
}

type node struct {
	key        []byte
	edges      edges
	data       []sequence
	removedFor []int64
}

type edge struct {
	label  []byte
	target *node
}

func newByteTree() *tree {
	return &tree{root: &node{}}
}

func (bt *tree) bytes() int {
	return bt._bytes
}

func (bt *tree) length() int {
	return bt._length
}

func (bt *tree) walk(ctx int64, fn func(key []byte, data []sequence) bool) {
	nodes := make([]*node, 0, bt._length)
	nodes = append(nodes, bt.root)
	for {
		if len(nodes) == 0 {
			break
		}
		n := nodes[0]
		nodes = nodes[1:]
		if n.data != nil {
			alreadyRemoved := n.wasRemovedFor(bt, ctx)
			if !alreadyRemoved {
				keep := fn(n.key, n.data)
				if !keep {
					n.doRemoveFor(bt, ctx)
				}
			}
		}
		for _, e := range n.edges {
			nodes = append(nodes, e.target)
		}
	}
}

func (bt *tree) remove(ctx int64, fullKey []byte) []sequence {
	// TODO: basic shape of this is very similar to update, dry violation
	n := bt.root
	key := fullKey
	// Try to update on existing edge
nodeLoop:
	for {
		for _, edge := range n.edges {
			labelLength := len(edge.label)
			keyLength := len(key)
			i := 0
			for ; i < keyLength && i < labelLength; i++ {
				if edge.label[i] != key[i] {
					break
				}
			}
			if i == keyLength && keyLength == labelLength {
				// found it
				alreadyRemoved := edge.target.wasRemovedFor(bt, ctx)
				if alreadyRemoved {
					return nil
				}
				edge.target.doRemoveFor(bt, ctx)
				return edge.target.data
			} else if i == labelLength && labelLength < keyLength {
				// descend
				n = edge.target
				key = key[labelLength:]
				continue nodeLoop
			}
		}

		// not found
		return nil
	}
}

func (bt *tree) copy() *tree {
	cp := &tree{_bytes: bt._bytes, _length: bt._length, root: &node{}}
	nodes := make([]*node, 0, bt.length())
	nodeCopies := make([]*node, 0, bt.length())
	nodes = append(nodes, bt.root)
	nodeCopies = append(nodeCopies, cp.root)

	for {
		if len(nodes) == 0 {
			break
		}
		n := nodes[0]
		cpn := nodeCopies[0]
		nodes = nodes[1:]
		nodeCopies = nodeCopies[1:]
		for _, e := range n.edges {
			cpt := &node{key: e.target.key, data: e.target.data}
			cpn.edges = append(cpn.edges, &edge{label: e.label, target: cpt})
			nodes = append(nodes, e.target)
			nodeCopies = append(nodeCopies, cpt)
		}
	}

	return cp
}

func (bt *tree) update(t *table, truncateBefore time.Time, key []byte, vals tsparams) int {
	bytesAdded, newNode := bt.doUpdate(t, truncateBefore, key, vals)
	bt._bytes += bytesAdded
	if newNode {
		bt._length++
	}
	return bytesAdded
}

func (bt *tree) doUpdate(t *table, truncateBefore time.Time, fullKey []byte, vals tsparams) (int, bool) {
	n := bt.root
	key := fullKey
	// Try to update on existing edge
nodeLoop:
	for {
		for _, edge := range n.edges {
			labelLength := len(edge.label)
			keyLength := len(key)
			i := 0
			for ; i < keyLength && i < labelLength; i++ {
				if edge.label[i] != key[i] {
					break
				}
			}
			if i == keyLength && keyLength == labelLength {
				// update existing node
				return edge.target.doUpdate(t, truncateBefore, fullKey, vals), false
			} else if i == labelLength && labelLength < keyLength {
				// descend
				n = edge.target
				key = key[labelLength:]
				continue nodeLoop
			} else if i > 0 {
				// common substring, split on that
				return edge.split(bt, t, truncateBefore, i, fullKey, key, vals), true
			}
		}

		// Create new edge
		target := &node{key: fullKey}
		n.edges = append(n.edges, &edge{key, target})
		return target.doUpdate(t, truncateBefore, fullKey, vals) + len(key), true
	}
}

func (n *node) doUpdate(t *table, truncateBefore time.Time, fullKey []byte, vals tsparams) int {
	bytesAdded := 0
	// Grow sequences to match number of fields in table
	for i := len(n.data); i < len(t.Fields); i++ {
		n.data = append(n.data, nil)
	}
	metadata := bytemap.ByteMap(fullKey)
	for i, field := range t.Fields {
		current := n.data[i]
		previousSize := len(current)
		updated := current.update(vals, metadata, field.Expr, t.Resolution, truncateBefore)
		n.data[i] = updated
		bytesAdded += len(updated) - previousSize
	}
	return bytesAdded
}

func (n *node) wasRemovedFor(bt *tree, ctx int64) bool {
	if ctx == 0 {
		return false
	}
	bt.mx.RLock()
	for _, _ctx := range n.removedFor {
		if _ctx == ctx {
			bt.mx.RUnlock()
			return true
		}
	}
	bt.mx.RUnlock()
	return false
}

func (n *node) doRemoveFor(bt *tree, ctx int64) {
	if ctx == 0 {
		return
	}
	bt.mx.Lock()
	n.removedFor = append(n.removedFor, ctx)
	bt.mx.Unlock()
}

func (e *edge) split(bt *tree, t *table, truncateBefore time.Time, splitOn int, fullKey []byte, key []byte, vals tsparams) int {
	newNode := &node{edges: edges{&edge{e.label[splitOn:], e.target}}}
	newLeaf := newNode
	if splitOn != len(key) {
		newLeaf = &node{key: fullKey}
		newNode.edges = append(newNode.edges, &edge{key[splitOn:], newLeaf})
	}
	e.label = e.label[:splitOn]
	e.target = newNode
	return len(key) - splitOn + newLeaf.doUpdate(t, truncateBefore, fullKey, vals)
}

type edges []*edge
