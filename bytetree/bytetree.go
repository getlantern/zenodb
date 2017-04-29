// Package bytetree provides a Radix tree that stores []byte keys and values.
// See https://en.wikipedia.org/wiki/Radix_tree
package bytetree

import (
	"github.com/getlantern/bytemap"
	"github.com/getlantern/zenodb/encoding"
	"github.com/getlantern/zenodb/expr"
	"sync"
	"time"
)

type Tree struct {
	outExprs      []expr.Expr
	inExprs       []expr.Expr
	subMergers    [][]expr.SubMerge
	outResolution time.Duration
	inResolution  time.Duration
	asOf          time.Time
	until         time.Time
	strideSlice   time.Duration
	root          *node
	bytes         int
	length        int
	mx            sync.RWMutex
}

type node struct {
	key        []byte
	edges      edges
	data       []encoding.Sequence
	removedFor []int64
}

type edge struct {
	label  []byte
	target *node
}

// New constructs a new Tree.
func New(
	outExprs []expr.Expr,
	inExprs []expr.Expr,
	outResolution time.Duration,
	inResolution time.Duration,
	asOf time.Time,
	until time.Time,
	strideSlice time.Duration,
) *Tree {
	var subMergers [][]expr.SubMerge
	for _, o := range outExprs {
		subMergers = append(subMergers, o.SubMergers(inExprs))
	}
	return &Tree{
		outExprs:      outExprs,
		inExprs:       inExprs,
		subMergers:    subMergers,
		outResolution: outResolution,
		inResolution:  inResolution,
		asOf:          asOf,
		until:         until,
		strideSlice:   strideSlice,
		root:          &node{},
	}
}

// Bytes returns an estimate of the number of bytes stored in this Tree.
func (bt *Tree) Bytes() int {
	return bt.bytes * 2
}

// Length returns the number of nodes in this Tree.
func (bt *Tree) Length() int {
	return bt.length
}

// Walk walks this Tree, calling the given fn with each node's key and data. If
// the fn returns false, the node will be removed from the Tree as viewed with
// the given ctx. Subsequent walks of the Tree using that same ctx will not see
// removed nodes, but walks using a different context will still see them.
func (bt *Tree) Walk(ctx int64, fn func(key []byte, data []encoding.Sequence) (more bool, keep bool, err error)) error {
	nodes := make([]*node, 0, bt.length)
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
				more, keep, err := fn(n.key, n.data)
				if !keep {
					n.doRemoveFor(bt, ctx)
				}
				if !more || err != nil {
					return err
				}
			}
		}
		for _, e := range n.edges {
			nodes = append(nodes, e.target)
		}
	}

	return nil
}

// Remove removes the given key from this Tree under the given ctx. When viewed
// from this ctx, the key will appear to be gone, but from other contexts it
// will remain visible.
func (bt *Tree) Remove(ctx int64, fullKey []byte) []encoding.Sequence {
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

// Copy makes a copy of this Tree.
func (bt *Tree) Copy() *Tree {
	cp := &Tree{bytes: bt.bytes, length: bt.length, root: &node{}}
	nodes := make([]*node, 0, bt.Length())
	nodeCopies := make([]*node, 0, bt.Length())
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

// Update updates all of the fields at the given timestamp with the given
// parameters.
func (bt *Tree) Update(key []byte, vals []encoding.Sequence, params encoding.TSParams, metadata bytemap.ByteMap) int {
	bytesAdded, newNode := bt.doUpdate(key, vals, params, metadata)
	bt.bytes += bytesAdded
	if newNode {
		bt.length++
	}
	return bytesAdded
}

func (bt *Tree) doUpdate(fullKey []byte, vals []encoding.Sequence, params encoding.TSParams, metadata bytemap.ByteMap) (int, bool) {
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
				return edge.target.doUpdate(bt, fullKey, vals, params, metadata), false
			} else if i == labelLength && labelLength < keyLength {
				// descend
				n = edge.target
				key = key[labelLength:]
				continue nodeLoop
			} else if i > 0 {
				// common substring, split on that
				return edge.split(bt, i, fullKey, key, vals, params, metadata), true
			}
		}

		// Create new edge
		target := &node{key: fullKey}
		n.edges = append(n.edges, &edge{key, target})
		return target.doUpdate(bt, fullKey, vals, params, metadata) + len(key), true
	}
}

func (n *node) doUpdate(bt *Tree, fullKey []byte, vals []encoding.Sequence, params encoding.TSParams, metadata bytemap.ByteMap) int {
	if n.data == nil {
		n.data = make([]encoding.Sequence, len(bt.outExprs))
	}
	bytesAdded := 0
	if params != nil {
		for o, ex := range bt.outExprs {
			current := n.data[o]
			previousSize := cap(current)
			updated := current.Update(params, metadata, ex, bt.outResolution, bt.asOf)
			n.data[o] = updated
			bytesAdded += cap(updated) - previousSize
		}
	} else {
		for o, subMergers := range bt.subMergers {
			out := n.data[o]
			outEx := bt.outExprs[o]
			for i, submerge := range subMergers {
				if submerge == nil {
					continue
				}
				in := vals[i]
				inEx := bt.inExprs[i]
				previousSize := cap(out)
				out = out.SubMerge(in, metadata, bt.outResolution, bt.inResolution, outEx, inEx, submerge, bt.asOf, bt.until, bt.strideSlice)
				n.data[o] = out
				bytesAdded += cap(out) - previousSize
			}
		}
	}
	return bytesAdded
}

func (n *node) wasRemovedFor(bt *Tree, ctx int64) bool {
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

func (n *node) doRemoveFor(bt *Tree, ctx int64) {
	if ctx == 0 {
		return
	}
	bt.mx.Lock()
	n.removedFor = append(n.removedFor, ctx)
	bt.mx.Unlock()
}

func (e *edge) split(bt *Tree, splitOn int, fullKey []byte, key []byte, vals []encoding.Sequence, params encoding.TSParams, metadata bytemap.ByteMap) int {
	newNode := &node{edges: edges{&edge{e.label[splitOn:], e.target}}}
	newLeaf := newNode
	if splitOn != len(key) {
		newLeaf = &node{key: fullKey}
		newNode.edges = append(newNode.edges, &edge{key[splitOn:], newLeaf})
	}
	e.label = e.label[:splitOn]
	e.target = newNode
	return len(key) - splitOn + newLeaf.doUpdate(bt, fullKey, vals, params, metadata)
}

type edges []*edge
