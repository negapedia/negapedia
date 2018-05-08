package preprocessor

import (
	"container/heap"

	"github.com/ebonetti/similgraph"
)

type edgeLooseLesser interface{ Less(x interface{}) bool }
type edgeIterMerge struct{ h edgeIterHeap }

func edgeNew(nexts ...func() (info similgraph.Edge, ok bool)) *edgeIterMerge {
	return edgeIterMergeFrom(nexts...)
}
func edgeIterMergeFrom(nexts ...func() (info similgraph.Edge, ok bool)) *edgeIterMerge {
	h := make(edgeIterHeap, 0, len(nexts))
	for _, next := range nexts {
		if info, ok := next(); ok {
			h = append(h, edgeIterator{info, next})
		}
	}
	heap.Init(&h)
	return &edgeIterMerge{h}
}
func (m *edgeIterMerge) Push(next func() (similgraph.Edge, bool)) {
	heap.Push(&m.h, next)
}
func (m edgeIterMerge) Peek() (info similgraph.Edge, ok bool) {
	if len(m.h) > 0 {
		info, ok = m.h[0].Info, true
	}
	return
}
func (m *edgeIterMerge) Next() (info similgraph.Edge, ok bool) {
	h := m.h
	if len(h) == 0 {
		return
	}
	info, ook := h[0].Next()
	h[0].Info, info = info, h[0].Info
	if ook {
		heap.Fix(&m.h, 0)
	} else {
		heap.Pop(&m.h)
	}
	return info, true
}

type edgeIterHeap []edgeIterator
type edgeIterator struct {
	Info similgraph.Edge
	Next func() (info similgraph.Edge, ok bool)
}

func (h edgeIterHeap) Len() int {
	return len(h)
}
func (h edgeIterHeap) Less(i, j int) bool {
	return h[i].Info.Less(h[j].Info)
}
func (h edgeIterHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}
func (h *edgeIterHeap) Push(x interface{}) {
	next := x.(func() (similgraph.Edge, bool))
	if info, ok := next(); ok {
		*h = append(*h, edgeIterator{info, next})
	}
}
func (h *edgeIterHeap) Pop() interface{} {
	_h := *h
	n := len(_h)
	x := _h[n-1]
	_h[n-1] = edgeIterator{}
	*h = _h[0 : n-1]
	return x
}
