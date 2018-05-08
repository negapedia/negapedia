package exporter

import (
	"container/heap"
)

type vfileLooseLesser interface{ Less(x interface{}) bool }
type vfileIterMerge struct{ h vfileIterHeap }

func vfileIterMergeFrom(nexts ...func() (info vFile, ok bool)) *vfileIterMerge {
	h := make(vfileIterHeap, 0, len(nexts))
	for _, next := range nexts {
		if info, ok := next(); ok {
			h = append(h, vfileIterator{info, next})
		}
	}
	heap.Init(&h)
	return &vfileIterMerge{h}
}
func (m *vfileIterMerge) Push(next func() (vFile, bool)) {
	heap.Push(&m.h, next)
}
func (m vfileIterMerge) Peek() (info vFile, ok bool) {
	if len(m.h) > 0 {
		info, ok = m.h[0].Info, true
	}
	return
}
func (m *vfileIterMerge) Next() (info vFile, ok bool) {
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

type vfileIterHeap []vfileIterator
type vfileIterator struct {
	Info vFile
	Next func() (info vFile, ok bool)
}

func (h vfileIterHeap) Len() int {
	return len(h)
}
func (h vfileIterHeap) Less(i, j int) bool {
	return h[i].Info.Less(h[j].Info)
}
func (h vfileIterHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}
func (h *vfileIterHeap) Push(x interface{}) {
	next := x.(func() (vFile, bool))
	if info, ok := next(); ok {
		*h = append(*h, vfileIterator{info, next})
	}
}
func (h *vfileIterHeap) Pop() interface{} {
	_h := *h
	n := len(_h)
	x := _h[n-1]
	_h[n-1] = vfileIterator{}
	*h = _h[0 : n-1]
	return x
}
