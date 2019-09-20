package exporter

import (
	"container/heap"
)

type extdataLooseLesser interface{ Less(x interface{}) bool }
type extdataIterMerge struct{ h extdataIterHeap }

func extdataIterMergeFrom(nexts ...func() (info ExtData, ok bool)) *extdataIterMerge {
	h := make(extdataIterHeap, 0, len(nexts))
	for _, next := range nexts {
		if info, ok := next(); ok {
			h = append(h, extdataIterator{info, next})
		}
	}
	heap.Init(&h)
	return &extdataIterMerge{h}
}
func (m *extdataIterMerge) Push(next func() (ExtData, bool)) {
	heap.Push(&m.h, next)
}
func (m extdataIterMerge) Peek() (info ExtData, ok bool) {
	if len(m.h) > 0 {
		info, ok = m.h[0].Info, true
	}
	return
}
func (m *extdataIterMerge) Next() (info ExtData, ok bool) {
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

type extdataIterHeap []extdataIterator
type extdataIterator struct {
	Info ExtData
	Next func() (info ExtData, ok bool)
}

func (h extdataIterHeap) Len() int {
	return len(h)
}
func (h extdataIterHeap) Less(i, j int) bool {
	return h[i].Info.Less(h[j].Info)
}
func (h extdataIterHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}
func (h *extdataIterHeap) Push(x interface{}) {
	next := x.(func() (ExtData, bool))
	if info, ok := next(); ok {
		*h = append(*h, extdataIterator{info, next})
	}
}
func (h *extdataIterHeap) Pop() interface{} {
	_h := *h
	n := len(_h)
	x := _h[n-1]
	_h[n-1] = extdataIterator{}
	*h = _h[0 : n-1]
	return x
}
