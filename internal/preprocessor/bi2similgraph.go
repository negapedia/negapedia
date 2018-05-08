package preprocessor

import (
	"context"
	"sync"

	"container/heap"

	"github.com/RoaringBitmap/roaring"
	"github.com/ebonetti/similgraph"
)

//go:generate gorewrite

type multiEdge struct {
	VertexA   uint32
	VerticesB []*roaring.Bitmap
}

type vertexLinks struct {
	From uint32
	To   []uint32
}

func newBi2Similgraph(ctx context.Context, in chan multiEdge, vertexACount, vertexBCount, edgeCount int, fail func(err error) error) <-chan vertexLinks {
	vertexLinksChan := make(chan vertexLinks, 1000)
	go func() {
		defer close(vertexLinksChan)
		weighedEdgeChan := make(chan similgraph.Edge, 10000)
		new2OldID := make([]uint32, 0, vertexACount)
		go func() {
			defer close(weighedEdgeChan)
			for me := range in {
				if len(me.VerticesB) == 0 {
					continue
				}
				newID := uint32(len(new2OldID))
				new2OldID = append(new2OldID, me.VertexA)
				me.VertexA = newID
				next := iteratorFromMultiEdge(me)
				for e, ok := next(); ok; e, ok = next() {
					weighedEdgeChan <- e
				}
			}
		}()

		g, nnew2newID, err := similgraph.New(func() (e similgraph.Edge, ok bool) {
			e, ok = <-weighedEdgeChan
			return
		}, vertexACount, vertexBCount, edgeCount)
		if err != nil {
			fail(err)
			return
		}
		for nnew, new := range nnew2newID {
			old := new2OldID[new]
			nnew2newID[nnew] = old
		}
		new2OldID = nnew2newID
		nnew2newID = nil

		workers := uint32(20)
		VC := g.VertexCount()
		wg := sync.WaitGroup{}
		for v := uint32(0); v < workers; v++ {
			wg.Add(1)
			go func(v uint32) {
				defer wg.Done()

				buffer := make([]similgraph.Edge, 10)
				for ; v < VC; v += workers {
					itsm, itbg, err := g.EdgeIterator(v)
					if err != nil {
						fail(err)
						return
					}
					n := topN(buffer, concat(itsm, itbg))
					links := make([]uint32, n)
					for i, e := range buffer[:n] {
						links[i] = new2OldID[e.VertexB]
					}
					select {
					case vertexLinksChan <- vertexLinks{From: new2OldID[v], To: links}:
						//proceed
					case <-ctx.Done():
						return
					}
				}
			}(v)
		}
		wg.Wait()
	}()
	return vertexLinksChan
}

func iteratorFromMultiEdge(me multiEdge) func() (similgraph.Edge, bool) {
	nexts := make([]func() (e similgraph.Edge, ok bool), len(me.VerticesB))
	w := float32(1)
	for i, vb := range me.VerticesB {
		it := vb.Iterator()
		w *= 10
		w := w
		nexts[i] = func() (e similgraph.Edge, ok bool) {
			if !it.HasNext() {
				return
			}
			return similgraph.Edge{VertexA: me.VertexA, VertexB: it.Next(), Weight: w}, true
		}
	}
	return edgeIterMergeFrom(nexts...).Next
}

func concat(i ...func() (similgraph.Edge, bool)) func() (similgraph.Edge, bool) {
	return func() (e similgraph.Edge, ok bool) {
		for len(i) > 0 {
			e, ok = i[0]()
			if ok {
				return
			}
			i = i[1:]
		}
		return
	}
}

//topN is topN filter (based on a min-heap of WeighedEdge with limited capacity).
func topN(top []similgraph.Edge, it func() (similgraph.Edge, bool)) (n int) {
	for i := range top {
		e, ok := it()
		if !ok {
			return i
		}
		top[i] = e
	}

	if e, ok := it(); ok {
		h := weighedEdgeHeap(top)
		heap.Init(&h)
		for ; ok; e, ok = it() {
			if e.Weight > top[0].Weight {
				top[0] = e
				heap.Fix(&h, 0)
			}
		}
	}
	return len(top)
}

// An weighedEdgeHeap is a min-heap of WeighedEdge.
type weighedEdgeHeap []similgraph.Edge

func (h weighedEdgeHeap) Len() int           { return len(h) }
func (h weighedEdgeHeap) Less(i, j int) bool { return h[i].Weight < h[j].Weight }
func (h weighedEdgeHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *weighedEdgeHeap) Push(x interface{}) {
	*h = append(*h, x.(similgraph.Edge))
}

func (h *weighedEdgeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
