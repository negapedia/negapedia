package preprocessor

import (
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"io"
	"os/exec"
	"runtime"
	"sort"
	"sync"

	"github.com/pkg/errors"

	"github.com/ebonetti/similgraph"
)

type multiEdge struct {
	VertexA   uint32
	VerticesB map[uint32]float64
}

type vertexLinks struct {
	From uint32
	To   []uint32
}

var _BufferSize = 10 * runtime.NumCPU()

func (p preprocessor) bi2Similgraph(ctx context.Context, in <-chan multiEdge) <-chan vertexLinks {
	vertexLinksChan := make(chan vertexLinks, _BufferSize)
	go func() {
		defer close(vertexLinksChan)
		g, new2OldID, err := p.newSimilgraph(ctx, in)
		if err != nil {
			p.Fail(err)
			return
		}

		pageIDsChan := make(chan uint32, _BufferSize)
		go func() { //Page ID producer
			defer close(pageIDsChan)
			for pageID := uint32(0); pageID < g.VertexCount(); pageID++ {
				pageIDsChan <- pageID
			}
		}()

		wg := sync.WaitGroup{}
		for workers := 0; workers < cap(pageIDsChan); workers++ {
			wg.Add(1)
			go func() { //Page ID consumers
				defer wg.Done()

				buffer := make([]similgraph.Edge, 10)
				for v := range pageIDsChan {
					itsm, itbg, err := g.EdgeIterator(v)
					if err != nil {
						p.Fail(err)
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
			}()
		}
		wg.Wait()
	}()
	return vertexLinksChan
}

func (p preprocessor) newSimilgraph(ctx context.Context, in <-chan multiEdge) (g *similgraph.SimilGraph, newoldVertexA []uint32, err error) {
	pageCount, users2PageCount := 0, map[uint32]int{}
	bigraphChan := make(chan similgraph.Edge, _BufferSize)
	sortedBigraphChan := p.sortEdges(ctx, bigraphChan)

	for me := range in {
		for UserID, Weight := range me.VerticesB {
			select {
			case bigraphChan <- similgraph.Edge{me.VertexA, UserID, float32(Weight)}:
				//proceed
			case <-ctx.Done():
				err = ctx.Err() //no need to close bigraphChan, it will sense ctx.Done itself
				return
			}
			users2PageCount[UserID]++
		}
		pageCount++
	}
	close(bigraphChan)

	if len(users2PageCount) == 0 {
		err = p.Fail(errors.New("Empty input graph"))
		return
	}

	//filtering users with too many pages, as they slow down computation beyond repair :(
	percentileFilter(users2PageCount, 0.997)

	//count unique edges
	edgeCount := 0
	for _, w := range users2PageCount {
		edgeCount += w
	}

	return similgraph.New(func() (e similgraph.Edge, ok bool) {
		for e = range sortedBigraphChan {
			if _, ok = users2PageCount[e.VertexB]; ok {
				break
			}
		}
		return
	}, pageCount, len(users2PageCount), edgeCount)
}

func (p preprocessor) sortEdges(ctx context.Context, edges <-chan similgraph.Edge) <-chan similgraph.Edge {
	result := make(chan similgraph.Edge, _BufferSize)
	go func() {
		defer close(result)
		cmd := exec.CommandContext(ctx, "sort", "-n", "-k", "1,1", "-k", "2,2", "-S", "10%", "-T", p.TmpDir)
		cmd.Dir = p.TmpDir
		var cmdStderr bytes.Buffer
		cmd.Stderr = &cmdStderr

		stdin, errin := cmd.StdinPipe()
		stdout, err := cmd.StdoutPipe()
		switch {
		case errin != nil:
			err = errin
			fallthrough
		case err != nil:
			p.Fail(errors.Wrap(err, "Error opening sort pipe"))
			return
		}

		go func() {
			defer stdin.Close()
			for e := range edges {
				_, err := fmt.Fprintln(stdin, e.VertexA, e.VertexB, e.Weight)
				if err != nil {
					p.Fail(errors.Wrap(err, "Error while inputting bigraph to sort"))
					return
				}
			}
		}()

		if err := cmd.Start(); err != nil {
			p.Fail(errors.Wrap(err, "Error while starting sort"))
			return
		}

		next := func() (e similgraph.Edge, ok bool) {
			_, err = fmt.Fscanln(stdout, &e.VertexA, &e.VertexB, &e.Weight)
			switch err {
			case nil:
				ok = true
			case io.EOF:
				err = nil
			default:
				p.Fail(errors.Wrap(err, "Error while fetching next edge after sort, with the following error stream:\n"+cmdStderr.String()))
			}
			return
		}

		for e, ok := next(); ok; e, ok = next() {
			select {
			case result <- e:
				//proceed
			case <-ctx.Done():
				break
			}
		}

		if err1 := cmd.Wait(); err == nil && err1 != nil {
			p.Fail(errors.Wrap(err1, "Error while waiting for sort end, with the following error stream:\n"+cmdStderr.String()))
		}
	}()
	return result
}

func percentileFilter(users2PageCount map[uint32]int, percentile float64) {
	usersWeights := make([]int, 0, len(users2PageCount))
	for _, w := range users2PageCount {
		usersWeights = append(usersWeights, w)
	}
	sort.Ints(usersWeights)
	percentileCut := usersWeights[int(percentile*float64(len(usersWeights)))]
	for u, w := range users2PageCount {
		if w < 2 || percentileCut < w {
			delete(users2PageCount, u)
		}
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
