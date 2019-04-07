package main

import (
	"sort"

	"github.com/RoaringBitmap/roaring"
)

type mapGraph map[uint32][]uint32

func (g mapGraph) Add(i, j uint32) {
	exist, position := g.Exist(i, j)
	if exist {
		return
	}

	adjList := g[i]
	adjList = append(adjList, 0)
	copy(adjList[position+1:], adjList[position:])
	adjList[position] = j
	g[i] = adjList
}

func (g mapGraph) Exist(i, j uint32) (exist bool, position int) {
	exist, position = uint32Exist(g[i], j)
	return
}

func uint32Exist(a []uint32, x uint32) (exist bool, position int) {
	position = uint32Search(a, x)
	exist = position < len(a) && a[position] == x
	return
}

func uint32Search(a []uint32, x uint32) int {
	return sort.Search(len(a), func(i int) bool { return a[i] >= x })
}

func (g mapGraph) Nodes() (nodes *roaring.Bitmap) {
	nodes = roaring.NewBitmap()
	for from, to := range g {
		nodes.Add(from)
		for _, ID := range to {
			nodes.Add(ID)
		}
	}
	return
}

func (g mapGraph) NodesCount() (count int) {
	return int(g.Nodes().GetCardinality())
}

func (g mapGraph) EdgesCount() (count int) {
	for _, to := range g {
		count += len(to)
	}
	return
}

//the graph must not be changed while iterating
//it modifies whitelist, adding all subgraph nodes
func (g mapGraph) InIterator(whitelist *roaring.Bitmap) (next func() (inI *roaring.Bitmap, ok bool)) {
	greylist := g.Nodes()
	greylist.AndNot(whitelist)
	beforeOk := true
	return func() (inI *roaring.Bitmap, ok bool) {
		inI = roaring.NewBitmap()
		if !beforeOk {
			return
		}

		for i := greylist.Iterator(); i.HasNext(); {
			from := i.Next()
			to := g[from]
			for _, ID := range to {
				if whitelist.Contains(ID) {
					inI.Add(from)
					ok = true
					break
				}
			}
		}
		whitelist.Or(inI)
		greylist.AndNot(inI)
		beforeOk = ok
		return
	}
}

func (g mapGraph) InSubgraph(nodes *roaring.Bitmap, depth int) (subgraph *roaring.Bitmap) { //if depth < 0 it's interpreted as +Inf
	subgraph = nodes.Clone()
	next := g.InIterator(subgraph)
	ok := true
	switch {
	case depth > 0:
		for d := 0; d < depth && ok; d++ {
			_, ok = next()
		}
	case depth < 0: //interpreted as +Inf
		for ok {
			_, ok = next()
		}
	}
	return
}

func (g mapGraph) Distances(nodes *roaring.Bitmap) (distances map[uint32]uint32) {
	distances = make(map[uint32]uint32, g.NodesCount())
	next := g.InIterator(nodes.Clone())
	Ini, ok := nodes, true
	for depth := uint32(0); ok; depth++ {
		for i := Ini.Iterator(); i.HasNext(); {
			v := i.Next()
			distances[v] = depth
		}
		Ini, ok = next()
	}
	return
}

func (g mapGraph) DeleteOutEdges(i uint32) {
	if _, ok := g[i]; ok {
		g[i] = nil
		delete(g, i)
	}
}

func (g mapGraph) ApplySubgraph(whitelist *roaring.Bitmap) {
	for from, to := range g {
		if whitelist.Contains(from) {
			newto := to[:0]
			for _, ID := range to {
				if whitelist.Contains(ID) {
					newto = append(newto, ID)
				}
			}
			if len(newto) > 0 {
				g[from] = newto
				continue
			}
		}
		g.DeleteOutEdges(from)
	}
}

func (g mapGraph) Delete(from, to uint32) {
	exist, position := g.Exist(from, to)
	outEdges := g[from]
	switch {
	case !exist:
		//do nothing
	case len(outEdges) == 1:
		g.DeleteOutEdges(from)
	default: //delete it
		g[from] = append(outEdges[:position], outEdges[position+1:]...)
	}
}

func (g mapGraph) DeleteSelfEdges() {
	for from := range g {
		g.Delete(from, from)
	}
}
