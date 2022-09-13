package graph

type DiEdgeWeight struct {
	from   int
	to     int
	weight int
}

func (a DiEdgeWeight) Compare(b DiEdgeWeight) bool {
	return a.weight > b.weight
}

type DiEdgeWeightedGraph struct {
	v   int
	e   int
	adj [][]DiEdgeWeight
}

func NewDiEdgeWeightedGraph(v int) *DiEdgeWeightedGraph {
	adj := make([][]DiEdgeWeight, v)
	for idx := range adj {
		adj[idx] = make([]DiEdgeWeight, 0)
	}

	return &DiEdgeWeightedGraph{
		v:   v,
		adj: adj,
	}
}

func (dg *DiEdgeWeightedGraph) AddEdge(edge DiEdgeWeight) {
	dg.adj[edge.from] = append(dg.adj[edge.from], edge)
	dg.e++
}

func (dg *DiEdgeWeightedGraph) Adj() [][]DiEdgeWeight {
	return dg.adj
}
