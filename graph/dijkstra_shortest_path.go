package graph

import "math"

type DijkstraShortestPath struct {
	// declare u, s collection, u is the shortest collection, s is the pending calculate shortest collection
	u      []int
	s      []int
	edgeTo []DiEdgeWeight // edgeTo is the shortest path
}

func (dijkstra *DijkstraShortestPath) ShortestPath(g *DiEdgeWeightedGraph, start int) {

	dijkstra.u = make([]int, g.v)
	dijkstra.s = make([]int, math.MaxInt64, g.v)
	dijkstra.edgeTo = make([]DiEdgeWeight, g.v)
	visited := make([]bool, g.v)

	// init start to start weight is 0
	dijkstra.u[start] = 0

	// init s
	edges := g.Adj()[start]
	for _, edge := range edges {
		dijkstra.s[edge.to] = edge.weight
	}

	for vertex := range g.Adj() {

		if visited[vertex] {
			continue
		}

		var (
			minWeight = math.MaxInt64
			minVertex = -1
		)

		// find the cost less vertex in the s
		for vertex, weight := range dijkstra.s {
			if !visited[vertex] && weight < minWeight {
				minWeight = weight
				minVertex = vertex
			}
		}

		visited[minVertex] = true
		dijkstra.u[minVertex] = minWeight
		// update the s collection

		for _, edge := range g.Adj()[minVertex] {
			from := edge.from
			to := edge.to
			if dijkstra.s[to] > dijkstra.s[from]+edge.weight {
				dijkstra.s[to] = dijkstra.s[from] + edge.weight
				dijkstra.edgeTo[to] = edge
			}
		}
	}
}
