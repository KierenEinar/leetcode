package graph

/***
对无向图进行的某个顶点进行深度优先搜索
*/

type Graph struct {
	adj 	[][]int
	visited []bool
}

// NewGraph 声明n个顶点的图
func NewGraph(v int) *Graph {
	adj := make([][]int, v)
	visited := make([]bool, v)
	return &Graph{adj, visited}
}

func (g *Graph) AddEdge(v, w int) {
	g.adj[v] = append(g.adj[v], w)
	g.adj[w] = append(g.adj[w], v)
}

func (g *Graph) DepthFirstSearch(v int) {
	g.visited[v] = true
	for _, w := range g.adj[v] {
		if !g.visited[w] {
			g.DepthFirstSearch(w)
		}
	}
}


