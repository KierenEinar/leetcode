package graph

/**
判断一个图是否强连通图
**/

type CC struct {
	adj     [][]int
	count   int // 计算强连通分量
	id      []int
	visited []bool
}

// NewCC 声明n个顶点的图
func NewCC(v int) *CC {
	adj := make([][]int, v)
	visited := make([]bool, v)
	id := make([]int, v)
	cc := &CC{adj, 0, id, visited}
	return cc
}

func (cc *CC) AddEdge(v, w int) {
	cc.adj[v] = append(cc.adj[v], w)
	cc.adj[w] = append(cc.adj[w], v)
}

func (cc *CC) initCC() {
	for idx := range cc.adj {
		if !cc.visited[idx] {
			cc.dfs(idx)
			cc.count++
		}
	}
}

func (cc *CC) dfs(v int) {

	cc.visited[v] = true
	cc.id[v] = cc.count
	for _, w := range cc.adj[v] {
		if !cc.visited[w] {
			cc.dfs(w)
		}
	}
}

// Connected v, w是否在一个强连通图中
func (cc *CC) Connected(v, w int) bool {
	return cc.id[v] == cc.id[w]
}

// Count 获得强连通分量
func (cc *CC) Count() int {
	return cc.count
}
