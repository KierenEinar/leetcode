package graph

/***
双色球问题, 任意一条边, 只要存在两个顶点的颜色一样, 那么就不是二分图
*/

type TwoColor struct {
	adj [][]int
	isTwoColorable bool
	color   []bool
	visited []bool
}

func (tw *TwoColor) initTwoColor() {
	tw.isTwoColorable = true
	for s := range tw.adj {
		if !tw.visited[s] {
			tw.dfs(s)
		}
	}
}

func (tw *TwoColor) dfs(v int) {
	tw.visited[v] = true
	for _, w := range tw.adj[v] {
		if !tw.visited[w] {
			tw.color[w] = !tw.color[v]
			tw.dfs(w)
		} else if tw.color[v] == tw.color[w] {
			tw.isTwoColorable = false
		}
	}
}
