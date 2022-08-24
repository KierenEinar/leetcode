package graph

/**
判断一副图是否自环
*/

type Cycle struct {
	adj [][]int
	visited []bool
	hasCycle bool
}

func (c *Cycle) initCycle() {
	for idx := range c.adj {
		if !c.visited[idx] {
			c.dfs(idx, idx)
		}
	}
}

func (c *Cycle) dfs(v int, u int) {
	c.visited[v] = true
	for _, w := range c.adj[v] {
		if !c.visited[w] {
			c.dfs(w, v)
		} else if w != u {
			c.hasCycle = true
		}
	}
}

func (c *Cycle) HasCycle() bool {
	return c.hasCycle
}