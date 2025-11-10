package dag

// DetectCycleLocal checks for cycles in a directed graph using
// Kahn's Algorithm (BFS topological ordering). If the number
// of nodes processed is less than total, a cycle exists.
func DetectCycleLocal(g map[string][]string) ([]string, bool) {
	// Compute indegree for each node
	inDegree := map[string]int{}
	for u := range g {
		if _, ok := inDegree[u]; !ok {
			inDegree[u] = 0
		}
		for _, v := range g[u] {
			inDegree[v]++
			if _, ok := g[v]; !ok {
				g[v] = nil // ensure every node appears
			}
		}
	}

	// Collect nodes with indegree 0
	queue := []string{}
	for node, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, node)
		}
	}

	var topo []string
	for len(queue) > 0 {
		u := queue[0]
		queue = queue[1:]
		topo = append(topo, u)
		for _, v := range g[u] {
			inDegree[v]--
			if inDegree[v] == 0 {
				queue = append(queue, v)
			}
		}
	}

	// If not all nodes were processed, we have a cycle.
	if len(topo) != len(inDegree) {
		// Return nodes that still have nonzero indegree as a hint of the cycle.
		cycle := []string{}
		for n, deg := range inDegree {
			if deg > 0 {
				cycle = append(cycle, n)
			}
		}
		return cycle, true
	}

	return nil, false
}
