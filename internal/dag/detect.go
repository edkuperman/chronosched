package dag

// Edge represents a directed edge in a DAG from From -> To.
type Edge struct {
	From string
	To   string
}

// DetectCycles runs a DFS-based cycle detection over the given directed edges.
// It returns a slice of cycles, where each cycle is represented as a slice
// of node IDs (strings) in order along the cycle.
func DetectCycles(edges []Edge) [][]string {
	// Build adjacency list.
	graph := make(map[string][]string)
	for _, e := range edges {
		graph[e.From] = append(graph[e.From], e.To)
		if _, ok := graph[e.To]; !ok {
			graph[e.To] = nil
		}
	}

	visited := make(map[string]bool)
	stack := make(map[string]bool)
	var path []string
	var cycles [][]string

	var dfs func(node string)
	dfs = func(node string) {
		visited[node] = true
		stack[node] = true
		path = append(path, node)

		for _, next := range graph[node] {
			if !visited[next] {
				dfs(next)
			} else if stack[next] {
				// Found a back-edge -> cycle. Extract cycle from path.
				start := 0
				for i, v := range path {
					if v == next {
						start = i
						break
					}
				}
				cycle := append([]string{}, path[start:]...)
				cycles = append(cycles, cycle)
			}
		}

		// pop from path and stack
		path = path[:len(path)-1]
		stack[node] = false
	}

	for node := range graph {
		if !visited[node] {
			dfs(node)
		}
	}

	return cycles
}

// HasCycle reports whether the given edges contain at least one cycle.
func HasCycle(edges []Edge) bool {
	return len(DetectCycles(edges)) > 0
}
