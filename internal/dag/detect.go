package dag

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Edge struct {
	Src string
	Dst string
}

type IEdges interface {
	Edges(ctx context.Context, dagID string) ([]Edge, error)
}

type EdgeCache []Edge

func (m EdgeCache) Edges(ctx context.Context, dagID string) ([]Edge, error) {
	return m, nil
}

type DBEdges struct {
	DB *pgxpool.Pool
}

func (s *DBEdges) Edges(ctx context.Context, dagID string) ([]Edge, error) {
	rows, err := s.DB.Query(ctx, `SELECT src, dst FROM dag_edges WHERE dag_id=$1`, dagID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Edge
	for rows.Next() {
		var e Edge
		if err := rows.Scan(&e.Src, &e.Dst); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}

type ICycleDetector interface {
	DetectCycles(ctx context.Context, src IEdges, dagID string) ([][]string, error)
	HasCycle(ctx context.Context, src IEdges, dagID string) (bool, error)
}

type DFSDetector struct{}

// DetectCycles finds all cycles and returns their node paths.
func (DFSDetector) DetectCycles(ctx context.Context, src IEdges, dagID string) ([][]string, error) {
	edges, err := src.Edges(ctx, dagID)
	if err != nil {
		return nil, err
	}

	graph := make(map[string][]string)
	for _, e := range edges {
		graph[e.Src] = append(graph[e.Src], e.Dst)
		if _, ok := graph[e.Dst]; !ok {
			graph[e.Dst] = nil
		}
	}

	var (
		cycles  [][]string
		visited = map[string]bool{}
		stack   = map[string]bool{}
		path    []string
	)

	var dfs func(string)
	dfs = func(n string) {
		visited[n] = true
		stack[n] = true
		path = append(path, n)

		for _, next := range graph[n] {
			if !visited[next] {
				dfs(next)
			} else if stack[next] {
				start := 0
				for i, v := range path {
					if v == next {
						start = i
						break
					}
				}
				cycle := append([]string(nil), path[start:]...)
				cycles = append(cycles, append(cycle, next))
			}
		}

		stack[n] = false
		path = path[:len(path)-1]
	}

	for node := range graph {
		if !visited[node] {
			dfs(node)
		}
	}

	return cycles, nil
}

func (DFSDetector) HasCycle(ctx context.Context, src IEdges, dagID string) (bool, error) {
	cycles, err := DFSDetector{}.DetectCycles(ctx, src, dagID)
	return len(cycles) > 0, err
}

