package repository

import "context"

type AdminRepository interface {
	CheckGlobalCycles(ctx context.Context) (map[string]any, error)
}
