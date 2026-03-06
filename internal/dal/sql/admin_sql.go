package sql

import (
	"context"
)

type AdminSQL struct{ store *Store }

func NewAdminSQL(dal *SQLDAL) *AdminSQL { return &AdminSQL{store: NewStore(dal)} }
func (a *AdminSQL) CheckGlobalCycles(ctx context.Context) (map[string]any, error) {
	return a.store.CheckGlobalCycles(ctx)
}
