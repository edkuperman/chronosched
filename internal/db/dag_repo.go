package db

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DAGRepo struct{ DB *pgxpool.Pool }

func NewDAGRepo(db *pgxpool.Pool) *DAGRepo { return &DAGRepo{DB: db} }

func (r *DAGRepo) CreateDAG(ctx context.Context, id, ns, name string) error {
	_, err := r.DB.Exec(ctx, `INSERT INTO dags(id, namespace, name) VALUES($1,$2,$3);`, id, ns, name)
	return err
}

func (r *DAGRepo) DeleteDAG(ctx context.Context, id string) error {
	_, err := r.DB.Exec(ctx, `CALL delete_dag_preserve_shared($1);`, id)
	return err
}
