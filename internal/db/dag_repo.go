package db

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DAGRepo struct{ DB *pgxpool.Pool }

func NewDAGRepo(db *pgxpool.Pool) *DAGRepo { return &DAGRepo{DB: db} }

func (r *DAGRepo) CreateDAG(ctx context.Context, ns, name string) (string, error) {
    var id string
    err := r.DB.QueryRow(ctx, `
        INSERT INTO dags (id, namespace, name)
        VALUES (gen_random_uuid(), $1, $2)
        RETURNING id;
    `, ns, name).Scan(&id)

    return id, err
}

func (r *DAGRepo) UpdateDAG(ctx context.Context, id, ns, name string) error {
	// Enforce active-name uniqueness within the namespace for DAGs:
	// no other non-deleted DAG in this namespace may use this name.
	var exists bool
	if err := r.DB.QueryRow(
			ctx,
			`SELECT EXISTS(SELECT 1 FROM dags WHERE namespace = $1 AND name = $2 AND deleted = FALSE AND id <> $3);`,
			ns, name, id,
		).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("dag name already exists in namespace")
	}
	res, err := r.DB.Exec(ctx, "UPDATE dags SET name=$1, version = version + 1 WHERE id=$2 AND namespace=$3;", name, id, ns)
	if err != nil {
		return err
	}
	if res.RowsAffected() == 0 {
		return fmt.Errorf("dag not found")
	}

    return nil
}

func (r *DAGRepo) DeleteDAG(ctx context.Context, id string) error {
	_, err := r.DB.Exec(ctx, `CALL delete_dag_preserve_shared($1);`, id)
	return err
}
