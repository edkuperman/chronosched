package db

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type NamespaceRepo struct {
	DB *pgxpool.Pool
}

func NewNamespaceRepo(pool *pgxpool.Pool) *NamespaceRepo {
	return &NamespaceRepo{DB: pool}
}

// Ensure table exists
func (r *NamespaceRepo) ensureTable(ctx context.Context) error {
	_, err := r.DB.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS namespaces (
			name TEXT PRIMARY KEY,
			created_at TIMESTAMPTZ DEFAULT now()
		);
	`)
	return err
}

// Create adds a namespace
func (r *NamespaceRepo) Create(ctx context.Context, name string) error {
	if err := r.ensureTable(ctx); err != nil {
		return err
	}
	_, err := r.DB.Exec(ctx, `INSERT INTO namespaces(name) VALUES($1) ON CONFLICT DO NOTHING;`, name)
	return err
}

// List returns union of all namespaces
func (r *NamespaceRepo) List(ctx context.Context) ([]string, error) {
	if err := r.ensureTable(ctx); err != nil {
		return nil, err
	}
	rows, err := r.DB.Query(ctx, `
		SELECT name FROM namespaces
		UNION
		SELECT DISTINCT namespace FROM dags
		UNION
		SELECT DISTINCT namespace FROM job_definitions
		ORDER BY name;
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		result = append(result, name)
	}
	return result, nil
}

// Exists checks if a namespace exists
func (r *NamespaceRepo) Exists(ctx context.Context, name string) (bool, error) {
	if err := r.ensureTable(ctx); err != nil {
		return false, err
	}
	row := r.DB.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM namespaces WHERE name=$1);`, name)
	var exists bool
	err := row.Scan(&exists)
	return exists, err
}

// Rename updates a namespace across tables
func (r *NamespaceRepo) Rename(ctx context.Context, oldName, newName string) error {
	if err := r.ensureTable(ctx); err != nil {
		return err
	}
	tx, err := r.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `UPDATE dags SET namespace=$1 WHERE namespace=$2;`, newName, oldName); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE job_definitions SET namespace=$1 WHERE namespace=$2;`, newName, oldName); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE namespaces SET name=$1 WHERE name=$2;`, newName, oldName); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// Delete removes a namespace safely
func (r *NamespaceRepo) Delete(ctx context.Context, name string) error {
	if err := r.ensureTable(ctx); err != nil {
		return err
	}
	_, err := r.DB.Exec(ctx, `
		DELETE FROM dags WHERE namespace=$1;
		DELETE FROM job_definitions WHERE namespace=$1;
		DELETE FROM namespaces WHERE name=$1;
	`, name)
	return err
}
