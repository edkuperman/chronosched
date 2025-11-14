package db

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Namespace struct {
	ID   string `json:"namespace_id"`
	Name string `json:"name"`
}

type NamespaceRepo struct {
	DB *pgxpool.Pool
}

func NewNamespaceRepo(pool *pgxpool.Pool) *NamespaceRepo {
	return &NamespaceRepo{DB: pool}
}

// ensureTable makes sure the namespaces table exists. Schema is also created by migrations,
// but this allows tests or ad-hoc environments to self-heal.
func (r *NamespaceRepo) ensureTable(ctx context.Context) error {
	_, err := r.DB.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS namespaces (
			namespace_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			name         TEXT NOT NULL UNIQUE,
			created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
		);
	`)
	return err
}

// Create inserts a namespace by name and returns its generated namespace_id.
func (r *NamespaceRepo) Create(ctx context.Context, name string) (string, error) {
	if err := r.ensureTable(ctx); err != nil {
		return "", err
	}
	var id string
	if err := r.DB.QueryRow(ctx, `
		INSERT INTO namespaces(name)
		VALUES ($1)
		ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
		RETURNING namespace_id;
	`, name).Scan(&id); err != nil {
		return "", err
	}
	return id, nil
}

// Exists checks if a namespace with the given name exists.
func (r *NamespaceRepo) Exists(ctx context.Context, name string) (bool, error) {
	if err := r.ensureTable(ctx); err != nil {
		return false, err
	}
	var dummy string
	err := r.DB.QueryRow(ctx, `
		SELECT namespace_id
		FROM namespaces
		WHERE name = $1;
	`, name).Scan(&dummy)
	if err != nil {
		if err == pgx.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// List returns all namespaces.
func (r *NamespaceRepo) List(ctx context.Context) ([]Namespace, error) {
	if err := r.ensureTable(ctx); err != nil {
		return nil, err
	}
	rows, err := r.DB.Query(ctx, `
		SELECT namespace_id, name
		FROM namespaces
		ORDER BY name;
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Namespace
	for rows.Next() {
		var n Namespace
		if err := rows.Scan(&n.ID, &n.Name); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// Rename updates the namespace name while preserving its id.
func (r *NamespaceRepo) Rename(ctx context.Context, oldName, newName string) error {
	if err := r.ensureTable(ctx); err != nil {
		return err
	}
	cmd, err := r.DB.Exec(ctx, `
		UPDATE namespaces
		SET name = $2
		WHERE name = $1;
	`, oldName, newName)
	if err != nil {
		return err
	}
	if cmd.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

// Delete removes a namespace and associated dags/definitions (by namespace name).
// NOTE: dags and job_definitions currently store namespace as a TEXT column which
// we interpret as the namespace *name*. This keeps backwards compatibility while
// the external API exposes namespace_id.
func (r *NamespaceRepo) Delete(ctx context.Context, name string) error {
	if err := r.ensureTable(ctx); err != nil {
		return err
	}
	_, err := r.DB.Exec(ctx, `
		DELETE FROM dags WHERE namespace = $1;
		DELETE FROM job_definitions WHERE namespace = $1;
		DELETE FROM namespaces WHERE name = $1;
	`, name)
	return err
}
