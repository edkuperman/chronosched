package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Namespace struct {
	ID   		string 		`json:"namespace_id"`
	Name 		string 		`json:"name"`
	CreatedAt   time.Time 	`json:"created_at"`
}

type NamespaceRepo struct {
	DB *pgxpool.Pool
}

func NewNamespaceRepo(pool *pgxpool.Pool) *NamespaceRepo {
	return &NamespaceRepo{DB: pool}
}


// Create inserts a namespace by name and returns its generated namespace_id.
func (r *NamespaceRepo) Create(ctx context.Context, name string) (string, error) {
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

func (r *NamespaceRepo) GetByName(ctx context.Context, name string) (*Namespace, error) {
    row := r.DB.QueryRow(ctx, `
        SELECT namespace_id, name, created_at
        FROM namespaces
        WHERE name = $1;
    `, name)

    var ns Namespace
    if err := row.Scan(&ns.ID, &ns.Name, &ns.CreatedAt); err != nil {
        if err == pgx.ErrNoRows {
            return nil, nil
        }
        return nil, err
    }
    return &ns, nil
}

// Exists checks if a namespace with the given name exists.
func (r *NamespaceRepo) Exists(ctx context.Context, name string) (bool, error) {
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

func (r *NamespaceRepo) ExistsByID(ctx context.Context, id string) (bool, error) {
    var exists bool
    err := r.DB.QueryRow(ctx, `
        SELECT EXISTS (
            SELECT 1
            FROM namespaces
            WHERE namespace_id = $1 AND deleted = FALSE
        );
    `, id).Scan(&exists)
    return exists, err
}


// List returns all namespaces.
func (r *NamespaceRepo) List(ctx context.Context) ([]Namespace, error) {
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
func (r *NamespaceRepo) Rename(ctx context.Context, namespace_id string, newName string) error {
    // bump version, change name
    cmd, err := r.DB.Exec(ctx, `
        UPDATE namespaces
        SET name = $2,
            version = version + 1
        WHERE namespace_id = $1
          AND deleted = FALSE;
    `, namespace_id, newName)
    if err != nil {
        // Handle unique constraint cleanly (duplicate name)
        if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "23505" {
            return fmt.Errorf("namespace name already exists")
        }
        return err
    }
    if cmd.RowsAffected() == 0 {
        return pgx.ErrNoRows
    }
    return nil
}

// Delete upticks the version and sets deleted flag to true
func (r *NamespaceRepo) Delete(ctx context.Context, namespace_id string) error {
    cmd, err := r.DB.Exec(ctx, `
        UPDATE namespaces
        SET deleted = TRUE,
            version = version + 1
        WHERE namespace_id = $1
          AND deleted = FALSE;
    `, namespace_id)
    if err != nil {
        return err
    }
    if cmd.RowsAffected() == 0 {
        return pgx.ErrNoRows
    }
    return nil
}

