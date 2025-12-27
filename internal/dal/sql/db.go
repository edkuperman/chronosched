package sql

import "github.com/jackc/pgx/v5/pgxpool"

type SQLDAL struct {
    DB *pgxpool.Pool
}

func NewSQLDAL(pool *pgxpool.Pool) *SQLDAL {
    return &SQLDAL{DB: pool}
}
