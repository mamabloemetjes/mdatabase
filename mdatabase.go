package mdatabase

import (
	"database/sql"

	"github.com/MonkyMars/gecho"
	"github.com/uptrace/bun"
)

type DB struct {
	*bun.DB
	sqlDB  *sql.DB
	logger *gecho.Logger
}
