package mdatabase

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"time"

	"github.com/MonkyMars/gecho"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"
)

type Config struct {
	Host         string
	Port         int
	User         string
	Password     string
	Name         string
	MaxConns     int
	MinConns     int
	MaxLifetime  time.Duration
	MaxIdleTime  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

type DB struct {
	*bun.DB
	sqlDB  *sql.DB
	logger *gecho.Logger
}

func Connect(logger *gecho.Logger, cfg Config) (*DB, error) {
	dsn := (&url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(cfg.User, cfg.Password),
		Host:     net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)),
		Path:     "/" + cfg.Name,
		RawQuery: "sslmode=require",
	}).String()

	connector := pgdriver.NewConnector(
		pgdriver.WithDSN(dsn),
		pgdriver.WithTimeout(30*time.Second),
		pgdriver.WithDialTimeout(10*time.Second),
		pgdriver.WithReadTimeout(cfg.ReadTimeout),
		pgdriver.WithWriteTimeout(cfg.WriteTimeout),
		pgdriver.WithTLSConfig(nil),
	)

	sqlDB := sql.OpenDB(connector)
	sqlDB.SetMaxOpenConns(cfg.MaxConns)
	sqlDB.SetMaxIdleConns(cfg.MinConns)
	sqlDB.SetConnMaxLifetime(cfg.MaxLifetime)
	sqlDB.SetConnMaxIdleTime(cfg.MaxIdleTime)

	bunDB := bun.NewDB(sqlDB, pgdialect.New())
	bunDB.AddQueryHook(&queryHook{logger: logger})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := bunDB.PingContext(ctx); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	logger.Info("Connected to database successfully",
		gecho.Field("host", cfg.Host),
		gecho.Field("database", cfg.Name),
		gecho.Field("max_conns", cfg.MaxConns),
	)

	return &DB{DB: bunDB, sqlDB: sqlDB, logger: logger}, nil
}

func (db *DB) Begin() (bun.Tx, error) {
	return db.DB.Begin()
}

func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (bun.Tx, error) {
	return db.DB.BeginTx(ctx, opts)
}

func (db *DB) Close() error {
	if err := db.DB.Close(); err != nil {
		return err
	}
	return db.sqlDB.Close()
}

func (db *DB) Health() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return db.PingContext(ctx)
}

func (db *DB) GetStats() sql.DBStats {
	return db.sqlDB.Stats()
}

type queryHook struct {
	logger *gecho.Logger
}

func (h *queryHook) BeforeQuery(ctx context.Context, event *bun.QueryEvent) context.Context {
	return ctx
}

func (h *queryHook) AfterQuery(ctx context.Context, event *bun.QueryEvent) {
	duration := time.Since(event.StartTime)
	if duration > 400*time.Millisecond {
		h.logger.Warn("Slow database query detected",
			gecho.Field("query", event.Query),
			gecho.Field("duration", duration),
		)
	}
	if event.Err != nil && event.Err != sql.ErrNoRows {
		h.logger.Error("Database query error",
			gecho.Field("error", event.Err),
			gecho.Field("query", event.Query),
			gecho.Field("duration", duration),
		)
	}
}
