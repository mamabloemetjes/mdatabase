package mdatabase

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/uptrace/bun"
)

// RawQuery executes a raw SQL query and returns results with automatic retry
func RawQuery[T any](db *DB, ctx context.Context, query string, args ...any) ([]T, error) {
	start := time.Now()
	var data []T

	err := WithRetry(ctx, func() error {
		data = nil // Reset on retry
		return db.NewRaw(query, args...).Scan(ctx, &data)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to execute raw query: %w (took %v)", err, time.Since(start))
	}

	return data, nil
}

// RawQueryOne executes a raw SQL query and returns a single result with automatic retry
func RawQueryOne[T any](db *DB, ctx context.Context, query string, args ...any) (*T, error) {
	start := time.Now()
	var data T

	err := WithRetry(ctx, func() error {
		return db.NewRaw(query, args...).Scan(ctx, &data)
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to execute raw query: %w (took %v)", err, time.Since(start))
	}

	return &data, nil
}

// RawExec executes a raw SQL command (INSERT, UPDATE, DELETE) without returning data with automatic retry
func RawExec(db *DB, ctx context.Context, query string, args ...any) (int, error) {
	start := time.Now()
	var rowsAffected int64

	err := func() error {
		res, err := db.ExecContext(ctx, query, args...)
		if err != nil {
			return err
		}
		rowsAffected, err = res.RowsAffected()
		return err
	}()
	if err != nil {
		return 0, fmt.Errorf("failed to execute raw command: %w (took %v)", err, time.Since(start))
	}

	return int(rowsAffected), nil
}

// Transaction executes a function within a database transaction with automatic retry
func Transaction(db *DB, ctx context.Context, fn func(bun.Tx) error) error {
	return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
		return fn(tx)
	})
}

// TransactionWithResult executes a function within a transaction and returns a result with automatic retry
func TransactionWithResult[T any](db *DB, ctx context.Context, fn func(bun.Tx) (T, error)) (T, error) {
	var result T
	err := db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
		var err error
		result, err = fn(tx)
		return err
	})

	return result, err
}

// TransactionWithOptions executes a transaction with custom options
func TransactionWithOptions(db *DB, ctx context.Context, opts *sql.TxOptions, fn func(bun.Tx) error) error {
	return db.RunInTx(ctx, opts, func(ctx context.Context, tx bun.Tx) error {
		return fn(tx)
	})
}

// ReadOnlyTransaction executes a read-only transaction
func ReadOnlyTransaction(db *DB, ctx context.Context, fn func(bun.Tx) error) error {
	return TransactionWithOptions(db, ctx, &sql.TxOptions{ReadOnly: true}, fn)
}

// Pagination represents pagination parameters
type Pagination struct {
	Page       int `json:"page" validate:"required,min=1"`
	PageSize   int `json:"page_size" validate:"required,min=1,max=100"`
	Total      int `json:"total" validate:"min=0"`
	TotalItems int `json:"total_items" validate:"min=0"`
	TotalPages int `json:"total_pages" validate:"min=0"`
}

// PaginationResult wraps paginated data with metadata
type PaginationResult[T any] struct {
	Data       []T        `json:"data"`
	Pagination Pagination `json:"pagination"`
}

// Paginate applies pagination to a query builder and returns results with metadata
func Paginate[T any](q *QueryBuilder[T], ctx context.Context, page, pageSize int) (*PaginationResult[T], error) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}
	if pageSize > 100 {
		pageSize = 100 // Max page size
	}

	// Get total count
	total, err := q.Count(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get total count: %w", err)
	}

	// Calculate offset
	offset := (page - 1) * pageSize

	// Get paginated data
	data, err := q.Limit(pageSize).Offset(offset).All(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get paginated data: %w", err)
	}

	return &PaginationResult[T]{
		Data: data,
		Pagination: Pagination{
			Page:       page,
			PageSize:   pageSize,
			Total:      total,
			TotalItems: total,
			TotalPages: (total + pageSize - 1) / pageSize,
		},
	}, nil
}

// CursorPagination represents cursor-based pagination parameters
type CursorPagination struct {
	NextCursor string `json:"next_cursor,omitempty" validate:"omitempty"`
	PrevCursor string `json:"prev_cursor,omitempty" validate:"omitempty"`
	HasMore    bool   `json:"has_more"`
	PageSize   int    `json:"page_size" validate:"required,min=1,max=100"`
}

// CursorPaginationResult wraps cursor-paginated data with metadata
type CursorPaginationResult[T any] struct {
	Data       []T              `json:"data"`
	Pagination CursorPagination `json:"pagination"`
}

// FindByID is a helper to find a record by ID
func FindByID[T any](db *DB, ctx context.Context, id any) (*T, error) {
	return Query[T](db).Where("id", id).First(ctx)
}

// FindByIDs is a helper to find multiple records by IDs
func FindByIDs[T any](db *DB, ctx context.Context, ids []any) ([]T, error) {
	return Query[T](db).WhereIn("id", ids).All(ctx)
}

// Create is a helper to insert a single record
func Create[T any](db *DB, ctx context.Context, data *T) (*T, error) {
	return Query[T](db).Insert(ctx, data)
}

// CreateMany is a helper to insert multiple records
func CreateMany[T any](db *DB, ctx context.Context, data []T) ([]T, error) {
	return Query[T](db).InsertMany(ctx, data)
}

// UpdateByID is a helper to update a record by ID
func UpdateByID[T any](db *DB, ctx context.Context, id any, data map[string]any) (int, error) {
	return Query[T](db).Where("id", id).Update(ctx, data)
}

// DeleteByID is a helper to delete a record by ID
func DeleteByID[T any](db *DB, ctx context.Context, id any) (int, error) {
	return Query[T](db).Where("id", id).Delete(ctx)
}

// SoftDelete performs a soft delete by setting deleted_at timestamp
func SoftDelete[T any](db *DB, ctx context.Context, id any) (int, error) {
	return Query[T](db).
		Where("id", id).
		Update(ctx, map[string]any{
			"deleted_at": time.Now(),
		})
}

// Restore restores a soft-deleted record
func Restore[T any](db *DB, ctx context.Context, id any) (int, error) {
	return Query[T](db).
		Where("id", id).
		Update(ctx, map[string]any{
			"deleted_at": nil,
		})
}

// ExcludeSoftDeleted adds a WHERE clause to exclude soft-deleted records
func ExcludeSoftDeleted[T any](q *QueryBuilder[T]) *QueryBuilder[T] {
	return q.WhereNull("deleted_at")
}

// OnlySoftDeleted adds a WHERE clause to only include soft-deleted records
func OnlySoftDeleted[T any](q *QueryBuilder[T]) *QueryBuilder[T] {
	return q.WhereNotNull("deleted_at")
}

// BatchProcess processes records in batches
func BatchProcess[T any](ctx context.Context, query *QueryBuilder[T], batchSize int, fn func([]T) error) error {
	if batchSize < 1 {
		batchSize = 100
	}

	offset := 0
	for {
		batch, err := query.Limit(batchSize).Offset(offset).All(ctx)
		if err != nil {
			return fmt.Errorf("failed to fetch batch at offset %d: %w", offset, err)
		}

		if len(batch) == 0 {
			break
		}

		if err := fn(batch); err != nil {
			return fmt.Errorf("batch processing failed at offset %d: %w", offset, err)
		}

		if len(batch) < batchSize {
			break
		}

		offset += batchSize
	}

	return nil
}

// Upsert performs an INSERT ... ON CONFLICT DO UPDATE operation
func Upsert[T any](db *DB, ctx context.Context, data *T, conflictColumn string, updateColumns ...string) (*T, error) {
	start := time.Now()

	err := WithRetry(ctx, func() error {
		query := db.NewInsert().Model(data)

		// Build ON CONFLICT clause
		query = query.On(fmt.Sprintf("CONFLICT (%s)", conflictColumn))

		if len(updateColumns) > 0 {
			// Update specified columns
			query = query.On("DO UPDATE")
			for _, col := range updateColumns {
				query = query.Set(fmt.Sprintf("%s = EXCLUDED.%s", col, col))
			}
		} else {
			// Do nothing on conflict
			query = query.On("DO NOTHING")
		}

		_, err := query.Exec(ctx)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("failed to execute upsert: %w (took %v)", err, time.Since(start))
	}

	return data, nil
}

// BulkUpsert performs bulk INSERT ... ON CONFLICT DO UPDATE
func BulkUpsert[T any](db *DB, ctx context.Context, data []T, conflictColumn string, updateColumns ...string) ([]T, error) {
	start := time.Now()

	if len(data) == 0 {
		return data, nil
	}

	err := WithRetry(ctx, func() error {
		query := db.NewInsert().Model(&data)

		// Build ON CONFLICT clause
		query = query.On(fmt.Sprintf("CONFLICT (%s)", conflictColumn))

		if len(updateColumns) > 0 {
			// Update specified columns
			query = query.On("DO UPDATE")
			for _, col := range updateColumns {
				query = query.Set(fmt.Sprintf("%s = EXCLUDED.%s", col, col))
			}
		} else {
			// Do nothing on conflict
			query = query.On("DO NOTHING")
		}

		_, err := query.Exec(ctx)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("failed to execute bulk upsert: %w (took %v)", err, time.Since(start))
	}

	return data, nil
}

// Chunk executes a callback for each chunk of results
func Chunk[T any](ctx context.Context, query *QueryBuilder[T], chunkSize int, fn func([]T, int) error) error {
	if chunkSize < 1 {
		chunkSize = 100
	}

	offset := 0
	chunkNumber := 0

	for {
		chunk, err := query.Limit(chunkSize).Offset(offset).All(ctx)
		if err != nil {
			return fmt.Errorf("failed to fetch chunk at offset %d: %w", offset, err)
		}

		if len(chunk) == 0 {
			break
		}

		if err := fn(chunk, chunkNumber); err != nil {
			return fmt.Errorf("chunk processing failed at chunk %d: %w", chunkNumber, err)
		}

		if len(chunk) < chunkSize {
			break
		}

		offset += chunkSize
		chunkNumber++
	}

	return nil
}

// FirstOrCreate finds the first record matching conditions or creates it
func FirstOrCreate[T any](db *DB, ctx context.Context, search, defaults map[string]any, result *T) error {
	// Try to find existing record
	q := Query[T](db)
	for key, value := range search {
		q = q.Where(key, value)
	}

	existing, err := q.First(ctx)
	if err != nil {
		return err
	}

	if existing != nil {
		*result = *existing
		return nil
	}

	// Record doesn't exist, create it using a transaction
	return Transaction(db, ctx, func(tx bun.Tx) error {
		// Merge search and defaults
		query := tx.NewInsert().Model(result)

		_, err := query.Exec(ctx)
		return err
	})
}

// UpdateOrCreate updates an existing record or creates a new one
func UpdateOrCreate[T any](db *DB, ctx context.Context, search, updates map[string]any, result *T) error {
	// Try to find and update existing record
	q := Query[T](db)
	for key, value := range search {
		q = q.Where(key, value)
	}

	results, err := q.UpdateReturning(ctx, updates)
	if err != nil {
		return err
	}

	if len(results) > 0 {
		*result = results[0]
		return nil
	}

	// Record doesn't exist, create it using a transaction
	return Transaction(db, ctx, func(tx bun.Tx) error {
		query := tx.NewInsert().Model(result)
		_, err := query.Exec(ctx)
		return err
	})
}

// BulkInsertWithBatch inserts records in batches for better performance
func BulkInsertWithBatch[T any](db *DB, ctx context.Context, data []T, batchSize int) error {
	if len(data) == 0 {
		return nil
	}

	if batchSize < 1 {
		batchSize = 1000
	}

	for i := 0; i < len(data); i += batchSize {
		end := i + batchSize
		if end > len(data) {
			end = len(data)
		}

		batch := data[i:end]

		err := WithRetry(ctx, func() error {
			_, err := db.NewInsert().Model(&batch).Exec(ctx)
			return err
		})
		if err != nil {
			return fmt.Errorf("failed to insert batch at index %d: %w", i, err)
		}
	}

	return nil
}
