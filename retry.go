package mdatabase

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// RetryConfig defines retry behavior
type RetryConfig struct {
	MaxAttempts  int
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Multiplier   float64
	EnableRetry  bool
}

// DefaultRetryConfig returns sensible defaults for retry behavior
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts:  3,
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     2 * time.Second,
		Multiplier:   2.0,
		EnableRetry:  true,
	}
}

// isRetryableError determines if an error should trigger a retry
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Don't retry context errors (timeout, cancellation)
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return false
	}

	// Don't retry "no rows" errors
	if errors.Is(err, sql.ErrNoRows) {
		return false
	}

	// Check for pgx/PostgreSQL specific errors
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		// Don't retry on constraint violations and data integrity errors
		switch pgErr.Code {
		case "23000", // integrity_constraint_violation
			"23001", // restrict_violation
			"23502", // not_null_violation
			"23503", // foreign_key_violation
			"23505", // unique_violation
			"23514", // check_violation
			"23P01": // exclusion_violation
			return false

		case "42000", // syntax_error_or_access_rule_violation
			"42601", // syntax_error
			"42501", // insufficient_privilege
			"42846", // cannot_coerce
			"42803", // grouping_error
			"42P20", // windowing_error
			"42P19", // invalid_recursion
			"42830", // invalid_foreign_key
			"42602", // invalid_name
			"42622", // name_too_long
			"42939", // reserved_name
			"42804", // datatype_mismatch
			"42P18", // indeterminate_datatype
			"42P21", // collation_mismatch
			"42P22", // indeterminate_collation
			"42809", // wrong_object_type
			"428C9", // generated_always
			"42703", // undefined_column
			"42883", // undefined_function
			"42P01", // undefined_table
			"42P02": // undefined_parameter
			return false

		case "40001", // serialization_failure
			"40P01": // deadlock_detected
			// These are retryable transaction conflicts
			return true

		case "08000", // connection_exception
			"08003", // connection_does_not_exist
			"08006", // connection_failure
			"08001", // sqlclient_unable_to_establish_sqlconnection
			"08004", // sqlserver_rejected_establishment_of_sqlconnection
			"08007", // transaction_resolution_unknown
			"08P01": // protocol_violation
			// Connection errors are retryable
			return true

		case "53000", // insufficient_resources
			"53100", // disk_full
			"53200", // out_of_memory
			"53300", // too_many_connections
			"53400": // configuration_limit_exceeded
			// Resource errors are retryable
			return true

		case "57P03": // cannot_connect_now
			return true

		case "25006": // read_only_sql_transaction
			return false

		default:
			// For other PostgreSQL errors, retry only if they seem transient
			return false
		}
	}

	// Check error message for common transient issues
	errMsg := strings.ToLower(err.Error())

	// Network and connection errors
	if strings.Contains(errMsg, "connection refused") ||
		strings.Contains(errMsg, "connection reset") ||
		strings.Contains(errMsg, "broken pipe") ||
		strings.Contains(errMsg, "no such host") ||
		strings.Contains(errMsg, "network is unreachable") ||
		strings.Contains(errMsg, "i/o timeout") ||
		strings.Contains(errMsg, "eof") ||
		strings.Contains(errMsg, "connection closed") ||
		strings.Contains(errMsg, "bad connection") {
		return true
	}

	// Database temporary issues
	if strings.Contains(errMsg, "too many clients") ||
		strings.Contains(errMsg, "server is not accepting") ||
		strings.Contains(errMsg, "connection pool exhausted") ||
		strings.Contains(errMsg, "temporary failure") {
		return true
	}

	// Default: don't retry
	return false
}

// RetryWithBackoff executes a function with exponential backoff retry logic
func RetryWithBackoff(ctx context.Context, config RetryConfig, operation func() error) error {
	if !config.EnableRetry {
		return operation()
	}

	var lastErr error
	delay := config.InitialDelay

	for attempt := 1; attempt <= config.MaxAttempts; attempt++ {
		// Execute the operation
		err := operation()

		// Success
		if err == nil {
			return nil
		}

		lastErr = err

		// Check if we should retry
		if !isRetryableError(err) {
			return err
		}

		// Don't retry on the last attempt
		if attempt >= config.MaxAttempts {
			break
		}

		// Check if context is cancelled
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
			// Calculate next delay with exponential backoff
			delay = time.Duration(float64(delay) * config.Multiplier)
			if delay > config.MaxDelay {
				delay = config.MaxDelay
			}
		}
	}

	return lastErr
}

// WithRetry wraps a database operation with retry logic
func WithRetry(ctx context.Context, fn func() error) error {
	return RetryWithBackoff(ctx, DefaultRetryConfig(), fn)
}
