package errors

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── Severity ────────────────────────────────────────────────────────────────

func TestSeverityString(t *testing.T) {
	tests := []struct {
		severity Severity
		expected string
	}{
		{Info, "INFO"},
		{Warning, "WARNING"},
		{Error, "ERROR"},
		{Critical, "CRITICAL"},
		{Severity(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, tt.severity.String())
	}
}

// ─── RetryBehavior ───────────────────────────────────────────────────────────

func TestRetryBehaviorString(t *testing.T) {
	tests := []struct {
		behavior RetryBehavior
		expected string
	}{
		{NonRetryable, "NON_RETRYABLE"},
		{Retryable, "RETRYABLE"},
		{RetryableWithBackoff, "RETRYABLE_WITH_BACKOFF"},
		{RetryBehavior(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, tt.behavior.String())
	}
}

// ─── baseError ───────────────────────────────────────────────────────────────

func TestBaseErrorWithUnderlying(t *testing.T) {
	underlying := fmt.Errorf("connection refused")
	err := NewRPCConnectionFailed("rpc", "GetBlock", "", underlying)

	assert.Contains(t, err.Error(), "RPC_CONNECTION_FAILED")
	assert.Contains(t, err.Error(), "connection refused")
	assert.Equal(t, CodeRPCConnectionFailed, err.Code())
	assert.Equal(t, Error, err.Severity())
	assert.Equal(t, Retryable, err.Retryable())
	assert.Equal(t, underlying, err.Unwrap())

	ctx := err.Context()
	assert.Equal(t, "rpc", ctx.Component)
	assert.Equal(t, "GetBlock", ctx.Operation)
	assert.False(t, ctx.Timestamp.IsZero())
}

func TestBaseErrorWithoutUnderlying(t *testing.T) {
	err := NewDocumentNotFound("defra", "GetBlock", "Block", "")
	assert.Contains(t, err.Error(), "DOCUMENT_NOT_FOUND")
	assert.NotContains(t, err.Error(), ":")
	assert.Nil(t, err.Unwrap())
}

// ─── Constructors ────────────────────────────────────────────────────────────

func TestNetworkErrorConstructors(t *testing.T) {
	underlying := fmt.Errorf("timeout")

	t.Run("NewHTTPConnectionFailed", func(t *testing.T) {
		err := NewHTTPConnectionFailed("http", "Fetch", "http://example.com", underlying)
		assert.Equal(t, CodeHTTPError, err.Code())
		assert.Equal(t, Error, err.Severity())
		assert.Equal(t, Retryable, err.Retryable())
		assert.True(t, IsNetworkError(err))
	})

	t.Run("NewRPCTimeout", func(t *testing.T) {
		err := NewRPCTimeout("rpc", "GetBlock", "block_123", underlying)
		assert.Equal(t, CodeRPCTimeout, err.Code())
		assert.Equal(t, Retryable, err.Retryable())
	})

	t.Run("NewRPCConnectionFailed", func(t *testing.T) {
		err := NewRPCConnectionFailed("rpc", "Connect", "", underlying)
		assert.Equal(t, CodeRPCConnectionFailed, err.Code())
		assert.Equal(t, Retryable, err.Retryable())
	})

	t.Run("NewRateLimited", func(t *testing.T) {
		err := NewRateLimited("rpc", "GetBlock", "", underlying)
		assert.Equal(t, CodeRateLimited, err.Code())
		assert.Equal(t, Warning, err.Severity())
		assert.Equal(t, RetryableWithBackoff, err.Retryable())
	})
}

func TestDataErrorConstructors(t *testing.T) {
	t.Run("NewInvalidHex", func(t *testing.T) {
		err := NewInvalidHex("converter", "ParseTx", "0xZZZ", nil)
		assert.Equal(t, CodeInvalidHex, err.Code())
		assert.Equal(t, NonRetryable, err.Retryable())
		assert.Contains(t, err.Error(), "0xZZZ")
		assert.True(t, IsDataError(err))
	})

	t.Run("NewInvalidBlockFormat", func(t *testing.T) {
		err := NewInvalidBlockFormat("converter", "ParseBlock", "{}", nil)
		assert.Equal(t, CodeInvalidBlockFormat, err.Code())
	})

	t.Run("NewInvalidInputFormat", func(t *testing.T) {
		err := NewInvalidInputFormat("api", "Validate", "bad", nil)
		assert.Equal(t, CodeInvalidInputFormat, err.Code())
	})

	t.Run("NewParsingFailed", func(t *testing.T) {
		err := NewParsingFailed("converter", "ParseJSON", "json_data", nil)
		assert.Equal(t, CodeParsingFailed, err.Code())
		assert.Contains(t, err.Error(), "json_data")
	})
}

func TestStorageErrorConstructors(t *testing.T) {
	underlying := fmt.Errorf("db down")

	t.Run("NewDBConnectionFailed", func(t *testing.T) {
		err := NewDBConnectionFailed("defra", "Connect", "", underlying)
		assert.Equal(t, CodeDBConnectionFailed, err.Code())
		assert.Equal(t, Critical, err.Severity())
		assert.Equal(t, Retryable, err.Retryable())
		assert.True(t, IsStorageError(err))
	})

	t.Run("NewQueryFailed", func(t *testing.T) {
		err := NewQueryFailed("defra", "Query", "SELECT *", underlying)
		assert.Equal(t, CodeQueryFailed, err.Code())
	})

	t.Run("NewDocumentNotFound", func(t *testing.T) {
		err := NewDocumentNotFound("defra", "GetBlock", "Block", "block_1")
		assert.Equal(t, CodeDocumentNotFound, err.Code())
		assert.Equal(t, Warning, err.Severity())
		assert.Equal(t, NonRetryable, err.Retryable())
		assert.Contains(t, err.Error(), "Block")
	})
}

func TestSystemErrorConstructors(t *testing.T) {
	t.Run("NewConfigurationError", func(t *testing.T) {
		err := NewConfigurationError("system", "Init", "missing key", "", nil)
		assert.Equal(t, CodeConfigurationError, err.Code())
		assert.Equal(t, Critical, err.Severity())
		assert.Equal(t, NonRetryable, err.Retryable())
		assert.Contains(t, err.Error(), "missing key")
	})

	t.Run("NewServiceUnavailable", func(t *testing.T) {
		err := NewServiceUnavailable("system", "Health", "redis", "", nil)
		assert.Equal(t, CodeServiceUnavailable, err.Code())
		assert.Equal(t, Critical, err.Severity())
		assert.Equal(t, RetryableWithBackoff, err.Retryable())
		assert.Contains(t, err.Error(), "redis")
	})
}

// ─── Context Options ─────────────────────────────────────────────────────────

func TestContextOptions(t *testing.T) {
	blockNum := int64(42)
	txHash := "0xabc123"

	err := NewRPCTimeout("rpc", "GetBlock", "",
		fmt.Errorf("timeout"),
		WithBlockNumber(blockNum),
		WithTxHash(txHash),
		WithMetadata("retry_count", 3),
	)

	ctx := err.Context()
	require.NotNil(t, ctx.BlockNumber)
	assert.Equal(t, blockNum, *ctx.BlockNumber)
	require.NotNil(t, ctx.TxHash)
	assert.Equal(t, txHash, *ctx.TxHash)
	assert.Equal(t, 3, ctx.Metadata["retry_count"])
}

func TestWithMetadataNilMap(t *testing.T) {
	// WithMetadata should initialize the map if nil
	ctx := &ErrorContext{}
	ctx.Metadata = nil
	opt := WithMetadata("key", "value")
	opt(ctx)
	assert.Equal(t, "value", ctx.Metadata["key"])
}

// ─── Utils ───────────────────────────────────────────────────────────────────

func TestIsRetryable(t *testing.T) {
	assert.True(t, IsRetryable(NewRPCTimeout("", "", "", nil)))
	assert.True(t, IsRetryable(NewRateLimited("", "", "", nil)))
	assert.False(t, IsRetryable(NewInvalidHex("", "", "", nil)))
	assert.False(t, IsRetryable(fmt.Errorf("standard error")))
}

func TestIsRetryableWithBackoff(t *testing.T) {
	assert.True(t, IsRetryableWithBackoff(NewRateLimited("", "", "", nil)))
	assert.True(t, IsRetryableWithBackoff(NewServiceUnavailable("", "", "", "", nil)))
	assert.False(t, IsRetryableWithBackoff(NewRPCTimeout("", "", "", nil)))
	assert.False(t, IsRetryableWithBackoff(fmt.Errorf("standard error")))
}

func TestIsCritical(t *testing.T) {
	assert.True(t, IsCritical(NewDBConnectionFailed("", "", "", nil)))
	assert.True(t, IsCritical(NewConfigurationError("", "", "", "", nil)))
	assert.False(t, IsCritical(NewRPCTimeout("", "", "", nil)))
	assert.False(t, IsCritical(fmt.Errorf("standard error")))
}

func TestIsNetworkError(t *testing.T) {
	assert.True(t, IsNetworkError(NewRPCTimeout("", "", "", nil)))
	assert.False(t, IsNetworkError(NewQueryFailed("", "", "", nil)))
	assert.False(t, IsNetworkError(fmt.Errorf("not network")))
}

func TestIsDataError(t *testing.T) {
	assert.True(t, IsDataError(NewInvalidHex("", "", "", nil)))
	assert.False(t, IsDataError(NewRPCTimeout("", "", "", nil)))
	assert.False(t, IsDataError(fmt.Errorf("not data")))
}

func TestIsStorageError(t *testing.T) {
	assert.True(t, IsStorageError(NewQueryFailed("", "", "", nil)))
	assert.False(t, IsStorageError(NewRPCTimeout("", "", "", nil)))
	assert.False(t, IsStorageError(fmt.Errorf("not storage")))
}

func TestGetErrorCode(t *testing.T) {
	assert.Equal(t, CodeRPCTimeout, GetErrorCode(NewRPCTimeout("", "", "", nil)))
	assert.Equal(t, "UNKNOWN", GetErrorCode(fmt.Errorf("standard error")))
}

func TestGetRetryDelay(t *testing.T) {
	// Non-retryable returns 0
	assert.Equal(t, time.Duration(0), GetRetryDelay(NewInvalidHex("", "", "", nil), 0))

	// Simple retryable returns base delay (1s)
	assert.Equal(t, time.Second, GetRetryDelay(NewRPCTimeout("", "", "", nil), 0))
	assert.Equal(t, time.Second, GetRetryDelay(NewRPCTimeout("", "", "", nil), 5))

	// Backoff: 1s * 2^attempts, capped at 30s
	rateLimited := NewRateLimited("", "", "", nil)
	assert.Equal(t, 2*time.Second, GetRetryDelay(rateLimited, 1))
	assert.Equal(t, 4*time.Second, GetRetryDelay(rateLimited, 2))
	assert.Equal(t, 8*time.Second, GetRetryDelay(rateLimited, 3))
	assert.Equal(t, 16*time.Second, GetRetryDelay(rateLimited, 4))
	// Should cap at 30s
	assert.Equal(t, 30*time.Second, GetRetryDelay(rateLimited, 10))

	// Standard error returns 0
	assert.Equal(t, time.Duration(0), GetRetryDelay(fmt.Errorf("nope"), 0))
}

func TestWrapError(t *testing.T) {
	// nil returns nil
	assert.Nil(t, WrapError(nil, "comp", "op"))

	// Already an IndexerError returns as-is
	original := NewRPCTimeout("rpc", "Get", "", nil)
	wrapped := WrapError(original, "other", "other_op")
	assert.Equal(t, original, wrapped)

	// Standard error gets wrapped as SystemError
	stdErr := fmt.Errorf("something failed")
	wrapped = WrapError(stdErr, "comp", "op")
	assert.Equal(t, "WRAPPED_ERROR", wrapped.Code())
	assert.Equal(t, Error, wrapped.Severity())
	assert.Equal(t, NonRetryable, wrapped.Retryable())

	// Verify it's a SystemError
	var sysErr *SystemError
	assert.True(t, errors.As(wrapped, &sysErr))
}

func TestLogContext(t *testing.T) {
	t.Run("IndexerError with all fields", func(t *testing.T) {
		err := NewRPCTimeout("rpc", "GetBlock", "", nil,
			WithBlockNumber(100),
			WithTxHash("0xabc"),
			WithMetadata("extra", "data"),
		)

		logCtx := LogContext(err)
		assert.Equal(t, CodeRPCTimeout, logCtx["error_code"])
		assert.Equal(t, "ERROR", logCtx["severity"])
		assert.Equal(t, "RETRYABLE", logCtx["retryable"])
		assert.Equal(t, "rpc", logCtx["component"])
		assert.Equal(t, "GetBlock", logCtx["operation"])
		assert.Equal(t, int64(100), logCtx["block_number"])
		assert.Equal(t, "0xabc", logCtx["tx_hash"])
		assert.Equal(t, "data", logCtx["extra"])
		assert.NotNil(t, logCtx["timestamp"])
	})

	t.Run("IndexerError without optional fields", func(t *testing.T) {
		err := NewInvalidHex("conv", "Parse", "0xZZ", nil)
		logCtx := LogContext(err)
		assert.Equal(t, CodeInvalidHex, logCtx["error_code"])
		_, hasBlock := logCtx["block_number"]
		assert.False(t, hasBlock)
		_, hasTx := logCtx["tx_hash"]
		assert.False(t, hasTx)
	})

	t.Run("standard error", func(t *testing.T) {
		err := fmt.Errorf("plain error")
		logCtx := LogContext(err)
		assert.Equal(t, "plain error", logCtx["error"])
		assert.Equal(t, "standard_error", logCtx["error_type"])
	})
}

// ─── Type assertions ─────────────────────────────────────────────────────────

func TestErrorTypeAssertions(t *testing.T) {
	networkErr := NewRPCTimeout("", "", "", nil)
	dataErr := NewInvalidHex("", "", "", nil)
	storageErr := NewQueryFailed("", "", "", nil)
	sysErr := NewConfigurationError("", "", "", "", nil)

	var ne *NetworkError
	assert.True(t, errors.As(networkErr, &ne))

	var de *DataError
	assert.True(t, errors.As(dataErr, &de))

	var se *StorageError
	assert.True(t, errors.As(storageErr, &se))

	var sy *SystemError
	assert.True(t, errors.As(sysErr, &sy))
}
