package loadgen

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
)

func TestClassifyStatus(t *testing.T) {
	assert.Equal(t, StatusAcked, classifyStatus(nil))

	pgErr := &pgconn.PgError{Code: "25006", Message: "cannot execute INSERT in a read-only transaction"}
	assert.Equal(t, StatusFailed, classifyStatus(pgErr))

	assert.Equal(t, StatusIndoubt, classifyStatus(errors.New("write: connection reset by peer")))
	assert.Equal(t, StatusIndoubt, classifyStatus(context.DeadlineExceeded))
}

func TestErrorClass(t *testing.T) {
	assert.Equal(t, "read-only", errorClass(&pgconn.PgError{Code: "25006"}))
	assert.Equal(t, "conn-refused", errorClass(errors.New("dial tcp: connection refused")))
	assert.Equal(t, "timeout", errorClass(context.DeadlineExceeded))
	assert.Equal(t, "other", errorClass(errors.New("boom")))
}
