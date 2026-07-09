package clickhouse_connector

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockLogger struct {
	mock.Mock
}

func (m *MockLogger) DebugF(msg string, args ...any) {
	m.Called(msg, args)
}

func (m *MockLogger) Debug(msg string) {
	m.Called(msg)
}

func (m *MockLogger) Info(msg string) {
	m.Called(msg)
}

func (m *MockLogger) Warn(msg string) {
	m.Called(msg)
}

func (m *MockLogger) Error(msg string) {
	m.Called(msg)
}

func TestNewClient_Success(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	config := &Config{
		Host:            "localhost:9000",
		Database:        "test",
		Username:        "default",
		Password:        "",
		DialTimeout:     5,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifeTime: 60,
		ConnStrategy:    "round_robin",
	}

	mockLogger := new(MockLogger)
	mockLogger.On("Info", mock.Anything).Return()
	mockLogger.On("Error", mock.Anything).Return()
	mockLogger.On("Debug", mock.Anything).Return()
	mockLogger.On("DebugF", mock.Anything, mock.Anything).Return()
	mockLogger.On("Warn", mock.Anything).Return()

	client, err := New(config, mockLogger)
	if err != nil {
		t.Skipf("ClickHouse server not available: %v", err)
	}

	assert.NoError(t, err)
	assert.NotNil(t, client)

	if client != nil {
		client.Close()
	}
}

func TestNewClient_ValidationErrors(t *testing.T) {
	tests := []struct {
		name   string
		config *Config
	}{
		{
			name:   "nil config",
			config: nil,
		},
		{
			name: "invalid host",
			config: &Config{
				Host:            "invalid_host:9999",
				Database:        "test",
				Username:        "user",
				Password:        "pass",
				DialTimeout:     1,
				MaxOpenConns:    1,
				MaxIdleConns:    1,
				ConnMaxLifeTime: 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockLogger := new(MockLogger)
			mockLogger.On("Error", mock.Anything).Return()
			mockLogger.On("Info", mock.Anything).Return()
			mockLogger.On("Debug", mock.Anything).Return()
			mockLogger.On("DebugF", mock.Anything, mock.Anything).Return()
			mockLogger.On("Warn", mock.Anything).Return()

			client, err := New(tt.config, mockLogger)
			assert.Error(t, err)
			assert.Nil(t, client)
		})
	}
}

func TestHelperFunctions(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{"select", "SELECT * FROM users", "select"},
		{"insert", "INSERT INTO users VALUES (1)", "insert"},
		{"update", "UPDATE users SET name='test'", "update"},
		{"delete", "DELETE FROM users", "delete"},
		{"create", "CREATE TABLE test", "create"},
		{"unknown", "EXPLAIN SELECT 1", "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := detectQueryType(tt.query)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTruncateQuery(t *testing.T) {
	longQuery := "SELECT * FROM very_long_table_name WHERE condition1 AND condition2 AND condition3"
	truncated := truncateQuery(longQuery, 50)
	assert.LessOrEqual(t, len(truncated), 53)
	assert.Contains(t, truncated, "...")
}

func TestClassifyError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: "none",
		},
		{
			name:     "deadline exceeded",
			err:      context.DeadlineExceeded,
			expected: "timeout",
		},
		{
			name:     "context canceled",
			err:      context.Canceled,
			expected: "canceled",
		},
		{
			name:     "timeout string",
			err:      fmt.Errorf("connection timeout"),
			expected: "timeout",
		},
		{
			name:     "deadline string",
			err:      fmt.Errorf("deadline exceeded"),
			expected: "timeout",
		},
		{
			name:     "connection refused",
			err:      fmt.Errorf("connection refused"),
			expected: "connection_refused",
		},
		{
			name:     "connection reset",
			err:      fmt.Errorf("connection reset by peer"),
			expected: "connection_reset",
		},
		{
			name:     "connection error",
			err:      fmt.Errorf("database connection error"),
			expected: "connection_error",
		},
		{
			name:     "syntax error",
			err:      fmt.Errorf("SQL syntax error near SELECT"),
			expected: "syntax_error",
		},
		{
			name:     "authentication error",
			err:      fmt.Errorf("authentication failed"),
			expected: "authentication_error",
		},
		{
			name:     "permission error",
			err:      fmt.Errorf("permission denied for table users"),
			expected: "permission_error",
		},
		{
			name:     "access denied",
			err:      fmt.Errorf("access denied"),
			expected: "permission_error",
		},
		{
			name:     "not found",
			err:      fmt.Errorf("table not found"),
			expected: "not_found",
		},
		{
			name:     "duplicate error",
			err:      fmt.Errorf("duplicate key value"),
			expected: "duplicate_error",
		},
		{
			name:     "unknown error",
			err:      errors.New("some random error"),
			expected: "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := classifyError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBuildAttributes(t *testing.T) {
	config := &Config{
		Host:         "localhost:9000",
		Database:     "test",
		Username:     "default",
		ConnStrategy: "round_robin",
	}

	attrs := buildQueryAttributes(config, "SELECT 1", "query", 0)
	assert.NotEmpty(t, attrs)
	assert.Greater(t, len(attrs), 5)

	attrs = buildExecAttributes(config, "INSERT INTO test VALUES (1)", 1)
	assert.NotEmpty(t, attrs)
	assert.Greater(t, len(attrs), 5)
}

func TestExtractRequestID(t *testing.T) {
	ctx := context.Background()
	assert.Empty(t, ExtractRequestID(ctx))

	ctx = context.WithValue(ctx, "X-Request-Id", "test-123")
	assert.Equal(t, "test-123", ExtractRequestID(ctx))
}

func BenchmarkDetectQueryType(b *testing.B) {
	queries := []string{
		"SELECT * FROM users WHERE id = 1",
		"INSERT INTO users (name, email) VALUES ('test', 'test@example.com')",
		"UPDATE users SET name = 'updated' WHERE id = 1",
		"DELETE FROM users WHERE id = 1",
		"CREATE TABLE test (id UInt64, name String) ENGINE = MergeTree() ORDER BY id",
		"DROP TABLE IF EXISTS test",
		"ALTER TABLE users ADD COLUMN age UInt8",
		"TRUNCATE TABLE users",
		"OPTIMIZE TABLE users FINAL",
		"EXPLAIN SELECT * FROM users",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, query := range queries {
			detectQueryType(query)
		}
	}
}

func BenchmarkTruncateQuery(b *testing.B) {
	query := "SELECT users.id, users.name, users.email, orders.total, orders.created_at FROM users INNER JOIN orders ON users.id = orders.user_id WHERE users.status = 'active' AND orders.status = 'completed' ORDER BY orders.created_at DESC LIMIT 100"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		truncateQuery(query, 200)
	}
}

func BenchmarkTruncateQuery_Short(b *testing.B) {
	query := "SELECT 1"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		truncateQuery(query, 200)
	}
}

func BenchmarkTruncateQuery_Multiline(b *testing.B) {
	query := `
		SELECT 
			u.id,
			u.name,
			u.email,
			COUNT(o.id) as order_count,
			SUM(o.total) as total_spent
		FROM users u
		LEFT JOIN orders o ON u.id = o.user_id
		WHERE u.created_at >= '2023-01-01'
		GROUP BY u.id, u.name, u.email
		HAVING COUNT(o.id) > 5
		ORDER BY total_spent DESC
		LIMIT 100
	`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		truncateQuery(query, 100)
	}
}

func BenchmarkClassifyError(b *testing.B) {
	errors := []error{
		nil,
		context.DeadlineExceeded,
		context.Canceled,
		fmt.Errorf("connection timeout"),
		fmt.Errorf("connection refused"),
		fmt.Errorf("connection reset by peer"),
		fmt.Errorf("database connection error"),
		fmt.Errorf("SQL syntax error near SELECT"),
		fmt.Errorf("authentication failed"),
		fmt.Errorf("permission denied for table users"),
		fmt.Errorf("table not found"),
		fmt.Errorf("duplicate key value"),
		errors.New("some random error"),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, err := range errors {
			classifyError(err)
		}
	}
}

func BenchmarkBuildQueryAttributes(b *testing.B) {
	config := &Config{
		Host:         "clickhouse-node1:9000,clickhouse-node2:9000",
		Database:     "analytics",
		Username:     "reader",
		ConnStrategy: "round_robin",
	}
	query := "SELECT user_id, event_type, timestamp FROM events WHERE date >= today() - 7"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buildQueryAttributes(config, query, "query", 0)
	}
}

func BenchmarkBuildExecAttributes(b *testing.B) {
	config := &Config{
		Host:         "clickhouse-node1:9000,clickhouse-node2:9000",
		Database:     "analytics",
		Username:     "writer",
		ConnStrategy: "in_order",
	}
	query := "INSERT INTO events (user_id, event_type, timestamp) VALUES (?, ?, ?)"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buildExecAttributes(config, query, 3)
	}
}

func BenchmarkExtractRequestID(b *testing.B) {
	ctxWithID := context.WithValue(context.Background(), "X-Request-Id", "req-12345-abcde")
	ctxWithoutID := context.Background()

	b.Run("with request id", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ExtractRequestID(ctxWithID)
		}
	})

	b.Run("without request id", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ExtractRequestID(ctxWithoutID)
		}
	})
}

func BenchmarkConfigValidation(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = &Config{
			Host:            "localhost:9000",
			Database:        "test",
			Username:        "default",
			Password:        "password",
			DialTimeout:     10,
			MaxOpenConns:    25,
			MaxIdleConns:    10,
			ConnMaxLifeTime: 60,
			ConnStrategy:    "round_robin",
		}
	}
}

func BenchmarkNoOpTracer(b *testing.B) {
	tracer := &NoOpTracer{}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, span := tracer.StartSpan(ctx, "test", nil)
		tracer.EndSpan(span, nil, time.Now())
	}
}

func BenchmarkDetectQueryType_Parallel(b *testing.B) {
	queries := []string{
		"SELECT * FROM users",
		"INSERT INTO users VALUES (1)",
		"UPDATE users SET name = 'test'",
		"DELETE FROM users",
		"CREATE TABLE test",
	}

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			detectQueryType(queries[i%len(queries)])
			i++
		}
	})
}

func BenchmarkClassifyError_Parallel(b *testing.B) {
	errors := []error{
		nil,
		context.DeadlineExceeded,
		fmt.Errorf("connection timeout"),
		fmt.Errorf("syntax error"),
		fmt.Errorf("unknown error"),
	}

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			classifyError(errors[i%len(errors)])
			i++
		}
	})
}
