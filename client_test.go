package clickhouse_connector

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
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

func newMockLogger() *MockLogger {
	l := new(MockLogger)
	l.On("Info", mock.Anything).Return()
	l.On("Error", mock.Anything).Return()
	l.On("Debug", mock.Anything).Return()
	l.On("DebugF", mock.Anything, mock.Anything).Return()
	l.On("Warn", mock.Anything).Return()
	return l
}

type fakeConn struct {
	pingErr  error
	closeErr error
	execErr  error
	queryErr error
	closed   bool
	pings    int
	opts     *clickhouse.Options
}

func (f *fakeConn) Ping(ctx context.Context) error {
	f.pings++
	return f.pingErr
}

func (f *fakeConn) Close() error {
	f.closed = true
	return f.closeErr
}

func (f *fakeConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	if f.queryErr != nil {
		return nil, f.queryErr
	}
	return &fakeRows{}, nil
}

func (f *fakeConn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	return &fakeRow{}
}

func (f *fakeConn) Exec(ctx context.Context, query string, args ...any) error {
	return f.execErr
}

func (f *fakeConn) Select(ctx context.Context, dest any, query string, args ...any) error {
	return f.execErr
}

func (f *fakeConn) AsyncInsert(ctx context.Context, query string, wait bool, args ...any) error {
	return f.execErr
}

func (f *fakeConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	return &fakeBatch{}, nil
}

func (f *fakeConn) Stats() driver.Stats {
	return driver.Stats{MaxOpenConns: 10, MaxIdleConns: 5, Open: 1, Idle: 1}
}

type fakeRows struct{}

func (r *fakeRows) Next() bool                       { return false }
func (r *fakeRows) Scan(dest ...any) error           { return nil }
func (r *fakeRows) ScanStruct(dest any) error        { return nil }
func (r *fakeRows) Columns() []string                { return nil }
func (r *fakeRows) ColumnTypes() []driver.ColumnType { return nil }
func (r *fakeRows) HasData() bool                    { return false }
func (r *fakeRows) Totals(dest ...any) error         { return nil }
func (r *fakeRows) Close() error                     { return nil }
func (r *fakeRows) Err() error                       { return nil }

type fakeRow struct{}

func (r *fakeRow) Scan(dest ...any) error    { return nil }
func (r *fakeRow) ScanStruct(dest any) error { return nil }
func (r *fakeRow) Err() error                { return nil }

type fakeBatch struct{}

func (b *fakeBatch) Abort() error                  { return nil }
func (b *fakeBatch) Append(v ...any) error         { return nil }
func (b *fakeBatch) AppendStruct(v any) error      { return nil }
func (b *fakeBatch) Column(int) driver.BatchColumn { return nil }
func (b *fakeBatch) Flush() error                  { return nil }
func (b *fakeBatch) Send() error                   { return nil }
func (b *fakeBatch) IsSent() bool                  { return false }
func (b *fakeBatch) Rows() int                     { return 0 }
func (b *fakeBatch) Columns() []column.Interface   { return nil }
func (b *fakeBatch) Close() error                  { return nil }

func testConfig() *Config {
	return &Config{
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
}

func withFakeConn(t *testing.T, f *fakeConn) {
	t.Helper()
	orig := openConnFunc
	openConnFunc = func(opts *clickhouse.Options) (conn, error) {
		f.opts = opts
		return f, nil
	}
	t.Cleanup(func() { openConnFunc = orig })
}

func TestNewClient_Success(t *testing.T) {
	f := &fakeConn{}
	withFakeConn(t, f)

	client, err := New(testConfig(), newMockLogger())
	require.NoError(t, err)
	require.NotNil(t, client)
	assert.Equal(t, 1, f.pings)
	assert.NotNil(t, f.opts)

	require.NoError(t, client.Close())
	assert.True(t, f.closed)
}

func TestNewClient_NilConfig(t *testing.T) {
	client, err := New(nil, newMockLogger())
	require.Error(t, err)
	assert.Nil(t, client)
	assert.Contains(t, err.Error(), "config cannot be nil")
}

func TestNewClient_OpenError(t *testing.T) {
	orig := openConnFunc
	defer func() { openConnFunc = orig }()
	openConnFunc = func(opts *clickhouse.Options) (conn, error) {
		return nil, errors.New("dial failed")
	}

	client, err := New(testConfig(), newMockLogger())
	require.Error(t, err)
	assert.Nil(t, client)
	assert.Contains(t, err.Error(), "dial failed")
}

func TestNewClient_PingError(t *testing.T) {
	f := &fakeConn{pingErr: errors.New("ping failed")}
	withFakeConn(t, f)

	client, err := New(testConfig(), newMockLogger())
	require.Error(t, err)
	assert.Nil(t, client)
	assert.Contains(t, err.Error(), "ping failed")
	assert.True(t, f.closed, "connection must be closed after failed ping")
}

func TestNewClient_NilLogger(t *testing.T) {
	f := &fakeConn{}
	withFakeConn(t, f)

	client, err := New(testConfig(), nil)
	require.NoError(t, err)
	require.NotNil(t, client)
	require.NoError(t, client.Close())
}

func TestCloseError(t *testing.T) {
	f := &fakeConn{closeErr: errors.New("close failed")}
	withFakeConn(t, f)

	client, err := New(testConfig(), newMockLogger())
	require.NoError(t, err)

	err = client.Close()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "close failed")
}

func TestPing(t *testing.T) {
	f := &fakeConn{}
	withFakeConn(t, f)

	client, err := New(testConfig(), newMockLogger())
	require.NoError(t, err)
	defer client.Close()

	require.NoError(t, client.Ping(context.Background()))
	f.pingErr = errors.New("down")
	require.Error(t, client.Ping(context.Background()))
}

func TestQueryWithoutTracer(t *testing.T) {
	f := &fakeConn{queryErr: errors.New("query failed")}
	withFakeConn(t, f)

	client, err := New(testConfig(), newMockLogger())
	require.NoError(t, err)
	defer client.Close()

	rows, err := client.Query(context.Background(), "SELECT 1")
	require.Error(t, err)
	assert.Nil(t, rows)
}

func TestQueryRowWithoutTracer(t *testing.T) {
	f := &fakeConn{}
	withFakeConn(t, f)

	client, err := New(testConfig(), newMockLogger())
	require.NoError(t, err)
	defer client.Close()

	row := client.QueryRow(context.Background(), "SELECT 1")
	require.NoError(t, row.Scan())
}

func TestExecWithoutTracer(t *testing.T) {
	f := &fakeConn{}
	withFakeConn(t, f)

	client, err := New(testConfig(), newMockLogger())
	require.NoError(t, err)
	defer client.Close()

	require.NoError(t, client.Exec(context.Background(), "INSERT INTO t VALUES (1)"))

	f.execErr = errors.New("insert failed")
	require.Error(t, client.Exec(context.Background(), "INSERT INTO t VALUES (2)"))
}

func TestSelectWithoutTracer(t *testing.T) {
	f := &fakeConn{}
	withFakeConn(t, f)

	client, err := New(testConfig(), newMockLogger())
	require.NoError(t, err)
	defer client.Close()

	var dest []int
	require.NoError(t, client.Select(context.Background(), &dest, "SELECT 1"))
}

func TestAsyncInsertWithoutTracer(t *testing.T) {
	f := &fakeConn{}
	withFakeConn(t, f)

	client, err := New(testConfig(), newMockLogger())
	require.NoError(t, err)
	defer client.Close()

	require.NoError(t, client.AsyncInsert(context.Background(), "INSERT INTO t VALUES (1)", true))
}

func TestPrepareBatchWithoutTracer(t *testing.T) {
	f := &fakeConn{}
	withFakeConn(t, f)

	client, err := New(testConfig(), newMockLogger())
	require.NoError(t, err)
	defer client.Close()

	batch, err := client.PrepareBatch(context.Background(), "INSERT INTO t VALUES (1)")
	require.NoError(t, err)
	require.NoError(t, batch.Send())
}

func TestStats(t *testing.T) {
	f := &fakeConn{}
	withFakeConn(t, f)

	client, err := New(testConfig(), newMockLogger())
	require.NoError(t, err)
	defer client.Close()

	stats := client.Stats()
	assert.Equal(t, 10, stats.MaxOpenConns)
	assert.Equal(t, 1, stats.Idle)
}

func TestWithTracerOption(t *testing.T) {
	tr := &NoOpTracer{}
	f := &fakeConn{}
	withFakeConn(t, f)

	client, err := New(testConfig(), newMockLogger(), WithTracer(tr))
	require.NoError(t, err)
	defer client.Close()
	assert.NotNil(t, client.tracer)
}

func TestQueriesWithTracer(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	tr := NewOpenTelemetryTracer(tp.Tracer("test"))
	f := &fakeConn{}
	withFakeConn(t, f)

	client, err := New(testConfig(), newMockLogger(), WithTracer(tr))
	require.NoError(t, err)
	defer client.Close()

	rows, err := client.Query(context.Background(), "SELECT 1")
	require.NoError(t, err)
	rows.Close()

	client.QueryRow(context.Background(), "SELECT 1").Scan()

	require.NoError(t, client.Exec(context.Background(), "INSERT INTO t VALUES (1)"))
	var dest []int
	require.NoError(t, client.Select(context.Background(), &dest, "SELECT 1"))
	require.NoError(t, client.AsyncInsert(context.Background(), "INSERT INTO t VALUES (1)", true))

	assert.GreaterOrEqual(t, len(recorder.Ended()), 5)
	for _, span := range recorder.Ended() {
		assert.Equal(t, trace.SpanKindClient, span.SpanKind())
	}
}

func TestQueriesWithTracerError(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	tr := NewOpenTelemetryTracer(tp.Tracer("test"))
	f := &fakeConn{execErr: errors.New("boom")}
	withFakeConn(t, f)

	client, err := New(testConfig(), newMockLogger(), WithTracer(tr))
	require.NoError(t, err)
	defer client.Close()

	require.Error(t, client.Exec(context.Background(), "INSERT INTO t VALUES (1)"))

	spans := recorder.Ended()
	require.NotEmpty(t, spans)
	status := spans[0].Status()
	assert.Equal(t, codes.Error, status.Code)
}

func TestBuildClickHouseOptions(t *testing.T) {
	for _, tc := range []struct {
		name     string
		strategy string
		expected clickhouse.ConnOpenStrategy
	}{
		{name: "round robin", strategy: "round_robin", expected: clickhouse.ConnOpenRoundRobin},
		{name: "random", strategy: "random", expected: clickhouse.ConnOpenRandom},
		{name: "in order", strategy: "in_order", expected: clickhouse.ConnOpenInOrder},
		{name: "default", strategy: "unknown", expected: clickhouse.ConnOpenRoundRobin},
		{name: "empty", strategy: "", expected: clickhouse.ConnOpenRoundRobin},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig()
			cfg.ConnStrategy = tc.strategy
			opts := buildClickHouseOptions(cfg)
			assert.Equal(t, tc.expected, opts.ConnOpenStrategy)
			assert.Equal(t, []string{"localhost:9000"}, opts.Addr)
			assert.NotNil(t, opts.TLS)
			assert.NotNil(t, opts.DialContext)
		})
	}
}

func TestDialContextHook(t *testing.T) {
	opts := buildClickHouseOptions(testConfig())

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	conn, err := opts.DialContext(context.Background(), l.Addr().String())
	require.NoError(t, err)
	require.NoError(t, conn.Close())
}

func TestDefaultOpenConnFunc(t *testing.T) {
	c, err := openConnFunc(&clickhouse.Options{Addr: []string{"127.0.0.1:1"}})
	require.NoError(t, err)
	require.NotNil(t, c)
	require.NoError(t, c.Close())
}

func TestNoopLoggerUsed(t *testing.T) {
	f := &fakeConn{}
	withFakeConn(t, f)
	client, err := New(testConfig(), NoopLogger{})
	require.NoError(t, err)
	require.NoError(t, client.Close())

	f2 := &fakeConn{pingErr: errors.New("down")}
	withFakeConn(t, f2)
	_, err = New(testConfig(), NoopLogger{})
	require.Error(t, err)
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
		{"drop", "DROP TABLE test", "drop"},
		{"alter", "ALTER TABLE users ADD COLUMN age UInt8", "alter"},
		{"truncate", "TRUNCATE TABLE users", "truncate"},
		{"optimize", "OPTIMIZE TABLE users FINAL", "optimize"},
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

	short := truncateQuery("SELECT 1", 500)
	assert.Equal(t, "SELECT 1", short)

	multiline := truncateQuery("SELECT\n\t1", 500)
	assert.Equal(t, "SELECT 1", multiline)
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
			name:     "deadline word",
			err:      fmt.Errorf("deadline"),
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

func TestOpenTelemetryTracer(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	tr := NewOpenTelemetryTracer(tp.Tracer("test"))

	ctx, span := tr.StartSpan(context.Background(), "op", nil)
	assert.NotNil(t, span)
	tr.EndSpan(span, nil, time.Now())

	_, span = tr.StartSpan(ctx, "op2", nil)
	tr.EndSpan(span, errors.New("failed"), time.Now())

	tr.EndSpan("not-a-span", nil, time.Now())

	spans := recorder.Ended()
	assert.Len(t, spans, 2)
}

func TestNoOpTracer(t *testing.T) {
	tr := &NoOpTracer{}
	ctx, span := tr.StartSpan(context.Background(), "op", nil)
	assert.Nil(t, span)
	assert.Equal(t, context.Background(), ctx)
	tr.EndSpan(span, nil, time.Now())
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
