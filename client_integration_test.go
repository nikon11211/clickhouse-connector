//go:build integration
// +build integration

package clickhouse_connector

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func getTestConfig() *Config {
	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		host = "localhost:9000"
	}

	db := os.Getenv("CLICKHOUSE_DB")
	if db == "" {
		db = "test"
	}

	user := os.Getenv("CLICKHOUSE_USER")
	if user == "" {
		user = "default"
	}

	password := os.Getenv("CLICKHOUSE_PASSWORD")

	return &Config{
		Host:            host,
		Database:        db,
		Username:        user,
		Password:        password,
		DialTimeout:     10,
		MaxOpenConns:    25,
		MaxIdleConns:    10,
		ConnMaxLifeTime: 60,
		ConnStrategy:    "round_robin",
	}
}

func TestIntegration_NewClient(t *testing.T) {
	config := getTestConfig()
	mockLogger := new(MockLogger)
	mockLogger.On("Info", mock.Anything).Return()
	mockLogger.On("Error", mock.Anything).Return()

	client, err := New(config, mockLogger)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	assert.NotNil(t, client)

	err = client.Ping(context.Background())
	assert.NoError(t, err)
}

func TestIntegration_QueryOperations(t *testing.T) {
	config := getTestConfig()
	mockLogger := new(MockLogger)
	mockLogger.On("Info", mock.Anything).Return()
	mockLogger.On("Error", mock.Anything).Return()

	client, err := New(config, mockLogger)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	createTableSQL := `
		CREATE TABLE IF NOT EXISTS test_users (
			id UInt64,
			name String,
			email String
		) ENGINE = Memory
	`
	err = client.Exec(ctx, createTableSQL)
	assert.NoError(t, err)

	err = client.Exec(ctx, "INSERT INTO test_users (id, name, email) VALUES (?, ?, ?)",
		1, "John Doe", "john@example.com")
	assert.NoError(t, err)

	rows, err := client.Query(ctx, "SELECT * FROM test_users WHERE id = ?", 1)
	assert.NoError(t, err)
	defer rows.Close()
	assert.True(t, rows.Next())

	var id uint64
	var name, email string
	err = rows.Scan(&id, &name, &email)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), id)
	assert.Equal(t, "John Doe", name)

	type User struct {
		ID    uint64 `ch:"id"`
		Name  string `ch:"name"`
		Email string `ch:"email"`
	}

	var users []User
	err = client.Select(ctx, &users, "SELECT * FROM test_users ORDER BY id")
	assert.NoError(t, err)
	assert.Len(t, users, 1)
	assert.Equal(t, "John Doe", users[0].Name)

	err = client.Exec(ctx, "DROP TABLE IF EXISTS test_users")
	assert.NoError(t, err)
}

func TestIntegration_WithTracer(t *testing.T) {
	config := getTestConfig()
	mockLogger := new(MockLogger)
	mockLogger.On("Info", mock.Anything).Return()
	mockLogger.On("Error", mock.Anything).Return()

	tracer := &NoOpTracer{}

	client, err := New(config, mockLogger, WithTracer(tracer))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	err = client.Exec(ctx, "SELECT 1")
	assert.NoError(t, err)
}

func TestIntegration_ConnectionPool(t *testing.T) {
	config := getTestConfig()
	mockLogger := new(MockLogger)
	mockLogger.On("Info", mock.Anything).Return()
	mockLogger.On("Error", mock.Anything).Return()

	client, err := New(config, mockLogger)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	stats := client.Stats()
	assert.NotNil(t, stats)
	assert.GreaterOrEqual(t, stats.MaxOpenConns, 0)
}
