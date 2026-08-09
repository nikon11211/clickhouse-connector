package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	clickhouse "github.com/nikon11211/clickhouse-connector"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/trace"
)

type SimpleLogger struct{}

func (l *SimpleLogger) DebugF(msg string, args ...any) {
	log.Printf("[DEBUG] "+msg, args...)
}

func (l *SimpleLogger) Debug(msg string) {
	log.Println("[DEBUG]", msg)
}

func (l *SimpleLogger) Info(msg string) {
	log.Println("[INFO]", msg)
}

func (l *SimpleLogger) Warn(msg string) {
	log.Println("[WARN]", msg)
}

func (l *SimpleLogger) Error(msg string) {
	log.Println("[ERROR]", msg)
}

type User struct {
	ID        uint64    `ch:"id"`
	Name      string    `ch:"name"`
	Email     string    `ch:"email"`
	CreatedAt time.Time `ch:"created_at"`
}

func main() {
	tp := initTracer()
	defer func() { _ = tp.Shutdown(context.Background()) }()

	config := &clickhouse.Config{
		Host:            getEnv("CLICKHOUSE_HOST", "localhost:9000"),
		Database:        getEnv("CLICKHOUSE_DB", "default"),
		Username:        getEnv("CLICKHOUSE_USER", "default"),
		Password:        getEnv("CLICKHOUSE_PASSWORD", ""),
		DialTimeout:     10,
		MaxOpenConns:    25,
		MaxIdleConns:    10,
		ConnMaxLifeTime: 60,
		ConnStrategy:    "round_robin",
	}

	logger := &SimpleLogger{}
	otelTracer := clickhouse.NewOpenTelemetryTracer(otel.Tracer("clickhouse-example"))

	client, err := clickhouse.New(config, logger, clickhouse.WithTracer(otelTracer))
	if err != nil {
		log.Fatalf("Failed to create ClickHouse client: %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()

	if err := createTable(ctx, client); err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	if err := insertSampleData(ctx, client); err != nil {
		log.Fatalf("Failed to insert data: %v", err)
	}

	users, err := getUsers(ctx, client)
	if err != nil {
		log.Fatalf("Failed to query users: %v", err)
	}

	fmt.Printf("\n📊 Users in database:\n")
	fmt.Println(strings.Repeat("-", 50))
	for _, user := range users {
		fmt.Printf("ID: %d | Name: %s | Email: %s | Created: %s\n",
			user.ID, user.Name, user.Email, user.CreatedAt.Format("2006-01-02 15:04:05"))
	}

	stats := client.Stats()
	fmt.Printf("\n📈 Connection Pool Stats:\n")
	fmt.Println(strings.Repeat("-", 50))
	fmt.Printf("Open Connections: %d\n", stats.Open)
	fmt.Printf("Idle Connections: %d\n", stats.Idle)
	fmt.Printf("Max Open Connections: %d\n", stats.MaxOpenConns)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	fmt.Println("\n👋 Shutting down gracefully...")
}

func createTable(ctx context.Context, client *clickhouse.Client) error {
	query := `
		CREATE TABLE IF NOT EXISTS users (
			id UInt64,
			name String,
			email String,
			created_at DateTime
		) ENGINE = MergeTree()
		ORDER BY id
	`
	return client.Exec(ctx, query)
}

func insertSampleData(ctx context.Context, client *clickhouse.Client) error {
	batch, err := client.PrepareBatch(ctx, "INSERT INTO users")
	if err != nil {
		return err
	}

	users := []User{
		{ID: 1, Name: "Alice Johnson", Email: "alice@example.com", CreatedAt: time.Now()},
		{ID: 2, Name: "Bob Smith", Email: "bob@example.com", CreatedAt: time.Now()},
		{ID: 3, Name: "Charlie Brown", Email: "charlie@example.com", CreatedAt: time.Now()},
	}

	for _, user := range users {
		if err := batch.Append(user.ID, user.Name, user.Email, user.CreatedAt); err != nil {
			return err
		}
	}

	return batch.Send()
}

func getUsers(ctx context.Context, client *clickhouse.Client) ([]User, error) {
	var users []User
	err := client.Select(ctx, &users, "SELECT * FROM users ORDER BY id")
	return users, err
}

func initTracer() *trace.TracerProvider {
	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		log.Fatal(err)
	}

	tp := trace.NewTracerProvider(
		trace.WithBatcher(exporter),
		trace.WithSampler(trace.AlwaysSample()),
	)

	otel.SetTracerProvider(tp)
	return tp
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
