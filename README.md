# ClickHouse Connector for Go

[![Go Reference](https://pkg.go.dev/badge/github.com/nikon11211/clickhouse-connector.svg)](https://pkg.go.dev/github.com/nikon11211/clickhouse-connector)
[![Go Report Card](https://goreportcard.com/badge/github.com/nikon11211/clickhouse-connector)](https://goreportcard.com/report/github.com/nikon11211/clickhouse-connector)
[![Tests](https://github.com/nikon11211/clickhouse-connector/actions/workflows/tests.yml/badge.svg)](https://github.com/nikon11211/clickhouse-connector/actions/workflows/tests.yml)
[![codecov](https://codecov.io/gh/nikon11211/clickhouse-connector/branch/main/graph/badge.svg)](https://codecov.io/gh/nikon11211/clickhouse-connector)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Go Version](https://img.shields.io/github/go-mod/go-version/nikon11211/clickhouse-connector)](https://go.dev/)

> 🚀 Production-ready ClickHouse client for Go with built-in OpenTelemetry tracing, connection pooling, and comprehensive error handling.

## ✨ Features

- 🔌 **Robust Connection Management** - Connection pooling with configurable strategies (round-robin, random, in-order)
- 📊 **OpenTelemetry Integration** - Automatic distributed tracing for all database operations
- 🛡️ **Production-Ready** - Built-in retry logic, health checks, and graceful shutdown
- 🎯 **Type-Safe API** - Full support for prepared statements and batch operations
- 🔍 **Observability** - Structured logging interface and connection pool metrics
- ⚡ **High Performance** - LZ4 compression and optimized connection handling
- 🧩 **Pluggable Architecture** - Custom tracers and loggers support
- 🔒 **TLS Support** - Secure connections to ClickHouse Cloud and self-managed clusters

## 📦 Installation

```bash
go get github.com/nikon11211/clickhouse-connector
```

## 🚀 Quick Start
```go
package main

import (
    "context"
    "log"
    
    clickhouse "github.com/nikon11211/clickhouse-connector"
)

type MyLogger struct{}

func (l *MyLogger) DebugF(msg string, args ...any) { log.Printf(msg, args...) }
func (l *MyLogger) Debug(msg string)               { log.Println(msg) }
func (l *MyLogger) Info(msg string)                { log.Println(msg) }
func (l *MyLogger) Warn(msg string)                { log.Println(msg) }
func (l *MyLogger) Error(msg string)               { log.Println(msg) }

func main() {
    config := &clickhouse.Config{
        Host:            "localhost:9000",
        Database:        "analytics",
        Username:        "default",
        Password:        "",
        DialTimeout:     10,
        MaxOpenConns:    25,
        MaxIdleConns:    10,
        ConnMaxLifeTime: 60,
        ConnStrategy:    "round_robin",
    }

    client, err := clickhouse.New(config, &MyLogger{})
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    ctx := context.Background()
    err = client.Exec(ctx, "INSERT INTO events (id, name) VALUES (?, ?)", 1, "page_view")
    
    type Event struct {
        ID   uint64 `ch:"id"`
        Name string `ch:"name"`
    }
    
    var events []Event
    err = client.Select(ctx, &events, "SELECT * FROM events")
}
```

## 🎯 Usage Examples

### With OpenTelemetry Tracing

```go
tracer := clickhouse.NewOpenTelemetryTracer(
otel.Tracer("clickhouse-service"),
)

client, err := clickhouse.New(config, logger,
clickhouse.WithTracer(tracer),
)
```

### Batch Operations

```go
batch, err := client.PrepareBatch(ctx, "INSERT INTO metrics")
if err != nil {
return err
}

for _, metric := range metrics {
batch.Append(
metric.Timestamp,
metric.Name,
metric.Value,
)
}

return batch.Send()
```

### Connection Pool Stats

```go
stats := client.Stats()
fmt.Printf("Open: %d, Idle: %d, MaxOpen: %d\n",
stats.Open, stats.Idle, stats.MaxOpenConns)
```

## 🔧 Advanced Usage

### Custom Logger Implementation

```go
type StructuredLogger struct {
logger *zap.Logger
}

func (l *StructuredLogger) Info(msg string) {
l.logger.Info(msg)
}
```

### Clustered Deployment

```go
config := &clickhouse.Config{
Host: "node1:9000,node2:9000,node3:9000",
ConnStrategy: "round_robin",
}
```

## 🧪 Testing

```bash
# Run unit tests
go test -v ./...

# Run with coverage
go test -v -race -coverprofile=coverage.txt -covermode=atomic ./...

# Run benchmarks
go test -bench=. -benchmem ./...

# Integration tests (requires ClickHouse)
CLICKHOUSE_HOST=localhost:9000 go test -tags=integration ./...
```

## 📚 Related Projects

### ClickHouse Go - Official ClickHouse Go client
### OpenTelemetry Go - OpenTelemetry Go SDK