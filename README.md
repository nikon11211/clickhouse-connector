<p align="center">
  <h1 align="center">ClickHouse-Connector — Cluster-Aware ClickHouse Client for Go</h1>
</p>

<p align="center">
  <a href="https://pkg.go.dev/github.com/nikon11211/clickhouse-connector">
    <img src="https://pkg.go.dev/badge/github.com/nikon11211/clickhouse-connector.svg" alt="Go Reference"/>
  </a>
  <a href="https://goreportcard.com/report/github.com/nikon11211/clickhouse-connector">
    <img src="https://goreportcard.com/badge/github.com/nikon11211/clickhouse-connector" alt="Go Report Card"/>
  </a>
  <a href="https://github.com/nikon11211/clickhouse-connector/actions/workflows/test.yaml">
    <img src="https://github.com/nikon11211/clickhouse-connector/actions/workflows/test.yaml/badge.svg" alt="Tests"/>
  </a>
  <a href="https://codecov.io/gh/nikon11211/clickhouse-connector">
    <img src="https://codecov.io/gh/nikon11211/clickhouse-connector/branch/main/graph/badge.svg" alt="Coverage"/>
  </a>
  <a href="https://sonarcloud.io/summary/overall?id=nikon11211_clickhouse-connector">
    <img src="https://sonarcloud.io/api/project_badges/measure?project=nikon11211_clickhouse-connector&metric=coverage" alt="SonarCloud Coverage"/>
  </a>
  <a href="https://opensource.org/licenses/MIT">
    <img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT"/>
  </a>
  <a href="https://golang.org/">
    <img src="https://img.shields.io/badge/Go-%3E%3D%201.26-blue" alt="Go Version"/>
  </a>
</p>

<p align="center">
  <b>A production-ready ClickHouse client for Go microservices</b><br/>
  <i>Cluster nodes • Connection strategies • OpenTelemetry tracing • Structured error classification</i>
</p>

---

## Overview

`clickhouse-connector` wraps the official
[clickhouse-go](https://github.com/ClickHouse/clickhouse-go) driver with the
features microservices need:

- **multi-node cluster support** — pass a comma-separated list of nodes, choose
  the connection strategy (`round_robin`, `random`, `in_order`);
- **automatic OpenTelemetry tracing** — every query, insert and batch operation
  is traced with rich semantic attributes and duration;
- **error classification** — errors are categorized (`timeout`,
  `connection_refused`, `syntax_error`, ...) and recorded on spans;
- **connection pooling** — timeouts, max open/idle connections and connection
  lifetime configured from a single `Config` struct;

## Install

```bash
go get github.com/nikon11211/clickhouse-connector
```

## Quick Start

```go
package main

import (
	"context"
	"fmt"

	"github.com/nikon11211/clickhouse-connector"
)

func main() {
	config := &clickhouse_connector.Config{
		Host:            "ch-1:9000,ch-2:9000",
		Database:        "analytics",
		Username:        "default",
		Password:        "",
		DialTimeout:     5,
		MaxOpenConns:    25,
		MaxIdleConns:    10,
		ConnMaxLifeTime: 60,
		ConnStrategy:    "round_robin",
	}

	client, err := clickhouse_connector.New(config, clickhouse_connector.NoopLogger{})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	var count uint64
	if err := client.Select(context.Background(), &count, "SELECT count() FROM events"); err != nil {
		panic(err)
	}
	fmt.Println("events:", count)
}
```

A complete runnable example lives in [`examples`](examples/).

## Configuration

| Field             | Type   | Description                                              |
|-------------------|--------|----------------------------------------------------------|
| `Host`            | string | Single node or comma-separated cluster nodes             |
| `Database`        | string | Default database name                                    |
| `Username`/`Password` | string | Credentials                                        |
| `DialTimeout`     | int    | Connection and ping timeout, seconds                     |
| `MaxOpenConns`    | int    | Maximum open connections                                 |
| `MaxIdleConns`    | int    | Maximum idle connections                                 |
| `ConnMaxLifeTime` | int    | Connection lifetime, minutes                             |
| `ConnStrategy`    | string | `round_robin`, `random` or `in_order` (default `round_robin`) |

All fields carry `mapstructure` tags for YAML/JSON configuration.

## Client API

| Method                 | Description                                   |
|------------------------|-----------------------------------------------|
| `Ping(ctx)`            | Health check                                  |
| `Query(ctx, sql, args...)` | Run a query, return `driver.Rows`         |
| `QueryRow(ctx, sql, args...)` | Run a query, return a single row       |
| `Exec(ctx, sql, args...)` | Execute a statement (INSERT/DDL)          |
| `Select(ctx, dest, sql, args...)` | Scan results into a struct/slice |
| `AsyncInsert(ctx, sql, wait, args...)` | Fire-and-forget insert      |
| `PrepareBatch(ctx, sql)` | Batch insert for high throughput           |
| `Stats()`             | Driver pool statistics                        |

## Tracing

```go
client, _ := clickhouse_connector.New(
	config,
	clickhouse_connector.NoopLogger{},
	clickhouse_connector.WithTracer(clickhouse_connector.NewOpenTelemetryTracer(provider.Tracer("app"))),
)
```

Spans are created with client kind and decorated with `db.system`,
`db.statement` (truncated to 500 chars), `db.operation`, `db.name`,
`clickhouse.query_type`, `clickhouse.args_count`, `clickhouse.strategy`,
`duration_ms`, and — on failure — `error.type` and `error` attributes.

## Error Classification

`classifyError` maps errors to stable categories: `none`, `timeout`,
`canceled`, `connection_refused`, `connection_reset`, `connection_error`,
`syntax_error`, `authentication_error`, `permission_error`, `not_found`,
`duplicate_error`, `unknown`.

## Testing & Coverage

Unit tests reach 100% statement coverage (excluding `examples/`) and run
entirely against a fake driver connection — no ClickHouse instance needed:

```bash
go test -race -coverprofile=coverage.txt -covermode=atomic $(go list ./... | grep -v /examples)
```

Run benchmarks:

```bash
go test -bench=. -benchmem -run '^$' .
```

| Benchmark               | What it measures                      |
|-------------------------|---------------------------------------|
| `BenchmarkDetectQueryType`| Query/insert classification logic    |

Integration tests (build tag `integration`) run against a real ClickHouse
server; CI spins one up as a service container:

```bash
go test -v -tags=integration ./...
```

CI enforces the 100% gate, runs `go vet`, benchmarks and
[golangci-lint](https://golangci-lint.run), and publishes coverage to Codecov
and SonarCloud.

## License

[MIT](LICENSE)
