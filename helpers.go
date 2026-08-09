package clickhouse_connector

import (
	"context"
	"errors"
	"strings"

	"go.opentelemetry.io/otel/attribute"
)

const maxQueryAttrLength = 500

func buildQueryAttributes(config *Config, query, operation string, argsCount int) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("db.system", "clickhouse"),
		attribute.String("db.statement", truncateQuery(query, maxQueryAttrLength)),
		attribute.String("db.operation", operation),
		attribute.String("db.name", config.Database),
		attribute.String("db.user", config.Username),
		attribute.String("net.peer.name", config.Host),
		attribute.String("net.transport", "tcp"),
		attribute.String("clickhouse.query_type", detectQueryType(query)),
		attribute.Int("clickhouse.args_count", argsCount),
		attribute.String("clickhouse.strategy", config.ConnStrategy),
	}
}

func buildExecAttributes(config *Config, query string, argsCount int) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("db.system", "clickhouse"),
		attribute.String("db.statement", truncateQuery(query, maxQueryAttrLength)),
		attribute.String("db.operation", "exec"),
		attribute.String("db.name", config.Database),
		attribute.String("db.user", config.Username),
		attribute.String("net.peer.name", config.Host),
		attribute.String("net.transport", "tcp"),
		attribute.String("clickhouse.query_type", detectQueryType(query)),
		attribute.Int("clickhouse.args_count", argsCount),
	}
}

func truncateQuery(query string, maxLen int) string {
	query = strings.ReplaceAll(query, "\n", " ")
	query = strings.ReplaceAll(query, "\t", " ")
	query = strings.Join(strings.Fields(query), " ")

	if len(query) > maxLen {
		return query[:maxLen] + "..."
	}
	return query
}

func classifyError(err error) string {
	if err == nil {
		return "none"
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}

	errMsg := err.Error()
	switch {
	case strings.Contains(errMsg, "timeout"),
		strings.Contains(errMsg, "deadline exceeded"),
		strings.Contains(errMsg, "deadline"):
		return "timeout"
	case strings.Contains(errMsg, "connection refused"):
		return "connection_refused"
	case strings.Contains(errMsg, "connection reset"):
		return "connection_reset"
	case strings.Contains(errMsg, "connection"):
		return "connection_error"
	case strings.Contains(errMsg, "syntax"):
		return "syntax_error"
	case strings.Contains(errMsg, "authentication"):
		return "authentication_error"
	case strings.Contains(errMsg, "permission"),
		strings.Contains(errMsg, "access denied"):
		return "permission_error"
	case strings.Contains(errMsg, "not found"):
		return "not_found"
	case strings.Contains(errMsg, "duplicate"):
		return "duplicate_error"
	default:
		return "unknown"
	}
}

func detectQueryType(query string) string {
	upper := strings.ToUpper(strings.TrimSpace(query))
	switch {
	case strings.HasPrefix(upper, "SELECT"):
		return "select"
	case strings.HasPrefix(upper, "INSERT"):
		return "insert"
	case strings.HasPrefix(upper, "UPDATE"):
		return "update"
	case strings.HasPrefix(upper, "DELETE"):
		return "delete"
	case strings.HasPrefix(upper, "CREATE"):
		return "create"
	case strings.HasPrefix(upper, "DROP"):
		return "drop"
	case strings.HasPrefix(upper, "ALTER"):
		return "alter"
	case strings.HasPrefix(upper, "TRUNCATE"):
		return "truncate"
	case strings.HasPrefix(upper, "OPTIMIZE"):
		return "optimize"
	default:
		return "unknown"
	}
}
