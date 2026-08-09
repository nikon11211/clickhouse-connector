package clickhouse_connector

type NoopLogger struct{}

func (NoopLogger) DebugF(msg string, args ...any) {}
func (NoopLogger) Debug(msg string)               {}
func (NoopLogger) Info(msg string)                {}
func (NoopLogger) Warn(msg string)                {}
func (NoopLogger) Error(msg string)               {}
