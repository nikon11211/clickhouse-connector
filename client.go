package clickhouse_connector

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type conn interface {
	Ping(ctx context.Context) error
	Close() error
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
	QueryRow(ctx context.Context, query string, args ...any) driver.Row
	Exec(ctx context.Context, query string, args ...any) error
	Select(ctx context.Context, dest any, query string, args ...any) error
	AsyncInsert(ctx context.Context, query string, wait bool, args ...any) error
	PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error)
	Stats() driver.Stats
}

var openConnFunc = func(opts *clickhouse.Options) (conn, error) {
	return clickhouse.Open(opts)
}

type Client struct {
	Logger
	conn   conn
	tracer Tracer
	config *Config
}

type Logger interface {
	DebugF(msg string, args ...any)
	Debug(msg string)
	Info(msg string)
	Warn(msg string)
	Error(msg string)
}

type Option func(*options)

type options struct {
	tracer Tracer
	logger Logger
}

func New(config *Config, logger Logger, opts ...Option) (*Client, error) {
	const op = "clickhouse.New"

	o := &options{
		logger: logger,
	}
	for _, opt := range opts {
		opt(o)
	}

	if config == nil {
		return nil, fmt.Errorf("%s: config cannot be nil", op)
	}
	if logger == nil {
		logger = NoopLogger{}
	}

	clickhouseOpts := buildClickHouseOptions(config)

	conn, err := openConnFunc(clickhouseOpts)
	if err != nil {
		logger.Error(fmt.Sprintf("(%s) failed to connect to database: %s", op, err.Error()))
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	pingCtx, cancel := context.WithTimeout(context.Background(), time.Duration(config.DialTimeout)*time.Second)
	defer cancel()

	if err := conn.Ping(pingCtx); err != nil {
		logger.Error(fmt.Sprintf("(%s) database ping failed: %s", op, err.Error()))
		_ = conn.Close()
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	logger.Info(fmt.Sprintf("(%s) successfully connected to ClickHouse", op))

	client := &Client{
		Logger: logger,
		conn:   conn,
		tracer: o.tracer,
		config: config,
	}

	return client, nil
}

func WithTracer(tracer Tracer) Option {
	return func(o *options) {
		o.tracer = tracer
	}
}

func (c *Client) Close() error {
	const op = "clickhouse.Close"
	if err := c.conn.Close(); err != nil {
		c.Error(fmt.Sprintf("(%s) failed to close connection: %s", op, err.Error()))
		return fmt.Errorf("%s: %w", op, err)
	}
	c.Info(fmt.Sprintf("(%s) connection closed successfully", op))
	return nil
}

func (c *Client) Ping(ctx context.Context) error {
	return c.conn.Ping(ctx)
}

func (c *Client) Query(ctx context.Context, query string, args ...any) (rows driver.Rows, err error) {
	if c.tracer == nil {
		return c.conn.Query(ctx, query, args...)
	}

	startTime := time.Now()
	ctx, span := c.tracer.StartSpan(ctx, "clickhouse.query",
		buildQueryAttributes(c.config, query, "query", len(args)),
	)
	defer func() {
		c.tracer.EndSpan(span, err, startTime)
	}()

	rows, err = c.conn.Query(ctx, query, args...)
	return rows, err
}

func (c *Client) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	if c.tracer == nil {
		return c.conn.QueryRow(ctx, query, args...)
	}

	ctx, span := c.tracer.StartSpan(ctx, "clickhouse.query_row",
		buildQueryAttributes(c.config, query, "query_row", len(args)),
	)
	defer c.tracer.EndSpan(span, nil, time.Now())

	return c.conn.QueryRow(ctx, query, args...)
}

func (c *Client) Exec(ctx context.Context, query string, args ...any) (err error) {
	if c.tracer == nil {
		return c.conn.Exec(ctx, query, args...)
	}

	startTime := time.Now()
	ctx, span := c.tracer.StartSpan(ctx, "clickhouse.exec",
		buildExecAttributes(c.config, query, len(args)),
	)
	defer func() {
		c.tracer.EndSpan(span, err, startTime)
	}()

	err = c.conn.Exec(ctx, query, args...)
	return err
}

func (c *Client) Select(ctx context.Context, dest any, query string, args ...any) (err error) {
	if c.tracer == nil {
		return c.conn.Select(ctx, dest, query, args...)
	}

	startTime := time.Now()
	ctx, span := c.tracer.StartSpan(ctx, "clickhouse.select",
		buildExecAttributes(c.config, query, len(args)),
	)
	defer func() {
		c.tracer.EndSpan(span, err, startTime)
	}()

	err = c.conn.Select(ctx, dest, query, args...)
	return err
}

func (c *Client) AsyncInsert(ctx context.Context, query string, wait bool, args ...any) (err error) {
	if c.tracer == nil {
		return c.conn.AsyncInsert(ctx, query, wait, args...)
	}

	startTime := time.Now()
	ctx, span := c.tracer.StartSpan(ctx, "clickhouse.async_insert",
		buildExecAttributes(c.config, query, len(args)),
	)
	defer func() {
		c.tracer.EndSpan(span, err, startTime)
	}()

	err = c.conn.AsyncInsert(ctx, query, wait, args...)
	return err
}

func (c *Client) PrepareBatch(ctx context.Context, query string) (driver.Batch, error) {
	return c.conn.PrepareBatch(ctx, query)
}

func (c *Client) Stats() driver.Stats {
	return c.conn.Stats()
}

func buildClickHouseOptions(config *Config) *clickhouse.Options {
	opts := &clickhouse.Options{
		TLS: &tls.Config{
			InsecureSkipVerify: false,
		},
		Addr: strings.Split(config.Host, ","),
		Auth: clickhouse.Auth{
			Database: config.Database,
			Username: config.Username,
			Password: config.Password,
		},
		DialTimeout:      time.Duration(config.DialTimeout) * time.Second,
		MaxOpenConns:     config.MaxOpenConns,
		MaxIdleConns:     config.MaxIdleConns,
		ConnMaxLifetime:  time.Duration(config.ConnMaxLifeTime) * time.Minute,
		ConnOpenStrategy: clickhouse.ConnOpenRoundRobin,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		DialContext: func(ctx context.Context, addr string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", addr)
		},
	}

	switch config.ConnStrategy {
	case "round_robin":
		opts.ConnOpenStrategy = clickhouse.ConnOpenRoundRobin
	case "random":
		opts.ConnOpenStrategy = clickhouse.ConnOpenRandom
	case "in_order":
		opts.ConnOpenStrategy = clickhouse.ConnOpenInOrder
	default:
		opts.ConnOpenStrategy = clickhouse.ConnOpenRoundRobin
	}

	return opts
}
