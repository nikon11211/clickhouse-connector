package clickhouse_connector

// Config represents ClickHouse connection configuration
type Config struct {
	// Host is the ClickHouse server address (single host or comma-separated cluster nodes)
	Host string `mapstructure:"host" validate:"required"`

	// Database is the default database name
	Database string `mapstructure:"database" validate:"required"`

	// Username for authentication
	Username string `mapstructure:"username" validate:"required"`

	// Password for authentication
	Password string `mapstructure:"password" validate:"required"`

	// DialTimeout is the connection timeout in seconds
	DialTimeout int `mapstructure:"dial_timeout" validate:"required"`

	// MaxOpenConns is the maximum number of open connections
	MaxOpenConns int `mapstructure:"max_open_conns" validate:"required"`

	// MaxIdleConns is the maximum number of idle connections
	MaxIdleConns int `mapstructure:"max_idle_conns" validate:"required"`

	// ConnMaxLifeTime is the maximum connection lifetime in minutes
	ConnMaxLifeTime int `mapstructure:"conn_max_lifetime" validate:"required"`

	// ConnStrategy defines the connection strategy: "round_robin", "random", or "in_order"
	ConnStrategy string `mapstructure:"conn_strategy"`
}
