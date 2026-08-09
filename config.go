package clickhouse_connector

type Config struct {
	Host string `mapstructure:"host" validate:"required"`

	Database string `mapstructure:"database" validate:"required"`

	Username string `mapstructure:"username" validate:"required"`

	Password string `mapstructure:"password" validate:"required"`

	DialTimeout int `mapstructure:"dial_timeout" validate:"required"`

	MaxOpenConns int `mapstructure:"max_open_conns" validate:"required"`

	MaxIdleConns int `mapstructure:"max_idle_conns" validate:"required"`

	ConnMaxLifeTime int `mapstructure:"conn_max_lifetime" validate:"required"`

	ConnStrategy string `mapstructure:"conn_strategy"`
}
