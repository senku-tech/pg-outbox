package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the complete dispatcher configuration
type Config struct {
	Dispatcher DispatcherConfig `yaml:"dispatcher"`
	Database   DatabaseConfig   `yaml:"database"`
	NATS       NATSConfig       `yaml:"nats"`
	Logging    LoggingConfig    `yaml:"logging"`
	Metrics    MetricsConfig    `yaml:"metrics"`
}

// DispatcherConfig holds dispatcher-specific settings
type DispatcherConfig struct {
	InstanceID        string        `yaml:"instance_id"`         // Unique identifier for this instance
	BatchSize         int           `yaml:"batch_size"`          // Number of events to process per batch
	MaxAttempts       int           `yaml:"max_attempts"`        // Maximum retry attempts before giving up
	PollInterval      time.Duration `yaml:"poll_interval"`       // How often to poll for new events (POLLING_MODE)
	FallbackInterval  time.Duration `yaml:"fallback_interval"`   // How often to fallback to polling (LISTEN_MODE)
	Workers           int           `yaml:"workers"`             // Number of concurrent batch processors
	ShutdownGrace     time.Duration `yaml:"shutdown_grace"`      // Grace period for shutdown
}

// DatabaseConfig holds database connection settings
type DatabaseConfig struct {
	DSN            string `yaml:"dsn"`             // PostgreSQL connection string
	MaxConnections int    `yaml:"max_connections"` // Maximum number of connections
	MaxIdle        int    `yaml:"max_idle"`        // Maximum idle connections
}

// NATSConfig holds NATS client configuration
type NATSConfig struct {
	URL         string            `yaml:"url"`          // NATS server URL
	MaxRetries  int               `yaml:"max_retries"`  // Connection retry attempts
	RetryDelay  time.Duration     `yaml:"retry_delay"`  // Delay between retries
	JetStream   JetStreamConfig   `yaml:"jetstream"`    // JetStream configuration
	TopicMap    map[string]string `yaml:"topic_map"`    // Simple topic mapping (original -> target)
}

// JetStreamConfig holds JetStream-specific settings
type JetStreamConfig struct {
	Enabled    bool   `yaml:"enabled"`     // Whether to use JetStream
	StreamName string `yaml:"stream_name"` // Stream to publish to
}

// LoggingConfig holds logging configuration
type LoggingConfig struct {
	Level  string `yaml:"level"`  // Log level (debug, info, warn, error)
	Format string `yaml:"format"` // Log format (json, console)
}

// MetricsConfig holds metrics configuration
type MetricsConfig struct {
	Enabled bool   `yaml:"enabled"` // Whether to expose metrics
	Port    int    `yaml:"port"`    // Port for metrics endpoint
	Path    string `yaml:"path"`    // Path for metrics endpoint
}


// LoadConfig loads configuration from a YAML file
func LoadConfig(path string) (*Config, error) {
	// Set default values
	cfg := &Config{
		Dispatcher: DispatcherConfig{
			InstanceID:       getHostname(),
			BatchSize:        100,
			MaxAttempts:      3,
			PollInterval:     1 * time.Second,
			FallbackInterval: 30 * time.Second,
			Workers:          5,
			ShutdownGrace:    30 * time.Second,
		},
		Database: DatabaseConfig{
			MaxConnections: 20,
			MaxIdle:        10,
		},
		NATS: NATSConfig{
			URL:        "nats://localhost:4222",
			MaxRetries: 5,
			RetryDelay: 2 * time.Second,
			JetStream: JetStreamConfig{
				Enabled:    true,
				StreamName: "EVENTS",
			},
			TopicMap: make(map[string]string),
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "json",
		},
		Metrics: MetricsConfig{
			Enabled: true,
			Port:    8080,
			Path:    "/metrics",
		},
	}

	// Load from file if provided
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}

		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("failed to parse config: %w", err)
		}
	}

	// Override with environment variables
	cfg.applyEnvOverrides()

	// Validate configuration
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return cfg, nil
}

// applyEnvOverrides applies environment variable overrides
func (c *Config) applyEnvOverrides() {
	if dsn := os.Getenv("DATABASE_DSN"); dsn != "" {
		c.Database.DSN = dsn
	}
	if natsURL := os.Getenv("NATS_URL"); natsURL != "" {
		c.NATS.URL = natsURL
	}
	if instanceID := os.Getenv("DISPATCHER_INSTANCE_ID"); instanceID != "" {
		c.Dispatcher.InstanceID = instanceID
	}
	if logLevel := os.Getenv("LOG_LEVEL"); logLevel != "" {
		c.Logging.Level = logLevel
	}
}

// validate ensures the configuration is valid
func (c *Config) validate() error {
	if c.Database.DSN == "" {
		return fmt.Errorf("database DSN is required")
	}
	if c.Dispatcher.BatchSize <= 0 {
		return fmt.Errorf("batch size must be positive")
	}
	if c.Dispatcher.Workers <= 0 {
		return fmt.Errorf("workers must be positive")
	}
	if c.Dispatcher.MaxAttempts <= 0 {
		return fmt.Errorf("max attempts must be positive")
	}
	return nil
}

// getHostname returns the hostname or a default value
func getHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "dispatcher-unknown"
	}
	return hostname
}
