package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/senku-tech/pg-outbox/pkg/config"
	"github.com/senku-tech/pg-outbox/internal/dispatcher"

	// PostgreSQL driver
	_ "github.com/jackc/pgx/v5/stdlib"
)

var (
	// Version information - can be set at build time
	version   = "dev"
	buildTime = "unknown"
	gitCommit = "unknown"
)

func main() {
	// Parse command-line flags
	var (
		configPath = flag.String("config", "config.yaml", "Path to configuration file")
		logLevel   = flag.String("log-level", "", "Override log level (debug, info, warn, error)")
		instance   = flag.String("instance", "", "Override instance ID")
	)
	flag.Parse()

	// Initialize logger
	logger, err := initLogger(*logLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	// Load configuration
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		logger.Fatal("Failed to load configuration", zap.Error(err))
	}

	// Apply command-line overrides
	if *instance != "" {
		cfg.Dispatcher.InstanceID = *instance
	}
	if *logLevel != "" {
		cfg.Logging.Level = *logLevel
	}

	logger.Info("🚀 Outbox Dispatcher Starting",
		zap.String("version", version),
		zap.String("build_time", buildTime),
		zap.String("git_commit", gitCommit),
		zap.String("config_file", *configPath))

	// Create dispatcher service
	service, err := dispatcher.NewService(cfg, logger)
	if err != nil {
		logger.Fatal("Failed to create service", zap.Error(err))
	}

	// Start health check server
	healthServer := dispatcher.NewHealthServer(service, cfg.Metrics.Port, logger)
	if err := healthServer.Start(); err != nil {
		logger.Fatal("Failed to start health server", zap.Error(err))
	}

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the service
	if err := service.Start(ctx); err != nil {
		logger.Fatal("Failed to start service", zap.Error(err))
	}

	// Set up signal handling for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal
	sig := <-sigCh
	logger.Info("Received shutdown signal", zap.String("signal", sig.String()))

	// Cancel context to stop processing new work
	cancel()

	// Create shutdown context with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.Dispatcher.ShutdownGrace)
	defer shutdownCancel()

	// Stop the service gracefully
	if err := service.Stop(shutdownCtx); err != nil {
		logger.Error("Error during shutdown", zap.Error(err))
	}

	logger.Info("Dispatcher shutdown complete")
}

// initLogger initializes the zap logger
func initLogger(level string) (*zap.Logger, error) {
	// Parse log level
	var zapLevel zapcore.Level
	switch level {
	case "debug":
		zapLevel = zapcore.DebugLevel
	case "info", "":
		zapLevel = zapcore.InfoLevel
	case "warn":
		zapLevel = zapcore.WarnLevel
	case "error":
		zapLevel = zapcore.ErrorLevel
	default:
		return nil, fmt.Errorf("invalid log level: %s", level)
	}

	// Create logger configuration
	config := zap.Config{
		Level:            zap.NewAtomicLevelAt(zapLevel),
		Development:      zapLevel == zapcore.DebugLevel,
		Encoding:         "json",
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
		EncoderConfig: zapcore.EncoderConfig{
			TimeKey:        "timestamp",
			LevelKey:       "level",
			NameKey:        "logger",
			CallerKey:      "caller",
			FunctionKey:    zapcore.OmitKey,
			MessageKey:     "msg",
			StacktraceKey:  "stacktrace",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.LowercaseLevelEncoder,
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: zapcore.MillisDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		},
	}

	// Use console encoding for development
	if zapLevel == zapcore.DebugLevel {
		config.Encoding = "console"
		config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	}

	return config.Build()
}