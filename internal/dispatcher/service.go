package dispatcher

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/senku-tech/pg-outbox/pkg/config"
	"github.com/senku-tech/pg-outbox/pkg/publisher"
)

// Service is the main dispatcher service
type Service struct {
	pool           *pgxpool.Pool
	publisher      *publisher.EventPublisher
	processor      *BatchProcessor
	hybridProcessor *HybridProcessor
	cfg            *config.Config
	logger         *zap.Logger
	metrics        *Metrics

	stopCh     chan struct{}
	stoppedCh  chan struct{}
	
	// Health tracking
	mu            sync.RWMutex
	lastProcessed time.Time
	isHealthy     bool
	isReady       bool
}


// NewService creates a new dispatcher service
func NewService(cfg *config.Config, logger *zap.Logger) (*Service, error) {
	// Create pgx pool
	poolConfig, err := pgxpool.ParseConfig(cfg.Database.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to parse pool config: %w", err)
	}
	
	poolConfig.MaxConns = int32(cfg.Database.MaxConnections)
	poolConfig.MinConns = int32(cfg.Database.MaxIdle / 2)

	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Test database connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Create NATS publisher
	pub, err := publisher.NewEventPublisher(&cfg.NATS, logger)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to create publisher: %w", err)
	}

	// Create metrics
	metrics := NewMetrics()

	// Create batch processor
	processor := NewBatchProcessor(pool, pub, &cfg.Dispatcher, logger, metrics)

	service := &Service{
		pool:            pool,
		publisher:       pub,
		processor:       processor,
		cfg:             cfg,
		logger:          logger,
		metrics:         metrics,
		stopCh:          make(chan struct{}),
		stoppedCh:       make(chan struct{}),
		isHealthy:       true,
		isReady:         false,
		lastProcessed:   time.Now(), // Initialize to now
	}

	// Create hybrid processor for state-based processing (needs service reference)
	hybridProcessor := NewHybridProcessor(pool, processor, &cfg.Dispatcher, logger, service)
	service.hybridProcessor = hybridProcessor

	return service, nil
}

// Start begins processing events
func (s *Service) Start(ctx context.Context) error {
	s.logStartupInfo()

	// Validate table structure
	if err := s.validateTableStructure(ctx); err != nil {
		return fmt.Errorf("table structure validation failed: %w", err)
	}

	// Start hybrid processor
	if err := s.hybridProcessor.Start(ctx); err != nil {
		return fmt.Errorf("failed to start hybrid processor: %w", err)
	}

	// Mark as ready
	s.mu.Lock()
	s.isReady = true
	s.mu.Unlock()

	s.logger.Info("Dispatcher service started successfully")
	return nil
}


// Stop gracefully shuts down the service
func (s *Service) Stop(ctx context.Context) error {
	s.logger.Info("Stopping dispatcher service")

	// Mark as not ready
	s.mu.Lock()
	s.isReady = false
	s.mu.Unlock()

	// Stop hybrid processor
	if err := s.hybridProcessor.Stop(ctx); err != nil {
		s.logger.Warn("Failed to stop hybrid processor cleanly", zap.Error(err))
	}

	// Close resources
	if err := s.publisher.Close(); err != nil {
		s.logger.Warn("Failed to close publisher", zap.Error(err))
	}

	s.pool.Close()

	close(s.stoppedCh)
	s.logger.Info("Dispatcher service stopped")
	return nil
}

// Health returns the health status of the service
func (s *Service) Health() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.isHealthy {
		return fmt.Errorf("service is unhealthy")
	}

	// Check database connection
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	
	if err := s.pool.Ping(ctx); err != nil {
		return fmt.Errorf("database ping failed: %w", err)
	}

	// Check NATS connection
	if err := s.publisher.Health(); err != nil {
		return fmt.Errorf("NATS health check failed: %w", err)
	}

	// Check if we've processed events recently (if we're supposed to be processing)
	if s.isReady && time.Since(s.lastProcessed) > 5*time.Minute {
		// Get metrics to see if there are pending events
		metrics, err := s.processor.GetMetrics(context.Background())
		if err == nil && metrics.PendingCount > 0 {
			return fmt.Errorf("no events processed in last 5 minutes but %d events pending", metrics.PendingCount)
		}
	}

	return nil
}

// Ready returns whether the service is ready to process events
func (s *Service) Ready() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.isReady {
		return fmt.Errorf("service is not ready")
	}

	if !s.isHealthy {
		return fmt.Errorf("service is not healthy")
	}

	return nil
}

// UpdateLastProcessed updates the timestamp of the last successful batch processing
func (s *Service) UpdateLastProcessed() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastProcessed = time.Now()
}

// GetMetrics returns current service metrics
func (s *Service) GetMetrics() MetricsSnapshot {
	return s.metrics.GetSnapshot()
}

// GetDatabaseMetrics returns database-level metrics
func (s *Service) GetDatabaseMetrics(ctx context.Context) (*DatabaseMetrics, error) {
	metrics, err := s.processor.GetMetrics(ctx)
	if err != nil {
		return nil, err
	}

	var oldestAge time.Duration
	if metrics.OldestUnpublished != nil {
		if ts, ok := metrics.OldestUnpublished.(time.Time); ok {
			oldestAge = time.Since(ts)
		}
	}

	return &DatabaseMetrics{
		PendingCount:      metrics.PendingCount,
		FailedCount:       metrics.FailedCount,
		OldestPendingAge:  oldestAge,
		PublishedLastMin:  metrics.PublishedLastMinute,
	}, nil
}

// DatabaseMetrics contains database-level metrics
type DatabaseMetrics struct {
	PendingCount     int64
	FailedCount      int64
	OldestPendingAge time.Duration
	PublishedLastMin int64
}

// logStartupInfo logs comprehensive service configuration at startup  
func (s *Service) logStartupInfo() {
	s.logger.Info("🚀 Starting Outbox Dispatcher Service",
		zap.String("service", "outbox-dispatcher"))

	// Dispatcher configuration
	s.logger.Info("📋 Dispatcher Configuration",
		zap.String("instance_id", s.cfg.Dispatcher.InstanceID),
		zap.String("processing_mode", "HYBRID (POLLING → LISTEN)"),
		zap.Int("batch_size", s.cfg.Dispatcher.BatchSize),
		zap.Int("max_attempts", s.cfg.Dispatcher.MaxAttempts),
		zap.Duration("poll_interval", s.cfg.Dispatcher.PollInterval),
		zap.Duration("fallback_interval", s.cfg.Dispatcher.FallbackInterval),
		zap.Duration("shutdown_grace", s.cfg.Dispatcher.ShutdownGrace))

	// Database configuration
	s.logger.Info("🗄️  Database Configuration",
		zap.String("table", "outboxes"),
		zap.Int("max_connections", s.cfg.Database.MaxConnections),
		zap.Int("max_idle", s.cfg.Database.MaxIdle))

	// NATS configuration
	if s.cfg.NATS.JetStream.Enabled {
		s.logger.Info("📡 NATS JetStream Configuration",
			zap.String("url", s.cfg.NATS.URL),
			zap.String("stream", s.cfg.NATS.JetStream.StreamName),
			zap.Int("max_retries", s.cfg.NATS.MaxRetries),
			zap.Duration("retry_delay", s.cfg.NATS.RetryDelay))
	} else {
		s.logger.Info("📡 NATS Core Configuration",
			zap.String("url", s.cfg.NATS.URL),
			zap.Int("max_retries", s.cfg.NATS.MaxRetries),
			zap.Duration("retry_delay", s.cfg.NATS.RetryDelay))
	}

	// Topic mappings
	if len(s.cfg.NATS.TopicMap) > 0 {
		s.logger.Info("🔄 Topic Mappings Configured",
			zap.Int("mapping_count", len(s.cfg.NATS.TopicMap)))
		
		for originalTopic, mappedTopic := range s.cfg.NATS.TopicMap {
			s.logger.Info("   └─ Topic Mapping",
				zap.String("from", originalTopic),
				zap.String("to", mappedTopic))
		}
	} else {
		s.logger.Info("🔄 Topic Mappings", zap.String("status", "none configured - using original topics"))
	}

	// Metrics configuration
	if s.cfg.Metrics.Enabled {
		s.logger.Info("📊 Metrics Enabled",
			zap.Int("port", s.cfg.Metrics.Port),
			zap.String("path", s.cfg.Metrics.Path))
	} else {
		s.logger.Info("📊 Metrics", zap.String("status", "disabled"))
	}

	s.logger.Info("✅ Service configuration loaded successfully")
}

// validateTableStructure checks if the outboxes table exists and has the required structure
func (s *Service) validateTableStructure(ctx context.Context) error {
	s.logger.Info("🔍 Validating table structure...")

	// Required columns for the outboxes table
	requiredColumns := map[string]string{
		"id":           "uuid",
		"seq":          "bigint",
		"created_at":   "timestamp",
		"topic":        "varchar",
		"metadata":     "jsonb",
		"payload":      "jsonb",
		"published_at": "timestamp",
		"attempts":     "integer",
		"last_error":   "text",
	}

	// Check if table exists and get column information
	query := `
		SELECT column_name, data_type, is_nullable
		FROM information_schema.columns 
		WHERE table_name = $1
		AND table_schema = 'public'
		ORDER BY column_name;
	`

	rows, err := s.pool.Query(ctx, query, "outboxes")
	if err != nil {
		return fmt.Errorf("failed to query table structure: %w", err)
	}
	defer rows.Close()

	foundColumns := make(map[string]string)
	var columnCount int

	for rows.Next() {
		var columnName, dataType, isNullable string
		if err := rows.Scan(&columnName, &dataType, &isNullable); err != nil {
			return fmt.Errorf("failed to scan column info: %w", err)
		}
		foundColumns[columnName] = dataType
		columnCount++
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error reading table structure: %w", err)
	}

	// Check if table exists
	if columnCount == 0 {
		return fmt.Errorf("table 'outboxes' not found - please ensure the table exists")
	}

	// Validate required columns
	var missingColumns []string
	var incompatibleColumns []string

	for reqCol, reqType := range requiredColumns {
		if foundType, exists := foundColumns[reqCol]; !exists {
			missingColumns = append(missingColumns, reqCol)
		} else {
			// Check type compatibility (simplified check)
			if !isCompatibleType(foundType, reqType) {
				incompatibleColumns = append(incompatibleColumns, 
					fmt.Sprintf("%s (found: %s, expected: %s)", reqCol, foundType, reqType))
			}
		}
	}

	// Report results
	if len(missingColumns) > 0 || len(incompatibleColumns) > 0 {
		s.logger.Error("❌ Table structure validation failed")
		if len(missingColumns) > 0 {
			s.logger.Error("Missing columns", zap.Strings("columns", missingColumns))
		}
		if len(incompatibleColumns) > 0 {
			s.logger.Error("Incompatible column types", zap.Strings("columns", incompatibleColumns))
		}
		return fmt.Errorf("table structure is incompatible")
	}

	s.logger.Info("✅ Table structure validation passed", 
		zap.String("table", "outboxes"),
		zap.Int("columns_validated", len(requiredColumns)))
	
	return nil
}

// isCompatibleType checks if the found database type is compatible with the required type
func isCompatibleType(foundType, requiredType string) bool {
	// Simplified type compatibility check
	typeMapping := map[string][]string{
		"uuid":      {"uuid"},
		"bigint":    {"bigint", "int8"},
		"timestamp": {"timestamp without time zone", "timestamp with time zone", "timestamptz"},
		"varchar":   {"character varying", "varchar", "text"},
		"jsonb":     {"jsonb", "json"},
		"integer":   {"integer", "int4", "bigint", "int8"},
		"text":      {"text", "character varying", "varchar"},
	}

	compatibleTypes, exists := typeMapping[requiredType]
	if !exists {
		return false
	}

	for _, compatibleType := range compatibleTypes {
		if foundType == compatibleType {
			return true
		}
	}
	
	return false
}