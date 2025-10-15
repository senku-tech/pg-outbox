package dispatcher

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/senku-tech/pg-outbox/pkg/config"
)

// ProcessingMode represents the current processing mode
type ProcessingMode int

const (
	POLLING_MODE ProcessingMode = iota
	LISTEN_MODE
)

func (m ProcessingMode) String() string {
	switch m {
	case POLLING_MODE:
		return "POLLING"
	case LISTEN_MODE:
		return "LISTEN"
	default:
		return "UNKNOWN"
	}
}

// HybridProcessor handles the state-based hybrid processing
type HybridProcessor struct {
	pool      *pgxpool.Pool
	processor *BatchProcessor
	service   *Service
	cfg       *config.DispatcherConfig
	logger    *zap.Logger

	// State management
	mu           sync.RWMutex
	currentMode  ProcessingMode
	listenConn   *pgxpool.Conn  // Pool connection, not raw conn

	// Control channels
	stopCh       chan struct{}
	runWg        sync.WaitGroup  // Track when run loop exits
	processingWg sync.WaitGroup  // Track in-flight batch processing
}

// NewHybridProcessor creates a new hybrid processor
func NewHybridProcessor(pool *pgxpool.Pool, processor *BatchProcessor, cfg *config.DispatcherConfig, logger *zap.Logger, service *Service) *HybridProcessor {
	// Ensure intervals are positive
	pollInterval := cfg.PollInterval
	if pollInterval <= 0 {
		pollInterval = 1 * time.Second
		logger.Warn("Poll interval was zero or negative, using default", zap.Duration("default", pollInterval))
	}

	fallbackInterval := cfg.FallbackInterval
	if fallbackInterval <= 0 {
		fallbackInterval = 30 * time.Second
		logger.Warn("Fallback interval was zero or negative, using default", zap.Duration("default", fallbackInterval))
	}

	// Create a safe copy of config with validated intervals
	safeCfg := *cfg
	safeCfg.PollInterval = pollInterval
	safeCfg.FallbackInterval = fallbackInterval

	return &HybridProcessor{
		pool:           pool,
		processor:      processor,
		service:        service,
		cfg:            &safeCfg,
		logger:         logger,
		currentMode:    POLLING_MODE, // Always start in polling mode
		stopCh:         make(chan struct{}),
	}
}

// GetCurrentMode returns the current processing mode
func (h *HybridProcessor) GetCurrentMode() ProcessingMode {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.currentMode
}

// setMode updates the current processing mode
func (h *HybridProcessor) setMode(mode ProcessingMode) {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	if h.currentMode != mode {
		h.logger.Info("🔄 Switching processing mode",
			zap.String("from", h.currentMode.String()),
			zap.String("to", mode.String()))
		h.currentMode = mode
	}
}

// Start begins the hybrid processing
func (h *HybridProcessor) Start(ctx context.Context) error {
	h.logger.Info("🚀 Starting hybrid processor", zap.String("initial_mode", "POLLING"))

	// Start the main processing loop
	h.runWg.Add(1)
	go func() {
		defer h.runWg.Done()
		h.run(ctx)
	}()

	return nil
}

// Stop gracefully stops the hybrid processor
func (h *HybridProcessor) Stop(ctx context.Context) error {
	h.logger.Info("🛑 Stopping hybrid processor")

	// Signal stop first
	close(h.stopCh)

	// Wait for run loop to exit completely
	h.runWg.Wait()

	// Wait for any in-flight batch processing to complete
	h.logger.Info("⏳ Waiting for in-flight batch processing to complete")
	h.processingWg.Wait()
	h.logger.Info("✅ All in-flight batches completed")

	// Then clean up resources (run loop should have exited now)
	h.mu.Lock()
	if h.listenConn != nil {
		h.listenConn.Release()
		h.listenConn = nil
	}
	h.mu.Unlock()

	return nil
}

// run is the main processing loop - completely sequential, no goroutines
func (h *HybridProcessor) run(ctx context.Context) {
	pollTicker := time.NewTicker(h.cfg.PollInterval)
	defer pollTicker.Stop()
	
	fallbackTicker := time.NewTicker(h.cfg.FallbackInterval)
	defer fallbackTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			h.logger.Info("Hybrid processor stopping due to context cancellation")
			return
		case <-h.stopCh:
			h.logger.Info("Hybrid processor stopping due to shutdown signal")
			return
		case <-pollTicker.C:
			// In POLLING_MODE: process batch
			if h.GetCurrentMode() == POLLING_MODE {
				h.processEventsWithModeCheck(ctx)
			}
		case <-fallbackTicker.C:
			// In LISTEN_MODE: check for missed events
			if h.GetCurrentMode() == LISTEN_MODE {
				h.fallbackCheck(ctx)
			}
		default:
			// In LISTEN_MODE: wait for notification (with timeout)
			if h.GetCurrentMode() == LISTEN_MODE && h.listenConn != nil {
				h.waitForNotification(ctx)
			} else {
				// Small sleep to prevent busy loop
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
}

// processEventsWithModeCheck processes events and checks if we should switch modes
func (h *HybridProcessor) processEventsWithModeCheck(ctx context.Context) {
	queueEmpty := h.processEvents(ctx)
	
	// Switch to LISTEN_MODE if queue is empty
	if queueEmpty && h.GetCurrentMode() == POLLING_MODE {
		if err := h.setupListenMode(ctx); err != nil {
			h.logger.Error("Failed to setup LISTEN mode, staying in POLLING mode", zap.Error(err))
		} else {
			h.setMode(LISTEN_MODE)
		}
	}
}

// processEvents processes a batch of events and returns true if queue is empty
func (h *HybridProcessor) processEvents(ctx context.Context) bool {
	// Track in-flight batch processing
	h.processingWg.Add(1)
	defer h.processingWg.Done()

	err := h.processor.ProcessBatch(ctx)
	if err != nil {
		h.logger.Error("Failed to process batch", zap.Error(err))
		return false
	}

	// Update lastProcessed timestamp after successful batch
	if h.service != nil {
		h.service.UpdateLastProcessed()
	}

	// Check if there are still pending events to determine if we processed any
	metrics, err := h.processor.GetMetrics(ctx)
	if err != nil {
		h.logger.Error("Failed to get metrics", zap.Error(err))
		return false
	}

	// If there are no pending events, we can switch to LISTEN mode
	return metrics.PendingCount == 0
}

// setupListenMode establishes the LISTEN connection
func (h *HybridProcessor) setupListenMode(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	// Close existing connection if any
	if h.listenConn != nil {
		h.listenConn.Release()
		h.listenConn = nil
	}
	
	// Create new connection for LISTEN
	conn, err := h.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection for LISTEN: %w", err)
	}
	
	h.listenConn = conn
	
	// Start listening for notifications
	_, err = conn.Exec(ctx, "LISTEN outbox_events")
	if err != nil {
		conn.Release()
		h.listenConn = nil
		return fmt.Errorf("failed to LISTEN: %w", err)
	}
	
	// NO GOROUTINE - everything is sequential now
	
	h.logger.Info("📡 LISTEN mode activated")
	return nil
}

// waitForNotification waits for a single notification (called from main loop)
func (h *HybridProcessor) waitForNotification(ctx context.Context) {
	// Check if we're stopping
	select {
	case <-h.stopCh:
		return
	default:
	}
	
	h.mu.RLock()
	conn := h.listenConn
	h.mu.RUnlock()
	
	if conn == nil {
		return
	}
	
	// Create a context with short timeout for non-blocking wait
	waitCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	
	// Wait for notification with timeout
	notification, err := conn.Conn().WaitForNotification(waitCtx)
	if err != nil {
		// Timeout is expected and ok
		if ctx.Err() != nil || waitCtx.Err() != nil {
			return
		}
		
		// Real error - switch back to polling
		h.logger.Error("Error waiting for notification", zap.Error(err))
		h.setMode(POLLING_MODE)
		return
	}
	
	if notification != nil {
		h.logger.Debug("📨 Received notification", 
			zap.String("channel", notification.Channel),
			zap.String("payload", notification.Payload))
		
		// Process events immediately
		h.processEvents(ctx)
	}
}

// fallbackCheck periodically checks for missed events in LISTEN_MODE
func (h *HybridProcessor) fallbackCheck(ctx context.Context) {
	h.logger.Debug("🔍 Performing fallback check")
	
	// Check if there are any pending events we missed
	metrics, err := h.processor.GetMetrics(ctx)
	if err != nil {
		h.logger.Error("Failed to get metrics during fallback check", zap.Error(err))
		return
	}
	
	if metrics.PendingCount > 0 {
		h.logger.Warn("📨 Fallback check found missed events", zap.Int64("pending", metrics.PendingCount))
		// Close LISTEN connection and switch back to polling mode
		h.mu.Lock()
		if h.listenConn != nil {
			h.listenConn.Release()
			h.listenConn = nil
		}
		h.mu.Unlock()
		
		// Switch back to polling mode to clear the backlog
		h.setMode(POLLING_MODE)
	}
}