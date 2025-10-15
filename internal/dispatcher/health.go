package dispatcher

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"go.uber.org/zap"
)

// HealthServer provides health and readiness endpoints
type HealthServer struct {
	service *Service
	logger  *zap.Logger
	port    int
}

// NewHealthServer creates a new health check server
func NewHealthServer(service *Service, port int, logger *zap.Logger) *HealthServer {
	return &HealthServer{
		service: service,
		logger:  logger,
		port:    port,
	}
}

// Start begins serving health check endpoints
func (hs *HealthServer) Start() error {
	mux := http.NewServeMux()
	
	// Health check endpoint (liveness)
	mux.HandleFunc("/healthz", hs.handleHealth)
	
	// Readiness check endpoint
	mux.HandleFunc("/readyz", hs.handleReady)
	
	// Metrics endpoint
	mux.HandleFunc("/metrics", hs.handleMetrics)
	
	// Detailed status endpoint
	mux.HandleFunc("/status", hs.handleStatus)

	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", hs.port),
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	hs.logger.Info("Starting health check server", zap.Int("port", hs.port))
	
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			hs.logger.Error("Health server error", zap.Error(err))
		}
	}()

	return nil
}

// handleHealth handles /healthz endpoint (Kubernetes liveness probe)
func (hs *HealthServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	err := hs.service.Health()
	
	response := HealthResponse{
		Status:    "ok",
		Timestamp: time.Now().Unix(),
	}

	if err != nil {
		response.Status = "unhealthy"
		response.Error = err.Error()
		w.WriteHeader(http.StatusServiceUnavailable)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleReady handles /readyz endpoint (Kubernetes readiness probe)
func (hs *HealthServer) handleReady(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	err := hs.service.Ready()
	
	response := ReadyResponse{
		Ready:     err == nil,
		Timestamp: time.Now().Unix(),
	}

	if err != nil {
		response.Error = err.Error()
		w.WriteHeader(http.StatusServiceUnavailable)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleMetrics handles /metrics endpoint
func (hs *HealthServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Get service metrics
	metrics := hs.service.GetMetrics()
	
	// Get database metrics
	dbMetrics, err := hs.service.GetDatabaseMetrics(r.Context())
	
	response := MetricsResponse{
		Timestamp:        time.Now().Unix(),
		EventsPublished:  sumMap(metrics.EventsPublished),
		EventsFailed:     sumMap(metrics.EventsFailed),
		EventsDeadLetter: sumMap(metrics.EventsDeadLetter),
		BatchesProcessed: metrics.BatchesProcessed,
		BatchesFailed:    metrics.BatchesFailed,
		LastBatchSize:    metrics.LastBatchSize,
		LastBatchMs:      metrics.LastBatchDuration.Milliseconds(),
		AvgProcessingMs:  metrics.AvgProcessingTime.Milliseconds(),
		ByTopic:          makeTopicMetrics(metrics),
	}

	if dbMetrics != nil && err == nil {
		response.Database = &DatabaseMetricsResponse{
			PendingCount:     dbMetrics.PendingCount,
			FailedCount:      0,
			OldestPendingAge: 0,
			PublishedLastMin: 0,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// handleStatus handles /status endpoint with detailed information
func (hs *HealthServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	healthErr := hs.service.Health()
	readyErr := hs.service.Ready()
	
	metrics := hs.service.GetMetrics()
	dbMetrics, _ := hs.service.GetDatabaseMetrics(r.Context())

	response := StatusResponse{
		Instance:  hs.service.cfg.Dispatcher.InstanceID,
		Timestamp: time.Now().Unix(),
		Health: HealthStatus{
			Healthy: healthErr == nil,
			Error:   errorString(healthErr),
		},
		Readiness: ReadinessStatus{
			Ready: readyErr == nil,
			Error: errorString(readyErr),
		},
		Configuration: ConfigStatus{
			BatchSize:    hs.service.cfg.Dispatcher.BatchSize,
			PollInterval: hs.service.cfg.Dispatcher.PollInterval.String(),
			MaxAttempts:  hs.service.cfg.Dispatcher.MaxAttempts,
		},
		Performance: PerformanceStatus{
			EventsPerSecond:  calculateEventsPerSecond(metrics),
			LastBatchMs:      metrics.LastBatchDuration.Milliseconds(),
			AvgProcessingMs:  metrics.AvgProcessingTime.Milliseconds(),
			BatchesProcessed: metrics.BatchesProcessed,
		},
	}

	if dbMetrics != nil {
		response.Database = &DatabaseStatus{
			PendingEvents:    dbMetrics.PendingCount,
			FailedEvents:     0,
			StuckEvents:      0,
			OldestPendingAge: "0s",
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// Response types

type HealthResponse struct {
	Status    string `json:"status"`
	Timestamp int64  `json:"timestamp"`
	Error     string `json:"error,omitempty"`
}

type ReadyResponse struct {
	Ready     bool   `json:"ready"`
	Timestamp int64  `json:"timestamp"`
	Error     string `json:"error,omitempty"`
}

type MetricsResponse struct {
	Timestamp        int64                       `json:"timestamp"`
	EventsPublished  int64                       `json:"events_published"`
	EventsFailed     int64                       `json:"events_failed"`
	EventsDeadLetter int64                       `json:"events_dead_letter"`
	BatchesProcessed int64                       `json:"batches_processed"`
	BatchesFailed    int64                       `json:"batches_failed"`
	LastBatchSize    int                         `json:"last_batch_size"`
	LastBatchMs      int64                       `json:"last_batch_ms"`
	AvgProcessingMs  int64                       `json:"avg_processing_ms"`
	ByTopic          map[string]TopicMetrics     `json:"by_topic"`
	Database         *DatabaseMetricsResponse    `json:"database,omitempty"`
}

type TopicMetrics struct {
	Published  int64 `json:"published"`
	Failed     int64 `json:"failed"`
	DeadLetter int64 `json:"dead_letter"`
}

type DatabaseMetricsResponse struct {
	PendingCount     int64   `json:"pending_count"`
	FailedCount      int64   `json:"failed_count"`
	OldestPendingAge float64 `json:"oldest_pending_age_seconds"`
	PublishedLastMin int64   `json:"published_last_minute"`
}

type StatusResponse struct {
	Instance      string            `json:"instance"`
	Timestamp     int64             `json:"timestamp"`
	Health        HealthStatus      `json:"health"`
	Readiness     ReadinessStatus   `json:"readiness"`
	Configuration ConfigStatus      `json:"configuration"`
	Performance   PerformanceStatus `json:"performance"`
	Database      *DatabaseStatus   `json:"database,omitempty"`
}

type HealthStatus struct {
	Healthy bool   `json:"healthy"`
	Error   string `json:"error,omitempty"`
}

type ReadinessStatus struct {
	Ready bool   `json:"ready"`
	Error string `json:"error,omitempty"`
}

type ConfigStatus struct {
	BatchSize    int    `json:"batch_size"`
	PollInterval string `json:"poll_interval"`
	MaxAttempts  int    `json:"max_attempts"`
}

type PerformanceStatus struct {
	EventsPerSecond  float64 `json:"events_per_second"`
	LastBatchMs      int64   `json:"last_batch_ms"`
	AvgProcessingMs  int64   `json:"avg_processing_ms"`
	BatchesProcessed int64   `json:"batches_processed"`
}

type DatabaseStatus struct {
	PendingEvents    int64  `json:"pending_events"`
	FailedEvents     int64  `json:"failed_events"`
	StuckEvents      int    `json:"stuck_events"`
	OldestPendingAge string `json:"oldest_pending_age"`
}

// Helper functions

func sumMap(m map[string]int64) int64 {
	var total int64
	for _, v := range m {
		total += v
	}
	return total
}

func makeTopicMetrics(metrics MetricsSnapshot) map[string]TopicMetrics {
	result := make(map[string]TopicMetrics)
	
	// Collect all topics
	topics := make(map[string]bool)
	for topic := range metrics.EventsPublished {
		topics[topic] = true
	}
	for topic := range metrics.EventsFailed {
		topics[topic] = true
	}
	for topic := range metrics.EventsDeadLetter {
		topics[topic] = true
	}
	
	// Build metrics for each topic
	for topic := range topics {
		result[topic] = TopicMetrics{
			Published:  metrics.EventsPublished[topic],
			Failed:     metrics.EventsFailed[topic],
			DeadLetter: metrics.EventsDeadLetter[topic],
		}
	}
	
	return result
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func calculateEventsPerSecond(metrics MetricsSnapshot) float64 {
	// Simple calculation - would be more sophisticated in production
	total := sumMap(metrics.EventsPublished)
	if metrics.AvgProcessingTime > 0 && metrics.BatchesProcessed > 0 {
		avgBatchSize := float64(total) / float64(metrics.BatchesProcessed)
		return avgBatchSize / metrics.AvgProcessingTime.Seconds()
	}
	return 0
}