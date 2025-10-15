package dispatcher

import (
	"sync"
	"time"
)

// Metrics tracks dispatcher performance metrics
type Metrics struct {
	mu sync.RWMutex

	// Counters
	EventsPublished   map[string]int64 // by topic
	EventsFailed      map[string]int64 // by topic
	EventsDeadLetter  map[string]int64 // by topic
	BatchesProcessed  int64
	BatchesFailed     int64

	// Gauges
	LastBatchSize     int
	LastBatchDuration time.Duration
	LastProcessedCount int // Number of events processed in last batch
	
	// Histograms (simplified - in production use Prometheus)
	ProcessingTimes []time.Duration
}

// NewMetrics creates a new metrics instance
func NewMetrics() *Metrics {
	return &Metrics{
		EventsPublished:  make(map[string]int64),
		EventsFailed:     make(map[string]int64),
		EventsDeadLetter: make(map[string]int64),
		ProcessingTimes:  make([]time.Duration, 0, 1000),
	}
}

// RecordEventPublished increments the published counter for a topic
func (m *Metrics) RecordEventPublished(topic string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.EventsPublished[topic]++
}

// RecordEventFailed increments the failed counter for a topic
func (m *Metrics) RecordEventFailed(topic string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.EventsFailed[topic]++
}

// RecordDeadLetter increments the dead letter counter for a topic
func (m *Metrics) RecordDeadLetter(topic string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.EventsDeadLetter[topic]++
}

// RecordBatchProcessed records batch processing metrics
func (m *Metrics) RecordBatchProcessed(success, failed int, duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.BatchesProcessed++
	if failed > 0 {
		m.BatchesFailed++
	}
	
	m.LastBatchSize = success + failed
	m.LastBatchDuration = duration
	m.LastProcessedCount = success
	
	// Keep last 1000 processing times
	if len(m.ProcessingTimes) >= 1000 {
		m.ProcessingTimes = m.ProcessingTimes[1:]
	}
	m.ProcessingTimes = append(m.ProcessingTimes, duration)
}

// GetSnapshot returns a snapshot of current metrics
func (m *Metrics) GetSnapshot() MetricsSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	// Copy maps to avoid race conditions
	published := make(map[string]int64)
	for k, v := range m.EventsPublished {
		published[k] = v
	}
	
	failed := make(map[string]int64)
	for k, v := range m.EventsFailed {
		failed[k] = v
	}
	
	deadLetter := make(map[string]int64)
	for k, v := range m.EventsDeadLetter {
		deadLetter[k] = v
	}
	
	// Calculate average processing time
	var avgProcessingTime time.Duration
	if len(m.ProcessingTimes) > 0 {
		var total time.Duration
		for _, t := range m.ProcessingTimes {
			total += t
		}
		avgProcessingTime = total / time.Duration(len(m.ProcessingTimes))
	}
	
	return MetricsSnapshot{
		EventsPublished:    published,
		EventsFailed:       failed,
		EventsDeadLetter:   deadLetter,
		BatchesProcessed:   m.BatchesProcessed,
		BatchesFailed:      m.BatchesFailed,
		LastBatchSize:      m.LastBatchSize,
		LastBatchDuration:  m.LastBatchDuration,
		LastProcessedCount: m.LastProcessedCount,
		AvgProcessingTime:  avgProcessingTime,
	}
}

// MetricsSnapshot represents a point-in-time view of metrics
type MetricsSnapshot struct {
	EventsPublished    map[string]int64
	EventsFailed       map[string]int64
	EventsDeadLetter   map[string]int64
	BatchesProcessed   int64
	BatchesFailed      int64
	LastBatchSize      int
	LastBatchDuration  time.Duration
	LastProcessedCount int
	AvgProcessingTime  time.Duration
}

// Reset clears all metrics (useful for testing)
func (m *Metrics) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.EventsPublished = make(map[string]int64)
	m.EventsFailed = make(map[string]int64)
	m.EventsDeadLetter = make(map[string]int64)
	m.BatchesProcessed = 0
	m.BatchesFailed = 0
	m.LastBatchSize = 0
	m.LastBatchDuration = 0
	m.LastProcessedCount = 0
	m.ProcessingTimes = make([]time.Duration, 0, 1000)
}

// RecordSuccess records a successful event processing
func (m *Metrics) RecordSuccess(duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.LastBatchDuration = duration
	if len(m.ProcessingTimes) >= 1000 {
		m.ProcessingTimes = m.ProcessingTimes[1:]
	}
	m.ProcessingTimes = append(m.ProcessingTimes, duration)
}

// RecordError records an error during processing
func (m *Metrics) RecordError() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.BatchesFailed++
}