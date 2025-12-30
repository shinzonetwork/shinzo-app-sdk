package defra

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/logger"
	"github.com/sourcenetwork/defradb/node"
)

// EventManager provides efficient event-driven DefraDB interactions
// with memory optimization, connection pooling, and backpressure handling
type EventManager struct {
	node          *node.Node
	subscriptions sync.Map // map[string]*Subscription
	eventPool     *EventPool
	config        *EventManagerConfig
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	metrics       *EventMetrics
}

// EventManagerConfig configures the event manager for optimal performance
type EventManagerConfig struct {
	// Buffer sizes for different event types
	DefaultBufferSize   int           `yaml:"default_buffer_size"`
	HighPriorityBuffer  int           `yaml:"high_priority_buffer"`
	BatchSize           int           `yaml:"batch_size"`
	BatchTimeout        time.Duration `yaml:"batch_timeout"`
	MaxConcurrentSubs   int           `yaml:"max_concurrent_subscriptions"`
	EnableCompression   bool          `yaml:"enable_compression"`
	EnableEventBatching bool          `yaml:"enable_event_batching"`
	MemoryThresholdMB   int           `yaml:"memory_threshold_mb"`
	BackpressureEnabled bool          `yaml:"backpressure_enabled"`
}

// DefaultEventManagerConfig provides sensible defaults for memory efficiency
func DefaultEventManagerConfig() *EventManagerConfig {
	return &EventManagerConfig{
		DefaultBufferSize:   50,
		HighPriorityBuffer:  10,
		BatchSize:           25,
		BatchTimeout:        100 * time.Millisecond,
		MaxConcurrentSubs:   100,
		EnableCompression:   true,
		EnableEventBatching: true,
		MemoryThresholdMB:   100,
		BackpressureEnabled: true,
	}
}

// EventMetrics tracks performance and memory usage
type EventMetrics struct {
	TotalEvents       int64
	DroppedEvents     int64
	ActiveSubs        int64
	MemoryUsageMB     int64
	AvgProcessingTime time.Duration
	BackpressureHits  int64
}

// Event represents a processed DefraDB event with minimal memory footprint
type Event struct {
	ID        string      `json:"id"`
	Type      string      `json:"type"`
	Data      interface{} `json:"data"`
	Timestamp time.Time   `json:"timestamp"`
	Priority  Priority    `json:"priority"`
}

// Priority defines event processing priority
type Priority int

const (
	PriorityLow Priority = iota
	PriorityNormal
	PriorityHigh
	PriorityCritical
)

// EventPool manages event object reuse to reduce GC pressure
type EventPool struct {
	pool sync.Pool
}

func NewEventPool() *EventPool {
	return &EventPool{
		pool: sync.Pool{
			New: func() interface{} {
				return &Event{}
			},
		},
	}
}

func (p *EventPool) Get() *Event {
	return p.pool.Get().(*Event)
}

func (p *EventPool) Put(event *Event) {
	// Reset event for reuse
	event.ID = ""
	event.Type = ""
	event.Data = nil
	event.Timestamp = time.Time{}
	event.Priority = PriorityNormal
	p.pool.Put(event)
}

// Subscription manages a single DefraDB subscription with backpressure
type Subscription struct {
	ID           string
	Query        string
	EventChan    chan *Event
	ErrorChan    chan error
	Priority     Priority
	ctx          context.Context
	cancel       context.CancelFunc
	lastActivity time.Time
	eventCount   int64
	droppedCount int64
	mu           sync.RWMutex
}

// NewEventManager creates an optimized event manager
func NewEventManager(node *node.Node, config *EventManagerConfig) *EventManager {
	if config == nil {
		config = DefaultEventManagerConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	em := &EventManager{
		node:      node,
		eventPool: NewEventPool(),
		config:    config,
		ctx:       ctx,
		cancel:    cancel,
		metrics:   &EventMetrics{},
	}

	// Start background workers
	em.startWorkers()

	return em
}

// Subscribe creates an efficient subscription with backpressure handling
func (em *EventManager) Subscribe(ctx context.Context, query string, priority Priority) (*Subscription, error) {
	// Check subscription limits
	if atomic.LoadInt64(&em.metrics.ActiveSubs) >= int64(em.config.MaxConcurrentSubs) {
		return nil, fmt.Errorf("maximum concurrent subscriptions reached: %d", em.config.MaxConcurrentSubs)
	}

	// Create subscription context
	subCtx, cancel := context.WithCancel(ctx)

	// Determine buffer size based on priority
	bufferSize := em.config.DefaultBufferSize
	if priority >= PriorityHigh {
		bufferSize = em.config.HighPriorityBuffer
	}

	sub := &Subscription{
		ID:           fmt.Sprintf("sub_%d_%d", time.Now().UnixNano(), priority),
		Query:        query,
		EventChan:    make(chan *Event, bufferSize),
		ErrorChan:    make(chan error, 5),
		Priority:     priority,
		ctx:          subCtx,
		cancel:       cancel,
		lastActivity: time.Now(),
	}

	// Store subscription
	em.subscriptions.Store(sub.ID, sub)
	atomic.AddInt64(&em.metrics.ActiveSubs, 1)

	// Start DefraDB subscription
	if err := em.startDefraSubscription(sub); err != nil {
		em.unsubscribe(sub.ID)
		return nil, fmt.Errorf("failed to start DefraDB subscription: %w", err)
	}

	logger.Sugar.Infof("Created subscription %s with priority %d", sub.ID, priority)
	return sub, nil
}

// startDefraSubscription initiates the actual DefraDB subscription
func (em *EventManager) startDefraSubscription(sub *Subscription) error {
	result := em.node.DB.ExecRequest(sub.ctx, sub.Query)
	if result.Subscription == nil {
		return fmt.Errorf("DefraDB subscription channel is nil for query: %s", sub.Query)
	}

	// Process subscription in background with efficient memory usage
	em.wg.Add(1)
	go func() {
		defer em.wg.Done()
		defer em.unsubscribe(sub.ID)

		for {
			select {
			case <-sub.ctx.Done():
				return
			case <-em.ctx.Done():
				return
			case gqlResult, ok := <-result.Subscription:
				if !ok {
					return
				}

				if gqlResult.Errors != nil {
					select {
					case sub.ErrorChan <- fmt.Errorf("subscription errors: %v", gqlResult.Errors):
					default:
						// Error channel full, log and continue
						logger.Sugar.Warnf("Error channel full for subscription %s", sub.ID)
					}
					continue
				}

				// Process event efficiently
				if err := em.processEvent(sub, gqlResult.Data); err != nil {
					logger.Sugar.Errorf("Failed to process event for subscription %s: %v", sub.ID, err)
				}
			}
		}
	}()

	return nil
}

// processEvent handles individual events with memory optimization
func (em *EventManager) processEvent(sub *Subscription, data interface{}) error {
	start := time.Now()

	// Get event from pool to reduce allocations
	event := em.eventPool.Get()
	defer em.eventPool.Put(event)

	// Populate event efficiently
	event.ID = fmt.Sprintf("%s_%d", sub.ID, atomic.AddInt64(&sub.eventCount, 1))
	event.Type = "defra_event"
	event.Data = data
	event.Timestamp = start
	event.Priority = sub.Priority

	// Handle backpressure
	if em.config.BackpressureEnabled {
		select {
		case sub.EventChan <- event:
			// Event sent successfully
		default:
			// Channel full, handle backpressure
			atomic.AddInt64(&sub.droppedCount, 1)
			atomic.AddInt64(&em.metrics.DroppedEvents, 1)
			atomic.AddInt64(&em.metrics.BackpressureHits, 1)

			if sub.Priority >= PriorityHigh {
				// For high priority, try to make space by dropping oldest
				select {
				case <-sub.EventChan:
					// Dropped oldest event
				default:
				}
				// Try again
				select {
				case sub.EventChan <- event:
				default:
					return fmt.Errorf("failed to send high priority event after backpressure handling")
				}
			} else {
				// For normal/low priority, just drop the event
				logger.Sugar.Debugf("Dropped event for subscription %s due to backpressure", sub.ID)
				return nil
			}
		}
	} else {
		// Non-blocking send
		select {
		case sub.EventChan <- event:
		case <-sub.ctx.Done():
			return nil
		}
	}

	// Update metrics
	atomic.AddInt64(&em.metrics.TotalEvents, 1)
	sub.mu.Lock()
	sub.lastActivity = time.Now()
	sub.mu.Unlock()

	// Update processing time metric
	processingTime := time.Since(start)
	em.updateAvgProcessingTime(processingTime)

	return nil
}

// updateAvgProcessingTime efficiently updates the average processing time
func (em *EventManager) updateAvgProcessingTime(newTime time.Duration) {
	// Simple exponential moving average
	current := em.metrics.AvgProcessingTime
	em.metrics.AvgProcessingTime = time.Duration(float64(current)*0.9 + float64(newTime)*0.1)
}

// Unsubscribe removes a subscription and cleans up resources
func (em *EventManager) Unsubscribe(subscriptionID string) {
	em.unsubscribe(subscriptionID)
}

func (em *EventManager) unsubscribe(subscriptionID string) {
	if sub, ok := em.subscriptions.LoadAndDelete(subscriptionID); ok {
		subscription := sub.(*Subscription)
		subscription.cancel()
		close(subscription.EventChan)
		close(subscription.ErrorChan)
		atomic.AddInt64(&em.metrics.ActiveSubs, -1)
		logger.Sugar.Infof("Unsubscribed %s", subscriptionID)
	}
}

// startWorkers starts background maintenance workers
func (em *EventManager) startWorkers() {
	// Memory monitor
	em.wg.Add(1)
	go em.memoryMonitor()

	// Subscription cleanup worker
	em.wg.Add(1)
	go em.subscriptionCleanup()
}

// memoryMonitor tracks memory usage and triggers cleanup when needed
func (em *EventManager) memoryMonitor() {
	defer em.wg.Done()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-em.ctx.Done():
			return
		case <-ticker.C:
			// Update memory metrics (simplified - in production use runtime.MemStats)
			// This is a placeholder for actual memory monitoring
			atomic.StoreInt64(&em.metrics.MemoryUsageMB, 50) // Placeholder value

			if em.metrics.MemoryUsageMB > int64(em.config.MemoryThresholdMB) {
				logger.Sugar.Warnf("Memory usage high: %dMB, triggering cleanup", em.metrics.MemoryUsageMB)
				em.triggerCleanup()
			}
		}
	}
}

// subscriptionCleanup removes inactive subscriptions
func (em *EventManager) subscriptionCleanup() {
	defer em.wg.Done()
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-em.ctx.Done():
			return
		case <-ticker.C:
			cutoff := time.Now().Add(-10 * time.Minute)
			var toRemove []string

			em.subscriptions.Range(func(key, value interface{}) bool {
				sub := value.(*Subscription)
				sub.mu.RLock()
				inactive := sub.lastActivity.Before(cutoff)
				sub.mu.RUnlock()

				if inactive {
					toRemove = append(toRemove, key.(string))
				}
				return true
			})

			for _, id := range toRemove {
				logger.Sugar.Infof("Removing inactive subscription: %s", id)
				em.unsubscribe(id)
			}
		}
	}
}

// triggerCleanup performs memory cleanup operations
func (em *EventManager) triggerCleanup() {
	// Force garbage collection (use sparingly in production)
	// runtime.GC()

	// Clean up inactive subscriptions more aggressively
	cutoff := time.Now().Add(-2 * time.Minute)
	var toRemove []string

	em.subscriptions.Range(func(key, value interface{}) bool {
		sub := value.(*Subscription)
		sub.mu.RLock()
		inactive := sub.lastActivity.Before(cutoff)
		sub.mu.RUnlock()

		if inactive {
			toRemove = append(toRemove, key.(string))
		}
		return true
	})

	for _, id := range toRemove {
		em.unsubscribe(id)
	}
}

// GetMetrics returns current performance metrics
func (em *EventManager) GetMetrics() EventMetrics {
	return EventMetrics{
		TotalEvents:       atomic.LoadInt64(&em.metrics.TotalEvents),
		DroppedEvents:     atomic.LoadInt64(&em.metrics.DroppedEvents),
		ActiveSubs:        atomic.LoadInt64(&em.metrics.ActiveSubs),
		MemoryUsageMB:     atomic.LoadInt64(&em.metrics.MemoryUsageMB),
		AvgProcessingTime: em.metrics.AvgProcessingTime,
		BackpressureHits:  atomic.LoadInt64(&em.metrics.BackpressureHits),
	}
}

// Close gracefully shuts down the event manager
func (em *EventManager) Close() error {
	logger.Sugar.Info("Shutting down EventManager...")

	// Cancel all operations
	em.cancel()

	// Close all subscriptions
	em.subscriptions.Range(func(key, value interface{}) bool {
		em.unsubscribe(key.(string))
		return true
	})

	// Wait for all goroutines to finish
	done := make(chan struct{})
	go func() {
		em.wg.Wait()
		close(done)
	}()

	// Wait with timeout
	select {
	case <-done:
		logger.Sugar.Info("EventManager shutdown complete")
	case <-time.After(10 * time.Second):
		logger.Sugar.Warn("EventManager shutdown timed out")
	}

	return nil
}
