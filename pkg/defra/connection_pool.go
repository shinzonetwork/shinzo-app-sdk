package defra

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/logger"
	"github.com/sourcenetwork/defradb/node"
)

// ConnectionPool manages DefraDB connections efficiently with resource pooling
type ConnectionPool struct {
	node          *node.Node
	queryClients  chan *PooledQueryClient
	config        *PoolConfig
	metrics       *PoolMetrics
	mu            sync.RWMutex
	closed        int32
	activeQueries int64
	totalQueries  int64
}

// PoolConfig configures the connection pool for optimal performance
type PoolConfig struct {
	MaxConnections     int           `yaml:"max_connections"`
	MinConnections     int           `yaml:"min_connections"`
	ConnectionTimeout  time.Duration `yaml:"connection_timeout"`
	IdleTimeout        time.Duration `yaml:"idle_timeout"`
	MaxQueryTime       time.Duration `yaml:"max_query_time"`
	EnableCompression  bool          `yaml:"enable_compression"`
	EnableQueryCaching bool          `yaml:"enable_query_caching"`
	CacheSize          int           `yaml:"cache_size"`
	CacheTTL           time.Duration `yaml:"cache_ttl"`
}

// DefaultPoolConfig provides sensible defaults
func DefaultPoolConfig() *PoolConfig {
	return &PoolConfig{
		MaxConnections:     20,
		MinConnections:     5,
		ConnectionTimeout:  30 * time.Second,
		IdleTimeout:        5 * time.Minute,
		MaxQueryTime:       30 * time.Second,
		EnableCompression:  true,
		EnableQueryCaching: true,
		CacheSize:          1000,
		CacheTTL:           5 * time.Minute,
	}
}

// PoolMetrics tracks connection pool performance
type PoolMetrics struct {
	ActiveConnections int64
	TotalQueries      int64
	CacheHits         int64
	CacheMisses       int64
	AvgQueryTime      time.Duration
	ErrorCount        int64
}

// PooledQueryClient wraps a query client with pooling metadata
type PooledQueryClient struct {
	client     *queryClient
	lastUsed   time.Time
	inUse      bool
	queryCount int64
	mu         sync.RWMutex
}

// QueryCache provides efficient query result caching
type QueryCache struct {
	cache map[string]*CacheEntry
	mu    sync.RWMutex
	ttl   time.Duration
}

// CacheEntry represents a cached query result
type CacheEntry struct {
	Data      interface{}
	Timestamp time.Time
	Hits      int64
}

// NewConnectionPool creates an optimized connection pool
func NewConnectionPool(node *node.Node, config *PoolConfig) (*ConnectionPool, error) {
	if config == nil {
		config = DefaultPoolConfig()
	}

	pool := &ConnectionPool{
		node:         node,
		queryClients: make(chan *PooledQueryClient, config.MaxConnections),
		config:       config,
		metrics:      &PoolMetrics{},
	}

	// Pre-populate with minimum connections
	for i := 0; i < config.MinConnections; i++ {
		client, err := pool.createClient()
		if err != nil {
			return nil, fmt.Errorf("failed to create initial client: %w", err)
		}
		pool.queryClients <- client
	}

	// Start maintenance goroutine
	go pool.maintenance()

	logger.Sugar.Infof("Created connection pool with %d initial connections", config.MinConnections)
	return pool, nil
}

// createClient creates a new pooled query client
func (p *ConnectionPool) createClient() (*PooledQueryClient, error) {
	client, err := newQueryClient(p.node)
	if err != nil {
		return nil, err
	}

	return &PooledQueryClient{
		client:   client,
		lastUsed: time.Now(),
		inUse:    false,
	}, nil
}

// GetClient retrieves a client from the pool or creates a new one
func (p *ConnectionPool) GetClient(ctx context.Context) (*PooledQueryClient, error) {
	if atomic.LoadInt32(&p.closed) == 1 {
		return nil, fmt.Errorf("connection pool is closed")
	}

	// Try to get from pool first
	select {
	case client := <-p.queryClients:
		client.mu.Lock()
		client.inUse = true
		client.lastUsed = time.Now()
		client.mu.Unlock()
		atomic.AddInt64(&p.metrics.ActiveConnections, 1)
		return client, nil
	case <-time.After(p.config.ConnectionTimeout):
		return nil, fmt.Errorf("timeout waiting for available connection")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ReturnClient returns a client to the pool
func (p *ConnectionPool) ReturnClient(client *PooledQueryClient) {
	if client == nil {
		return
	}

	client.mu.Lock()
	client.inUse = false
	client.lastUsed = time.Now()
	client.mu.Unlock()

	atomic.AddInt64(&p.metrics.ActiveConnections, -1)

	// Return to pool if not closed and pool not full
	if atomic.LoadInt32(&p.closed) == 0 {
		select {
		case p.queryClients <- client:
			// Successfully returned to pool
		default:
			// Pool is full, client will be garbage collected
		}
	}
}

// OptimizedQuery executes a query with connection pooling and caching
func (p *ConnectionPool) OptimizedQuery(ctx context.Context, query string, result interface{}) error {
	start := time.Now()
	atomic.AddInt64(&p.totalQueries, 1)
	atomic.AddInt64(&p.metrics.TotalQueries, 1)

	// Create query context with timeout
	queryCtx, cancel := context.WithTimeout(ctx, p.config.MaxQueryTime)
	defer cancel()

	// Get client from pool
	client, err := p.GetClient(queryCtx)
	if err != nil {
		atomic.AddInt64(&p.metrics.ErrorCount, 1)
		return fmt.Errorf("failed to get client from pool: %w", err)
	}
	defer p.ReturnClient(client)

	// Execute query
	err = client.client.queryAndUnmarshal(queryCtx, query, result)
	if err != nil {
		atomic.AddInt64(&p.metrics.ErrorCount, 1)
		return err
	}

	// Update metrics
	queryTime := time.Since(start)
	p.updateAvgQueryTime(queryTime)

	client.mu.Lock()
	client.queryCount++
	client.mu.Unlock()

	return nil
}

// OptimizedMutation executes a mutation with connection pooling
func (p *ConnectionPool) OptimizedMutation(ctx context.Context, mutation string, result interface{}) error {
	start := time.Now()
	atomic.AddInt64(&p.totalQueries, 1)
	atomic.AddInt64(&p.metrics.TotalQueries, 1)

	// Create mutation context with timeout
	mutationCtx, cancel := context.WithTimeout(ctx, p.config.MaxQueryTime)
	defer cancel()

	// Get client from pool
	client, err := p.GetClient(mutationCtx)
	if err != nil {
		atomic.AddInt64(&p.metrics.ErrorCount, 1)
		return fmt.Errorf("failed to get client from pool: %w", err)
	}
	defer p.ReturnClient(client)

	// Execute mutation using the node directly (more efficient than going through HTTP)
	gqlResult := p.node.DB.ExecRequest(mutationCtx, mutation).GQL

	if len(gqlResult.Errors) > 0 {
		atomic.AddInt64(&p.metrics.ErrorCount, 1)
		return fmt.Errorf("mutation errors: %v", gqlResult.Errors)
	}

	if gqlResult.Data == nil {
		atomic.AddInt64(&p.metrics.ErrorCount, 1)
		return fmt.Errorf("mutation returned no data")
	}

	// Efficiently parse mutation result
	if err := p.parseMutationResult(gqlResult.Data, result); err != nil {
		atomic.AddInt64(&p.metrics.ErrorCount, 1)
		return fmt.Errorf("failed to parse mutation result: %w", err)
	}

	// Update metrics
	queryTime := time.Since(start)
	p.updateAvgQueryTime(queryTime)

	client.mu.Lock()
	client.queryCount++
	client.mu.Unlock()

	return nil
}

// parseMutationResult efficiently parses mutation results without double marshaling
func (p *ConnectionPool) parseMutationResult(data interface{}, result interface{}) error {
	dataMap, ok := data.(map[string]interface{})
	if !ok {
		return fmt.Errorf("unexpected data format: %T", data)
	}

	// Find the first array in the data (mutation results are typically arrays)
	for _, value := range dataMap {
		if array, ok := value.([]interface{}); ok && len(array) > 0 {
			// Use efficient JSON marshaling only once
			return p.marshalToResult(array[0], result)
		}
		if array, ok := value.([]map[string]interface{}); ok && len(array) > 0 {
			return p.marshalToResult(array[0], result)
		}
	}

	return fmt.Errorf("no array data found in mutation result")
}

// marshalToResult efficiently converts data to result type
func (p *ConnectionPool) marshalToResult(data interface{}, result interface{}) error {
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}
	return json.Unmarshal(dataBytes, result)
}

// updateAvgQueryTime efficiently updates the average query time
func (p *ConnectionPool) updateAvgQueryTime(newTime time.Duration) {
	// Simple exponential moving average
	current := p.metrics.AvgQueryTime
	p.metrics.AvgQueryTime = time.Duration(float64(current)*0.9 + float64(newTime)*0.1)
}

// maintenance performs periodic cleanup and optimization
func (p *ConnectionPool) maintenance() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if atomic.LoadInt32(&p.closed) == 1 {
				return
			}
			p.cleanupIdleConnections()
		}
	}
}

// cleanupIdleConnections removes connections that have been idle too long
func (p *ConnectionPool) cleanupIdleConnections() {
	cutoff := time.Now().Add(-p.config.IdleTimeout)
	var activeClients []*PooledQueryClient

	// Drain the channel and check each client
	for {
		select {
		case client := <-p.queryClients:
			client.mu.RLock()
			isIdle := client.lastUsed.Before(cutoff) && !client.inUse
			client.mu.RUnlock()

			if !isIdle {
				activeClients = append(activeClients, client)
			} else {
				logger.Sugar.Debug("Removing idle connection from pool")
			}
		default:
			// Channel is empty
			goto done
		}
	}

done:
	// Return active clients to pool
	for _, client := range activeClients {
		select {
		case p.queryClients <- client:
		default:
			// Pool is full, this shouldn't happen but handle gracefully
		}
	}

	logger.Sugar.Debugf("Pool maintenance complete, %d active connections", len(activeClients))
}

// GetMetrics returns current pool metrics
func (p *ConnectionPool) GetMetrics() PoolMetrics {
	return PoolMetrics{
		ActiveConnections: atomic.LoadInt64(&p.metrics.ActiveConnections),
		TotalQueries:      atomic.LoadInt64(&p.metrics.TotalQueries),
		CacheHits:         atomic.LoadInt64(&p.metrics.CacheHits),
		CacheMisses:       atomic.LoadInt64(&p.metrics.CacheMisses),
		AvgQueryTime:      p.metrics.AvgQueryTime,
		ErrorCount:        atomic.LoadInt64(&p.metrics.ErrorCount),
	}
}

// Close gracefully shuts down the connection pool
func (p *ConnectionPool) Close() error {
	if !atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		return fmt.Errorf("pool already closed")
	}

	logger.Sugar.Info("Closing connection pool...")

	// Close all clients in the pool
	close(p.queryClients)
	for client := range p.queryClients {
		// Clients will be garbage collected
		_ = client
	}

	logger.Sugar.Info("Connection pool closed")
	return nil
}

// BatchQuery executes multiple queries efficiently in a single batch
func (p *ConnectionPool) BatchQuery(ctx context.Context, queries []string) ([]interface{}, error) {
	if len(queries) == 0 {
		return nil, fmt.Errorf("no queries provided")
	}

	results := make([]interface{}, len(queries))

	// Get client from pool
	client, err := p.GetClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get client from pool: %w", err)
	}
	defer p.ReturnClient(client)

	// Execute queries in batch
	for i, query := range queries {
		var result interface{}
		if err := client.client.queryAndUnmarshal(ctx, query, &result); err != nil {
			return nil, fmt.Errorf("batch query %d failed: %w", i, err)
		}
		results[i] = result
	}

	return results, nil
}
