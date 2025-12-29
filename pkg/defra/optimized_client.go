package defra

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/config"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/logger"
	"github.com/sourcenetwork/defradb/node"
)

// OptimizedDefraClient provides a high-performance, memory-efficient interface to DefraDB
type OptimizedDefraClient struct {
	node           *node.Node
	networkHandler *NetworkHandler
	eventManager   *EventManager
	connectionPool *ConnectionPool
	config         *OptimizedClientConfig
	mu             sync.RWMutex
	closed         bool
}

// OptimizedClientConfig configures the optimized client
type OptimizedClientConfig struct {
	EventManager   *EventManagerConfig `yaml:"event_manager"`
	ConnectionPool *PoolConfig         `yaml:"connection_pool"`
	EnableMetrics  bool                `yaml:"enable_metrics"`
	MetricsPort    int                 `yaml:"metrics_port"`
}

// DefaultOptimizedClientConfig provides sensible defaults
func DefaultOptimizedClientConfig() *OptimizedClientConfig {
	return &OptimizedClientConfig{
		EventManager:   DefaultEventManagerConfig(),
		ConnectionPool: DefaultPoolConfig(),
		EnableMetrics:  true,
		MetricsPort:    8080,
	}
}

// NewOptimizedDefraClient creates a new optimized DefraDB client
func NewOptimizedDefraClient(cfg *config.Config, schemaApplier SchemaApplier, collectionsOfInterest ...string) (*OptimizedDefraClient, error) {
	// Start DefraDB node
	node, networkHandler, err := StartDefraInstance(cfg, schemaApplier, collectionsOfInterest...)
	if err != nil {
		return nil, fmt.Errorf("failed to start DefraDB instance: %w", err)
	}

	// Create optimized client
	clientConfig := DefaultOptimizedClientConfig()

	// Create connection pool
	pool, err := NewConnectionPool(node, clientConfig.ConnectionPool)
	if err != nil {
		node.Close(context.Background())
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Create event manager
	eventManager := NewEventManager(node, clientConfig.EventManager)

	client := &OptimizedDefraClient{
		node:           node,
		networkHandler: networkHandler,
		eventManager:   eventManager,
		connectionPool: pool,
		config:         clientConfig,
	}

	logger.Sugar.Info("OptimizedDefraClient created successfully")
	return client, nil
}

// Network control methods

// StartNetwork activates P2P networking
func (c *OptimizedDefraClient) StartNetwork() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("client is closed")
	}

	return c.networkHandler.StartNetwork()
}

// StopNetwork deactivates P2P networking
func (c *OptimizedDefraClient) StopNetwork() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("client is closed")
	}

	return c.networkHandler.StopNetwork()
}

// ToggleNetwork switches P2P networking on/off
func (c *OptimizedDefraClient) ToggleNetwork() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("client is closed")
	}

	return c.networkHandler.ToggleNetwork()
}

// IsNetworkActive returns whether P2P networking is currently active
func (c *OptimizedDefraClient) IsNetworkActive() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return false
	}

	return c.networkHandler.IsNetworkActive()
}

// IsHostRunning returns whether the host is running
func (c *OptimizedDefraClient) IsHostRunning() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return false
	}
	return c.networkHandler.IsHostRunning()
}

// AddPeer adds a new peer at runtime and attempts connection if network is active
func (c *OptimizedDefraClient) AddPeer(peerAddr string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return fmt.Errorf("client is closed")
	}
	return c.networkHandler.AddPeer(peerAddr)
}

// RemovePeer removes a peer from the handler
func (c *OptimizedDefraClient) RemovePeer(peerAddr string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return fmt.Errorf("client is closed")
	}
	return c.networkHandler.RemovePeer(peerAddr)
}

// GetPeerStates returns a copy of all peer connection states
func (c *OptimizedDefraClient) GetPeerStates() map[string]PeerState {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return nil
	}
	return c.networkHandler.GetPeers()
}

// GetConnectedPeers returns a list of currently connected peer addresses
func (c *OptimizedDefraClient) GetConnectedPeers() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return nil
	}
	return c.networkHandler.GetConnectedPeers()
}

// GetConnectionStats returns connection statistics
func (c *OptimizedDefraClient) GetConnectionStats() ConnectionStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return ConnectionStats{}
	}
	return c.networkHandler.GetConnectionStats()
}

// Query executes a GraphQL query with connection pooling and optimization
func (c *OptimizedDefraClient) Query(ctx context.Context, query string, result interface{}) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return fmt.Errorf("client is closed")
	}

	return c.connectionPool.OptimizedQuery(ctx, query, result)
}

// QuerySingle executes a query expecting a single result
func (c *OptimizedDefraClient) QuerySingle(ctx context.Context, query string, result interface{}) error {
	return c.Query(ctx, query, result)
}

// QueryArray executes a query expecting an array result
func (c *OptimizedDefraClient) QueryArray(ctx context.Context, query string, result interface{}) error {
	return c.Query(ctx, query, result)
}

// Mutation executes a GraphQL mutation with optimization
func (c *OptimizedDefraClient) Mutation(ctx context.Context, mutation string, result interface{}) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return fmt.Errorf("client is closed")
	}

	return c.connectionPool.OptimizedMutation(ctx, mutation, result)
}

// MutationSingle executes a mutation expecting a single result
func (c *OptimizedDefraClient) MutationSingle(ctx context.Context, mutation string, result interface{}) error {
	return c.Mutation(ctx, mutation, result)
}

// Subscribe creates an optimized subscription with backpressure handling
func (c *OptimizedDefraClient) Subscribe(ctx context.Context, query string, priority Priority) (*Subscription, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, fmt.Errorf("client is closed")
	}

	return c.eventManager.Subscribe(ctx, query, priority)
}

// SubscribeTyped creates a typed subscription channel
func (c *OptimizedDefraClient) SubscribeTyped(ctx context.Context, query string, priority Priority, resultType interface{}) (<-chan interface{}, <-chan error, error) {
	sub, err := c.Subscribe(ctx, query, priority)
	if err != nil {
		return nil, nil, err
	}

	// Create typed channels
	typedChan := make(chan interface{}, 50)
	errorChan := make(chan error, 10)

	// Convert events to typed results
	go func() {
		defer close(typedChan)
		defer close(errorChan)

		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-sub.EventChan:
				if !ok {
					return
				}

				// Convert event data to typed result
				if err := marshalUnmarshal(event.Data, &resultType); err != nil {
					select {
					case errorChan <- fmt.Errorf("failed to convert event data: %w", err):
					case <-ctx.Done():
						return
					default:
						// Error channel full, log and continue
						logger.Sugar.Warnf("Error channel full, dropping error: %v", err)
					}
					continue
				}

				select {
				case typedChan <- resultType:
				case <-ctx.Done():
					return
				}
			case err, ok := <-sub.ErrorChan:
				if !ok {
					return
				}
				select {
				case errorChan <- err:
				case <-ctx.Done():
					return
				default:
					// Error channel full, log and continue
					logger.Sugar.Warnf("Error channel full, dropping error: %v", err)
				}
			}
		}
	}()

	return typedChan, errorChan, nil
}

// BatchQuery executes multiple queries efficiently
func (c *OptimizedDefraClient) BatchQuery(ctx context.Context, queries []string) ([]interface{}, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, fmt.Errorf("client is closed")
	}

	return c.connectionPool.BatchQuery(ctx, queries)
}

// Unsubscribe removes a subscription
func (c *OptimizedDefraClient) Unsubscribe(subscriptionID string) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.closed {
		c.eventManager.Unsubscribe(subscriptionID)
	}
}

// GetMetrics returns comprehensive performance metrics
func (c *OptimizedDefraClient) GetMetrics() ClientMetrics {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return ClientMetrics{}
	}

	return ClientMetrics{
		EventManager:   c.eventManager.GetMetrics(),
		ConnectionPool: c.connectionPool.GetMetrics(),
		NodeInfo:       c.getNodeInfo(),
	}
}

// ClientMetrics aggregates all client metrics
type ClientMetrics struct {
	EventManager   EventMetrics `json:"event_manager"`
	ConnectionPool PoolMetrics  `json:"connection_pool"`
	NodeInfo       NodeInfo     `json:"node_info"`
}

// NodeInfo provides DefraDB node information
type NodeInfo struct {
	PeerID      string `json:"peer_id"`
	PeerCount   int    `json:"peer_count"`
	Collections int    `json:"collections"`
}

// getNodeInfo retrieves current node information
func (c *OptimizedDefraClient) getNodeInfo() NodeInfo {
	info := NodeInfo{}

	// Get peer info
	if peerInfo, err := c.node.DB.PeerInfo(); err == nil && len(peerInfo) > 0 {
		info.PeerID = peerInfo[0]
	}

	// Get peer count (simplified - in production, implement proper peer counting)
	info.PeerCount = 0 // Placeholder

	// Get collection count (simplified - in production, query actual collections)
	info.Collections = 0 // Placeholder

	return info
}

// HealthCheck performs a comprehensive health check
func (c *OptimizedDefraClient) HealthCheck(ctx context.Context) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return fmt.Errorf("client is closed")
	}

	// Check node connectivity
	if _, err := c.node.DB.PeerInfo(); err != nil {
		return fmt.Errorf("node connectivity check failed: %w", err)
	}

	// Check connection pool
	poolMetrics := c.connectionPool.GetMetrics()
	if poolMetrics.ErrorCount > poolMetrics.TotalQueries/2 {
		return fmt.Errorf("connection pool error rate too high: %d/%d",
			poolMetrics.ErrorCount, poolMetrics.TotalQueries)
	}

	// Check event manager
	eventMetrics := c.eventManager.GetMetrics()
	if eventMetrics.DroppedEvents > eventMetrics.TotalEvents/4 {
		return fmt.Errorf("event manager drop rate too high: %d/%d",
			eventMetrics.DroppedEvents, eventMetrics.TotalEvents)
	}

	return nil
}

// OptimizePerformance adjusts client settings based on current metrics
func (c *OptimizedDefraClient) OptimizePerformance() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return
	}

	metrics := c.GetMetrics()

	// Log performance insights
	logger.Sugar.Infof("Performance metrics - Events: %d (dropped: %d), Queries: %d (errors: %d), Avg query time: %v",
		metrics.EventManager.TotalEvents,
		metrics.EventManager.DroppedEvents,
		metrics.ConnectionPool.TotalQueries,
		metrics.ConnectionPool.ErrorCount,
		metrics.ConnectionPool.AvgQueryTime,
	)

	// Auto-optimization logic could be added here
	// For example, adjusting buffer sizes based on drop rates
	if metrics.EventManager.DroppedEvents > 0 {
		logger.Sugar.Warn("Consider increasing event buffer sizes or implementing better backpressure handling")
	}

	if metrics.ConnectionPool.AvgQueryTime > 5*time.Second {
		logger.Sugar.Warn("Query performance is degraded, consider optimizing queries or increasing connection pool size")
	}
}

// Close gracefully shuts down the optimized client
func (c *OptimizedDefraClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("client already closed")
	}

	c.closed = true

	logger.Sugar.Info("Closing OptimizedDefraClient...")

	// Close event manager
	if err := c.eventManager.Close(); err != nil {
		logger.Sugar.Errorf("Error closing event manager: %v", err)
	}

	// Close connection pool
	if err := c.connectionPool.Close(); err != nil {
		logger.Sugar.Errorf("Error closing connection pool: %v", err)
	}

	// Close DefraDB node
	if err := c.node.Close(context.Background()); err != nil {
		logger.Sugar.Errorf("Error closing DefraDB node: %v", err)
		return err
	}

	logger.Sugar.Info("OptimizedDefraClient closed successfully")
	return nil
}

// Legacy compatibility functions

// StartOptimizedDefraInstance creates an optimized DefraDB instance (replaces StartDefraInstance)
func StartOptimizedDefraInstance(cfg *config.Config, schemaApplier SchemaApplier, collectionsOfInterest ...string) (*OptimizedDefraClient, error) {
	return NewOptimizedDefraClient(cfg, schemaApplier, collectionsOfInterest...)
}

// OptimizedSubscribe provides a drop-in replacement for the old Subscribe function
func OptimizedSubscribe(ctx context.Context, client *OptimizedDefraClient, subscription string, resultType interface{}) (<-chan interface{}, error) {
	typedChan, _, err := client.SubscribeTyped(ctx, subscription, PriorityNormal, resultType)
	return typedChan, err
}

// OptimizedPostMutation provides a drop-in replacement for PostMutation
func OptimizedPostMutation(ctx context.Context, client *OptimizedDefraClient, mutation string, result interface{}) error {
	return client.MutationSingle(ctx, mutation, result)
}
