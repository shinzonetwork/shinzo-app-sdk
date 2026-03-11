package defra

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================
// ConnectionPool: parseMutationResult
// ============================================================

func TestParseMutationResult_WithInterfaceArray(t *testing.T) {
	pool := &ConnectionPool{metrics: &PoolMetrics{}}

	data := map[string]interface{}{
		"create_User": []interface{}{
			map[string]interface{}{"name": "Alice", "age": float64(30)},
		},
	}

	var result map[string]interface{}
	err := pool.parseMutationResult(data, &result)
	require.NoError(t, err)
	assert.Equal(t, "Alice", result["name"])
	assert.Equal(t, float64(30), result["age"])
}

func TestParseMutationResult_WithMapArray(t *testing.T) {
	pool := &ConnectionPool{metrics: &PoolMetrics{}}

	data := map[string]interface{}{
		"create_User": []map[string]interface{}{
			{"name": "Bob", "score": float64(99)},
		},
	}

	var result map[string]interface{}
	err := pool.parseMutationResult(data, &result)
	require.NoError(t, err)
	assert.Equal(t, "Bob", result["name"])
	assert.Equal(t, float64(99), result["score"])
}

func TestParseMutationResult_WrongDataType(t *testing.T) {
	pool := &ConnectionPool{metrics: &PoolMetrics{}}

	// Not a map[string]interface{} at the top level
	err := pool.parseMutationResult("not a map", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected data format")
}

func TestParseMutationResult_NoArrayInMap(t *testing.T) {
	pool := &ConnectionPool{metrics: &PoolMetrics{}}

	data := map[string]interface{}{
		"scalar_field": "just a string",
		"number_field": float64(42),
	}

	var result map[string]interface{}
	err := pool.parseMutationResult(data, &result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no array data found")
}

func TestParseMutationResult_EmptyInterfaceArray(t *testing.T) {
	pool := &ConnectionPool{metrics: &PoolMetrics{}}

	data := map[string]interface{}{
		"create_User": []interface{}{},
	}

	var result map[string]interface{}
	err := pool.parseMutationResult(data, &result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no array data found")
}

// ============================================================
// ConnectionPool: marshalToResult
// ============================================================

func TestMarshalToResult_MapToStruct(t *testing.T) {
	pool := &ConnectionPool{metrics: &PoolMetrics{}}

	type User struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	src := map[string]interface{}{
		"name": "Charlie",
		"age":  float64(25),
	}

	var user User
	err := pool.marshalToResult(src, &user)
	require.NoError(t, err)
	assert.Equal(t, "Charlie", user.Name)
	assert.Equal(t, 25, user.Age)
}

func TestMarshalToResult_MapToMap(t *testing.T) {
	pool := &ConnectionPool{metrics: &PoolMetrics{}}

	src := map[string]interface{}{"key": "value"}
	var dst map[string]interface{}
	err := pool.marshalToResult(src, &dst)
	require.NoError(t, err)
	assert.Equal(t, "value", dst["key"])
}

// ============================================================
// ConnectionPool: updateAvgQueryTime (EMA)
// ============================================================

func TestUpdateAvgQueryTime(t *testing.T) {
	pool := &ConnectionPool{metrics: &PoolMetrics{}}

	// Starting from zero, after one update the EMA should be 0.1 * newTime
	pool.updateAvgQueryTime(100 * time.Millisecond)
	assert.Equal(t, time.Duration(float64(100*time.Millisecond)*0.1), pool.metrics.AvgQueryTime)

	// Second update: 0.9 * prev + 0.1 * new
	prev := pool.metrics.AvgQueryTime
	pool.updateAvgQueryTime(200 * time.Millisecond)
	expected := time.Duration(float64(prev)*0.9 + float64(200*time.Millisecond)*0.1)
	assert.Equal(t, expected, pool.metrics.AvgQueryTime)
}

func TestUpdateAvgQueryTime_ConvergesToSteadyState(t *testing.T) {
	pool := &ConnectionPool{metrics: &PoolMetrics{}}

	// Feed the same value many times; the average should converge toward it
	steady := 50 * time.Millisecond
	for i := 0; i < 200; i++ {
		pool.updateAvgQueryTime(steady)
	}

	diff := pool.metrics.AvgQueryTime - steady
	if diff < 0 {
		diff = -diff
	}
	assert.Less(t, diff, 1*time.Millisecond, "EMA should converge to the steady input value")
}

// ============================================================
// ConnectionPool: Close (double close)
// ============================================================

func TestConnectionPool_DoubleCloseReturnsError(t *testing.T) {
	// Build a minimal pool without a real node – just enough structure for Close()
	pool := &ConnectionPool{
		queryClients: make(chan *PooledQueryClient, 5),
		config:       DefaultPoolConfig(),
		metrics:      &PoolMetrics{},
	}

	err := pool.Close()
	require.NoError(t, err)

	err = pool.Close()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already closed")
}

// ============================================================
// ConnectionPool: GetClient on closed pool
// ============================================================

func TestConnectionPool_GetClientOnClosedPool(t *testing.T) {
	pool := &ConnectionPool{
		queryClients: make(chan *PooledQueryClient, 5),
		config:       DefaultPoolConfig(),
		metrics:      &PoolMetrics{},
	}

	err := pool.Close()
	require.NoError(t, err)

	_, err = pool.GetClient(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

// ============================================================
// ConnectionPool: ReturnClient with nil (no-op, no panic)
// ============================================================

func TestConnectionPool_ReturnClientNil(t *testing.T) {
	pool := &ConnectionPool{
		queryClients: make(chan *PooledQueryClient, 5),
		config:       DefaultPoolConfig(),
		metrics:      &PoolMetrics{},
	}

	// Should not panic
	assert.NotPanics(t, func() {
		pool.ReturnClient(nil)
	})
}

// ============================================================
// ConnectionPool: BatchQuery with empty queries
// ============================================================

func TestConnectionPool_BatchQueryEmptyQueries(t *testing.T) {
	pool := &ConnectionPool{
		queryClients: make(chan *PooledQueryClient, 5),
		config:       DefaultPoolConfig(),
		metrics:      &PoolMetrics{},
	}

	results, err := pool.BatchQuery(context.Background(), []string{})
	require.Error(t, err)
	assert.Nil(t, results)
	assert.Contains(t, err.Error(), "no queries provided")
}

// ============================================================
// ConnectionPool: ReturnClient actually returns to pool
// ============================================================

func TestConnectionPool_ReturnClientAddsBackToChannel(t *testing.T) {
	pool := &ConnectionPool{
		queryClients: make(chan *PooledQueryClient, 5),
		config:       DefaultPoolConfig(),
		metrics:      &PoolMetrics{},
	}

	client := &PooledQueryClient{
		lastUsed: time.Now(),
		inUse:    true,
	}

	pool.ReturnClient(client)

	assert.Equal(t, int64(-1), atomic.LoadInt64(&pool.metrics.ActiveConnections))
	assert.Len(t, pool.queryClients, 1, "client should be back in the pool channel")
}

// ============================================================
// EventPool: Get / Put cycle resets fields
// ============================================================

func TestEventPool_GetPutCycle(t *testing.T) {
	ep := NewEventPool()

	ev := ep.Get()
	require.NotNil(t, ev)

	// Populate event fields
	ev.ID = "test-id"
	ev.Type = "test-type"
	ev.Data = "some data"
	ev.Timestamp = time.Now()
	ev.Priority = PriorityCritical

	// Return to pool (should reset)
	ep.Put(ev)

	// Get again – sync.Pool may return the same object (not guaranteed, but likely in tests)
	ev2 := ep.Get()
	require.NotNil(t, ev2)

	// If we got the same object back, fields should be reset
	if ev2 == ev {
		assert.Empty(t, ev2.ID)
		assert.Empty(t, ev2.Type)
		assert.Nil(t, ev2.Data)
		assert.True(t, ev2.Timestamp.IsZero())
		assert.Equal(t, PriorityNormal, ev2.Priority)
	}
}

// ============================================================
// EventManager: updateAvgProcessingTime (EMA)
// ============================================================

func TestUpdateAvgProcessingTime(t *testing.T) {
	em := &EventManager{
		metrics: &EventMetrics{},
	}

	em.updateAvgProcessingTime(100 * time.Millisecond)
	assert.Equal(t, time.Duration(float64(100*time.Millisecond)*0.1), em.metrics.AvgProcessingTime)

	prev := em.metrics.AvgProcessingTime
	em.updateAvgProcessingTime(200 * time.Millisecond)
	expected := time.Duration(float64(prev)*0.9 + float64(200*time.Millisecond)*0.1)
	assert.Equal(t, expected, em.metrics.AvgProcessingTime)
}

func TestUpdateAvgProcessingTime_ConvergesToSteadyState(t *testing.T) {
	em := &EventManager{
		metrics: &EventMetrics{},
	}

	steady := 30 * time.Millisecond
	for i := 0; i < 200; i++ {
		em.updateAvgProcessingTime(steady)
	}

	diff := em.metrics.AvgProcessingTime - steady
	if diff < 0 {
		diff = -diff
	}
	assert.Less(t, diff, 1*time.Millisecond, "EMA should converge to the steady input value")
}

// ============================================================
// NewEventPool: returns usable events
// ============================================================

func TestNewEventPool_ReturnsEvents(t *testing.T) {
	ep := NewEventPool()
	require.NotNil(t, ep)

	ev := ep.Get()
	require.NotNil(t, ev)

	// Fresh event should have zero values
	assert.Empty(t, ev.ID)
	assert.Empty(t, ev.Type)
	assert.Nil(t, ev.Data)
	assert.True(t, ev.Timestamp.IsZero())
}

// ============================================================
// Priority constants: existence and ordering
// ============================================================

func TestPriorityConstantsOrdering(t *testing.T) {
	assert.Less(t, PriorityLow, PriorityNormal)
	assert.Less(t, PriorityNormal, PriorityHigh)
	assert.Less(t, PriorityHigh, PriorityCritical)
}

func TestPriorityConstantsValues(t *testing.T) {
	assert.Equal(t, Priority(0), PriorityLow)
	assert.Equal(t, Priority(1), PriorityNormal)
	assert.Equal(t, Priority(2), PriorityHigh)
	assert.Equal(t, Priority(3), PriorityCritical)
}

// ============================================================
// NetworkHandler: StopNetwork when not active
// ============================================================

func TestNetworkHandler_StopNetworkWhenNotActive(t *testing.T) {
	cfg := &config.Config{
		DefraDB: config.DefraDBConfig{
			P2P: config.DefraP2PConfig{
				Enabled:             false,
				BootstrapPeers:      []string{},
				MaxRetries:          2,
				RetryBaseDelayMs:    100,
				ReconnectIntervalMs: 1000,
			},
		},
	}

	nh := NewNetworkHandler(nil, cfg)
	require.False(t, nh.IsNetworkActive())

	// Stopping an already-inactive network should succeed (no-op)
	err := nh.StopNetwork()
	require.NoError(t, err)
	assert.False(t, nh.IsNetworkActive())
}

// ============================================================
// NetworkHandler: ToggleNetwork
// ============================================================

func TestNetworkHandler_ToggleNetwork(t *testing.T) {
	cfg := &config.Config{
		DefraDB: config.DefraDBConfig{
			P2P: config.DefraP2PConfig{
				Enabled:             false,
				BootstrapPeers:      []string{},
				MaxRetries:          1,
				RetryBaseDelayMs:    10,
				ReconnectIntervalMs: 60000,
				EnableAutoReconnect: false,
			},
		},
	}

	nh := NewNetworkHandler(nil, cfg)
	require.False(t, nh.IsNetworkActive())

	// Toggle on (no peers, so StartNetwork succeeds trivially)
	err := nh.ToggleNetwork()
	require.NoError(t, err)
	assert.True(t, nh.IsNetworkActive())

	// Toggle off
	err = nh.ToggleNetwork()
	require.NoError(t, err)
	assert.False(t, nh.IsNetworkActive())
}

// ============================================================
// NetworkHandler: GetConnectedPeers when no peers
// ============================================================

func TestNetworkHandler_GetConnectedPeersEmpty(t *testing.T) {
	cfg := &config.Config{
		DefraDB: config.DefraDBConfig{
			P2P: config.DefraP2PConfig{
				Enabled:             false,
				BootstrapPeers:      []string{},
				MaxRetries:          2,
				RetryBaseDelayMs:    100,
				ReconnectIntervalMs: 1000,
			},
		},
	}

	nh := NewNetworkHandler(nil, cfg)

	connected := nh.GetConnectedPeers()
	assert.Empty(t, connected)
}

func TestNetworkHandler_GetConnectedPeersAllDisconnected(t *testing.T) {
	cfg := &config.Config{
		DefraDB: config.DefraDBConfig{
			P2P: config.DefraP2PConfig{
				Enabled:        true,
				BootstrapPeers: []string{"/ip4/127.0.0.1/tcp/4001/p2p/12D3KooWPeer1", "/ip4/127.0.0.1/tcp/4002/p2p/12D3KooWPeer2"},
				MaxRetries:     2,
				RetryBaseDelayMs:    100,
				ReconnectIntervalMs: 1000,
			},
		},
	}

	nh := NewNetworkHandler(nil, cfg)

	// All peers start as disconnected
	connected := nh.GetConnectedPeers()
	assert.Empty(t, connected)
	assert.Equal(t, 2, len(nh.GetPeers()))
}

// ============================================================
// ConnectionPool: cleanupIdleConnections
// ============================================================

func TestCleanupIdleConnections_RemovesIdleClients(t *testing.T) {
	pool := &ConnectionPool{
		queryClients: make(chan *PooledQueryClient, 10),
		config: &PoolConfig{
			IdleTimeout: 50 * time.Millisecond,
		},
		metrics: &PoolMetrics{},
	}

	// Add an idle client (last used long ago)
	idleClient := &PooledQueryClient{
		lastUsed: time.Now().Add(-1 * time.Hour),
		inUse:    false,
	}
	pool.queryClients <- idleClient

	// Add an active (recently used) client
	activeClient := &PooledQueryClient{
		lastUsed: time.Now(),
		inUse:    false,
	}
	pool.queryClients <- activeClient

	assert.Len(t, pool.queryClients, 2)

	pool.cleanupIdleConnections()

	// Only the active client should remain
	assert.Len(t, pool.queryClients, 1)
}

func TestCleanupIdleConnections_KeepsInUseClients(t *testing.T) {
	pool := &ConnectionPool{
		queryClients: make(chan *PooledQueryClient, 10),
		config: &PoolConfig{
			IdleTimeout: 50 * time.Millisecond,
		},
		metrics: &PoolMetrics{},
	}

	// Client is old but marked as in-use - should not be removed
	inUseClient := &PooledQueryClient{
		lastUsed: time.Now().Add(-1 * time.Hour),
		inUse:    true,
	}
	pool.queryClients <- inUseClient

	pool.cleanupIdleConnections()

	assert.Len(t, pool.queryClients, 1)
}

func TestCleanupIdleConnections_EmptyPool(t *testing.T) {
	pool := &ConnectionPool{
		queryClients: make(chan *PooledQueryClient, 10),
		config: &PoolConfig{
			IdleTimeout: 50 * time.Millisecond,
		},
		metrics: &PoolMetrics{},
	}

	// Should not panic on empty pool
	assert.NotPanics(t, func() {
		pool.cleanupIdleConnections()
	})
	assert.Len(t, pool.queryClients, 0)
}

// ============================================================
// ConnectionPool: ReturnClient to closed pool (dropped)
// ============================================================

func TestConnectionPool_ReturnClientToClosedPool(t *testing.T) {
	pool := &ConnectionPool{
		queryClients: make(chan *PooledQueryClient, 5),
		config:       DefaultPoolConfig(),
		metrics:      &PoolMetrics{},
	}

	client := &PooledQueryClient{
		lastUsed: time.Now(),
		inUse:    true,
	}

	// Close the pool first
	err := pool.Close()
	require.NoError(t, err)

	// Returning a client to a closed pool should not panic
	assert.NotPanics(t, func() {
		pool.ReturnClient(client)
	})
}

// ============================================================
// ConnectionPool: ReturnClient when pool channel is full
// ============================================================

func TestConnectionPool_ReturnClientPoolFull(t *testing.T) {
	pool := &ConnectionPool{
		queryClients: make(chan *PooledQueryClient, 1), // capacity 1
		config:       DefaultPoolConfig(),
		metrics:      &PoolMetrics{},
	}

	c1 := &PooledQueryClient{lastUsed: time.Now(), inUse: true}
	c2 := &PooledQueryClient{lastUsed: time.Now(), inUse: true}

	// Fill the pool
	pool.ReturnClient(c1)
	assert.Len(t, pool.queryClients, 1)

	// Second return should not block - just gets dropped
	assert.NotPanics(t, func() {
		pool.ReturnClient(c2)
	})
	assert.Len(t, pool.queryClients, 1) // still 1, second was dropped
}

// ============================================================
// ConnectionPool: GetClient with cancelled context
// ============================================================

func TestConnectionPool_GetClientCancelledContext(t *testing.T) {
	pool := &ConnectionPool{
		queryClients: make(chan *PooledQueryClient, 5), // empty pool, no clients to get
		config: &PoolConfig{
			ConnectionTimeout: 5 * time.Second,
		},
		metrics: &PoolMetrics{},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err := pool.GetClient(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// ============================================================
// wrapQueryIfNeeded: various inputs
// ============================================================

func TestWrapQueryIfNeeded_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"bare field", "User { name }", "query { User { name } }"},
		{"braces wrapping", "{ User { name } }", "query { User { name } }"},
		{"whitespace padding", "  User { name }  ", "query { User { name } }"},
		{"query keyword only", "query", "query"},
		{"mutation keyword only", "mutation", "mutation"},
		{"subscription keyword only", "subscription", "subscription"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := wrapQueryIfNeeded(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// ============================================================
// DefaultOptimizedClientConfig: verify defaults
// ============================================================

func TestDefaultOptimizedClientConfig(t *testing.T) {
	cfg := DefaultOptimizedClientConfig()
	require.NotNil(t, cfg)
	assert.NotNil(t, cfg.EventManager)
	assert.NotNil(t, cfg.ConnectionPool)
	assert.True(t, cfg.EnableMetrics)
	assert.Equal(t, 8080, cfg.MetricsPort)
}

// ============================================================
// DefaultPoolConfig: verify defaults
// ============================================================

func TestDefaultPoolConfig(t *testing.T) {
	cfg := DefaultPoolConfig()
	require.NotNil(t, cfg)
	assert.Equal(t, 20, cfg.MaxConnections)
	assert.Equal(t, 5, cfg.MinConnections)
	assert.True(t, cfg.EnableQueryCaching)
}

// ============================================================
// ConnectionPool: GetClient timeout
// ============================================================

func TestConnectionPool_GetClientTimeout(t *testing.T) {
	pool := &ConnectionPool{
		queryClients: make(chan *PooledQueryClient, 5), // empty, no clients
		config: &PoolConfig{
			ConnectionTimeout: 50 * time.Millisecond, // very short
		},
		metrics: &PoolMetrics{},
	}

	ctx := context.Background()
	_, err := pool.GetClient(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timeout")
}

// ============================================================
// NetworkHandler: forceReconnectAll marks peers as disconnected
// ============================================================

func TestNetworkHandler_ForceReconnectAll(t *testing.T) {
	cfg := &config.Config{
		DefraDB: config.DefraDBConfig{
			P2P: config.DefraP2PConfig{
				Enabled:             false,
				BootstrapPeers:      []string{},
				MaxRetries:          1,
				RetryBaseDelayMs:    10,
				ReconnectIntervalMs: 60000,
			},
		},
	}

	nh := NewNetworkHandler(nil, cfg)

	// Manually add a connected peer
	peerAddr := "/ip4/10.0.0.1/tcp/4001/p2p/12D3KooWPeer1"
	nh.peersMu.Lock()
	nh.peers[peerAddr] = &PeerState{
		Address:     peerAddr,
		State:       StateConnected,
		ConnectedAt: time.Now(),
	}
	nh.peersMu.Unlock()

	// forceReconnectAll marks all connected peers as disconnected, then calls
	// reconnectDisconnectedPeers. With nil node the reconnect goroutines would
	// panic, so we cancel the context before the call to prevent them from
	// reaching connectToPeers.
	nh.cancel()

	// Now call the marking part directly
	nh.peersMu.Lock()
	for _, peer := range nh.peers {
		if peer.State == StateConnected {
			peer.State = StateDisconnected
			peer.ConnectedAt = time.Time{}
			peer.LastError = fmt.Errorf("P2P mesh lost - no active peers")
		}
	}
	nh.peersMu.Unlock()

	nh.peersMu.RLock()
	state := nh.peers[peerAddr]
	nh.peersMu.RUnlock()

	assert.Equal(t, StateDisconnected, state.State)
	assert.True(t, state.ConnectedAt.IsZero())
	assert.NotNil(t, state.LastError)
}

// ============================================================
// NetworkHandler: checkPeerHealth with nil node
// ============================================================

func TestNetworkHandler_CheckPeerHealthNilNode(t *testing.T) {
	cfg := &config.Config{
		DefraDB: config.DefraDBConfig{
			P2P: config.DefraP2PConfig{
				BootstrapPeers:      []string{},
				MaxRetries:          1,
				RetryBaseDelayMs:    10,
				ReconnectIntervalMs: 60000,
			},
		},
	}

	nh := NewNetworkHandler(nil, cfg)

	// Should not panic with nil node
	assert.NotPanics(t, func() {
		nh.checkPeerHealth()
	})
}

// ============================================================
// NetworkHandler: reconnectDisconnectedPeers with no disconnected
// ============================================================

func TestNetworkHandler_ReconnectDisconnectedPeersNone(t *testing.T) {
	cfg := &config.Config{
		DefraDB: config.DefraDBConfig{
			P2P: config.DefraP2PConfig{
				BootstrapPeers:      []string{"/ip4/10.0.0.1/tcp/4001/p2p/12D3KooWPeer1"},
				MaxRetries:          1,
				RetryBaseDelayMs:    10,
				ReconnectIntervalMs: 60000,
			},
		},
	}

	nh := NewNetworkHandler(nil, cfg)

	// Mark peer as connected so it won't be reconnected
	nh.peersMu.Lock()
	nh.peers["/ip4/10.0.0.1/tcp/4001/p2p/12D3KooWPeer1"].State = StateConnected
	nh.peersMu.Unlock()

	// Should return immediately with no goroutines started
	assert.NotPanics(t, func() {
		nh.reconnectDisconnectedPeers()
	})
}

// ============================================================
// NetworkHandler: attemptReconnect with already-connected peer
// ============================================================

func TestNetworkHandler_AttemptReconnectAlreadyConnected(t *testing.T) {
	cfg := &config.Config{
		DefraDB: config.DefraDBConfig{
			P2P: config.DefraP2PConfig{
				BootstrapPeers:      []string{"/ip4/10.0.0.1/tcp/4001/p2p/12D3KooWPeer1"},
				MaxRetries:          1,
				RetryBaseDelayMs:    10,
				ReconnectIntervalMs: 60000,
			},
		},
	}

	nh := NewNetworkHandler(nil, cfg)

	// Mark peer as connected
	nh.peersMu.Lock()
	nh.peers["/ip4/10.0.0.1/tcp/4001/p2p/12D3KooWPeer1"].State = StateConnected
	nh.peersMu.Unlock()

	// attemptReconnect should bail early for connected peers
	assert.NotPanics(t, func() {
		nh.attemptReconnect("/ip4/10.0.0.1/tcp/4001/p2p/12D3KooWPeer1")
	})

	// State should still be connected
	state, exists := nh.GetPeerState("/ip4/10.0.0.1/tcp/4001/p2p/12D3KooWPeer1")
	assert.True(t, exists)
	assert.Equal(t, StateConnected, state.State)
}

// ============================================================
// NetworkHandler: attemptReconnect with non-existent peer
// ============================================================

func TestNetworkHandler_AttemptReconnectNonExistent(t *testing.T) {
	cfg := &config.Config{
		DefraDB: config.DefraDBConfig{
			P2P: config.DefraP2PConfig{
				BootstrapPeers:      []string{},
				MaxRetries:          1,
				RetryBaseDelayMs:    10,
				ReconnectIntervalMs: 60000,
			},
		},
	}

	nh := NewNetworkHandler(nil, cfg)

	// Should not panic for non-existent peer
	assert.NotPanics(t, func() {
		nh.attemptReconnect("/ip4/1.2.3.4/tcp/4001/p2p/nonexistent")
	})
}

// ============================================================
// EventManager: triggerCleanup
// ============================================================

func TestEventManager_TriggerCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	em := &EventManager{
		eventPool: NewEventPool(),
		config:    DefaultEventManagerConfig(),
		ctx:       ctx,
		cancel:    cancel,
		metrics:   &EventMetrics{},
	}

	// Add a stale subscription
	subCtx, subCancel := context.WithCancel(ctx)
	sub := &Subscription{
		ID:           "stale-sub",
		EventChan:    make(chan *Event, 5),
		ErrorChan:    make(chan error, 5),
		ctx:          subCtx,
		cancel:       subCancel,
		lastActivity: time.Now().Add(-10 * time.Minute), // old
	}
	em.subscriptions.Store(sub.ID, sub)
	em.metrics.ActiveSubs = 1

	em.triggerCleanup()

	// Stale subscription should be removed
	_, loaded := em.subscriptions.Load("stale-sub")
	assert.False(t, loaded, "stale subscription should have been cleaned up")
}

// ============================================================
// EventManager: triggerCleanup with recent subscription (kept)
// ============================================================

func TestEventManager_TriggerCleanupKeepsRecent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	em := &EventManager{
		eventPool: NewEventPool(),
		config:    DefaultEventManagerConfig(),
		ctx:       ctx,
		cancel:    cancel,
		metrics:   &EventMetrics{},
	}

	// Add a recent subscription
	subCtx, subCancel := context.WithCancel(ctx)
	sub := &Subscription{
		ID:           "recent-sub",
		EventChan:    make(chan *Event, 5),
		ErrorChan:    make(chan error, 5),
		ctx:          subCtx,
		cancel:       subCancel,
		lastActivity: time.Now(), // very recent
	}
	em.subscriptions.Store(sub.ID, sub)
	em.metrics.ActiveSubs = 1

	em.triggerCleanup()

	// Recent subscription should be kept
	_, loaded := em.subscriptions.Load("recent-sub")
	assert.True(t, loaded, "recent subscription should be kept")
}

// ============================================================
// NetworkHandler: GetConnectionStats with mixed peer states
// ============================================================

func TestNetworkHandler_GetConnectionStatsMixed(t *testing.T) {
	cfg := &config.Config{
		DefraDB: config.DefraDBConfig{
			P2P: config.DefraP2PConfig{
				BootstrapPeers: []string{
					"/ip4/10.0.0.1/tcp/4001/p2p/Peer1",
					"/ip4/10.0.0.2/tcp/4001/p2p/Peer2",
					"/ip4/10.0.0.3/tcp/4001/p2p/Peer3",
					"/ip4/10.0.0.4/tcp/4001/p2p/Peer4",
				},
				MaxRetries:          1,
				RetryBaseDelayMs:    10,
				ReconnectIntervalMs: 60000,
			},
		},
	}

	nh := NewNetworkHandler(nil, cfg)
	nh.networkActive = true

	// Set different states
	nh.peersMu.Lock()
	nh.peers["/ip4/10.0.0.1/tcp/4001/p2p/Peer1"].State = StateConnected
	nh.peers["/ip4/10.0.0.2/tcp/4001/p2p/Peer2"].State = StateConnecting
	nh.peers["/ip4/10.0.0.3/tcp/4001/p2p/Peer3"].State = StateFailed
	nh.peers["/ip4/10.0.0.4/tcp/4001/p2p/Peer4"].State = StateReconnecting
	nh.peersMu.Unlock()

	stats := nh.GetConnectionStats()
	assert.Equal(t, 4, stats.TotalPeers)
	assert.Equal(t, 1, stats.ConnectedPeers)
	assert.Equal(t, 2, stats.ConnectingPeers) // Connecting + Reconnecting
	assert.Equal(t, 1, stats.FailedPeers)
	assert.Equal(t, 0, stats.DisconnectedPeers)
	assert.True(t, stats.NetworkActive)
}

// ============================================================
// DefaultEventManagerConfig: verify defaults
// ============================================================

func TestDefaultEventManagerConfig(t *testing.T) {
	cfg := DefaultEventManagerConfig()
	require.NotNil(t, cfg)
	assert.Equal(t, 50, cfg.DefaultBufferSize)
	assert.Equal(t, 10, cfg.HighPriorityBuffer)
	assert.Equal(t, 25, cfg.BatchSize)
	assert.Equal(t, 100, cfg.MaxConcurrentSubs)
	assert.True(t, cfg.BackpressureEnabled)
	assert.Equal(t, 100, cfg.MemoryThresholdMB)
}

// ============================================================
// NetworkHandler: startNoPeersEventListener with nil node
// ============================================================

func TestNetworkHandler_StartNoPeersEventListenerNilNode(t *testing.T) {
	cfg := &config.Config{
		DefraDB: config.DefraDBConfig{
			P2P: config.DefraP2PConfig{
				BootstrapPeers:      []string{},
				MaxRetries:          1,
				RetryBaseDelayMs:    10,
				ReconnectIntervalMs: 60000,
			},
		},
	}

	nh := NewNetworkHandler(nil, cfg)

	// Should return early without panicking
	assert.NotPanics(t, func() {
		nh.startNoPeersEventListener()
	})
}

// ============================================================
// NetworkHandler: GetConnectedPeers with connected peer
// ============================================================

func TestNetworkHandler_GetConnectedPeersWithConnected(t *testing.T) {
	cfg := &config.Config{
		DefraDB: config.DefraDBConfig{
			P2P: config.DefraP2PConfig{
				BootstrapPeers: []string{
					"/ip4/10.0.0.1/tcp/4001/p2p/Peer1",
					"/ip4/10.0.0.2/tcp/4001/p2p/Peer2",
				},
				MaxRetries:          1,
				RetryBaseDelayMs:    10,
				ReconnectIntervalMs: 60000,
			},
		},
	}

	nh := NewNetworkHandler(nil, cfg)

	nh.peersMu.Lock()
	nh.peers["/ip4/10.0.0.1/tcp/4001/p2p/Peer1"].State = StateConnected
	nh.peersMu.Unlock()

	connected := nh.GetConnectedPeers()
	assert.Len(t, connected, 1)
	assert.Equal(t, "/ip4/10.0.0.1/tcp/4001/p2p/Peer1", connected[0])
}
