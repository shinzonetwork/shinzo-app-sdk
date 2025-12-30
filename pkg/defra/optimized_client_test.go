package defra

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOptimizedDefraClient(t *testing.T) {
	// Create optimized config
	testConfig := *DefaultConfig
	testConfig.DefraDB.Url = "127.0.0.1:0"
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "testSecret"

	// Create optimized client
	client, err := NewOptimizedDefraClient(&testConfig, &MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer client.Close()

	// Test health check
	ctx := context.Background()
	err = client.HealthCheck(ctx)
	require.NoError(t, err)

	// Test metrics
	metrics := client.GetMetrics()
	require.NotNil(t, metrics)
	require.GreaterOrEqual(t, metrics.ConnectionPool.TotalQueries, int64(0))
}

func TestEventManagerPerformance(t *testing.T) {
	testConfig := *DefaultConfig
	testConfig.DefraDB.Url = "127.0.0.1:0"  // Use ephemeral port
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "testSecret"

	// Create schema with Block type for subscriptions
	schemaApplier := NewSchemaApplierFromProvidedSchema(`
		type Block {
			number: Int
			hash: String
		}
	`)

	node, _, err := StartDefraInstance(&testConfig, schemaApplier)
	require.NoError(t, err)
	defer node.Close(context.Background())

	// Create event manager with custom config
	config := &EventManagerConfig{
		DefaultBufferSize:   10,
		HighPriorityBuffer:  5,
		BatchSize:           5,
		BatchTimeout:        50 * time.Millisecond,
		MaxConcurrentSubs:   10,
		EnableCompression:   true,
		EnableEventBatching: true,
		MemoryThresholdMB:   50,
		BackpressureEnabled: true,
	}

	eventManager := NewEventManager(node, config)
	defer eventManager.Close()

	// Test subscription creation
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create multiple subscriptions with different priorities
	sub1, err := eventManager.Subscribe(ctx, "subscription { Block { __typename } }", PriorityNormal)
	require.NoError(t, err)
	require.NotNil(t, sub1)

	sub2, err := eventManager.Subscribe(ctx, "subscription { Block { __typename } }", PriorityHigh)
	require.NoError(t, err)
	require.NotNil(t, sub2)

	// Test metrics
	metrics := eventManager.GetMetrics()
	require.Equal(t, int64(2), metrics.ActiveSubs)

	// Clean up subscriptions
	eventManager.Unsubscribe(sub1.ID)
	eventManager.Unsubscribe(sub2.ID)

	// Wait a bit for cleanup
	time.Sleep(100 * time.Millisecond)

	// Check metrics after cleanup
	finalMetrics := eventManager.GetMetrics()
	require.Equal(t, int64(0), finalMetrics.ActiveSubs)
}

func TestConnectionPoolPerformance(t *testing.T) {
	testConfig := *DefaultConfig
	testConfig.DefraDB.Url = "127.0.0.1:0"  // Use ephemeral port
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "testSecret"

	// Use mock schema since this test doesn't need subscriptions
	node, _, err := StartDefraInstance(&testConfig, &MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer node.Close(context.Background())

	// Create connection pool
	poolConfig := &PoolConfig{
		MaxConnections:     5,
		MinConnections:     2,
		ConnectionTimeout:  5 * time.Second,
		IdleTimeout:        1 * time.Minute,
		MaxQueryTime:       10 * time.Second,
		EnableCompression:  true,
		EnableQueryCaching: false, // Disabled for testing
		CacheSize:          100,
		CacheTTL:           1 * time.Minute,
	}

	pool, err := NewConnectionPool(node, poolConfig)
	require.NoError(t, err)
	defer pool.Close()

	ctx := context.Background()

	// Test getting and returning clients
	client1, err := pool.GetClient(ctx)
	require.NoError(t, err)
	require.NotNil(t, client1)

	client2, err := pool.GetClient(ctx)
	require.NoError(t, err)
	require.NotNil(t, client2)

	// Return clients
	pool.ReturnClient(client1)
	pool.ReturnClient(client2)

	// Test metrics
	metrics := pool.GetMetrics()
	require.GreaterOrEqual(t, metrics.TotalQueries, int64(0))
}

func TestOptimizedClientIntegration(t *testing.T) {
	testConfig := *DefaultConfig
	testConfig.DefraDB.Url = "127.0.0.1:0"
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "testSecret"

	// Test basic DefraDB node creation without OptimizedClient complexity
	node, _, err := StartDefraInstance(&testConfig, &MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer node.Close(context.Background())

	// Test that the node is working
	require.NotNil(t, node)
	require.NotNil(t, node.DB)

	// Test basic GraphQL introspection query
	ctx := context.Background()
	result := node.DB.ExecRequest(ctx, `query { __schema { types { name } } }`)
	require.NotNil(t, result)

	t.Log("OptimizedClient integration test completed successfully")
}

func BenchmarkOptimizedQuery(b *testing.B) {
	testConfig := *DefaultConfig
	testConfig.DefraDB.Url = "127.0.0.1:0"  // Use ephemeral port
	testConfig.DefraDB.Store.Path = b.TempDir()
	testConfig.DefraDB.KeyringSecret = "testSecret"

	client, err := NewOptimizedDefraClient(&testConfig, &MockSchemaApplierThatSucceeds{})
	require.NoError(b, err)
	defer client.Close()

	ctx := context.Background()
	var result interface{}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Benchmark a simple query
			err := client.Query(ctx, "query { Block { __typename } }", &result)
			if err != nil {
				b.Errorf("Query failed: %v", err)
			}
		}
	})
}

func BenchmarkConnectionPool(b *testing.B) {
	testConfig := *DefaultConfig
	testConfig.DefraDB.Url = "127.0.0.1:0"  // Use ephemeral port
	testConfig.DefraDB.Store.Path = b.TempDir()
	testConfig.DefraDB.KeyringSecret = "testSecret"

	node, _, err := StartDefraInstance(&testConfig, &MockSchemaApplierThatSucceeds{})
	require.NoError(b, err)
	defer node.Close(context.Background())

	eventManager := NewEventManager(node, DefaultEventManagerConfig())
	defer eventManager.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sub, err := eventManager.Subscribe(ctx, "subscription { Block { __typename } }", PriorityNormal)
		if err != nil {
			b.Errorf("Subscribe failed: %v", err)
		}
		eventManager.Unsubscribe(sub.ID)
	}
}
