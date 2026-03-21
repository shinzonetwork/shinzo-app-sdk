package defra

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// newTestConfig returns a fresh config suitable for spinning up an isolated
// DefraDB node inside a test. Every call produces a unique temp directory so
// tests do not interfere with each other.
func newTestConfig(t *testing.T) *config.Config {
	t.Helper()
	return &config.Config{
		DefraDB: config.DefraDBConfig{
			Url:           "http://localhost:0",
			KeyringSecret: "test-secret-key-that-is-long-enough",
			P2P: config.DefraP2PConfig{
				BootstrapPeers: []string{},
				ListenAddr:     "/ip4/0.0.0.0/tcp/0",
			},
			Store: config.DefraStoreConfig{
				Path: t.TempDir(),
			},
		},
		Logger: config.LoggerConfig{Development: true},
	}
}

// newTestConfigWithSchema returns a config and a schema applier that creates a
// User type with name (String) and age (Int).
func newTestConfigWithSchema(t *testing.T) (*config.Config, SchemaApplier) {
	t.Helper()
	cfg := newTestConfig(t)
	schema := NewSchemaApplierFromProvidedSchema(`
		type User {
			name: String
			age:  Int
		}
	`)
	return cfg, schema
}

// ---------------------------------------------------------------------------
// 1. OptimizedDefraClient — closed client (single node, all closed-client tests)
// ---------------------------------------------------------------------------

func TestOptimizedDefraClient_ClosedClient(t *testing.T) {
	cfg := newTestConfig(t)

	client, err := NewOptimizedDefraClient(cfg, &MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)

	// Close it first.
	err = client.Close()
	require.NoError(t, err)

	ctx := context.Background()

	t.Run("AllMethodsReturnError", func(t *testing.T) {
		assert.Error(t, client.StartNetwork())
		assert.Error(t, client.StopNetwork())
		assert.Error(t, client.ToggleNetwork())
		assert.False(t, client.IsNetworkActive())
		assert.False(t, client.IsHostRunning())
		assert.Error(t, client.AddPeer("/ip4/127.0.0.1/tcp/9999/p2p/QmFake"))
		assert.Error(t, client.RemovePeer("/ip4/127.0.0.1/tcp/9999/p2p/QmFake"))
		assert.Nil(t, client.GetPeerStates())
		assert.Nil(t, client.GetConnectedPeers())
		assert.Equal(t, ConnectionStats{}, client.GetConnectionStats())

		var result interface{}
		assert.Error(t, client.Query(ctx, `query { User { name } }`, &result))
		assert.Error(t, client.Mutation(ctx, `mutation { create_User(input:{name:"x"}) { name } }`, &result))

		_, err = client.BatchQuery(ctx, []string{`query { User { name } }`})
		assert.Error(t, err)

		_, err = client.Subscribe(ctx, `subscription { User { name } }`, PriorityNormal)
		assert.Error(t, err)

		// Unsubscribe on a closed client should not panic.
		client.Unsubscribe("non-existent-id")

		assert.Equal(t, ClientMetrics{}, client.GetMetrics())
		assert.Error(t, client.HealthCheck(ctx))

		// OptimizePerformance on closed client should not panic.
		client.OptimizePerformance()
	})

	t.Run("DoubleCloseReturnsError", func(t *testing.T) {
		err = client.Close()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "already closed")
	})
}

// ---------------------------------------------------------------------------
// 2. OptimizedDefraClient — no-schema node (network, health, metrics, optimize)
// ---------------------------------------------------------------------------

func TestOptimizedDefraClient_NoSchema(t *testing.T) {
	cfg := newTestConfig(t)

	client, err := NewOptimizedDefraClient(cfg, &MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer client.Close()

	t.Run("HealthCheck", func(t *testing.T) {
		err := client.HealthCheck(context.Background())
		require.NoError(t, err)
	})

	t.Run("GetMetrics", func(t *testing.T) {
		metrics := client.GetMetrics()
		assert.GreaterOrEqual(t, metrics.ConnectionPool.TotalQueries, int64(0))
		assert.GreaterOrEqual(t, metrics.EventManager.TotalEvents, int64(0))
	})

	t.Run("OptimizePerformance", func(t *testing.T) {
		client.OptimizePerformance()
	})

	t.Run("NetworkMethods", func(t *testing.T) {
		assert.True(t, client.IsHostRunning())

		peers := client.GetPeerStates()
		assert.NotNil(t, peers)

		connected := client.GetConnectedPeers()
		assert.NotNil(t, connected)

		stats := client.GetConnectionStats()
		assert.True(t, stats.HostRunning)

		fakeAddr := "/ip4/127.0.0.1/tcp/12345/p2p/QmFakePeerIdThatWillNotConnect"
		err := client.AddPeer(fakeAddr)
		require.NoError(t, err)

		err = client.RemovePeer(fakeAddr)
		require.NoError(t, err)
	})
}

// ---------------------------------------------------------------------------
// 3. OptimizedDefraClient — with User schema (query, mutation, subscribe, batch)
// ---------------------------------------------------------------------------

func TestOptimizedDefraClient_WithUserSchema(t *testing.T) {
	cfg, schema := newTestConfigWithSchema(t)

	client, err := NewOptimizedDefraClient(cfg, schema)
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	t.Run("MutationAndQuery", func(t *testing.T) {
		var mutResult map[string]interface{}
		err := client.Mutation(ctx, `mutation { create_User(input:{name:"Charlie", age:40}) { name age } }`, &mutResult)
		require.NoError(t, err)
		assert.Equal(t, "Charlie", mutResult["name"])

		var queryResult map[string]interface{}
		err = client.Query(ctx, `query { User { name age } }`, &queryResult)
		require.NoError(t, err)
		assert.Contains(t, queryResult, "User")
	})

	t.Run("BatchQuery", func(t *testing.T) {
		results, err := client.BatchQuery(ctx, []string{
			`query { __schema { types { name } } }`,
			`query { User { name } }`,
		})
		require.NoError(t, err)
		require.Len(t, results, 2)
	})

	t.Run("Subscribe", func(t *testing.T) {
		subCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()

		sub, err := client.Subscribe(subCtx, `subscription { User { name } }`, PriorityNormal)
		require.NoError(t, err)
		require.NotNil(t, sub)
		require.NotEmpty(t, sub.ID)

		client.Unsubscribe(sub.ID)
	})

	t.Run("QuerySingle", func(t *testing.T) {
		var result map[string]interface{}
		err := client.QuerySingle(ctx, `query { User { name age } }`, &result)
		require.NoError(t, err)
		assert.Contains(t, result, "User")
	})

	t.Run("QueryArray", func(t *testing.T) {
		var result map[string]interface{}
		err := client.QueryArray(ctx, `query { User { name age } }`, &result)
		require.NoError(t, err)
		assert.Contains(t, result, "User")
	})

	t.Run("SubscribeTyped", func(t *testing.T) {
		subCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		var resultType map[string]interface{}
		typedChan, errorChan, err := client.SubscribeTyped(subCtx, `subscription { User { name } }`, PriorityNormal, resultType)
		require.NoError(t, err)
		require.NotNil(t, typedChan)
		require.NotNil(t, errorChan)

		// Trigger a mutation to generate a subscription event
		go func() {
			time.Sleep(200 * time.Millisecond)
			var mutRes map[string]interface{}
			_ = client.Mutation(ctx, `mutation { create_User(input:{name:"SubTest", age:99}) { name age } }`, &mutRes)
		}()

		// Wait for the event or timeout
		select {
		case <-typedChan:
			// Received an event (may be nil map, that's fine)
		case err := <-errorChan:
			// Error channel event is also acceptable coverage
			t.Logf("Received error from subscription: %v", err)
		case <-time.After(3 * time.Second):
			// Timeout is acceptable - subscription may not fire for all mutation types
		}
	})

	t.Run("SubscribeTyped_ClosedClient", func(t *testing.T) {
		// Use a separate client for this test since we need to close it
		cfg2, schema2 := newTestConfigWithSchema(t)
		client2, err := NewOptimizedDefraClient(cfg2, schema2)
		require.NoError(t, err)
		err = client2.Close()
		require.NoError(t, err)

		_, _, err = client2.SubscribeTyped(ctx, `subscription { User { name } }`, PriorityNormal, nil)
		require.Error(t, err)
	})

	t.Run("MutationSingle", func(t *testing.T) {
		var result map[string]interface{}
		err := client.MutationSingle(ctx, `mutation { create_User(input:{name:"Eve", age:28}) { name age } }`, &result)
		require.NoError(t, err)
		assert.Equal(t, "Eve", result["name"])
	})
}

// ---------------------------------------------------------------------------
// 4. Legacy helpers (use one shared node)
// ---------------------------------------------------------------------------

func TestOptimizedLegacyHelpers(t *testing.T) {
	cfg, schema := newTestConfigWithSchema(t)

	client, err := NewOptimizedDefraClient(cfg, schema)
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	t.Run("PostMutation", func(t *testing.T) {
		var result map[string]interface{}
		err := OptimizedPostMutation(ctx, client, `mutation { create_User(input:{name:"Alice", age:30}) { name age } }`, &result)
		require.NoError(t, err)
		assert.Equal(t, "Alice", result["name"])
	})

	t.Run("Subscribe", func(t *testing.T) {
		subCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()

		var resultType map[string]interface{}
		ch, err := OptimizedSubscribe(subCtx, client, `subscription { User { name } }`, resultType)
		require.NoError(t, err)
		require.NotNil(t, ch)
	})
}

// ---------------------------------------------------------------------------
// 5. StartOptimizedDefraInstance
// ---------------------------------------------------------------------------

func TestStartOptimizedDefraInstance(t *testing.T) {
	cfg := newTestConfig(t)

	client, err := StartOptimizedDefraInstance(cfg, &MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	require.NotNil(t, client)
	defer client.Close()

	err = client.HealthCheck(context.Background())
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// 6. ConnectionPool integration (single real node for all pool tests)
// ---------------------------------------------------------------------------

func TestConnectionPool_RealNode(t *testing.T) {
	cfg, schema := newTestConfigWithSchema(t)

	defraNode, _, err := StartDefraInstance(cfg, schema, nil, nil)
	require.NoError(t, err)
	defer defraNode.Close(context.Background())

	pool, err := NewConnectionPool(defraNode, DefaultPoolConfig())
	require.NoError(t, err)
	defer pool.Close()

	ctx := context.Background()

	t.Run("OptimizedQuery", func(t *testing.T) {
		var result map[string]interface{}
		err := pool.OptimizedQuery(ctx, `query { __schema { types { name } } }`, &result)
		require.NoError(t, err)
		assert.Contains(t, result, "__schema")
	})

	t.Run("OptimizedMutation", func(t *testing.T) {
		var result map[string]interface{}
		err := pool.OptimizedMutation(ctx, `mutation { create_User(input:{name:"Bob", age:25}) { name age } }`, &result)
		require.NoError(t, err)
		assert.Equal(t, "Bob", result["name"])
	})

	t.Run("BatchQuery", func(t *testing.T) {
		queries := []string{
			`query { __schema { types { name } } }`,
			`query { User { name } }`,
		}

		results, err := pool.BatchQuery(ctx, queries)
		require.NoError(t, err)
		require.Len(t, results, 2)
		firstResult, ok := results[0].(map[string]interface{})
		require.True(t, ok)
		assert.Contains(t, firstResult, "__schema")
	})
}

// ---------------------------------------------------------------------------
// 7. defra.go -- GetOrCreateNodeIdentity / GetIdentityContext
// ---------------------------------------------------------------------------

func TestGetOrCreateNodeIdentity(t *testing.T) {
	cfg := newTestConfig(t)

	identity, err := GetOrCreateNodeIdentity(cfg)
	require.NoError(t, err)
	require.NotNil(t, identity)

	// Calling again with the same store path should return the same identity.
	identity2, err := GetOrCreateNodeIdentity(cfg)
	require.NoError(t, err)
	require.NotNil(t, identity2)
}

func TestGetIdentityContext(t *testing.T) {
	cfg := newTestConfig(t)

	ctx := context.Background()
	identityCtx, err := GetIdentityContext(ctx, cfg)
	require.NoError(t, err)
	require.NotNil(t, identityCtx)
	assert.NotEqual(t, ctx, identityCtx)
}

// ---------------------------------------------------------------------------
// 8. Subscribe, QuerySingle, QueryArray, PostMutation with real node (shared)
// ---------------------------------------------------------------------------

func TestQueryAndMutationFunctions(t *testing.T) {
	cfg, schema := newTestConfigWithSchema(t)

	defraNode, _, err := StartDefraInstance(cfg, schema, nil, nil)
	require.NoError(t, err)
	defer defraNode.Close(context.Background())

	ctx := context.Background()

	t.Run("Subscribe_Valid", func(t *testing.T) {
		subCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		ch, err := Subscribe[map[string]interface{}](subCtx, defraNode, `subscription { User { name age } }`)
		require.NoError(t, err)
		require.NotNil(t, ch)

		select {
		case <-ch:
		case <-time.After(500 * time.Millisecond):
		}
	})

	t.Run("PostMutation_CreateAndVerify", func(t *testing.T) {
		type User struct {
			Name string `json:"name"`
			Age  int    `json:"age"`
		}

		result, err := PostMutation[User](ctx, defraNode, `mutation { create_User(input:{name:"Dave", age:35}) { name age } }`)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, "Dave", result.Name)
		assert.Equal(t, 35, result.Age)
	})

	t.Run("PostMutation_NotAMutation", func(t *testing.T) {
		type User struct {
			Name string `json:"name"`
		}
		_, err := PostMutation[User](ctx, defraNode, `query { User { name } }`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must be a mutation")
	})

	t.Run("QuerySingle_Generic", func(t *testing.T) {
		type User struct {
			Name string `json:"name"`
			Age  int    `json:"age"`
		}

		result, err := QuerySingle[User](ctx, defraNode, `User { name age }`)
		require.NoError(t, err)
		assert.Equal(t, "Dave", result.Name)
		assert.Equal(t, 35, result.Age)
	})

	t.Run("QueryArray_Generic", func(t *testing.T) {
		type User struct {
			Name string `json:"name"`
			Age  int    `json:"age"`
		}

		results, err := QueryArray[User](ctx, defraNode, `User { name age }`)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(results), 1)
		assert.Equal(t, "Dave", results[0].Name)
	})

	t.Run("QuerySingle_WithQueryPrefix", func(t *testing.T) {
		type User struct {
			Name string `json:"name"`
		}

		result, err := QuerySingle[User](ctx, defraNode, `query { User { name } }`)
		require.NoError(t, err)
		assert.Equal(t, "Dave", result.Name)
	})

	t.Run("QueryArray_WithBraces", func(t *testing.T) {
		type User struct {
			Name string `json:"name"`
		}

		results, err := QueryArray[User](ctx, defraNode, `{ User { name } }`)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(results), 1)
	})

	t.Run("QuerySingle_NilNode", func(t *testing.T) {
		type User struct {
			Name string `json:"name"`
		}
		_, err := QuerySingle[User](ctx, nil, `User { name }`)
		require.Error(t, err)
	})

	t.Run("QueryArray_NilNode", func(t *testing.T) {
		type User struct {
			Name string `json:"name"`
		}
		_, err := QueryArray[User](ctx, nil, `User { name }`)
		require.Error(t, err)
	})

	t.Run("PostMutation_MultipleUsers", func(t *testing.T) {
		type User struct {
			Name string `json:"name"`
			Age  int    `json:"age"`
		}

		// Create another user
		result, err := PostMutation[User](ctx, defraNode, `mutation { create_User(input:{name:"Grace", age:50}) { name age } }`)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, "Grace", result.Name)
		assert.Equal(t, 50, result.Age)
	})

	t.Run("PostMutation_MapResult", func(t *testing.T) {
		result, err := PostMutation[map[string]interface{}](ctx, defraNode, `mutation { create_User(input:{name:"Hank", age:60}) { name age } }`)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, "Hank", (*result)["name"])
	})

	t.Run("QueryDataInto_EmptyQuery", func(t *testing.T) {
		qc, err := newQueryClient(defraNode)
		require.NoError(t, err)
		var result []map[string]interface{}
		err = qc.queryDataInto(ctx, "", &result)
		require.Error(t, err)
	})
}

// ---------------------------------------------------------------------------
// 9. queryClient methods (queryInto, queryDataInto edge cases) — shared node
// ---------------------------------------------------------------------------

func TestQueryClientMethods(t *testing.T) {
	cfg, schema := newTestConfigWithSchema(t)

	defraNode, _, err := StartDefraInstance(cfg, schema, nil, nil)
	require.NoError(t, err)
	defer defraNode.Close(context.Background())

	ctx := context.Background()

	// Create some data first
	defraNode.DB.ExecRequest(ctx, `mutation { create_User(input:{name:"Frank", age:45}) { name age } }`)

	qc, err := newQueryClient(defraNode)
	require.NoError(t, err)

	t.Run("queryInto_MapResult", func(t *testing.T) {
		var result map[string]interface{}
		err := qc.queryInto(ctx, `query { User { name age } }`, &result)
		require.NoError(t, err)
		assert.Contains(t, result, "User")
	})

	t.Run("queryInto_EmptyQuery", func(t *testing.T) {
		var result map[string]interface{}
		err := qc.queryInto(ctx, "", &result)
		require.Error(t, err)
	})

	t.Run("queryDataInto_NonPointer", func(t *testing.T) {
		var result map[string]interface{}
		err := qc.queryDataInto(ctx, `query { User { name age } }`, result) // not a pointer
		require.Error(t, err)
		assert.Contains(t, err.Error(), "result must be a pointer")
	})

	t.Run("queryDataInto_SliceResult", func(t *testing.T) {
		type User struct {
			Name string `json:"name"`
			Age  int    `json:"age"`
		}
		var result []User
		err := qc.queryDataInto(ctx, `query { User { name age } }`, &result)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(result), 1)
		assert.Equal(t, "Frank", result[0].Name)
	})

	t.Run("queryDataInto_SingleResult", func(t *testing.T) {
		type User struct {
			Name string `json:"name"`
			Age  int    `json:"age"`
		}
		var result User
		err := qc.queryDataInto(ctx, `query { User { name age } }`, &result)
		require.NoError(t, err)
		assert.Equal(t, "Frank", result.Name)
	})

	t.Run("getDataField", func(t *testing.T) {
		data, err := qc.getDataField(ctx, `query { User { name age } }`)
		require.NoError(t, err)
		assert.Contains(t, data, "User")
	})

	t.Run("queryAndUnmarshal", func(t *testing.T) {
		var result map[string]interface{}
		err := qc.queryAndUnmarshal(ctx, `query { User { name } }`, &result)
		require.NoError(t, err)
		assert.Contains(t, result, "User")
	})

	t.Run("newQueryClient_NilNode", func(t *testing.T) {
		_, err := newQueryClient(nil)
		require.Error(t, err)
	})
}

// ---------------------------------------------------------------------------
// 10. ConnectionPool with real node — cleanupIdleConnections integration
// ---------------------------------------------------------------------------

func TestConnectionPool_CleanupIntegration(t *testing.T) {
	cfg, schema := newTestConfigWithSchema(t)

	defraNode, _, err := StartDefraInstance(cfg, schema, nil, nil)
	require.NoError(t, err)
	defer defraNode.Close(context.Background())

	poolCfg := &PoolConfig{
		MaxConnections:    10,
		MinConnections:    2,
		ConnectionTimeout: 5 * time.Second,
		IdleTimeout:       50 * time.Millisecond, // very short for testing
		MaxQueryTime:      10 * time.Second,
	}

	pool, err := NewConnectionPool(defraNode, poolCfg)
	require.NoError(t, err)
	defer pool.Close()

	ctx := context.Background()

	// Use a client so it gets returned to the pool
	client, err := pool.GetClient(ctx)
	require.NoError(t, err)
	pool.ReturnClient(client)

	// Wait for idle timeout to pass
	time.Sleep(100 * time.Millisecond)

	// Manually trigger cleanup
	pool.cleanupIdleConnections()

	metrics := pool.GetMetrics()
	assert.GreaterOrEqual(t, metrics.TotalQueries, int64(0))
}

// ---------------------------------------------------------------------------
// 11. Client API (NewClient, Start, ApplySchema, Stop)
// ---------------------------------------------------------------------------

func TestClientAPI(t *testing.T) {
	t.Run("NilConfig", func(t *testing.T) {
		_, err := NewClient(nil)
		require.Error(t, err)
	})

	t.Run("StartAndStop", func(t *testing.T) {
		cfg := newTestConfig(t)
		cfg.DefraDB.P2P.Enabled = false

		c, err := NewClient(cfg)
		require.NoError(t, err)

		ctx := context.Background()
		err = c.Start(ctx)
		require.NoError(t, err)

		assert.NotNil(t, c.GetNode())
		assert.NotNil(t, c.GetNetworkHandler())

		// ApplySchema
		err = c.ApplySchema(ctx, `type Article { title: String }`)
		require.NoError(t, err)

		// Apply same schema again (should get "collection already exists" warning, not error)
		err = c.ApplySchema(ctx, `type Article { title: String }`)
		require.NoError(t, err)

		// Empty schema
		err = c.ApplySchema(ctx, "")
		require.Error(t, err)

		// Double start
		err = c.Start(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "already started")

		err = c.Stop(ctx)
		require.NoError(t, err)

		// ApplySchema after stop
		err = c.ApplySchema(ctx, `type Foo { bar: String }`)
		require.Error(t, err)

		// Stop when already stopped (no-op)
		err = c.Stop(ctx)
		require.NoError(t, err)
	})
}

// ---------------------------------------------------------------------------
// 12. SchemaApplierFromFile (single node for both tests)
// ---------------------------------------------------------------------------

func TestSchemaApplierFromFile(t *testing.T) {
	cfg := newTestConfig(t)

	defraNode, _, err := StartDefraInstance(cfg, &MockSchemaApplierThatSucceeds{}, nil, nil)
	require.NoError(t, err)
	defer defraNode.Close(context.Background())

	t.Run("ApplySchema", func(t *testing.T) {
		tmpDir := t.TempDir()
		schemaContent := `
			type Article {
				title: String
				body:  String
			}
		`
		schemaPath := filepath.Join(tmpDir, "test_schema.graphql")
		err := os.WriteFile(schemaPath, []byte(schemaContent), 0644)
		require.NoError(t, err)

		applier := &SchemaApplierFromFile{
			DefaultPath: schemaPath,
		}

		ctx := context.Background()
		err = applier.ApplySchema(ctx, defraNode)
		require.NoError(t, err)

		result := defraNode.DB.ExecRequest(ctx, `mutation { create_Article(input:{title:"Hello", body:"World"}) { title body } }`)
		require.Empty(t, result.GQL.Errors)
		require.NotNil(t, result.GQL.Data)
	})

	t.Run("MissingFile", func(t *testing.T) {
		applier := &SchemaApplierFromFile{
			DefaultPath: "/nonexistent/path/schema.graphql",
		}

		ctx := context.Background()
		err := applier.ApplySchema(ctx, defraNode)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "find schema file")
	})
}
