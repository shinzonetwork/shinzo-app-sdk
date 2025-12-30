package defra

import (
	"context"
	"testing"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostMutation(t *testing.T) {
	// Create test config
	testConfig := &config.Config{
		DefraDB: config.DefraDBConfig{
			Url:           "http://localhost:0", // Use port 0 for random available port
			KeyringSecret: "test-secret",
			P2P: config.DefraP2PConfig{
				BootstrapPeers: []string{},
				ListenAddr:     "",
			},
			Store: config.DefraStoreConfig{
				Path: t.TempDir(),
			},
		},
		Logger: config.LoggerConfig{
			Development: true,
		},
	}

	// Create schema applier with basic User schema
	schemaApplier := NewSchemaApplierFromProvidedSchema(`
		type User {
			name: String
		}
	`)

	// Start Defra instance
	defraNode, _, err := StartDefraInstance(testConfig, schemaApplier)
	require.NoError(t, err)
	defer defraNode.Close(context.Background())

	ctx := context.Background()

	t.Run("create user mutation", func(t *testing.T) {
		createUserQuery := `
			mutation {
				create_User(input: {name: "Test User"}) {
					name
				}
			}
		`

		// Test with TestUser struct
		user, err := PostMutation[TestUser](ctx, defraNode, createUserQuery)
		require.NoError(t, err)
		assert.Equal(t, "Test User", user.Name)
	})

	t.Run("invalid mutation query", func(t *testing.T) {
		invalidQuery := `
			query {
				User {
					name
				}
			}
		`

		_, err := PostMutation[TestUser](ctx, defraNode, invalidQuery)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Query must be a mutation")
	})
}
