package defra

import (
	"context"
	"testing"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/config"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/file"
	"github.com/stretchr/testify/require"
)

func TestStartDefra(t *testing.T) {
	// Create a copy of DefaultConfig to avoid modifying the shared instance
	testConfig := *DefaultConfig
	testConfig.DefraDB.Url = "127.0.0.1:0"
	testConfig.DefraDB.Store.Path = t.TempDir() // Use isolated temp directory for each test
	testConfig.DefraDB.KeyringSecret = "testSecret"
	myNode, _, err := StartDefraInstance(&testConfig, &MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	myNode.Close(context.Background())
}

func TestStartDefraUsingConfig(t *testing.T) {
	configPath, err := file.FindFile("config.yaml")
	require.NoError(t, err)

	testConfig, err := config.LoadConfig(configPath)
	require.NoError(t, err)

	testConfig.DefraDB.Url = "127.0.0.1:0"      // In case we have something else running
	testConfig.DefraDB.Store.Path = t.TempDir() // Use isolated temp directory for each test

	myNode, _, err := StartDefraInstance(testConfig, &MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	myNode.Close(context.Background())
}

func TestSubsequentRestartsYieldTheSameIdentity(t *testing.T) {
	testConfig := DefaultConfig
	testConfig.DefraDB.KeyringSecret = "testSecret"
	myNode, _, err := StartDefraInstance(testConfig, &MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)

	peerInfo, err := myNode.DB.PeerInfo()
	require.NoError(t, err)
	require.NotNil(t, peerInfo)
	require.Greater(t, len(peerInfo), 0)

	err = myNode.Close(t.Context())
	require.NoError(t, err)

	myNode, _, err = StartDefraInstance(testConfig, &MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)

	newPeerInfo, err := myNode.DB.PeerInfo()
	require.NoError(t, err)
	require.ElementsMatch(t, peerInfo, newPeerInfo)
}
