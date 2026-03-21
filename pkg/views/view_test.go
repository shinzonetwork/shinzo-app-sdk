package views

import (
	"context"
	"testing"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/shinzonetwork/view-creator/core/models"
	"github.com/stretchr/testify/require"
)

func TestSubscribeToView(t *testing.T) {
	query := "Log {address topics data transactionHash blockNumber}"
	sdl := "type FilteredAndDecodedLogs {transactionHash: String}"
	testView := View{
		Query:     &query,
		Sdl:       &sdl,
		Transform: models.Transform{},
		Name:      "FilteredAndDecodedLogs",
	}

	myDefra, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	err = testView.SubscribeTo(context.Background(), myDefra)
	require.NoError(t, err)
}

func TestSubscribeToInvalidViewFails(t *testing.T) {
	query := "Log {address topics data transactionHash blockNumber}"
	sdl := "type FilteredAndDecodedLogs @materialized(if: false) {transactionHash: String}"
	testView := View{
		Query:     &query,
		Sdl:       &sdl,
		Transform: models.Transform{},
		Name:      "FilteredAndDecodedLogs",
	}

	myDefra, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	err = testView.SubscribeTo(context.Background(), myDefra)
	require.Error(t, err)
}

func TestSubscribeToView_P2PCollectionError(t *testing.T) {
	// Valid schema applied, but wrong collection name for P2P subscription
	sdl := "type ValidSchema {name: String}"
	query := "ValidSchema {name}"
	testView := View{
		Query:     &query,
		Sdl:       &sdl,
		Transform: models.Transform{},
		Name:      "NonExistentCollection",
	}

	myDefra, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	err = testView.SubscribeTo(context.Background(), myDefra)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Error subscribing to collection")
}
