package attestation

import (
	"testing"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/stretchr/testify/require"
)

func TestAddAttestationRecordCollection(t *testing.T) {
	ctx := t.Context()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)
	err = AddAttestationRecordCollection(ctx, defraNode, "SampleViewName")
	require.NoError(t, err)
}
