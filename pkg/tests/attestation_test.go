package tests

import (
	"fmt"
	"testing"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/attestation"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/stretchr/testify/require"
)

// These tests are located here, instead of the attestation package, so that we can import the attestation posting methods from the host without an import cycle
func TestGetAttestationRecords(t *testing.T) {
	ctx := t.Context()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema(`type AttestationRecord {
		attested_doc: String @index
		source_doc: String @index
		CIDs: [String]
		docType: String @index
		vote_count: Int @crdt(type: pcounter)
	}`), "AttestationRecord")
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	for i := 0; i < 10; i++ {
		docId := fmt.Sprintf("ArbitraryDocId: %d", i+1)
		sourceDocId := fmt.Sprintf("ArbitrarySourceDocId: %d", i+1)
		cids := []string{"Some CID", "Some Other CID"}
		
		// Create AttestationRecord directly using GraphQL mutation
		mutation := fmt.Sprintf(`mutation {
			create_AttestationRecord(input: {
				attested_doc: "%s",
				source_doc: "%s",
				CIDs: ["%s", "%s"],
				docType: "Block",
				vote_count: 1
			}) {
				attested_doc
				source_doc
				CIDs
				docType
				vote_count
			}
		}`, docId, sourceDocId, cids[0], cids[1])
		
		_, err := defra.PostMutation[attestation.AttestationRecord](ctx, defraNode, mutation)
		require.NoError(t, err)
	}

	records, err := attestation.GetAttestationRecords(ctx, defraNode, "SampleView", []string{"ArbitraryDocId: 1", "ArbitraryDocId: 7"})
	require.NoError(t, err)
	require.NotNil(t, records)
	require.Len(t, records, 2)
	for _, record := range records {
		if record.AttestedDocId != "ArbitraryDocId: 1" && record.AttestedDocId != "ArbitraryDocId: 7" {
			t.Fatalf("Encountered unexpected AttestedDocId: %s", record.AttestedDocId)
		}
		require.Len(t, record.CIDs, 2)
	}
}
