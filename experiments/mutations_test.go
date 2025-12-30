package experiments

import (
	"testing"
	"time"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/attestation"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/stretchr/testify/require"
)

type UserResult struct {
	Name string `json:"name"`
}

// In this test, we see that if a node tries to post the same new document twice, it will fail
func TestAttemptingToReCreateAnExistingDocFails(t *testing.T) {
	schema := "type User { name: String }"
	testDefra, err := defra.StartDefraInstanceWithTestConfig(t, nil, defra.NewSchemaApplierFromProvidedSchema(schema), "User")
	require.NoError(t, err)
	defer testDefra.Close(t.Context())

	mutation := `mutation {
			create_User(input: { name: "Quinn" }) {
				name
			}
		}`
	user, err := defra.PostMutation[UserResult](t.Context(), testDefra, mutation)
	require.NoError(t, err)
	require.Equal(t, "Quinn", user.Name)

	_, err = defra.PostMutation[UserResult](t.Context(), testDefra, mutation)
	require.Error(t, err)
	require.ErrorContains(t, err, "already exists")
}

type UserWithVersion struct {
	Name    string                `json:"name"`
	Version []attestation.Version `json:"_version"`
}

// In this test, we see that if a defra node attempts to re-create a doc it has received from another node, it will fail
// We also investigate whether or not this will add an entry (signature) to the _version array - we see that it does not
// This makes our attestation system more complex with our current implementations:
//
//	We have a race condition here that will occur if an Indexer node receives the correct block information for the current block from another Indexer node before they post it as a mutation
//	Should this occur, the Indexer who has received the doc will fail when they attempt to create it. The attestation record is never recorded
//	This scenario has the Indexer doing work (posting the primitive doc) that they will not be rewarded for and the network will not benefit from
//	As a result, it becomes clear that Indexers (and Hosts) should catch any failed create doc mutations and then query to see what the existing docs contents are
//	If and only if the existing document matches what the Indexer/Host intended to post, then they should submit a mutation that does nothing to the doc aside from signing it
func TestAttemptToReCreateAnExistingDocReceivedFromAnotherNode(t *testing.T) {
	schema := "type User { name: String }"
	firstDefra, err := defra.StartDefraInstanceWithTestConfig(t, nil, defra.NewSchemaApplierFromProvidedSchema(schema), "User")
	require.NoError(t, err)
	defer firstDefra.Close(t.Context())
	secondDefra, err := defra.StartDefraInstanceWithTestConfig(t, nil, defra.NewSchemaApplierFromProvidedSchema(schema), "User")
	require.NoError(t, err)
	defer secondDefra.Close(t.Context())

	mutation := `mutation {
			create_User(input: { name: "Quinn" }) {
				name
			}
		}`
	user, err := defra.PostMutation[UserResult](t.Context(), firstDefra, mutation)
	require.NoError(t, err)
	require.Equal(t, "Quinn", user.Name)

	peerInfo, err := secondDefra.DB.PeerInfo()
	require.NoError(t, err)
	err = firstDefra.DB.SetReplicator(t.Context(), peerInfo, "User")
	require.NoError(t, err)

	time.Sleep(3 * time.Second) // Allow a moment for data to sync

	query := `query {
			User(limit: 1) {
				name
			}
		}`
	userResult, err := defra.QuerySingle[UserResult](t.Context(), secondDefra, query)
	require.NoError(t, err)
	require.NotNil(t, userResult)
	require.Equal(t, "Quinn", userResult.Name)
	// Second Defra instance now has the document

	_, err = defra.PostMutation[UserResult](t.Context(), secondDefra, mutation)
	require.Error(t, err)
	require.ErrorContains(t, err, "already exists")

	// We have failed to re-create the document
	// But now, we wonder, has this impacted the _version array?

	query = `query {
			User(limit: 1) {
				name
				_version {
					cid
					signature {
						type
						identity
						value
						__typename
					}
				}
			}
		}`
	userWithVersion, err := defra.QueryArray[UserWithVersion](t.Context(), secondDefra, query)
	require.NoError(t, err)
	require.NotNil(t, userWithVersion)
	require.Len(t, userWithVersion, 1)            // Confirm we do only have one user document
	require.Len(t, userWithVersion[0].Version, 1) // It has not impacted the version array
}
