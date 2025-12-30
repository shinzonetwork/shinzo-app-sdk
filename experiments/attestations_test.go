package experiments

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/attestation"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/require"
)

// This test has multiple defra nodes writing the same data to be read by another node
// This closely mimics the Shinzo setup, where multiple Indexers and Hosts will be writing the same data
// This test allows us to explore what the built-in `__version` field defra provides will look like in this case,
// Informing our design for the attestation system
// The key observation is that we receive a `[]Version` whose length is equal to the number of writers (and unique signatures)
// Another observation of note is that each `Version` entry has a height of 1, meaning that each writer signed off on the original version of the doc
func TestSyncFromMultipleWriters(t *testing.T) {
	ctx := context.Background()
	cfg := defra.DefaultConfig
	schema := "type User { name: String }"

	writerDefras := []*node.Node{}
	defraNodes := 5
	for i := 0; i < defraNodes; i++ {
		writerDefra, err := defra.StartDefraInstanceWithTestConfig(t, cfg, defra.NewSchemaApplierFromProvidedSchema(schema), "User")
		require.NoError(t, err)
		defer writerDefra.Close(ctx)
		writerDefras = append(writerDefras, writerDefra)
	}

	// Create a reader instance
	readerDefra, err := defra.StartDefraInstanceWithTestConfig(t, cfg, defra.NewSchemaApplierFromProvidedSchema(schema), "User")
	require.NoError(t, err)
	defer readerDefra.Close(ctx)

	// Connect reader to all writers
	for _, writer := range writerDefras {
		peerInfo, err := writer.DB.PeerInfo()
		require.NoError(t, err)
		err = readerDefra.DB.Connect(ctx, peerInfo)
		require.NoError(t, err)
	}

	// Write data from each writer
	type UserResult struct {
		Name string `json:"name"`
	}

	type UserWithDocID struct {
		DocID string `json:"_docID"`
		Name  string `json:"name"`
	}

	for i, writer := range writerDefras {
		mutation := `mutation {
			create_User(input: { name: "Quinn" }) {
				name
			}
		}`

		result, err := defra.PostMutation[UserResult](ctx, writer, mutation)
		if err != nil && strings.Contains(err.Error(), "already exists") {
			t.Logf("Writer %d: Document already exists, signing existing document", i+1)
			
			// Document exists, get its ID and sign it with empty update
			query := `query { 
				User(limit: 1) { 
					_docID 
					name 
				} 
			}`
			existingDoc, queryErr := defra.QuerySingle[UserWithDocID](ctx, writer, query)
			require.NoError(t, queryErr, "Failed to query existing document for writer %d", i+1)
			require.Equal(t, "Quinn", existingDoc.Name, "Existing document has wrong name for writer %d", i+1)
			
			// Sign with empty update mutation
			signMutation := fmt.Sprintf(`mutation {
				update_User(docID: "%s", input: { }) {
					name
				}
			}`, existingDoc.DocID)
			
			signResult, signErr := defra.PostMutation[UserResult](ctx, writer, signMutation)
			require.NoError(t, signErr, "Failed to sign existing document for writer %d", i+1)
			require.Equal(t, "Quinn", signResult.Name, "Sign result has wrong name for writer %d", i+1)
			t.Logf("Writer %d: Successfully signed existing document", i+1)
			continue
		}
		require.NoError(t, err, "Unexpected error for writer %d", i+1)
		require.Equal(t, "Quinn", result.Name)
		t.Logf("Writer %d: Successfully created new document", i+1)
	}

	// Wait for sync and verify reader has all data
	type UserWithVersion struct {
		Name    string                `json:"name"`
		Version []attestation.Version `json:"_version"`
	}

	var userWithVersion UserWithVersion
	var queryErr error
	for attempts := 1; attempts < 60; attempts++ {
		query := `query {
			User(limit: 1) {
				name
				_version {
					cid
					height
					signature {
						type
						identity
						value
						__typename
					}
				}
			}
		}`

		userWithVersion, queryErr = defra.QuerySingle[UserWithVersion](ctx, readerDefra, query)
		if queryErr == nil && userWithVersion.Name == "Quinn" {
			break
		}
		t.Logf("Attempt %d to query username from readerDefra failed. Trying again...", attempts)
		time.Sleep(1 * time.Second)
	}

	require.NoError(t, queryErr)
	require.Equal(t, "Quinn", userWithVersion.Name)
	require.Equal(t, defraNodes, len(userWithVersion.Version))
	for _, version := range userWithVersion.Version {
		require.Equal(t, uint(1), version.Height)
	}
}

// This test mimics TestSyncFromMultipleWriters and is also designed to help us explore Defra's built in `_version` attestation system
// In this test, unlike in TestSyncFromMultipleWriters, we have one of our writers write slightly different data
// We see that this gives us two separate documents in our collection, each with a version array of their own
func TestSyncFromMultipleWritersWithSomeOverlappingData(t *testing.T) {
	ctx := context.Background()
	cfg := defra.DefaultConfig
	schema := "type User { name: String, friends: [String] }"

	writerDefras := []*node.Node{}
	defraNodes := 5
	for i := 0; i < defraNodes; i++ {
		writerDefra, err := defra.StartDefraInstanceWithTestConfig(t, cfg, defra.NewSchemaApplierFromProvidedSchema(schema), "User")
		require.NoError(t, err)
		defer writerDefra.Close(ctx)

		writerDefras = append(writerDefras, writerDefra)
	}

	// Create a reader instance
	readerDefra, err := defra.StartDefraInstanceWithTestConfig(t, cfg, defra.NewSchemaApplierFromProvidedSchema(schema), "User")
	require.NoError(t, err)
	defer readerDefra.Close(ctx)

	// Connect reader to all writers
	for _, writer := range writerDefras {
		peerInfo, err := writer.DB.PeerInfo()
		require.NoError(t, err)
		err = readerDefra.DB.Connect(ctx, peerInfo)
		require.NoError(t, err)
	}

	// Standard friends array for writers
	standardFriends := []string{"Alice", "Bob", "Charlie", "Diana"}
	// Modified friends array for 1 (malicious?) writer (remove "Bob", add "Eve" and "Frank")
	modifiedFriends := []string{"Alice", "Charlie", "Diana", "Eve", "Frank"}

	// Write data to each writer
	type UserResult struct {
		Name    string   `json:"name"`
		Friends []string `json:"friends"`
	}

	for i, writer := range writerDefras {
		var friendsStr string
		var friends []string
		if i == len(writerDefras)-1 {
			friends = modifiedFriends
			t.Logf("Writer %d posting with MODIFIED friends: %v", i+1, modifiedFriends)
		} else {
			friends = standardFriends
			t.Logf("Writer %d posting with STANDARD friends: %v", i+1, standardFriends)
		}

		// Format friends array for GraphQL
		friendsStr = "["
		for j, friend := range friends {
			if j > 0 {
				friendsStr += ", "
			}
			friendsStr += fmt.Sprintf(`"%s"`, friend)
		}
		friendsStr += "]"

		mutation := fmt.Sprintf(`mutation {
			create_User(input: { name: "Quinn", friends: %s }) {
				name
				friends
			}
		}`, friendsStr)

		result, err := defra.PostMutation[UserResult](ctx, writer, mutation)
		require.NoError(t, err)
		require.Equal(t, "Quinn", result.Name)
	}

	// Wait for sync
	time.Sleep(5 * time.Second)

	// Query all User entries to see how DefraDB handles the conflicting data
	query := `query {
		User {
			name
			friends
			_version {
				cid
				height
				signature {
					identity
				}
			}
		}
	}`

	type UserWithFriendsAndVersion struct {
		Name    string                `json:"name"`
		Friends []string              `json:"friends"`
		Version []attestation.Version `json:"_version"`
	}

	users, err := defra.QueryArray[UserWithFriendsAndVersion](ctx, readerDefra, query)
	require.NoError(t, err)
	require.NotNil(t, users)
	require.Len(t, users, 2)

	for _, user := range users {
		if len(user.Friends) == 4 {
			require.Len(t, user.Version, defraNodes-1)
		} else if len(user.Friends) == 5 {
			require.Len(t, user.Version, 1)
		} else {
			t.Fatalf("Unexpected user object %+v", user)
		}
	}
}

// This test mimics TestSyncFromMultipleWritersWithSomeOverlappingData but adds in a vote_count [GCounter CRDT](https://github.com/sourcenetwork/defradb/tree/develop/internal/core/crdt#gcounter---increment-only-counter)
// Here we see that a malicious actor is able to manipulate the vote_count and increment it multiple times by themselves - giving the allusion of multiple attestations - by modifying the document in subsequent mutation queries
// We also notice that the compromised document (written by the malicious node) has the same number of `_version` instances as it was written to - giving us an avenue for detecting this, by checking the signing identities
// Simply checking the _version length is insufficient as an attestation system. We must also check the number of unique signers.
// An important observation is that the `[]Version` for the compromised doc consists of a number of `Version` entries where `height != 1`. With each subsequent 'edit' to the doc (and signature), the malicious writer increases the height of the `[]Version`
// this makes detecting malicious behaviour more approachable
func TestSyncFromMultipleWritersWithSomeOverlappingDataAndVoteCounts(t *testing.T) {
	ctx := context.Background()
	cfg := defra.DefaultConfig
	schema := "type User { name: String, friends: [String], vote_count: Int @crdt(type: pcounter) }"

	writerDefras := []*node.Node{}
	defraNodes := 5
	for i := 0; i < defraNodes; i++ {
		writerDefra, err := defra.StartDefraInstanceWithTestConfig(t, cfg, defra.NewSchemaApplierFromProvidedSchema(schema))
		require.NoError(t, err)
		defer writerDefra.Close(ctx)

		err = writerDefra.DB.AddP2PCollections(ctx, "User")
		require.NoError(t, err)

		writerDefras = append(writerDefras, writerDefra)
	}

	// Create a reader instance
	readerDefra, err := defra.StartDefraInstanceWithTestConfig(t, cfg, defra.NewSchemaApplierFromProvidedSchema(schema))
	require.NoError(t, err)
	defer readerDefra.Close(ctx)

	err = readerDefra.DB.AddP2PCollections(ctx, "User")
	require.NoError(t, err)

	// Connect reader to all writers
	for _, writer := range writerDefras {
		peerInfo, err := writer.DB.PeerInfo()
		require.NoError(t, err)
		err = readerDefra.DB.Connect(ctx, peerInfo)
		require.NoError(t, err)
	}

	// Standard friends array for  writers
	standardFriends := []string{"Alice", "Bob", "Charlie", "Diana"}
	// Modified friends array for 1 (malicious) writer (remove "Bob", add "Eve" and "Frank")
	modifiedFriends := []string{"Alice", "Charlie", "Diana", "Eve", "Frank"}

	// Helper function to create or update user with vote count
	createOrUpdateUser := func(writer *node.Node, friends []string) error {
		// Format friends array
		friendsStr := "["
		for j, friend := range friends {
			if j > 0 {
				friendsStr += ", "
			}
			friendsStr += fmt.Sprintf(`"%s"`, friend)
		}
		friendsStr += "]"

		// Try to create
		createMutation := fmt.Sprintf(`mutation {
			create_User(input: { name: "Quinn", friends: %s, vote_count: 1 }) {
				name
				friends
				vote_count
			}
		}`, friendsStr)

		type UserResult struct {
			Name      string   `json:"name"`
			Friends   []string `json:"friends"`
			VoteCount int      `json:"vote_count"`
		}

		_, err := defra.PostMutation[UserResult](ctx, writer, createMutation)
		if err != nil {
			// Document exists, update it
			// Query for the document
			queryStr := `query {
				User(filter: {name: {_eq: "Quinn"}}) {
					_docID
					vote_count
					friends
				}
			}`

			type UserWithDocID struct {
				DocID     string   `json:"_docID"`
				VoteCount int      `json:"vote_count"`
				Friends   []string `json:"friends"`
			}

			users, err := defra.QueryArray[UserWithDocID](ctx, writer, queryStr)
			if err != nil {
				return err
			}

			// Find matching document by friends array
			var matchingDoc *UserWithDocID
			for i := range users {
				if len(users[i].Friends) == len(friends) {
					allMatch := true
					for j, friend := range friends {
						if j >= len(users[i].Friends) || users[i].Friends[j] != friend {
							allMatch = false
							break
						}
					}
					if allMatch {
						matchingDoc = &users[i]
						break
					}
				}
			}

			if matchingDoc != nil {
				// Update with increment of 1
				updateMutation := fmt.Sprintf(`mutation {
					update_User(docID: "%s", input: { vote_count: 1 }) {
						name
						friends
						vote_count
					}
				}`, matchingDoc.DocID)

				_, err = defra.PostMutation[UserResult](ctx, writer, updateMutation)
				return err
			}
		}
		return nil
	}

	// Write data to each writer
	for i, writer := range writerDefras {
		if i == len(writerDefras)-1 {
			// Last writer posts modified friends array 10 times (malicious behavior)
			t.Logf("Writer %d (MALICIOUS) posting with MODIFIED friends 10 times: %v", i+1, modifiedFriends)
			for j := 0; j < 10; j++ {
				err := createOrUpdateUser(writer, modifiedFriends)
				require.NoError(t, err)
				t.Logf("  Malicious writer post #%d complete", j+1)
			}
		} else {
			// First writers post standard friends array once
			t.Logf("Writer %d posting with STANDARD friends: %v", i+1, standardFriends)
			err := createOrUpdateUser(writer, standardFriends)
			require.NoError(t, err)
		}
	}

	// Wait for sync
	time.Sleep(5 * time.Second)

	// Query all User entries to see how DefraDB handles the conflicting data and CRDT counters
	query := `query {
		User {
			name
			friends
			vote_count
			_version {
				cid
				height
				signature {
					identity
				}
			}
		}
	}`

	type UserWithFriendsVoteCountAndVersion struct {
		Name      string                `json:"name"`
		Friends   []string              `json:"friends"`
		VoteCount int                   `json:"vote_count"`
		Version   []attestation.Version `json:"_version"`
	}
	users, err := defra.QueryArray[UserWithFriendsVoteCountAndVersion](ctx, readerDefra, query)
	require.NoError(t, err)
	require.NotNil(t, users)
	require.Len(t, users, 2)

	for _, user := range users {
		if len(user.Friends) == 4 {
			require.Len(t, user.Version, defraNodes-1)
			require.Equal(t, defraNodes-1, user.VoteCount)
		} else if len(user.Friends) == 5 {
			require.Len(t, user.Version, 10)
			require.Equal(t, 10, user.VoteCount)
			for i, version := range user.Version {
				require.Equal(t, uint(len(user.Version)-i), version.Height)
			}
		} else {
			t.Fatalf("Unexpected user object %+v", user)
		}
	}
}

// In this test, we see that a (potentially) malicious Defra node is able to overwrite a document
// And, most concerningly, in doing so, they append to the _version array
// This test illustrates how a malicious node could overwrite a document and, optionally, add additional "attestations" to it by sending "empty" mutations by sending immaterial data (or none at all) with the intention on signing and creating a new _version entry
// This exposes a potential attack vector where a malicious actor can not only create malicious resources that look legit (by adding artificial "attestations"), but they can also maliciously modify existing resources and carry the legitimate attestations forward
// By inspection, we see that, as above, subsequent writes by the malicious actor increment the `height` of each `Version` entry, allowing us to identify documents that have been modified
func TestSyncFromMultipleWritersWithOneMaliciouslyOverwritingData(t *testing.T) {
	ctx := context.Background()
	cfg := defra.DefaultConfig
	schema := "type User { name: String, friends: [String] }"

	writerDefras := []*node.Node{}
	defraNodes := 5
	for i := 0; i < defraNodes; i++ {
		writerDefra, err := defra.StartDefraInstanceWithTestConfig(t, cfg, defra.NewSchemaApplierFromProvidedSchema(schema))
		require.NoError(t, err)
		defer writerDefra.Close(ctx)

		err = writerDefra.DB.AddP2PCollections(ctx, "User")
		require.NoError(t, err)

		writerDefras = append(writerDefras, writerDefra)
	}

	// Create a reader instance
	readerDefra, err := defra.StartDefraInstanceWithTestConfig(t, cfg, defra.NewSchemaApplierFromProvidedSchema(schema))
	require.NoError(t, err)
	defer readerDefra.Close(ctx)

	err = readerDefra.DB.AddP2PCollections(ctx, "User")
	require.NoError(t, err)

	// Connect reader to all writers
	for _, writer := range writerDefras {
		peerInfo, err := writer.DB.PeerInfo()
		require.NoError(t, err)
		err = readerDefra.DB.Connect(ctx, peerInfo)
		require.NoError(t, err)
	}

	// Standard friends array for legitimate writers
	standardFriends := []string{"Alice", "Bob", "Charlie", "Diana"}
	// Modified friends array that malicious writer will use
	maliciousFriendsList := []string{"Alice", "Charlie", "Diana", "Eve", "Frank"}

	type UserResult struct {
		Name    string   `json:"name"`
		Friends []string `json:"friends"`
	}

	// First 4 writers create the document with standard friends
	for i := 0; i < defraNodes-1; i++ {
		writer := writerDefras[i]
		friendsStr := "["
		for j, friend := range standardFriends {
			if j > 0 {
				friendsStr += ", "
			}
			friendsStr += fmt.Sprintf(`"%s"`, friend)
		}
		friendsStr += "]"

		createMutation := fmt.Sprintf(`mutation {
			create_User(input: { name: "Quinn", friends: %s }) {
				name
				friends
			}
		}`, friendsStr)

		_, err := defra.PostMutation[UserResult](ctx, writer, createMutation)
		require.NoError(t, err)
	}

	// Wait a bit for P2P sync
	time.Sleep(2 * time.Second)

	// Now the malicious writer (last one) hijacks the document
	maliciousWriter := writerDefras[defraNodes-1]

	// Query for the existing document
	queryStr := `query {
		User(filter: {name: {_eq: "Quinn"}}) {
			_docID
			friends
		}
	}`

	type UserWithDocID struct {
		DocID   string   `json:"_docID"`
		Friends []string `json:"friends"`
	}

	var existingDoc UserWithDocID
	// Keep trying until we find the synced document
	for attempts := 0; attempts < 30; attempts++ {
		users, err := defra.QueryArray[UserWithDocID](ctx, maliciousWriter, queryStr)
		if err == nil && len(users) > 0 {
			existingDoc = users[0]
			t.Logf("Malicious writer found existing document with docID: %s, friends: %v", existingDoc.DocID, existingDoc.Friends)
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	require.NotEmpty(t, existingDoc.DocID, "Malicious writer should have synced the document")

	// Malicious writer overwrites the document with modified friends
	modifiedFriendsStr := "["
	for j, friend := range maliciousFriendsList {
		if j > 0 {
			modifiedFriendsStr += ", "
		}
		modifiedFriendsStr += fmt.Sprintf(`"%s"`, friend)
	}
	modifiedFriendsStr += "]"

	overwriteMutation := fmt.Sprintf(`mutation {
		update_User(docID: "%s", input: { friends: %s }) {
			name
			friends
		}
	}`, existingDoc.DocID, modifiedFriendsStr)

	_, err = defra.PostMutation[UserResult](ctx, maliciousWriter, overwriteMutation)
	require.NoError(t, err)

	// Now post 9 more "empty" updates to inflate the signature count
	// We'll just increment vote_count by 1 each time to ensure there's a change
	for i := 0; i < 9; i++ {
		updateMutation := fmt.Sprintf(`mutation {
			update_User(docID: "%s", input: { }) {
				name
				friends
			}
		}`, existingDoc.DocID)

		_, err := defra.PostMutation[UserResult](ctx, maliciousWriter, updateMutation)
		require.NoError(t, err)
	}

	// Wait for sync
	time.Sleep(5 * time.Second)

	// Query all User entries to see what happened
	query := `query {
		User {
			name
			friends
			_version {
				cid
				height
				signature {
					identity
				}
			}
		}
	}`

	type UserWithFriendsVoteCountAndVersion struct {
		Name    string                `json:"name"`
		Friends []string              `json:"friends"`
		Version []attestation.Version `json:"_version"`
	}

	users, err := defra.QueryArray[UserWithFriendsVoteCountAndVersion](ctx, readerDefra, query)
	require.NoError(t, err)
	require.NotNil(t, users)
	require.Len(t, users, 1)
	require.ElementsMatch(t, maliciousFriendsList, users[0].Friends)
	require.Len(t, users[0].Version, 14)
}

func TestSyncFromMultipleWritersWithOneMaliciouslyOverwritingDataButThenRecoverViaTimeTravelQueries(t *testing.T) {
	ctx := context.Background()
	cfg := defra.DefaultConfig
	schema := "type User { name: String, friends: [String] }"

	writerDefras := []*node.Node{}
	defraNodes := 5
	for i := 0; i < defraNodes; i++ {
		writerDefra, err := defra.StartDefraInstanceWithTestConfig(t, cfg, defra.NewSchemaApplierFromProvidedSchema(schema), "User")
		require.NoError(t, err)
		defer writerDefra.Close(ctx)

		writerDefras = append(writerDefras, writerDefra)
	}

	// Create a reader instance
	readerDefra, err := defra.StartDefraInstanceWithTestConfig(t, cfg, defra.NewSchemaApplierFromProvidedSchema(schema), "User")
	require.NoError(t, err)
	defer readerDefra.Close(ctx)

	// Connect reader to all writers
	for _, writer := range writerDefras {
		peerInfo, err := writer.DB.PeerInfo()
		require.NoError(t, err)
		err = readerDefra.DB.Connect(ctx, peerInfo)
		require.NoError(t, err)
	}

	// Standard friends array for legitimate writers
	standardFriends := []string{"Alice", "Bob", "Charlie", "Diana"}
	// Modified friends array that malicious writer will use
	maliciousFriendsList := []string{"Alice", "Charlie", "Diana", "Eve", "Frank"}

	type UserResult struct {
		Name    string   `json:"name"`
		Friends []string `json:"friends"`
	}

	// First 4 writers create the document with standard friends
	for i := 0; i < defraNodes-1; i++ {
		writer := writerDefras[i]
		friendsStr := "["
		for j, friend := range standardFriends {
			if j > 0 {
				friendsStr += ", "
			}
			friendsStr += fmt.Sprintf(`"%s"`, friend)
		}
		friendsStr += "]"

		createMutation := fmt.Sprintf(`mutation {
			create_User(input: { name: "Quinn", friends: %s }) {
				name
				friends
			}
		}`, friendsStr)

		_, err := defra.PostMutation[UserResult](ctx, writer, createMutation)
		require.NoError(t, err)
	}

	// Wait a bit for P2P sync
	time.Sleep(2 * time.Second)

	// Now the malicious writer (last one) hijacks the document
	maliciousWriter := writerDefras[defraNodes-1]

	// Query for the existing document
	queryStr := `query {
		User(filter: {name: {_eq: "Quinn"}}) {
			_docID
			friends
		}
	}`

	type UserWithDocID struct {
		DocID   string   `json:"_docID"`
		Friends []string `json:"friends"`
	}

	var existingDoc UserWithDocID
	// Keep trying until we find the synced document
	for attempts := 0; attempts < 30; attempts++ {
		users, err := defra.QueryArray[UserWithDocID](ctx, maliciousWriter, queryStr)
		if err == nil && len(users) > 0 {
			existingDoc = users[0]
			t.Logf("Malicious writer found existing document with docID: %s, friends: %v", existingDoc.DocID, existingDoc.Friends)
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	require.NotEmpty(t, existingDoc.DocID, "Malicious writer should have synced the document")

	// Malicious writer overwrites the document with modified friends
	modifiedFriendsStr := "["
	for j, friend := range maliciousFriendsList {
		if j > 0 {
			modifiedFriendsStr += ", "
		}
		modifiedFriendsStr += fmt.Sprintf(`"%s"`, friend)
	}
	modifiedFriendsStr += "]"

	overwriteMutation := fmt.Sprintf(`mutation {
		update_User(docID: "%s", input: { friends: %s }) {
			name
			friends
		}
	}`, existingDoc.DocID, modifiedFriendsStr)

	_, err = defra.PostMutation[UserResult](ctx, maliciousWriter, overwriteMutation)
	require.NoError(t, err)

	// Now post 9 more "empty" updates to inflate the signature count
	// We'll just increment vote_count by 1 each time to ensure there's a change
	for i := 0; i < 9; i++ {
		updateMutation := fmt.Sprintf(`mutation {
			update_User(docID: "%s", input: { }) {
				name
				friends
			}
		}`, existingDoc.DocID)

		_, err := defra.PostMutation[UserResult](ctx, maliciousWriter, updateMutation)
		require.NoError(t, err)
	}

	// Wait for sync
	time.Sleep(5 * time.Second)

	// Query all User entries to see what happened
	query := `query {
		User {
			name
			friends
			_docID
			_version {
				cid
				height
				signature {
					identity
				}
			}
		}
	}`

	type UserWithFriendsDocIdAndVersion struct {
		Name    string                `json:"name"`
		Friends []string              `json:"friends"`
		Version []attestation.Version `json:"_version"`
		DocId   string                `json:"_docID"`
	}

	users, err := defra.QueryArray[UserWithFriendsDocIdAndVersion](ctx, readerDefra, query)
	require.NoError(t, err)
	require.NotNil(t, users)
	require.Len(t, users, 1)
	require.ElementsMatch(t, maliciousFriendsList, users[0].Friends)
	require.Len(t, users[0].Version, 14)

	// Now, let's try to recover our data integrity via timetravel queries
	user := users[0]
	require.True(t, hasDocBeenModified(user.Version))
	// Extract CIDs with height of 1 (i.e. original versions of the doc)
	originalVersionCIDs := []string{}
	for _, version := range user.Version {
		if version.Height == uint(1) {
			originalVersionCIDs = append(originalVersionCIDs, version.CID)
		}
	}
	require.Greater(t, len(originalVersionCIDs), 0)
	// Fetch the data at those CIDs using time travelling queries
	for _, originalCID := range originalVersionCIDs {
		query := fmt.Sprintf(`query {
			User (
				cid: "%s",
				docID: "%s"
			){
				name
				friends
			}
		}`, originalCID, user.DocId)
		originalUserEntry, err := defra.QuerySingle[UserResult](ctx, readerDefra, query)
		require.NoError(t, err)
		require.Equal(t, "Quinn", originalUserEntry.Name)
		require.ElementsMatch(t, standardFriends, originalUserEntry.Friends)
	}
}

func hasDocBeenModified(versions []attestation.Version) bool {
	for _, version := range versions {
		if version.Height > uint(1) {
			return true
		}
	}
	return false
}
