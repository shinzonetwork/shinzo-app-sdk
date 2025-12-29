package experiments

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/logger"
	"github.com/sourcenetwork/defradb/http"
	"github.com/sourcenetwork/defradb/node"
	"github.com/sourcenetwork/go-p2p"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	logger.Init(true, "./logs")
	exitCode := m.Run()
	os.Exit(exitCode)
}

// This test shows us a simple example of P2P active replication
// We spinup two separate defra instances, one who writes data, the other who attempts to read the data
// You'll notice that the reader defra instance is not able to read the data until it is set as a replicator
func TestSimpleP2PReplication(t *testing.T) {
	listenAddress := "/ip4/127.0.0.1/tcp/0"
	defraUrl := "127.0.0.1:0"
	options := []node.Option{
		node.WithDisableAPI(false),
		node.WithDisableP2P(false),
		node.WithStorePath(t.TempDir()),
		http.WithAddress(defraUrl),
		p2p.WithListenAddresses(listenAddress),
	}
	ctx := context.Background()
	writerDefra := StartDefraInstance(t, ctx, options)
	defer writerDefra.Close(ctx)

	options = []node.Option{
		node.WithDisableAPI(false),
		node.WithDisableP2P(false),
		node.WithStorePath(t.TempDir()),
		http.WithAddress(defraUrl),
		p2p.WithListenAddresses(listenAddress),
	}
	readerDefra := StartDefraInstance(t, ctx, options)
	defer readerDefra.Close(ctx)

	addSchema(t, ctx, writerDefra)
	addSchema(t, ctx, readerDefra)

	postBasicData(t, ctx, writerDefra)

	result, err := getUserName(ctx, writerDefra)
	require.NoError(t, err)
	require.Equal(t, "Quinn", result)

	time.Sleep(10 * time.Second) // Allow some time to give the data a chance to sync to the readerDefra instance (it won't since they aren't connected, but we give it time anyways just in case)

	result, err = getUserName(ctx, readerDefra)
	require.Error(t, err)

	peerInfo, err := readerDefra.DB.PeerInfo()
	require.NoError(t, err)
	err = writerDefra.DB.SetReplicator(ctx, peerInfo)
	require.NoError(t, err)

	result, err = getUserName(ctx, readerDefra)
	for attempts := 1; attempts < 100; attempts++ { // It may take some time to sync now that we are connected
		if err == nil {
			break
		}
		t.Logf("Attempt %d to query username from readerDefra failed. Trying again...", attempts)
		time.Sleep(1 * time.Second)
		result, err = getUserName(ctx, readerDefra)
	}
	require.NoError(t, err)
	require.Equal(t, "Quinn", result)
}

func getUserName(ctx context.Context, readerDefra *node.Node) (string, error) {
	query := `query GetUserName{
		User(limit: 1) {
			name
		}
	}`

	type UserResult struct {
		Name string `json:"name"`
	}

	user, err := defra.QuerySingle[UserResult](ctx, readerDefra, query)
	if err != nil {
		return "", fmt.Errorf("Error querying user: %v", err)
	}
	if len(user.Name) == 0 {
		return "", fmt.Errorf("No users found")
	}
	return user.Name, nil
}

func addSchema(t *testing.T, ctx context.Context, writerDefra *node.Node) {
	schema := "type User { name: String }"
	_, err := writerDefra.DB.AddSchema(ctx, schema)
	require.NoError(t, err)
}

func postBasicData(t *testing.T, ctx context.Context, writerDefra *node.Node) {
	query := `mutation {
		create_User(input: { name: "Quinn" }) {
			name
		}
	}`

	type UserResult struct {
		Name string `json:"name"`
	}

	result, err := defra.PostMutation[UserResult](ctx, writerDefra, query)
	require.NoError(t, err)
	require.Equal(t, "Quinn", result.Name)
}

// This test shows us what active replication looks like with multiple tenants
// The test spins up many defra instances. The first instance will write some basic data.
// From here, we essentially form a chain of defra instances, all of which want to read the data.
// Data gets propogated down the chain of defra instances by having each defra instance in turn set the next defra instance as a replicator.
// i.e. Writer Node -> replicates to Node B -> replicates to Node C -> replicates to Node D -> ...
func TestMultiTenantP2PReplication_ManualReplicatorAssignment(t *testing.T) {
	listenAddress := "/ip4/127.0.0.1/tcp/0"
	defraUrl := "127.0.0.1:0"
	ctx := context.Background()
	writerDefra := createWriterDefraInstanceAndPostBasicData(t, ctx, defraUrl, listenAddress)

	// Collect all instances for proper cleanup
	allInstances := []*node.Node{writerDefra}
	defer func() {
		// Close instances in reverse order with delays to prevent race conditions
		for i := len(allInstances) - 1; i >= 0; i-- {
			allInstances[i].Close(ctx)
			time.Sleep(100 * time.Millisecond) // Allow time for cleanup
		}
	}()

	previousDefra := writerDefra
	readerDefraInstances := []*node.Node{}
	for i := 0; i < 10; i++ {
		readerDefraOptions := []node.Option{
			node.WithDisableAPI(false),
			node.WithDisableP2P(false),
			node.WithStorePath(t.TempDir()),
			http.WithAddress(defraUrl),
			p2p.WithListenAddresses(listenAddress),
		}
		newDefraInstance := StartDefraInstance(t, ctx, readerDefraOptions)
		allInstances = append(allInstances, newDefraInstance)

		assertDefraInstanceDoesNotHaveData(t, ctx, newDefraInstance)

		addSchema(t, ctx, newDefraInstance)

		assertDefraInstanceDoesNotHaveData(t, ctx, newDefraInstance)
		peerInfo, err := newDefraInstance.DB.PeerInfo()
		require.NoError(t, err)
		err = previousDefra.DB.SetReplicator(ctx, peerInfo)
		require.NoError(t, err)
		readerDefraInstances = append(readerDefraInstances, newDefraInstance)
		previousDefra = newDefraInstance
	}

	assertReaderDefraInstancesHaveLatestData(t, ctx, readerDefraInstances)
}

// This test shows us what passive replication looks like with multiple tenants
// The test mirrors TestMultiTenantP2PReplication_ManualReplicatorAssignment in terms of setup
// However, in this test, instead of using Active Replication, we form a connection and use Passive Replication
// i.e. Writer Node -> replicates to Node B -> replicates to Node C -> replicates to Node D -> ...
func TestMultiTenantP2PReplication_ConnectToPeers(t *testing.T) {
	listenAddress := "/ip4/127.0.0.1/tcp/0"
	defraUrl := "127.0.0.1:0"
	ctx := context.Background()
	writerDefra := createWriterDefraInstanceAndApplySchema(t, ctx, defraUrl, listenAddress)

	// Collect all instances for proper cleanup
	allInstances := []*node.Node{writerDefra}
	defer func() {
		// Close instances in reverse order with delays to prevent race conditions
		for i := len(allInstances) - 1; i >= 0; i-- {
			allInstances[i].Close(ctx)
			time.Sleep(100 * time.Millisecond) // Allow time for cleanup
		}
	}()

	err := writerDefra.DB.AddP2PCollections(ctx, "User")
	require.NoError(t, err)

	previousDefra := writerDefra
	readerDefraInstances := []*node.Node{}
	for i := 0; i < 10; i++ {
		readerDefraOptions := []node.Option{
			node.WithDisableAPI(false),
			node.WithDisableP2P(false),
			node.WithStorePath(t.TempDir()),
			http.WithAddress(defraUrl),
			p2p.WithListenAddresses(listenAddress),
		}
		newDefraInstance := StartDefraInstance(t, ctx, readerDefraOptions)
		allInstances = append(allInstances, newDefraInstance)

		peerInfo, err := previousDefra.DB.PeerInfo()
		require.NoError(t, err)
		err = newDefraInstance.DB.Connect(ctx, peerInfo)
		require.NoError(t, err)

		addSchema(t, ctx, newDefraInstance)

		err = newDefraInstance.DB.AddP2PCollections(ctx, "User")
		require.NoError(t, err)

		readerDefraInstances = append(readerDefraInstances, newDefraInstance)
		previousDefra = newDefraInstance
	}

	postBasicData(t, ctx, writerDefra)

	result, err := getUserName(ctx, writerDefra)
	require.NoError(t, err)
	require.Equal(t, "Quinn", result)

	assertReaderDefraInstancesHaveLatestData(t, ctx, readerDefraInstances)
}

func createWriterDefraInstanceAndPostBasicData(t *testing.T, ctx context.Context, defraUrl string, listenAddress string) *node.Node {
	writerDefra := createWriterDefraInstanceAndApplySchema(t, ctx, defraUrl, listenAddress)

	postBasicData(t, ctx, writerDefra)

	result, err := getUserName(ctx, writerDefra)
	require.NoError(t, err)
	require.Equal(t, "Quinn", result)

	return writerDefra
}

func createWriterDefraInstanceAndApplySchema(t *testing.T, ctx context.Context, defraUrl string, listenAddress string) *node.Node {
	options := []node.Option{
		node.WithDisableAPI(false),
		node.WithDisableP2P(false),
		node.WithStorePath(t.TempDir()),
		http.WithAddress(defraUrl),
		p2p.WithListenAddresses(listenAddress),
	}
	writerDefra := StartDefraInstance(t, ctx, options)

	addSchema(t, ctx, writerDefra)
	return writerDefra
}

func createDefraInstanceAndApplySchema(t *testing.T, ctx context.Context, options []node.Option) *node.Node {
	instance := StartDefraInstance(t, ctx, options)
	addSchema(t, ctx, instance)
	return instance
}

func assertReaderDefraInstancesHaveLatestData(t *testing.T, ctx context.Context, readerDefraInstances []*node.Node) {
	for i, readerDefra := range readerDefraInstances {
		result, err := getUserName(ctx, readerDefra)
		for attempts := 1; attempts < 60; attempts++ { // It may take some time to sync now that we are connected
			if err == nil {
				break
			}
			t.Logf("Attempt %d to query username from readerDefra %d failed. Trying again...", attempts, i)
			time.Sleep(1 * time.Second)
			result, err = getUserName(ctx, readerDefra)
		}
		require.NoError(t, err, fmt.Sprintf("Received unexpected error when checking user name for node %d: %v", i, err))
		require.Equal(t, "Quinn", result)
	}
}

// This test aims to mimic more closely the setup we will have for Shinzo
// This test introduces the concept of a "big peer"
// The "big peer" serves as an entrypoint into the P2P network
// In this test, we have a defra node that writes data and many defra nodes that want to read the data
// Each node forms a connection with the "big peer" when it starts, then data is passively replicated to each of them
// The data travels from writerNode -> big peer -> each of the readerNodes
func TestMultiTenantP2PReplication_ConnectToBigPeer(t *testing.T) {
	listenAddress := "/ip4/127.0.0.1/tcp/0"
	defraUrl := "127.0.0.1:0"
	ctx := context.Background()

	bigPeer := createWriterDefraInstanceAndApplySchema(t, ctx, defraUrl, listenAddress)
	defer bigPeer.Close(ctx)
	err := bigPeer.DB.AddP2PCollections(ctx, "User")
	require.NoError(t, err)

	options := []node.Option{
		node.WithDisableAPI(false),
		node.WithDisableP2P(false),
		node.WithStorePath(t.TempDir()),
		http.WithAddress(defraUrl),
		p2p.WithListenAddresses(listenAddress),
	}
	writerDefra := createDefraInstanceAndApplySchema(t, ctx, options)
	defer writerDefra.Close(ctx)
	err = writerDefra.DB.AddP2PCollections(ctx, "User")
	require.NoError(t, err)

	peerInfo, err := bigPeer.DB.PeerInfo()
	require.NoError(t, err)
	err = writerDefra.DB.Connect(ctx, peerInfo)
	require.NoError(t, err)

	readerDefraInstances := []*node.Node{}
	for i := 0; i < 10; i++ {
		readerDefraOptions := []node.Option{
			node.WithDisableAPI(false),
			node.WithDisableP2P(false),
			node.WithStorePath(t.TempDir()),
			http.WithAddress(defraUrl),
			p2p.WithListenAddresses(listenAddress),
		}
		newDefraInstance := StartDefraInstance(t, ctx, readerDefraOptions)
		defer newDefraInstance.Close(ctx)

		assertDefraInstanceDoesNotHaveData(t, ctx, newDefraInstance)

		peerInfo, err := bigPeer.DB.PeerInfo()
		require.NoError(t, err)
		err = newDefraInstance.DB.Connect(ctx, peerInfo)
		require.NoError(t, err)

		addSchema(t, ctx, newDefraInstance)

		assertDefraInstanceDoesNotHaveData(t, ctx, newDefraInstance)

		err = newDefraInstance.DB.AddP2PCollections(ctx, "User")
		require.NoError(t, err)

		readerDefraInstances = append(readerDefraInstances, newDefraInstance)
	}

	postBasicData(t, ctx, writerDefra)

	result, err := getUserName(ctx, writerDefra)
	require.NoError(t, err)
	require.Equal(t, "Quinn", result)

	assertReaderDefraInstancesHaveLatestData(t, ctx, readerDefraInstances)
}

func assertDefraInstanceDoesNotHaveData(t *testing.T, ctx context.Context, readerDefra *node.Node) {
	_, err := getUserName(ctx, readerDefra)
	require.Error(t, err)
}

// This test mirrors the setup of TestMultiTenantP2PReplication_ConnectToBigPeer
// Except, in this test, our "big peer" doesn't subscribe to any collections
// With the "big peer" not subscribed to the "User" collection, data is not passively replicated to it
// We see that the data is able to "hop" past the "big peer" and make it to our reader nodes
func TestMultiTenantP2PReplication_ConnectToBigPeerWhoDoesNotDeclareInterestInTopics(t *testing.T) {
	ipAddress, err := getLANIP() // Must use external address like IP address instead of loop back address for this to work - otherwise we will not hop past our big peer
	require.NoError(t, err)
	listenAddress := fmt.Sprintf("/ip4/%s/tcp/0", ipAddress)
	defraUrl := fmt.Sprintf("%s:0", ipAddress)
	ctx := context.Background()

	bigPeer := createWriterDefraInstanceAndApplySchema(t, ctx, defraUrl, listenAddress)
	defer bigPeer.Close(ctx)
	// Notice the big peer does not add any P2P Collections

	options := []node.Option{
		node.WithDisableAPI(false),
		node.WithDisableP2P(false),
		node.WithStorePath(t.TempDir()),
		http.WithAddress(defraUrl),
		p2p.WithListenAddresses(listenAddress),
	}
	writerDefra := createDefraInstanceAndApplySchema(t, ctx, options)
	defer writerDefra.Close(ctx)
	err = writerDefra.DB.AddP2PCollections(ctx, "User")
	require.NoError(t, err)

	peerInfo, err := bigPeer.DB.PeerInfo()
	require.NoError(t, err)
	err = writerDefra.DB.Connect(ctx, peerInfo)
	require.NoError(t, err)

	readerDefraInstances := []*node.Node{}
	for i := 0; i < 5; i++ { // In my testing, with the current configuration, there does seem to be a cap of concurrent P2P connections I can get on this system - it consistently fails when trying to connect the 8th reader node to the big peer
		readerDefraOptions := []node.Option{
			node.WithDisableAPI(false),
			node.WithDisableP2P(false),
			node.WithStorePath(t.TempDir()),
			http.WithAddress(defraUrl),
			p2p.WithListenAddresses(listenAddress),
		}
		newDefraInstance := StartDefraInstance(t, ctx, readerDefraOptions)
		defer newDefraInstance.Close(ctx)

		assertDefraInstanceDoesNotHaveData(t, ctx, newDefraInstance)

		peerInfo, err := bigPeer.DB.PeerInfo()
		require.NoError(t, err)
		err = newDefraInstance.DB.Connect(ctx, peerInfo)
		require.NoError(t, err, fmt.Sprintf("Unexpected error connecting reader node instance %d to big peer: %v", (i+1), err))

		addSchema(t, ctx, newDefraInstance)

		assertDefraInstanceDoesNotHaveData(t, ctx, newDefraInstance)

		err = newDefraInstance.DB.AddP2PCollections(ctx, "User")
		require.NoError(t, err)

		readerDefraInstances = append(readerDefraInstances, newDefraInstance)
	}

	postBasicData(t, ctx, writerDefra)

	result, err := getUserName(ctx, writerDefra)
	require.NoError(t, err)
	require.Equal(t, "Quinn", result)

	assertReaderDefraInstancesHaveLatestData(t, ctx, readerDefraInstances)
}
