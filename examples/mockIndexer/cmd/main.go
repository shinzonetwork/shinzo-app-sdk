package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/networking"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/node"
)

const targetAddress = "0x1e3aA9fE4Ef01D3cB3189c129a49E3C03126C636"

func main() {
	log.Println("🎭 Starting Mock Indexer...")
	log.Println("📝 This will post dummy blockchain data to DefraDB")

	// Configure DefraDB
	cfg := defra.DefaultConfig
	ipAddress, err := networking.GetLANIP()
	if err != nil {
		log.Fatal(err)
	}
	listenAddress := fmt.Sprintf("/ip4/%s/tcp/0", ipAddress)
	defraUrl := fmt.Sprintf("%s:0", ipAddress)
	cfg.DefraDB.Store.Path = "./.defra"
	cfg.DefraDB.Url = defraUrl
	cfg.DefraDB.P2P.ListenAddr = listenAddress
	cfg.DefraDB.P2P.BootstrapPeers = append(defra.DefaultConfig.DefraDB.P2P.BootstrapPeers, "/ip4/192.168.4.22/tcp/9176/p2p/12D3KooWLttXvtbokAphdVWL6hx7VEviDnHYwQs5SmAw1Y1yfcZT")
	cfg.Logger.Development = false

	log.Println("⏳ Starting DefraDB instance...")
	defraNode, err := defra.StartDefraInstance(cfg,
		&defra.SchemaApplierFromFile{DefaultPath: "../indexer/schema/schema.graphql"},
		"Block", "Transaction", "AccessListEntry", "Log")
	if err != nil {
		log.Fatalf("Failed to start DefraDB: %v", err)
	}
	defer defraNode.Close(context.Background())

	log.Println("✓ DefraDB started successfully!")

	// Initialize random seed
	rand.Seed(time.Now().UnixNano())

	// Get current Ethereum block number
	log.Println("🔍 Fetching current Ethereum block number...")
	currentBlock, err := getCurrentEthereumBlockNumber()
	if err != nil {
		panic(err)
	} else {
		log.Printf("✓ Current Ethereum block: %d", currentBlock)
	}

	startBlockNumber := currentBlock
	blockCounter := 0

	log.Println("🔄 Running continuously... Press Ctrl+C to stop")
	log.Println("🎯 All logs will use target address: " + targetAddress)
	log.Println("")

	// Run continuously until Ctrl+C
	for {
		// Post dummy data
		numBlocks := 5 + rand.Intn(6) // 5-10 blocks
		log.Printf("📦 Posting %d blocks with dummy data...", numBlocks)

		for i := 0; i < numBlocks; i++ {
			blockNumber := startBlockNumber + uint64(blockCounter)
			blockCounter++

			// Post block
			if err := postDummyBlock(defraNode, blockNumber); err != nil {
				log.Printf("❌ Error posting block %d: %v", blockNumber, err)
				continue
			}

			// Post 2-8 transactions per block
			numTxs := 2 + rand.Intn(7)
			for j := 0; j < numTxs; j++ {
				if err := postDummyTransaction(defraNode, blockNumber); err != nil {
					log.Printf("❌ Error posting transaction for block %d: %v", blockNumber, err)
				}
			}

			// Post 3-12 logs per block - ALL with target address
			numLogs := 3 + rand.Intn(10)
			for j := 0; j < numLogs; j++ {
				if err := postDummyLog(defraNode, targetAddress, blockNumber); err != nil {
					log.Printf("❌ Error posting log for block %d: %v", blockNumber, err)
				}
			}

			log.Printf("  ✓ Block %d: %d txs, %d logs (all with target address)",
				blockNumber, numTxs, numLogs)
		}

		log.Printf("✅ Batch complete! Posted %d blocks", numBlocks)
		log.Printf("💤 Sleeping for 1 minute before posting more...\n")
		time.Sleep(1 * time.Minute)
	}
}

func postDummyBlock(defraNode *node.Node, blockNumber uint64) error {
	ctx := context.Background()

	dummyBlock := map[string]any{
		"hash":             generateDummyHash(),
		"number":           int64(blockNumber),
		"timestamp":        time.Now().UTC().Format(time.RFC3339),
		"parentHash":       generateDummyHash(),
		"difficulty":       "0xdummy1",
		"totalDifficulty":  fmt.Sprintf("0xdummy%d", blockNumber),
		"gasUsed":          fmt.Sprintf("0x%x", 5000000+rand.Intn(10000000)),
		"gasLimit":         "0x1c9c380",
		"baseFeePerGas":    fmt.Sprintf("0x%x", 20000000000+rand.Int63n(50000000000)),
		"nonce":            int64(rand.Intn(100000)),
		"miner":            targetAddress, // Sometimes mined by our target
		"size":             fmt.Sprintf("0x%x", 1000+rand.Intn(50000)),
		"stateRoot":        generateDummyHash(),
		"sha3Uncles":       "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
		"transactionsRoot": generateDummyHash(),
		"receiptsRoot":     generateDummyHash(),
		"logsBloom":        generateLogsBloom(),
		"extraData":        "0x",
		"mixHash":          generateDummyHash(),
		"uncles":           []string{},
	}

	blockCollection, err := defraNode.DB.GetCollectionByName(ctx, "Block")
	if err != nil {
		return fmt.Errorf("failed to get Block collection: %w", err)
	}

	dummyDoc, err := client.NewDocFromMap(dummyBlock, blockCollection.Version())
	if err != nil {
		return fmt.Errorf("failed to create block document: %w", err)
	}

	return blockCollection.Save(ctx, dummyDoc)
}

func postDummyTransaction(defraNode *node.Node, blockNumber uint64) error {
	ctx := context.Background()

	// Randomly decide if transaction involves target address
	var from, to string
	if rand.Float32() < 0.3 {
		from = targetAddress
		to = generateRandomAddress()
	} else if rand.Float32() < 0.5 {
		from = generateRandomAddress()
		to = targetAddress
	} else {
		from = generateRandomAddress()
		to = generateRandomAddress()
	}

	dummyTransaction := map[string]any{
		"hash":             generateDummyHash(),
		"blockHash":        generateDummyHash(),
		"blockNumber":      int64(blockNumber),
		"from":             from,
		"to":               to,
		"value":            fmt.Sprintf("%d", rand.Int63n(100000000000000000)), // Up to 0.1 ETH
		"gas":              fmt.Sprintf("%d", 21000+rand.Intn(1000000)),
		"gasPrice":         fmt.Sprintf("%d", 20000000000+rand.Int63n(100000000000)),
		"input":            "0x",
		"nonce":            fmt.Sprintf("%d", rand.Intn(10000)),
		"transactionIndex": int64(rand.Intn(100)),
		"type":             "0x2",
		"chainId":          "0x1",
	}

	txCollection, err := defraNode.DB.GetCollectionByName(ctx, "Transaction")
	if err != nil {
		return fmt.Errorf("failed to get Transaction collection: %w", err)
	}

	dummyDoc, err := client.NewDocFromMap(dummyTransaction, txCollection.Version())
	if err != nil {
		return fmt.Errorf("failed to create transaction document: %w", err)
	}

	return txCollection.Save(ctx, dummyDoc)
}

func postDummyLog(defraNode *node.Node, address string, blockNumber uint64) error {
	ctx := context.Background()

	dummyLog := map[string]any{
		"address":          address,
		"blockNumber":      int64(blockNumber),
		"data":             generateLogData(),
		"topics":           generateRandomTopics(),
		"transactionHash":  generateDummyHash(),
		"blockHash":        generateDummyHash(),
		"transactionIndex": int64(rand.Intn(100)),
		"logIndex":         int64(rand.Intn(50)),
		"removed":          "false",
	}

	logCollection, err := defraNode.DB.GetCollectionByName(ctx, "Log")
	if err != nil {
		return fmt.Errorf("failed to get Log collection: %w", err)
	}

	dummyDoc, err := client.NewDocFromMap(dummyLog, logCollection.Version())
	if err != nil {
		return fmt.Errorf("failed to create log document: %w", err)
	}

	return logCollection.Save(ctx, dummyDoc)
}

// Helper functions

func getCurrentEthereumBlockNumber() (uint64, error) {
	rpcURL := "https://ethereum-rpc.publicnode.com"

	// Create JSON-RPC request for eth_blockNumber
	requestBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_blockNumber",
		"params":  []interface{}{},
		"id":      1,
	}

	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Make HTTP request
	resp, err := http.Post(rpcURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return 0, fmt.Errorf("failed to make RPC request: %w", err)
	}
	defer resp.Body.Close()

	// Parse response
	var rpcResponse struct {
		Result string `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&rpcResponse); err != nil {
		return 0, fmt.Errorf("failed to decode response: %w", err)
	}

	if rpcResponse.Error != nil {
		return 0, fmt.Errorf("RPC error: %s", rpcResponse.Error.Message)
	}

	// Parse hex block number (result is like "0x123abc")
	blockNumberStr := rpcResponse.Result
	if len(blockNumberStr) > 2 && blockNumberStr[:2] == "0x" {
		blockNumberStr = blockNumberStr[2:]
	}

	blockNumber, err := strconv.ParseUint(blockNumberStr, 16, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse block number %s: %w", rpcResponse.Result, err)
	}

	return blockNumber, nil
}

func generateDummyHash() string {
	// Ethereum hashes are 32 bytes = 64 hex chars + 0x prefix = 66 total
	// Generate using 4 random int64s to get 64 hex chars
	return fmt.Sprintf("0x%016x%016x%016x%016x", rand.Int63(), rand.Int63(), rand.Int63(), rand.Int63())
}

func generateRandomAddress() string {
	// Ethereum addresses are 20 bytes = 40 hex chars + 0x prefix = 42 total
	// Generate using 3 random int64s (we'll trim the extra)
	return fmt.Sprintf("0x%016x%016x%08x", rand.Int63(), rand.Int63(), rand.Int31())[:42]
}

func generateRandomTopics() []string {
	numTopics := 1 + rand.Intn(4) // 1-4 topics
	topics := make([]string, numTopics)

	// Common event signatures (real Ethereum event signatures)
	commonTopics := []string{
		"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef", // Transfer(address,address,uint256)
		"0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925", // Approval(address,address,uint256)
		"0x7f26b83ff96e1f2b6a682f133852f6798a09c465da95921460cefb3847402498", // RoleGranted
	}

	for i := 0; i < numTopics; i++ {
		if i == 0 {
			// First topic is usually the event signature
			topics[i] = commonTopics[rand.Intn(len(commonTopics))]
		} else {
			topics[i] = generateDummyHash()
		}
	}

	return topics
}

func generateLogsBloom() string {
	// LogsBloom is 256 bytes = 512 hex chars + 0x prefix = 514 total
	// Generate in chunks to create proper 512 hex chars
	bloom := "0x"
	for i := 0; i < 32; i++ { // 32 * 16 = 512 hex chars
		bloom += fmt.Sprintf("%016x", rand.Int63())
	}
	return bloom
}

func generateLogData() string {
	// Log data - 32 bytes = 64 hex chars
	// Common pattern: padded uint256 values
	return fmt.Sprintf("0x%064x", rand.Int63())
}
