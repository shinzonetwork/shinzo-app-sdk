package defra

import (
	"context"
	"testing"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/config"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Blockchain test types that work with DefraDB
type Block struct {
	DocID            string        `json:"_docID"`
	Hash             string        `json:"hash"`
	Number           int           `json:"number"`
	Timestamp        string        `json:"timestamp"`
	ParentHash       string        `json:"parentHash"`
	Difficulty       string        `json:"difficulty"`
	TotalDifficulty  string        `json:"totalDifficulty"`
	GasUsed          string        `json:"gasUsed"`
	GasLimit         string        `json:"gasLimit"`
	BaseFeePerGas    string        `json:"baseFeePerGas"`
	Nonce            int           `json:"nonce"`
	Miner            string        `json:"miner"`
	Size             string        `json:"size"`
	StateRoot        string        `json:"stateRoot"`
	Sha3Uncles       string        `json:"sha3Uncles"`
	TransactionsRoot string        `json:"transactionsRoot"`
	ReceiptsRoot     string        `json:"receiptsRoot"`
	LogsBloom        string        `json:"logsBloom"`
	ExtraData        string        `json:"extraData"`
	MixHash          string        `json:"mixHash"`
	Uncles           []string      `json:"uncles"`
	Transactions     []Transaction `json:"transactions"`
}

type Transaction struct {
	DocID            string            `json:"_docID"`
	Hash             string            `json:"hash"`
	BlockHash        string            `json:"blockHash"`
	BlockNumber      int               `json:"blockNumber"`
	From             string            `json:"from"`
	To               string            `json:"to"`
	Value            string            `json:"value"`
	GasPrice         string            `json:"gasPrice"`
	Gas              string            `json:"gas"`
	Input            string            `json:"input"`
	Nonce            string            `json:"nonce"`
	TransactionIndex int               `json:"transactionIndex"`
	Type             string            `json:"type"`
	AccessList       []AccessListEntry `json:"accessList"`
	Logs             []Log             `json:"logs"`
}

type Log struct {
	DocID            string      `json:"_docID"`
	Address          string      `json:"address"`
	Topics           []string    `json:"topics"`
	Data             string      `json:"data"`
	TransactionHash  string      `json:"transactionHash"`
	BlockHash        string      `json:"blockHash"`
	BlockNumber      int         `json:"blockNumber"`
	TransactionIndex int         `json:"transactionIndex"`
	LogIndex         int         `json:"logIndex"`
	Removed          string      `json:"removed"`
	Block            Block       `json:"block"`
	Transaction      Transaction `json:"transaction"`
}

type AccessListEntry struct {
	Address     string      `json:"address"`
	StorageKeys []string    `json:"storageKeys"`
	Transaction Transaction `json:"transaction"`
}

func setupTestComplexObjectClient(t *testing.T) *node.Node {
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

	// Create schema applier with proper blockchain objects and relationships
	schemaApplier := NewSchemaApplierFromProvidedSchema(`
		type User {
			name: String
		}

		type Block {
			hash: String @index(unique: true)
			number: Int @index
			timestamp: String
			parentHash: String
			difficulty: String
			totalDifficulty: String
			gasUsed: String
			gasLimit: String
			baseFeePerGas: String
			nonce: Int
			miner: String
			size: String
			stateRoot: String
			sha3Uncles: String
			transactionsRoot: String
			receiptsRoot: String
			logsBloom: String
			extraData: String
			mixHash: String
			uncles: [String]
			# Relationships
			transactions: [Transaction] @relation(name: "block_transactions")
		}

		type Transaction {
			hash: String @index(unique: true)
			blockHash: String @index
			blockNumber: Int @index
			from: String
			to: String
			value: String
			gas: String
			gasPrice: String
			maxFeePerGas: String
			maxPriorityFeePerGas: String
			input: String
			nonce: String
			transactionIndex: Int @index
			type: String
			chainId: String
			v: String
			r: String
			s: String
			status: Boolean
			cumulativeGasUsed: String
			effectiveGasPrice: String
			# Relationships
			block: Block @relation(name: "block_transactions")
			logs: [Log] @relation(name: "transaction_logs")
			accessList: [AccessListEntry] @relation(name: "transaction_accessList")
		}

		type AccessListEntry {
			address: String
			storageKeys: [String]
			transaction: Transaction @relation(name: "transaction_accessList")
		}

		type Log {
			address: String
			topics: [String]
			data: String
			transactionHash: String
			blockHash: String
			blockNumber: Int @index
			transactionIndex: Int
			logIndex: Int
			removed: String
			# Relationships
			block: Block @index @relation(name: "block_transactions")
			transaction: Transaction @index @relation(name: "transaction_logs")
		}
	`)

	// Start Defra instance
	defraNode, _, err := StartDefraInstance(testConfig, schemaApplier)
	require.NoError(t, err)

	return defraNode
}

func TestComplexObjectWriteAndQuery(t *testing.T) {
	defraNode := setupTestComplexObjectClient(t)
	defer defraNode.Close(context.Background())

	ctx := context.Background()

	t.Run("create block with transactions and logs, then query with GetBlockWithTransactions", func(t *testing.T) {
		// Step 1: Create a block first (without relationships)
		createBlockQuery := `
			mutation {
				create_Block(input: {
					hash: "0xblock123456789"
					number: 54321
					timestamp: "2023-12-01T12:00:00Z"
					parentHash: "0xparent123456789"
					difficulty: "2000000"
					gasUsed: "42000"
					gasLimit: "30000000"
					nonce: 123
					miner: "0xminer456"
					size: "2000"
					stateRoot: "0xstateroot456"
					sha3Uncles: "0xuncles456"
					transactionsRoot: "0xtxroot456"
					receiptsRoot: "0xreceiptroot456"
					logsBloom: "0xbloom456"
					extraData: "0xextra456"
					mixHash: "0xmix456"
					uncles: ["0xuncle3", "0xuncle4"]
				}) {
					_docID
					hash
					number
				}
			}
		`

		blockResult, err := PostMutation[Block](ctx, defraNode, createBlockQuery)
		require.NoError(t, err)
		t.Logf("Created block with _docID: %s", blockResult.DocID)

		// Step 2: Create 3 transactions (without relationships)
		// Transaction 1: No logs
		createTx1Query := `
			mutation {
				create_Transaction(input: {
					hash: "0xtx1"
					blockHash: "0xblock123456789"
					blockNumber: 54321
					from: "0xfrom1"
					to: "0xto1"
					value: "1000000000000000000"
					gas: "21000"
					gasPrice: "20000000000"
					input: "0x"
					nonce: "1"
					transactionIndex: 0
					type: "0x2"
					status: true
				}) {
					_docID
					hash
				}
			}
		`
		tx1Result, err := PostMutation[Transaction](ctx, defraNode, createTx1Query)
		require.NoError(t, err)
		t.Logf("Created transaction 1 with _docID: %s", tx1Result.DocID)

		// Transaction 2: 1 log
		createTx2Query := `
			mutation {
				create_Transaction(input: {
					hash: "0xtx2"
					blockHash: "0xblock123456789"
					blockNumber: 54321
					from: "0xfrom2"
					to: "0xto2"
					value: "2000000000000000000"
					gas: "42000"
					gasPrice: "25000000000"
					input: "0x"
					nonce: "2"
					transactionIndex: 1
					type: "0x2"
					status: true
				}) {
					_docID
					hash
				}
			}
		`
		tx2Result, err := PostMutation[Transaction](ctx, defraNode, createTx2Query)
		require.NoError(t, err)
		t.Logf("Created transaction 2 with _docID: %s", tx2Result.DocID)

		// Transaction 3: 2 logs
		createTx3Query := `
			mutation {
				create_Transaction(input: {
					hash: "0xtx3"
					blockHash: "0xblock123456789"
					blockNumber: 54321
					from: "0xfrom3"
					to: "0xto3"
					value: "3000000000000000000"
					gas: "63000"
					gasPrice: "30000000000"
					input: "0x"
					nonce: "3"
					transactionIndex: 2
					type: "0x2"
					status: true
				}) {
					_docID
					hash
				}
			}
		`
		tx3Result, err := PostMutation[Transaction](ctx, defraNode, createTx3Query)
		require.NoError(t, err)
		t.Logf("Created transaction 3 with _docID: %s", tx3Result.DocID)

		// Step 3: Create logs for transactions 2 and 3
		// Log 1 for transaction 2
		createLog1Query := `
			mutation {
				create_Log(input: {
					address: "0xcontract1"
					topics: ["0xevent1", "0xparam1"]
					data: "0xdata1"
					transactionHash: "0xtx2"
					blockHash: "0xblock123456789"
					blockNumber: 54321
					transactionIndex: 1
					logIndex: 0
					removed: "false"
				}) {
					_docID
					address
					transactionHash
				}
			}
		`
		log1Result, err := PostMutation[Log](ctx, defraNode, createLog1Query)
		require.NoError(t, err)
		t.Logf("Created log 1 with _docID: %s", log1Result.DocID)

		// Log 1 for transaction 3
		createLog2Query := `
			mutation {
				create_Log(input: {
					address: "0xcontract2"
					topics: ["0xevent2", "0xparam2"]
					data: "0xdata2"
					transactionHash: "0xtx3"
					blockHash: "0xblock123456789"
					blockNumber: 54321
					transactionIndex: 2
					logIndex: 0
					removed: "false"
				}) {
					_docID
					address
					transactionHash
				}
			}
		`
		log2Result, err := PostMutation[Log](ctx, defraNode, createLog2Query)
		require.NoError(t, err)
		t.Logf("Created log 2 with _docID: %s", log2Result.DocID)

		// Log 2 for transaction 3
		createLog3Query := `
			mutation {
				create_Log(input: {
					address: "0xcontract3"
					topics: ["0xevent3", "0xparam3"]
					data: "0xdata3"
					transactionHash: "0xtx3"
					blockHash: "0xblock123456789"
					blockNumber: 54321
					transactionIndex: 2
					logIndex: 1
					removed: "false"
				}) {
					_docID
					address
					transactionHash
				}
			}
		`
		log3Result, err := PostMutation[Log](ctx, defraNode, createLog3Query)
		require.NoError(t, err)
		t.Logf("Created log 3 with _docID: %s", log3Result.DocID)

		// Step 4: Update relationships using update mutations
		// Link all transactions to the block
		updateTx1Query := `
			mutation {
				update_Transaction(filter: {hash: {_eq: "0xtx1"}}, input: {block: "` + blockResult.DocID + `"}) {
					_docID
				}
			}
		`
		_, err = PostMutation[Transaction](ctx, defraNode, updateTx1Query)
		require.NoError(t, err)

		updateTx2Query := `
			mutation {
				update_Transaction(filter: {hash: {_eq: "0xtx2"}}, input: {block: "` + blockResult.DocID + `"}) {
					_docID
				}
			}
		`
		_, err = PostMutation[Transaction](ctx, defraNode, updateTx2Query)
		require.NoError(t, err)

		updateTx3Query := `
			mutation {
				update_Transaction(filter: {hash: {_eq: "0xtx3"}}, input: {block: "` + blockResult.DocID + `"}) {
					_docID
				}
			}
		`
		_, err = PostMutation[Transaction](ctx, defraNode, updateTx3Query)
		require.NoError(t, err)

		// Link logs to their transactions and the block
		updateLog1Query := `
			mutation {
				update_Log(filter: {logIndex: {_eq: 0}, transactionHash: {_eq: "0xtx2"}}, input: {
					block: "` + blockResult.DocID + `",
					transaction: "` + tx2Result.DocID + `"
				}) {
					_docID
				}
			}
		`
		_, err = PostMutation[Log](ctx, defraNode, updateLog1Query)
		require.NoError(t, err)

		updateLog2Query := `
			mutation {
				update_Log(filter: {logIndex: {_eq: 0}, transactionHash: {_eq: "0xtx3"}}, input: {
					block: "` + blockResult.DocID + `",
					transaction: "` + tx3Result.DocID + `"
				}) {
					_docID
				}
			}
		`
		_, err = PostMutation[Log](ctx, defraNode, updateLog2Query)
		require.NoError(t, err)

		updateLog3Query := `
			mutation {
				update_Log(filter: {logIndex: {_eq: 1}, transactionHash: {_eq: "0xtx3"}}, input: {
					block: "` + blockResult.DocID + `",
					transaction: "` + tx3Result.DocID + `"
				}) {
					_docID
				}
			}
		`
		_, err = PostMutation[Log](ctx, defraNode, updateLog3Query)
		require.NoError(t, err)

		// Now query using your exact GetBlockWithTransactions query
		query := `
			query GetBlockWithTransactions {
				Block(limit: 1) {
					hash
					number
					parentHash
					difficulty
					gasUsed
					gasLimit
					nonce
					miner
					size
					stateRoot
					transactionsRoot
					receiptsRoot
					extraData
					transactions {
						hash
						blockHash
						blockNumber
						from
						to
						value
						gasPrice
						input
						nonce
						transactionIndex
						logs {
							address
							topics
							data
							blockNumber
							transactionHash
							transactionIndex
							blockHash
							logIndex
							removed
						}
					}
				}
			}
		`

		block, err := QuerySingle[Block](ctx, defraNode, query)
		require.NoError(t, err)

		t.Logf("Block: %+v", block)
		t.Logf("Transactions: %+v", block.Transactions)
		assert.Equal(t, "0xblock123456789", block.Hash)
		assert.Equal(t, 54321, block.Number)
		assert.Equal(t, "0xparent123456789", block.ParentHash)
		assert.Equal(t, "2000000", block.Difficulty)
		assert.Equal(t, "42000", block.GasUsed)
		assert.Equal(t, "30000000", block.GasLimit)
		assert.Equal(t, 123, block.Nonce)
		assert.Equal(t, "0xminer456", block.Miner)
		assert.Equal(t, "2000", block.Size)
		assert.Equal(t, "0xstateroot456", block.StateRoot)
		assert.Equal(t, "0xtxroot456", block.TransactionsRoot)
		assert.Equal(t, "0xreceiptroot456", block.ReceiptsRoot)
		assert.Equal(t, "0xextra456", block.ExtraData)

		// Check that we have 3 transactions
		assert.Len(t, block.Transactions, 3, "Expected 3 transactions, got %d", len(block.Transactions))

		// Find transactions by hash since order may vary
		var tx1, tx2, tx3 *Transaction
		for i := range block.Transactions {
			switch block.Transactions[i].Hash {
			case "0xtx1":
				tx1 = &block.Transactions[i]
			case "0xtx2":
				tx2 = &block.Transactions[i]
			case "0xtx3":
				tx3 = &block.Transactions[i]
			}
		}

		// Transaction 1: No logs
		require.NotNil(t, tx1, "Transaction 1 (0xtx1) not found")
		assert.Equal(t, "0xtx1", tx1.Hash)
		assert.Equal(t, "0xfrom1", tx1.From)
		assert.Equal(t, "0xto1", tx1.To)
		assert.Equal(t, "1", tx1.Nonce)
		assert.Equal(t, 0, tx1.TransactionIndex)
		assert.Len(t, tx1.Logs, 0, "Transaction 1 should have no logs")

		// Transaction 2: 1 log
		require.NotNil(t, tx2, "Transaction 2 (0xtx2) not found")
		assert.Equal(t, "0xtx2", tx2.Hash)
		assert.Equal(t, "0xfrom2", tx2.From)
		assert.Equal(t, "0xto2", tx2.To)
		assert.Equal(t, "2", tx2.Nonce)
		assert.Equal(t, 1, tx2.TransactionIndex)
		assert.Len(t, tx2.Logs, 1, "Transaction 2 should have 1 log")

		log1 := tx2.Logs[0]
		assert.Equal(t, "0xcontract1", log1.Address)
		assert.Len(t, log1.Topics, 2)
		assert.Equal(t, "0xevent1", log1.Topics[0])
		assert.Equal(t, "0xparam1", log1.Topics[1])
		assert.Equal(t, "0xdata1", log1.Data)
		assert.Equal(t, 0, log1.LogIndex)

		// Transaction 3: 2 logs
		require.NotNil(t, tx3, "Transaction 3 (0xtx3) not found")
		assert.Equal(t, "0xtx3", tx3.Hash)
		assert.Equal(t, "0xfrom3", tx3.From)
		assert.Equal(t, "0xto3", tx3.To)
		assert.Equal(t, "3", tx3.Nonce)
		assert.Equal(t, 2, tx3.TransactionIndex)
		assert.Len(t, tx3.Logs, 2, "Transaction 3 should have 2 logs")

		// Find logs by address since order may vary
		var log2, log3 *Log
		for i := range tx3.Logs {
			switch tx3.Logs[i].Address {
			case "0xcontract2":
				log2 = &tx3.Logs[i]
			case "0xcontract3":
				log3 = &tx3.Logs[i]
			}
		}

		require.NotNil(t, log2, "Log 2 (0xcontract2) not found")
		assert.Equal(t, "0xcontract2", log2.Address)
		assert.Len(t, log2.Topics, 2)
		assert.Equal(t, "0xevent2", log2.Topics[0])
		assert.Equal(t, "0xparam2", log2.Topics[1])
		assert.Equal(t, "0xdata2", log2.Data)
		assert.Equal(t, 0, log2.LogIndex)

		require.NotNil(t, log3, "Log 3 (0xcontract3) not found")
		assert.Equal(t, "0xcontract3", log3.Address)
		assert.Len(t, log3.Topics, 2)
		assert.Equal(t, "0xevent3", log3.Topics[0])
		assert.Equal(t, "0xparam3", log3.Topics[1])
		assert.Equal(t, "0xdata3", log3.Data)
		assert.Equal(t, 1, log3.LogIndex)
	})
}
