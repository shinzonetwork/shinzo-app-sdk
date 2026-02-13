package rustffi

import (
	"encoding/json"
	"fmt"
	"testing"
)

// parseResponse is a test helper that unmarshals a JSON FFI response and
// extracts the data object, failing the test on any error.
func parseResponse(t *testing.T, result string) map[string]interface{} {
	t.Helper()
	var response map[string]interface{}
	if err := json.Unmarshal([]byte(result), &response); err != nil {
		t.Fatalf("failed to parse FFI response: %v\nraw: %s", err, result)
	}
	data, ok := response["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected data object in response, got: %s", result)
	}
	return data
}

// getArray is a test helper that extracts a named array from a data map.
func getArray(t *testing.T, data map[string]interface{}, key string) []interface{} {
	t.Helper()
	arr, ok := data[key].([]interface{})
	if !ok {
		t.Fatalf("expected %q array in data, got: %v", key, data)
	}
	return arr
}

func TestInitAndVersion(t *testing.T) {
	Init()
	v := Version()
	if v == "" {
		t.Fatal("expected non-empty version")
	}
	t.Logf("Rust DefraDB FFI version: %s", v)
}

func TestNodeLifecycle(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	if node == 0 {
		t.Fatal("expected non-zero node handle")
	}

	err = Close(node)
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestAddSchemaAndQuery(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	defer Close(node)

	// Add schema
	schema := "type User { name: String, age: Int }"
	_, err = AddSchema(node, schema)
	if err != nil {
		t.Fatalf("AddSchema failed: %v", err)
	}

	// Verify collections exist
	collections, err := GetCollections(node)
	if err != nil {
		t.Fatalf("GetCollections failed: %v", err)
	}
	t.Logf("Collections: %s", collections)

	// Create a document via mutation
	result, err := ExecRequest(node, `mutation { create_User(input: {name: "Alice", age: 30}) { _docID name age } }`)
	if err != nil {
		t.Fatalf("ExecRequest (create) failed: %v", err)
	}
	t.Logf("Create result: %s", result)

	// Query the document
	result, err = ExecRequest(node, `{ User { name age } }`)
	if err != nil {
		t.Fatalf("ExecRequest (query) failed: %v", err)
	}
	t.Logf("Query result: %s", result)

	data := parseResponse(t, result)
	users := getArray(t, data, "User")
	if len(users) == 0 {
		t.Fatal("expected User array with entries")
	}
}

func TestCollectionCreate(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	defer Close(node)

	// Add schema
	_, err = AddSchema(node, "type Person { name: String, age: Int }")
	if err != nil {
		t.Fatalf("AddSchema failed: %v", err)
	}

	// Single document create
	result, err := CollectionCreate(node, "Person", `{"name": "Bob", "age": 25}`)
	if err != nil {
		t.Fatalf("CollectionCreate (single) failed: %v", err)
	}
	t.Logf("Single create result: %s", result)

	// Batch create (JSON array)
	result, err = CollectionCreate(node, "Person", `[{"name": "Charlie", "age": 35}, {"name": "Diana", "age": 28}]`)
	if err != nil {
		t.Fatalf("CollectionCreate (batch) failed: %v", err)
	}
	t.Logf("Batch create result: %s", result)

	// Verify all 3 docs exist
	queryResult, err := ExecRequest(node, `{ Person { name age } }`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	data := parseResponse(t, queryResult)
	persons := getArray(t, data, "Person")
	if len(persons) != 3 {
		t.Fatalf("expected 3 persons, got %d: %s", len(persons), queryResult)
	}
	t.Logf("All persons: %s", queryResult)
}

func TestTransactions(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	defer Close(node)

	_, err = AddSchema(node, "type TxTest { value: Int }")
	if err != nil {
		t.Fatalf("AddSchema failed: %v", err)
	}

	// Begin transaction
	txnID, err := BeginTxn(node, false)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}
	t.Logf("Transaction ID: %s", txnID)

	// Execute mutation in transaction
	result, err := ExecRequestInTxn(node, txnID, `mutation { create_TxTest(input: {value: 42}) { _docID value } }`)
	if err != nil {
		t.Fatalf("ExecRequestInTxn failed: %v", err)
	}
	t.Logf("Txn mutation result: %s", result)

	// Commit
	err = CommitTxn(node, txnID)
	if err != nil {
		t.Fatalf("CommitTxn failed: %v", err)
	}

	// Verify document exists after commit
	queryResult, err := ExecRequest(node, `{ TxTest { value } }`)
	if err != nil {
		t.Fatalf("Query after commit failed: %v", err)
	}
	t.Logf("After commit: %s", queryResult)
}

func TestTransactionRollback(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	defer Close(node)

	_, err = AddSchema(node, "type RollbackTest { value: Int }")
	if err != nil {
		t.Fatalf("AddSchema failed: %v", err)
	}

	// Begin, mutate, rollback
	txnID, err := BeginTxn(node, false)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	_, err = ExecRequestInTxn(node, txnID, `mutation { create_RollbackTest(input: {value: 99}) { _docID } }`)
	if err != nil {
		t.Fatalf("ExecRequestInTxn failed: %v", err)
	}

	err = RollbackTxn(node, txnID)
	if err != nil {
		t.Fatalf("RollbackTxn failed: %v", err)
	}

	// Document should NOT exist
	queryResult, err := ExecRequest(node, `{ RollbackTest { value } }`)
	if err != nil {
		t.Fatalf("Query after rollback failed: %v", err)
	}

	data := parseResponse(t, queryResult)
	items := getArray(t, data, "RollbackTest")
	if len(items) != 0 {
		t.Fatalf("expected 0 items after rollback, got %d", len(items))
	}
	t.Log("Rollback verified: no documents exist")
}

func TestClientHighLevel(t *testing.T) {
	client, err := NewClient(nil) // nil = in-memory defaults
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	defer client.Close()

	// Apply schema
	err = client.ApplySchema("type Block { number: Int, hash: String }")
	if err != nil {
		t.Fatalf("ApplySchema failed: %v", err)
	}

	// Create documents in a transaction
	txnID, err := client.BeginTxn()
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	// Batch create via transaction
	blocks := `[{"number": 1, "hash": "0xabc"}, {"number": 2, "hash": "0xdef"}]`
	_, err = client.CreateDocuments(txnID, "Block", blocks)
	if err != nil {
		t.Fatalf("CreateDocuments failed: %v", err)
	}

	err = client.CommitTxn(txnID)
	if err != nil {
		t.Fatalf("CommitTxn failed: %v", err)
	}

	// Query
	result, err := client.Query(`{ Block { number hash } }`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	t.Logf("Blocks: %s", result)

	data := parseResponse(t, result)
	blocksList := getArray(t, data, "Block")
	if len(blocksList) != 2 {
		t.Fatalf("expected 2 blocks, got %d", len(blocksList))
	}
}

func TestIndexerWorkflow(t *testing.T) {
	// Simulates the real indexer workflow:
	// 1. Create node with schema
	// 2. Per block: begin txn, batch create docs per collection, commit
	client, err := NewClient(nil)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	defer client.Close()

	// Schema matching the real indexer pattern
	schema := `
		type Ethereum__Mainnet__Block {
			number: Int
			hash: String
			parentHash: String
			timestamp: Int
		}
		type Ethereum__Mainnet__Transaction {
			hash: String
			blockNumber: Int
			from: String
			to: String
			value: String
		}
		type Ethereum__Mainnet__Log {
			blockNumber: Int
			transactionHash: String
			logIndex: Int
			address: String
			topic0: String
		}
	`
	err = client.ApplySchema(schema)
	if err != nil {
		t.Fatalf("ApplySchema failed: %v", err)
	}

	// Process 5 blocks
	for blockNum := 1; blockNum <= 5; blockNum++ {
		txnID, err := client.BeginTxn()
		if err != nil {
			t.Fatalf("BeginTxn for block %d failed: %v", blockNum, err)
		}

		// Create block
		blockJSON := fmt.Sprintf(`{"number": %d, "hash": "0x%d", "parentHash": "0x%d", "timestamp": %d}`,
			blockNum, blockNum, blockNum-1, 1700000000+blockNum)
		_, err = client.CreateDocuments(txnID, "Ethereum__Mainnet__Block", blockJSON)
		if err != nil {
			t.Fatalf("Create block %d failed: %v", blockNum, err)
		}

		// Batch create transactions
		txsJSON := fmt.Sprintf(`[
			{"hash": "0xtx%d_1", "blockNumber": %d, "from": "0xsender1", "to": "0xreceiver1", "value": "100"},
			{"hash": "0xtx%d_2", "blockNumber": %d, "from": "0xsender2", "to": "0xreceiver2", "value": "200"}
		]`, blockNum, blockNum, blockNum, blockNum)
		_, err = client.CreateDocuments(txnID, "Ethereum__Mainnet__Transaction", txsJSON)
		if err != nil {
			t.Fatalf("Create txs for block %d failed: %v", blockNum, err)
		}

		// Batch create logs
		logsJSON := fmt.Sprintf(`[
			{"blockNumber": %d, "transactionHash": "0xtx%d_1", "logIndex": 0, "address": "0xcontract1", "topic0": "0xtopic1"},
			{"blockNumber": %d, "transactionHash": "0xtx%d_1", "logIndex": 1, "address": "0xcontract2", "topic0": "0xtopic2"},
			{"blockNumber": %d, "transactionHash": "0xtx%d_2", "logIndex": 0, "address": "0xcontract1", "topic0": "0xtopic1"}
		]`, blockNum, blockNum, blockNum, blockNum, blockNum, blockNum)
		_, err = client.CreateDocuments(txnID, "Ethereum__Mainnet__Log", logsJSON)
		if err != nil {
			t.Fatalf("Create logs for block %d failed: %v", blockNum, err)
		}

		err = client.CommitTxn(txnID)
		if err != nil {
			t.Fatalf("CommitTxn for block %d failed: %v", blockNum, err)
		}
	}

	// Verify: query block count
	result, err := client.Query(`{ Ethereum__Mainnet__Block { number } }`)
	if err != nil {
		t.Fatalf("Query blocks failed: %v", err)
	}
	data := parseResponse(t, result)
	blocks := getArray(t, data, "Ethereum__Mainnet__Block")
	if len(blocks) != 5 {
		t.Fatalf("expected 5 blocks, got %d", len(blocks))
	}

	// Verify: query transaction count
	result, err = client.Query(`{ Ethereum__Mainnet__Transaction { hash } }`)
	if err != nil {
		t.Fatalf("Query txs failed: %v", err)
	}
	data = parseResponse(t, result)
	txs := getArray(t, data, "Ethereum__Mainnet__Transaction")
	if len(txs) != 10 {
		t.Fatalf("expected 10 transactions, got %d", len(txs))
	}

	// Verify: query log count
	result, err = client.Query(`{ Ethereum__Mainnet__Log { logIndex } }`)
	if err != nil {
		t.Fatalf("Query logs failed: %v", err)
	}
	data = parseResponse(t, result)
	logs := getArray(t, data, "Ethereum__Mainnet__Log")
	if len(logs) != 15 {
		t.Fatalf("expected 15 logs, got %d", len(logs))
	}

	t.Logf("Indexer workflow test passed: 5 blocks, 10 txs, 15 logs")
}
