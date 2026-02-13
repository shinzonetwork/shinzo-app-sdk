package rustffi

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/config"
)

// Client provides a high-level interface to the Rust DefraDB FFI backend.
// It manages a single node and provides methods for schema, document,
// transaction, and query operations.
type Client struct {
	node   NodeHandle
	config *config.RustFFIConfig
	mu     sync.Mutex
}

// NewClient creates and initializes a new Rust FFI DefraDB client.
func NewClient(cfg *config.RustFFIConfig) (*Client, error) {
	if cfg == nil {
		cfg = &config.RustFFIConfig{InMemory: true}
	}

	Init()

	opts := NodeOptions{
		DBPath:   cfg.DBPath,
		InMemory: cfg.InMemory,
	}

	node, err := NewNode(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create rust ffi node: %w", err)
	}

	return &Client{
		node:   node,
		config: cfg,
	}, nil
}

// Close shuts down the Rust FFI node.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.node == 0 {
		return nil
	}
	err := Close(c.node)
	c.node = 0
	return err
}

// Node returns the underlying node handle for direct FFI calls.
func (c *Client) Node() NodeHandle {
	return c.node
}

// ApplySchema adds a GraphQL SDL schema to the node.
func (c *Client) ApplySchema(schemaSDL string) error {
	_, err := AddSchema(c.node, schemaSDL)
	return err
}

// CreateDocuments creates document(s) in a collection within a transaction.
// jsonData can be a JSON object (single) or JSON array (batch).
func (c *Client) CreateDocuments(txnID, collectionName, jsonData string) (string, error) {
	if txnID != "" {
		// Use GraphQL mutation in transaction
		return c.createDocsInTxn(txnID, collectionName, jsonData)
	}
	return CollectionCreate(c.node, collectionName, jsonData)
}

// createDocsInTxn creates documents within an existing transaction by building
// a GraphQL create mutation and executing it in the transaction context.
func (c *Client) createDocsInTxn(txnID, collectionName, jsonData string) (string, error) {
	// Parse the JSON to determine if it's a single doc or batch
	var raw json.RawMessage
	if err := json.Unmarshal([]byte(jsonData), &raw); err != nil {
		return "", fmt.Errorf("invalid JSON data: %w", err)
	}

	// Build GraphQL input from JSON
	input, err := jsonToGraphQLInput(jsonData)
	if err != nil {
		return "", fmt.Errorf("failed to convert JSON to GraphQL input: %w", err)
	}

	mutation := fmt.Sprintf("mutation { create_%s(input: %s) { _docID } }", collectionName, input)
	return ExecRequestInTxn(c.node, txnID, mutation)
}

// BeginTxn starts a new read-write transaction.
func (c *Client) BeginTxn() (string, error) {
	return BeginTxn(c.node, false)
}

// CommitTxn commits a transaction.
func (c *Client) CommitTxn(txnID string) error {
	return CommitTxn(c.node, txnID)
}

// RollbackTxn rolls back a transaction.
func (c *Client) RollbackTxn(txnID string) error {
	return RollbackTxn(c.node, txnID)
}

// Query executes a GraphQL query and returns the JSON response.
func (c *Client) Query(query string) (string, error) {
	return ExecRequest(c.node, query)
}

// jsonToGraphQLInput converts a JSON string to GraphQL input syntax.
// GraphQL uses bare identifiers for object keys: {name: "Alice"} not {"name": "Alice"}.
func jsonToGraphQLInput(jsonStr string) (string, error) {
	var v interface{}
	if err := json.Unmarshal([]byte(jsonStr), &v); err != nil {
		return "", err
	}
	return formatGraphQLValue(v), nil
}

func formatGraphQLValue(v interface{}) string {
	switch val := v.(type) {
	case nil:
		return "null"
	case bool:
		if val {
			return "true"
		}
		return "false"
	case float64:
		// Check if it's actually an integer
		if val == float64(int64(val)) {
			return fmt.Sprintf("%d", int64(val))
		}
		return fmt.Sprintf("%g", val)
	case string:
		b, _ := json.Marshal(val) // properly escapes the string
		return string(b)
	case []interface{}:
		items := make([]string, len(val))
		for i, item := range val {
			items[i] = formatGraphQLValue(item)
		}
		return "[" + joinStrings(items, ", ") + "]"
	case map[string]interface{}:
		fields := make([]string, 0, len(val))
		for k, fv := range val {
			fields = append(fields, k+": "+formatGraphQLValue(fv))
		}
		return "{" + joinStrings(fields, ", ") + "}"
	default:
		return fmt.Sprintf("%v", val)
	}
}

func joinStrings(ss []string, sep string) string {
	if len(ss) == 0 {
		return ""
	}
	result := ss[0]
	for _, s := range ss[1:] {
		result += sep + s
	}
	return result
}
