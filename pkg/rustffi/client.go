package rustffi

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/config"
)

// ErrClientClosed is returned when an operation is attempted on a closed client.
var ErrClientClosed = fmt.Errorf("defra ffi: client is closed")

// validCollectionName matches GraphQL type names: start with letter/underscore,
// followed by alphanumerics and underscores.
var validCollectionName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Client provides a high-level interface to the Rust DefraDB FFI backend.
// It manages a single node and provides methods for schema, document,
// transaction, and query operations. All methods are safe for concurrent use.
type Client struct {
	node   NodeHandle
	config *config.RustFFIConfig
	mu     sync.RWMutex
}

// NewClient creates and initializes a new Rust FFI DefraDB client.
func NewClient(cfg *config.RustFFIConfig) (*Client, error) {
	if cfg == nil {
		cfg = &config.RustFFIConfig{InMemory: true}
	}

	Init()

	opts := NodeOptions{
		DBPath:           cfg.DBPath,
		InMemory:         cfg.InMemory,
		DatastoreBackend: cfg.DatastoreBackend,
		EnableSigning:    true,
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
// The handle becomes invalid after Close() is called.
func (c *Client) Node() (NodeHandle, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.node == 0 {
		return 0, ErrClientClosed
	}
	return c.node, nil
}

// ApplySchema adds a GraphQL SDL schema to the node.
func (c *Client) ApplySchema(schemaSDL string) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.node == 0 {
		return ErrClientClosed
	}
	_, err := AddSchema(c.node, schemaSDL)
	return err
}

// CreateDocuments creates document(s) in a collection within a transaction.
// jsonData can be a JSON object (single) or JSON array (batch).
func (c *Client) CreateDocuments(txnID, collectionName, jsonData string) (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.node == 0 {
		return "", ErrClientClosed
	}
	if txnID != "" {
		return c.createDocsInTxn(txnID, collectionName, jsonData)
	}
	return CollectionCreate(c.node, collectionName, jsonData)
}

// createDocsInTxn creates documents within an existing transaction by building
// a GraphQL create mutation and executing it in the transaction context.
// Caller must hold c.mu.RLock().
func (c *Client) createDocsInTxn(txnID, collectionName, jsonData string) (string, error) {
	if !validCollectionName.MatchString(collectionName) {
		return "", fmt.Errorf("invalid collection name: %q", collectionName)
	}

	input, err := jsonToGraphQLInput(jsonData)
	if err != nil {
		return "", fmt.Errorf("failed to convert JSON to GraphQL input: %w", err)
	}

	mutation := fmt.Sprintf("mutation { create_%s(input: %s) { _docID } }", collectionName, input)
	return ExecRequestInTxn(c.node, txnID, mutation)
}

// BeginTxn starts a new read-write transaction.
func (c *Client) BeginTxn() (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.node == 0 {
		return "", ErrClientClosed
	}
	return BeginTxn(c.node, false)
}

// CommitTxn commits a transaction.
func (c *Client) CommitTxn(txnID string) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.node == 0 {
		return ErrClientClosed
	}
	return CommitTxn(c.node, txnID)
}

// RollbackTxn rolls back a transaction.
func (c *Client) RollbackTxn(txnID string) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.node == 0 {
		return ErrClientClosed
	}
	return RollbackTxn(c.node, txnID)
}

// Query executes a GraphQL query and returns the JSON response.
func (c *Client) Query(query string) (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.node == 0 {
		return "", ErrClientClosed
	}
	return ExecRequest(c.node, query)
}

// BatchStart starts (or resets) batch CID collection.
// sessionID must be unique per concurrent batch (e.g., UUID per block).
func (c *Client) BatchStart(sessionID string) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.node == 0 {
		return ErrClientClosed
	}
	return BatchStart(c.node, sessionID)
}

// BatchSign signs all collected batch CIDs and returns the JSON batch signature.
// sessionID must match the one passed to BatchStart.
func (c *Client) BatchSign(sessionID string) (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.node == 0 {
		return "", ErrClientClosed
	}
	return BatchSign(c.node, sessionID)
}

// QueryWithBatch executes a GraphQL query and collects CIDs under the given batch session.
func (c *Client) QueryWithBatch(query, batchSessionID string) (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.node == 0 {
		return "", ErrClientClosed
	}
	return ExecRequestWithBatch(c.node, query, batchSessionID)
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
		if val == float64(int64(val)) {
			return fmt.Sprintf("%d", int64(val))
		}
		return fmt.Sprintf("%g", val)
	case string:
		b, _ := json.Marshal(val)
		return string(b)
	case []interface{}:
		items := make([]string, len(val))
		for i, item := range val {
			items[i] = formatGraphQLValue(item)
		}
		return "[" + strings.Join(items, ", ") + "]"
	case map[string]interface{}:
		fields := make([]string, 0, len(val))
		for k, fv := range val {
			fields = append(fields, k+": "+formatGraphQLValue(fv))
		}
		return "{" + strings.Join(fields, ", ") + "}"
	default:
		return fmt.Sprintf("%v", val)
	}
}
