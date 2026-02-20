package pruner

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/rustffi"
	"github.com/sourcenetwork/defradb/node"
)

// DatabaseAdapter abstracts database operations needed by the pruner,
// allowing it to work with either a Go embedded node or Rust FFI client.
type DatabaseAdapter interface {
	ExecQuery(ctx context.Context, query string) (map[string]any, error)
	PurgeByDocIDs(ctx context.Context, collectionName string, docIDs []string, pruneHistory bool) (int64, error)
}

// GoDBAdapter wraps a Go DefraDB node to implement DatabaseAdapter.
type GoDBAdapter struct {
	Node *node.Node
}

func (a *GoDBAdapter) ExecQuery(ctx context.Context, query string) (map[string]any, error) {
	result := a.Node.DB.ExecRequest(ctx, query)
	if len(result.GQL.Errors) > 0 {
		return nil, fmt.Errorf("query failed: %v", result.GQL.Errors[0])
	}
	data, ok := result.GQL.Data.(map[string]any)
	if !ok {
		return nil, nil
	}
	return data, nil
}

func (a *GoDBAdapter) PurgeByDocIDs(ctx context.Context, collectionName string, docIDs []string, pruneHistory bool) (int64, error) {
	col, err := a.Node.DB.GetCollectionByName(ctx, collectionName)
	if err != nil {
		return 0, fmt.Errorf("failed to get collection %s: %w", collectionName, err)
	}
	purgeCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	result, err := col.PurgeByDocIDs(purgeCtx, docIDs, pruneHistory)
	if err != nil {
		return 0, err
	}
	return result.Count, nil
}

// RustFFIAdapter wraps a Rust FFI client to implement DatabaseAdapter.
type RustFFIAdapter struct {
	Client *rustffi.Client
}

func (a *RustFFIAdapter) ExecQuery(_ context.Context, query string) (map[string]any, error) {
	resp, err := a.Client.Query(query)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	var parsed struct {
		Data   map[string]any `json:"data"`
		Errors []any          `json:"errors"`
	}
	if err := json.Unmarshal([]byte(resp), &parsed); err != nil {
		return nil, fmt.Errorf("failed to parse query response: %w", err)
	}
	if len(parsed.Errors) > 0 {
		return nil, fmt.Errorf("query failed: %v", parsed.Errors[0])
	}
	return parsed.Data, nil
}

func (a *RustFFIAdapter) PurgeByDocIDs(_ context.Context, collectionName string, docIDs []string, _ bool) (int64, error) {
	return a.Client.DeleteDocuments(collectionName, docIDs)
}
