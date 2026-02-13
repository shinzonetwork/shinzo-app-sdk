package pruner

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/logger"
	"github.com/sourcenetwork/defradb/node"
)

// Pruner handles periodic removal of old blockchain documents from DefraDB.
// It supports two queue types:
//   - IndexerQueue: for indexers that track docIDs at creation time
//   - EventQueue: for hosts that track docIDs from P2P replication events
//
// When no queue is set or the queue is underfilled, falls back to filter-based pruning.
type Pruner struct {
	cfg         *Config
	collections CollectionConfig
	defraNode   *node.Node
	queue       PrunerQueue // IndexerQueue or EventQueue
	stopChan    chan struct{}
	wg          sync.WaitGroup
	mu          sync.RWMutex

	// Metrics
	lastPruneTime     time.Time
	totalBlocksPruned int64
	totalDocsPruned   int64
	isRunning         bool
}

// Metrics holds pruning statistics.
type Metrics struct {
	Enabled           bool      `json:"enabled"`
	IsRunning         bool      `json:"is_running"`
	LastPruneTime     time.Time `json:"last_prune_time"`
	TotalBlocksPruned int64     `json:"total_blocks_pruned"`
	TotalDocsPruned   int64     `json:"total_docs_pruned"`
}

// NewPruner creates a new Pruner instance.
func NewPruner(cfg *Config, defraNode *node.Node, collections ...CollectionConfig) *Pruner {
	cols := DefaultCollectionConfig()
	if len(collections) > 0 {
		cols = collections[0]
	}
	return &Pruner{
		cfg:         cfg,
		collections: cols,
		defraNode:   defraNode,
		stopChan:    make(chan struct{}),
	}
}

// SetQueue sets the queue implementation for queue-based pruning.
func (p *Pruner) SetQueue(queue PrunerQueue) {
	p.queue = queue
}

// Start begins the pruning loop in a background goroutine.
func (p *Pruner) Start(ctx context.Context) error {
	if !p.cfg.Enabled {
		logger.Sugar.Info("Pruner is disabled")
		return nil
	}

	if p.defraNode == nil {
		logger.Sugar.Warn("Pruner requires embedded DefraDB node, skipping")
		return nil
	}

	p.mu.Lock()
	if p.isRunning {
		p.mu.Unlock()
		return nil
	}
	p.isRunning = true
	p.mu.Unlock()

	logger.Sugar.Debugf("Starting pruner (max_blocks=%d, threshold=%d, interval=%ds)",
		p.cfg.MaxBlocks, p.cfg.PruneThreshold, p.cfg.IntervalSeconds)

	p.wg.Add(1)
	go p.pruneLoop(ctx)

	return nil
}

// Stop signals the pruner to stop and waits for it to complete.
func (p *Pruner) Stop() {
	p.mu.Lock()
	if !p.isRunning {
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()

	logger.Sugar.Infof("Pruner stopping, waiting for current operation to finish...")
	close(p.stopChan)
	p.wg.Wait()

	// Save queue to disk for fast restart
	if p.queue != nil {
		queueLen := p.queue.Len()
		logger.Sugar.Infof("Saving prune queue to disk (%d entries)...", queueLen)
		if err := p.queue.Save(); err != nil {
			logger.Sugar.Errorf("Failed to save prune queue: %v", err)
		} else {
			logger.Sugar.Infof("Prune queue saved successfully")
		}
	}

	p.mu.Lock()
	p.isRunning = false
	p.mu.Unlock()

	logger.Sugar.Info("Pruner stopped")
}

// GetMetrics returns current pruning statistics.
func (p *Pruner) GetMetrics() Metrics {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return Metrics{
		Enabled:           p.cfg.Enabled,
		IsRunning:         p.isRunning,
		LastPruneTime:     p.lastPruneTime,
		TotalBlocksPruned: p.totalBlocksPruned,
		TotalDocsPruned:   p.totalDocsPruned,
	}
}

// pruneLoop runs the periodic pruning check.
func (p *Pruner) pruneLoop(ctx context.Context) {
	defer p.wg.Done()

	// Startup: clean up blocks from previous runs that aren't in the queue
	logger.Sugar.Debugf("Running startup cleanup for pre-existing blocks...")
	if err := p.startupCleanup(ctx); err != nil {
		logger.Sugar.Errorf("Startup cleanup failed: %v", err)
	}

	ticker := time.NewTicker(time.Duration(p.cfg.IntervalSeconds) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopChan:
			return
		case <-ticker.C:
			if err := p.runPrune(ctx); err != nil {
				logger.Sugar.Errorf("Prune failed: %v", err)
			}
			p.runStorageGC()
		}
	}
}

// runPrune executes the appropriate pruning strategy based on queue type and state.
func (p *Pruner) runPrune(ctx context.Context) error {
	if p.queue == nil {
		return p.filterBasedPrune(ctx)
	}

	switch q := p.queue.(type) {
	case *IndexerQueue:
		return p.runIndexerQueuePrune(ctx, q)
	case *EventQueue:
		return p.runEventQueuePrune(ctx, q)
	default:
		return p.filterBasedPrune(ctx)
	}
}

// runIndexerQueuePrune drains the IndexerQueue and purges by docIDs.
func (p *Pruner) runIndexerQueuePrune(ctx context.Context, q *IndexerQueue) error {
	queueLen := int64(q.Len())
	threshold := p.cfg.MaxBlocks + p.cfg.PruneThreshold

	if queueLen <= threshold {
		return p.filterBasedPrune(ctx)
	}

	result := q.Drain(int(p.cfg.MaxBlocks), p.collections)
	if result == nil {
		return nil
	}

	logger.Sugar.Infof("Pruning %d blocks (queue was %d, keeping %d)",
		result.BlockCount, queueLen, p.cfg.MaxBlocks)

	return p.purgeFromDrainResult(ctx, result)
}

// runEventQueuePrune drains the EventQueue and purges by docIDs.
func (p *Pruner) runEventQueuePrune(ctx context.Context, q *EventQueue) error {
	blockCount := int64(q.BlockCount())
	threshold := p.cfg.MaxBlocks + p.cfg.PruneThreshold

	if blockCount <= threshold {
		return p.filterBasedPrune(ctx)
	}

	excess := int(blockCount - p.cfg.MaxBlocks)
	result := q.DrainBlocks(excess)
	if result == nil {
		return nil
	}

	logger.Sugar.Infof("Pruning %d blocks (queue had %d blocks, keeping %d)",
		result.BlockCount, blockCount, p.cfg.MaxBlocks)

	return p.purgeFromDrainResult(ctx, result)
}

// purgeFromDrainResult deletes documents from a DrainResult.
// Deletes dependent collections in parallel first, then the block collection last.
func (p *Pruner) purgeFromDrainResult(ctx context.Context, result *DrainResult) error {
	startTime := time.Now()
	totalPurged := int64(0)

	// Phase 1: Delete dependent collections in parallel
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, colName := range p.collections.DependentCollections {
		docIDs, ok := result.DocIDsByCollection[colName]
		if !ok || len(docIDs) == 0 || ctx.Err() != nil {
			continue
		}
		wg.Add(1)
		go func(name string, ids []string) {
			defer wg.Done()
			purged, err := p.purgeByDocIDs(ctx, name, ids)
			if err != nil {
				logger.Sugar.Errorf("Failed to purge %s: %v", name, err)
			} else {
				mu.Lock()
				totalPurged += purged
				mu.Unlock()
			}
		}(colName, docIDs)
	}

	wg.Wait()

	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Phase 2: Delete block collection last
	if blockIDs, ok := result.DocIDsByCollection[p.collections.BlockCollection]; ok && len(blockIDs) > 0 {
		purged, err := p.purgeByDocIDs(ctx, p.collections.BlockCollection, blockIDs)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			logger.Sugar.Errorf("Failed to purge blocks: %v", err)
		} else {
			totalPurged += purged
		}
	}

	elapsed := time.Since(startTime)
	logger.Sugar.Infof("Prune complete: removed %d docs for %d blocks in %v",
		totalPurged, result.BlockCount, elapsed)

	p.mu.Lock()
	p.totalBlocksPruned += int64(result.BlockCount)
	p.totalDocsPruned += totalPurged
	p.lastPruneTime = time.Now()
	p.mu.Unlock()

	return nil
}

// startupCleanup removes blocks left over from previous runs that aren't in the queue.
func (p *Pruner) startupCleanup(ctx context.Context) error {
	lowest, err := p.getLowestBlockNumber(ctx)
	if err != nil {
		return err
	}

	highest, err := p.getHighestBlockNumber(ctx)
	if err != nil {
		return err
	}

	if lowest == 0 && highest == 0 {
		logger.Sugar.Debugf("No existing blocks in database")
		return nil
	}

	currentCount := highest - lowest + 1
	if currentCount <= p.cfg.MaxBlocks {
		logger.Sugar.Debugf("Existing blocks %d-%d (count=%d) within limit, no cleanup needed",
			lowest, highest, currentCount)
		return nil
	}

	toPrune := currentCount - p.cfg.MaxBlocks
	cutoffBlock := lowest + toPrune - 1

	logger.Sugar.Infof("Startup cleanup: pruning blocks %d-%d (%d blocks, keeping %d-%d)",
		lowest, cutoffBlock, toPrune, cutoffBlock+1, highest)

	totalPurged, err := p.pruneBlockRange(ctx, lowest, cutoffBlock)
	if err != nil {
		logger.Sugar.Errorf("Startup: failed to prune blocks %d-%d: %v", lowest, cutoffBlock, err)
		return err
	}

	logger.Sugar.Infof("Startup cleanup complete: purged %d documents", totalPurged)

	p.mu.Lock()
	p.totalBlocksPruned += toPrune
	p.totalDocsPruned += totalPurged
	p.lastPruneTime = time.Now()
	p.mu.Unlock()

	return nil
}

// runStorageGC reclaims disk space from deleted entries by running Badger value log GC.
func (p *Pruner) runStorageGC() {
	if p.defraNode == nil {
		return
	}
	startTime := time.Now()
	if err := p.defraNode.RunStorageGC(); err != nil {
		logger.Sugar.Debugf("Storage GC error (non-fatal): %v", err)
	}
	elapsed := time.Since(startTime)
	if elapsed > time.Second {
		logger.Sugar.Infof("Storage GC completed in %v", elapsed)
	}
}

// filterBasedPrune checks the actual DB block count and prunes excess blocks.
// This is the fallback for when the queue is underfilled (e.g., after a crash).
func (p *Pruner) filterBasedPrune(ctx context.Context) error {
	highest, err := p.getHighestBlockNumber(ctx)
	if err != nil || highest == 0 {
		return nil
	}

	lowest, err := p.getLowestBlockNumber(ctx)
	if err != nil || lowest == 0 {
		return nil
	}

	dbBlockCount := highest - lowest + 1
	if dbBlockCount <= p.cfg.MaxBlocks {
		return nil
	}

	excess := dbBlockCount - p.cfg.MaxBlocks
	cutoff := lowest + excess - 1

	logger.Sugar.Infof("Filter-based prune: %d excess blocks (%d-%d), pruning %d-%d",
		excess, lowest, highest, lowest, cutoff)

	purged, err := p.pruneBlockRange(ctx, lowest, cutoff)
	if err != nil {
		return err
	}

	p.mu.Lock()
	p.totalBlocksPruned += excess
	p.totalDocsPruned += purged
	p.lastPruneTime = time.Now()
	p.mu.Unlock()

	return nil
}

// pruneBlockRange removes all documents for blocks in the given range using filter queries.
func (p *Pruner) pruneBlockRange(ctx context.Context, startBlock, endBlock int64) (int64, error) {
	totalPurged := int64(0)

	blockNumberFilter := map[string]any{
		"blockNumber": map[string]any{
			"_geq": startBlock,
			"_leq": endBlock,
		},
	}

	blockFilter := map[string]any{
		p.collections.BlockNumberField: map[string]any{
			"_geq": startBlock,
			"_leq": endBlock,
		},
	}

	// Cascade delete: dependent collections first, block collection last
	for _, colName := range p.collections.DependentCollections {
		if purged, err := p.purgeCollection(ctx, colName, blockNumberFilter); err != nil {
			return totalPurged, err
		} else {
			totalPurged += purged
		}
	}

	if purged, err := p.purgeCollection(ctx, p.collections.BlockCollection, blockFilter); err != nil {
		return totalPurged, err
	} else {
		totalPurged += purged
	}

	return totalPurged, nil
}

// purgeCollection queries for docIDs matching the filter, then deletes them.
func (p *Pruner) purgeCollection(ctx context.Context, collectionName string, filter map[string]any) (int64, error) {
	docIDs, err := p.queryDocIDsWithFilter(ctx, collectionName, filter)
	if err != nil {
		return 0, err
	}

	if len(docIDs) == 0 {
		return 0, nil
	}

	return p.purgeByDocIDs(ctx, collectionName, docIDs)
}

// queryDocIDsWithFilter queries for docIDs matching the given filter via GraphQL.
func (p *Pruner) queryDocIDsWithFilter(ctx context.Context, collectionName string, filter map[string]any) ([]string, error) {
	filterStr := BuildGraphQLFilter(filter)

	query := fmt.Sprintf(`query {
		%s(filter: %s) {
			_docID
		}
	}`, collectionName, filterStr)

	result := p.defraNode.DB.ExecRequest(ctx, query)
	if len(result.GQL.Errors) > 0 {
		return nil, fmt.Errorf("query failed: %v", result.GQL.Errors[0])
	}

	data, ok := result.GQL.Data.(map[string]any)
	if !ok {
		return nil, nil
	}

	docs, ok := data[collectionName].([]any)
	if !ok || len(docs) == 0 {
		return nil, nil
	}

	docIDs := make([]string, 0, len(docs))
	for _, doc := range docs {
		docMap, ok := doc.(map[string]any)
		if !ok {
			continue
		}
		if docID, ok := docMap["_docID"].(string); ok {
			docIDs = append(docIDs, docID)
		}
	}

	return docIDs, nil
}

// purgeByDocIDs deletes documents by their docIDs directly.
func (p *Pruner) purgeByDocIDs(ctx context.Context, collectionName string, docIDs []string) (int64, error) {
	if len(docIDs) == 0 {
		return 0, nil
	}

	startTime := time.Now()
	logger.Sugar.Debugf("Purging %d documents from %s", len(docIDs), collectionName)

	col, err := p.defraNode.DB.GetCollectionByName(ctx, collectionName)
	if err != nil {
		return 0, fmt.Errorf("failed to get collection %s: %w", collectionName, err)
	}

	purgeCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	result, err := col.PurgeByDocIDs(purgeCtx, docIDs, p.cfg.PruneHistory)
	if err != nil {
		logger.Sugar.Errorf("Purge failed for %s: %v", collectionName, err)
		return 0, err
	}

	logger.Sugar.Infof("Purged %d/%d documents from %s in %v",
		result.Count, len(docIDs), collectionName, time.Since(startTime))

	return result.Count, nil
}

// ─── Block number queries ────────────────────────────────────────────────────

func (p *Pruner) getLowestBlockNumber(ctx context.Context) (int64, error) {
	query := `query {
		` + p.collections.BlockCollection + ` (order: {` + p.collections.BlockNumberField + `: ASC}, limit: 1) {
			` + p.collections.BlockNumberField + `
		}
	}`

	result := p.defraNode.DB.ExecRequest(ctx, query)
	if len(result.GQL.Errors) > 0 {
		return 0, result.GQL.Errors[0]
	}

	return p.extractBlockNumber(result.GQL.Data)
}

func (p *Pruner) getHighestBlockNumber(ctx context.Context) (int64, error) {
	query := `query {
		` + p.collections.BlockCollection + ` (order: {` + p.collections.BlockNumberField + `: DESC}, limit: 1) {
			` + p.collections.BlockNumberField + `
		}
	}`

	result := p.defraNode.DB.ExecRequest(ctx, query)
	if len(result.GQL.Errors) > 0 {
		return 0, result.GQL.Errors[0]
	}

	return p.extractBlockNumber(result.GQL.Data)
}

func (p *Pruner) extractBlockNumber(gqlData any) (int64, error) {
	data, ok := gqlData.(map[string]interface{})
	if !ok {
		return 0, nil
	}

	blocksRaw := data[p.collections.BlockCollection]

	if blocksTyped, ok := blocksRaw.([]map[string]interface{}); ok {
		if len(blocksTyped) == 0 {
			return 0, nil
		}
		if number, ok := blocksTyped[0][p.collections.BlockNumberField]; ok {
			return parseBlockNumber(number)
		}
		return 0, nil
	}

	blocks, ok := blocksRaw.([]interface{})
	if !ok || len(blocks) == 0 {
		return 0, nil
	}

	block, ok := blocks[0].(map[string]interface{})
	if !ok {
		return 0, nil
	}

	if number, ok := block[p.collections.BlockNumberField]; ok {
		return parseBlockNumber(number)
	}
	return 0, nil
}

func parseBlockNumber(number interface{}) (int64, error) {
	switch v := number.(type) {
	case float64:
		return int64(v), nil
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	}
	return 0, nil
}

// BuildGraphQLFilter builds a GraphQL filter string from a map.
// Operators for the same field are combined: {field: {_geq: X, _leq: Y}}.
func BuildGraphQLFilter(filter map[string]any) string {
	var parts []string
	for field, condition := range filter {
		condMap, ok := condition.(map[string]any)
		if !ok {
			continue
		}
		var ops []string
		for op, val := range condMap {
			ops = append(ops, fmt.Sprintf("%s: %v", op, val))
		}
		parts = append(parts, fmt.Sprintf("%s: {%s}", field, join(ops, ", ")))
	}
	return "{" + join(parts, ", ") + "}"
}

func join(s []string, sep string) string {
	if len(s) == 0 {
		return ""
	}
	result := s[0]
	for _, v := range s[1:] {
		result += sep + v
	}
	return result
}
