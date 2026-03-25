package pruner

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── UUID Helpers ────────────────────────────────────────────────────────────

func initDocIDPrefix() {
	// Reset the prefix for testing
	docIDPrefixOnce = sync.Once{}
	docIDPrefix = ""
}

func TestParseUUIDHex(t *testing.T) {
	t.Run("valid UUID", func(t *testing.T) {
		uuid, err := parseUUIDHex("550e8400-e29b-41d4-a716-446655440000")
		require.NoError(t, err)
		assert.Equal(t, 16, len(uuid))
	})

	t.Run("invalid UUID length", func(t *testing.T) {
		_, err := parseUUIDHex("short")
		assert.Error(t, err)
	})

	t.Run("invalid hex characters", func(t *testing.T) {
		_, err := parseUUIDHex("ZZZZZZZZ-ZZZZ-ZZZZ-ZZZZ-ZZZZZZZZZZZZ")
		assert.Error(t, err)
	})
}

func TestFormatUUID(t *testing.T) {
	uuid, err := parseUUIDHex("550e8400-e29b-41d4-a716-446655440000")
	require.NoError(t, err)

	formatted := formatUUID(uuid)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", formatted)
}

func TestExtractUUID(t *testing.T) {
	initDocIDPrefix()

	t.Run("valid docID", func(t *testing.T) {
		uuid, err := extractUUID("bae-550e8400-e29b-41d4-a716-446655440000")
		require.NoError(t, err)
		assert.Equal(t, 16, len(uuid))
		assert.Equal(t, "bae", docIDPrefix)
	})

	t.Run("invalid docID no dash", func(t *testing.T) {
		_, err := extractUUID("nodash")
		assert.Error(t, err)
	})
}

func TestRestoreDocID(t *testing.T) {
	initDocIDPrefix()
	// Set a known prefix
	docIDPrefixOnce.Do(func() {
		docIDPrefix = "bae"
	})

	uuid, err := parseUUIDHex("550e8400-e29b-41d4-a716-446655440000")
	require.NoError(t, err)

	restored := RestoreDocID(uuid)
	assert.Equal(t, "bae-550e8400-e29b-41d4-a716-446655440000", restored)
}

func TestPackUnpackDocIDs(t *testing.T) {
	initDocIDPrefix()

	docIDs := []string{
		"bae-550e8400-e29b-41d4-a716-446655440000",
		"bae-660e8400-e29b-41d4-a716-446655440001",
	}

	packed, err := packDocIDs(docIDs)
	require.NoError(t, err)
	assert.Equal(t, 32, len(packed)) // 2 * 16 bytes

	unpacked := UnpackDocIDs(packed)
	assert.Len(t, unpacked, 2)
	assert.Equal(t, docIDs[0], unpacked[0])
	assert.Equal(t, docIDs[1], unpacked[1])
}

func TestPackDocIDsEmpty(t *testing.T) {
	packed, err := packDocIDs(nil)
	require.NoError(t, err)
	assert.Nil(t, packed)

	packed, err = packDocIDs([]string{})
	require.NoError(t, err)
	assert.Nil(t, packed)
}

func TestUnpackDocIDsEmpty(t *testing.T) {
	assert.Nil(t, UnpackDocIDs(nil))
	assert.Nil(t, UnpackDocIDs([]byte{}))
}

func TestPackDocIDsInvalid(t *testing.T) {
	_, err := packDocIDs([]string{"invalid"})
	assert.Error(t, err)
}

// ─── IndexerQueue ────────────────────────────────────────────────────────────

func TestIndexerQueueBasic(t *testing.T) {
	initDocIDPrefix()
	q := NewIndexerQueue()
	assert.Equal(t, 0, q.Len())
	assert.Equal(t, int64(0), q.HighestBlockNumber())
}

func TestIndexerQueueTrackAndDrain(t *testing.T) {
	initDocIDPrefix()
	q := NewIndexerQueue()
	cols := DefaultCollectionConfig()

	// Track some blocks
	for i := int64(1); i <= 5; i++ {
		err := q.TrackBlockDocIDs(i,
			"bae-550e8400-e29b-41d4-a716-446655440000",
			map[string][]string{
				"Ethereum__Mainnet__Transaction": {"bae-660e8400-e29b-41d4-a716-446655440001"},
			},
			"",
		)
		require.NoError(t, err)
	}

	assert.Equal(t, 5, q.Len())
	assert.Equal(t, int64(5), q.HighestBlockNumber())

	// Drain keeping 3
	result := q.Drain(3, cols)
	require.NotNil(t, result)
	assert.Equal(t, 2, result.BlockCount)
	assert.Equal(t, 3, q.Len())
}

func TestIndexerQueueDrainNothingToDrain(t *testing.T) {
	q := NewIndexerQueue()
	cols := DefaultCollectionConfig()

	// Empty queue
	assert.Nil(t, q.Drain(10, cols))

	// Queue smaller than keep
	initDocIDPrefix()
	q.TrackBlockDocIDs(1, "bae-550e8400-e29b-41d4-a716-446655440000", nil, "")
	assert.Nil(t, q.Drain(10, cols))
}

func TestIndexerQueueDrainByDocCount(t *testing.T) {
	initDocIDPrefix()
	q := NewIndexerQueue()
	cols := DefaultCollectionConfig()

	// Track block with transactions
	err := q.TrackBlockDocIDs(1,
		"bae-550e8400-e29b-41d4-a716-446655440000",
		map[string][]string{
			"Ethereum__Mainnet__Transaction": {
				"bae-660e8400-e29b-41d4-a716-446655440001",
				"bae-770e8400-e29b-41d4-a716-446655440002",
			},
			"Ethereum__Mainnet__Log": {
				"bae-880e8400-e29b-41d4-a716-446655440003",
			},
		},
		"bae-990e8400-e29b-41d4-a716-446655440004",
	)
	require.NoError(t, err)

	// DocCount: 1 block + 2 tx + 1 log + 1 blocksig = 5
	assert.Equal(t, 5, q.DocCount())

	// Empty queue returns nil
	q2 := NewIndexerQueue()
	assert.Nil(t, q2.DrainByDocCount(10, cols))

	// Zero excess returns nil
	assert.Nil(t, q.DrainByDocCount(0, cols))

	// Drain by doc count
	result := q.DrainByDocCount(3, cols)
	require.NotNil(t, result)
	assert.Equal(t, 1, result.BlockCount)
}

func TestIndexerQueueTrackInvalidDocIDs(t *testing.T) {
	initDocIDPrefix()
	q := NewIndexerQueue()

	// Invalid block docID
	err := q.TrackBlockDocIDs(1, "invalid-no-uuid", nil, "")
	assert.Error(t, err)

	// Invalid transaction docID
	err = q.TrackBlockDocIDs(1, "bae-550e8400-e29b-41d4-a716-446655440000",
		map[string][]string{"Ethereum__Mainnet__Transaction": {"invalid"}}, "")
	assert.Error(t, err)

	// Invalid log docID
	err = q.TrackBlockDocIDs(1, "bae-550e8400-e29b-41d4-a716-446655440000",
		map[string][]string{"Ethereum__Mainnet__Log": {"invalid"}}, "")
	assert.Error(t, err)

	// Invalid ALE docID
	err = q.TrackBlockDocIDs(1, "bae-550e8400-e29b-41d4-a716-446655440000",
		map[string][]string{"Ethereum__Mainnet__AccessListEntry": {"invalid"}}, "")
	assert.Error(t, err)

	// Invalid block sig docID
	err = q.TrackBlockDocIDs(1, "bae-550e8400-e29b-41d4-a716-446655440000", nil, "invalid")
	assert.Error(t, err)
}

func TestIndexerQueueSaveLoad(t *testing.T) {
	initDocIDPrefix()
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "queue.gob")

	q := NewIndexerQueue()

	// Set file path (file doesn't exist yet)
	count, err := q.LoadFromFile(filePath)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Track entries
	q.TrackBlockDocIDs(1, "bae-550e8400-e29b-41d4-a716-446655440000", nil, "")
	q.TrackBlockDocIDs(2, "bae-660e8400-e29b-41d4-a716-446655440001", nil, "")

	// Save
	err = q.Save()
	require.NoError(t, err)

	// Load into new queue
	q2 := NewIndexerQueue()
	count, err = q2.LoadFromFile(filePath)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
	assert.Equal(t, 2, q2.Len())
}

func TestIndexerQueueSaveEmptyRemovesFile(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "queue.gob")

	// Create a file
	os.WriteFile(filePath, []byte("data"), 0644)

	q := NewIndexerQueue()
	q.LoadFromFile(filePath)
	// Queue is empty (file had invalid data, but LoadFromFile sets filePath)

	err := q.Save()
	require.NoError(t, err)

	// File should be removed
	_, err = os.Stat(filePath)
	assert.True(t, os.IsNotExist(err))
}

func TestIndexerQueueSaveNoFilePath(t *testing.T) {
	q := NewIndexerQueue()
	err := q.Save()
	require.NoError(t, err) // no-op
}

// ─── EventQueue ──────────────────────────────────────────────────────────────

func TestEventQueueBasic(t *testing.T) {
	cols := DefaultCollectionConfig()
	q := NewEventQueue(cols)
	assert.Equal(t, 0, q.Len())
	assert.Equal(t, 0, q.BlockCount())
}

func TestEventQueuePush(t *testing.T) {
	initDocIDPrefix()
	cols := DefaultCollectionConfig()
	q := NewEventQueue(cols)

	// Push a block
	q.Push("Ethereum__Mainnet__Block", "bae-550e8400-e29b-41d4-a716-446655440000")
	assert.Equal(t, 1, q.Len())
	assert.Equal(t, 1, q.BlockCount())

	// Push a transaction
	q.Push("Ethereum__Mainnet__Transaction", "bae-660e8400-e29b-41d4-a716-446655440001")
	assert.Equal(t, 2, q.Len())
	assert.Equal(t, 1, q.BlockCount()) // still 1 block

	// Push with invalid docID (silently skipped)
	q.Push("Ethereum__Mainnet__Block", "invalid")
	assert.Equal(t, 2, q.Len())

	// Push with unknown collection (silently skipped)
	q.Push("Unknown__Collection", "bae-770e8400-e29b-41d4-a716-446655440002")
	assert.Equal(t, 2, q.Len())
}

func TestEventQueueDrainDocs(t *testing.T) {
	initDocIDPrefix()
	cols := DefaultCollectionConfig()
	q := NewEventQueue(cols)

	// Empty queue
	assert.Nil(t, q.DrainDocs(5))

	// Zero count
	q.Push("Ethereum__Mainnet__Block", "bae-550e8400-e29b-41d4-a716-446655440000")
	assert.Nil(t, q.DrainDocs(0))

	// Add more entries
	q.Push("Ethereum__Mainnet__Transaction", "bae-660e8400-e29b-41d4-a716-446655440001")
	q.Push("Ethereum__Mainnet__Log", "bae-770e8400-e29b-41d4-a716-446655440002")

	// Drain 2 of 3
	result := q.DrainDocs(2)
	require.NotNil(t, result)
	assert.Equal(t, 1, q.Len())
	assert.Equal(t, 1, result.BlockCount) // block was in the first 2

	// Drain more than available
	q.Push("Ethereum__Mainnet__Block", "bae-880e8400-e29b-41d4-a716-446655440003")
	result = q.DrainDocs(100)
	require.NotNil(t, result)
	assert.Equal(t, 0, q.Len())
}

func TestEventQueueDrainBlocks(t *testing.T) {
	initDocIDPrefix()
	cols := DefaultCollectionConfig()
	q := NewEventQueue(cols)

	// Empty queue
	assert.Nil(t, q.DrainBlocks(1))

	// Zero count
	q.Push("Ethereum__Mainnet__Block", "bae-550e8400-e29b-41d4-a716-446655440000")
	assert.Nil(t, q.DrainBlocks(0))

	// Queue now: block(0)
	// Add: tx, tx, block, tx → queue: block(0), tx, tx, block(3), tx
	q.Push("Ethereum__Mainnet__Transaction", "bae-660e8400-e29b-41d4-a716-446655440001")
	q.Push("Ethereum__Mainnet__Transaction", "bae-770e8400-e29b-41d4-a716-446655440002")
	q.Push("Ethereum__Mainnet__Block", "bae-880e8400-e29b-41d4-a716-446655440003")
	q.Push("Ethereum__Mainnet__Transaction", "bae-990e8400-e29b-41d4-a716-446655440004")

	// DrainBlocks(1): walk front, block at index 0 → cutoff=1, drain 1 entry
	result := q.DrainBlocks(1)
	require.NotNil(t, result)
	assert.Equal(t, 1, result.BlockCount)
	assert.Equal(t, 4, q.Len()) // tx, tx, block, tx remaining

	// DrainBlocks(1): walk front, tx(0), tx(1), block at index 2 → cutoff=3
	result = q.DrainBlocks(1)
	require.NotNil(t, result)
	assert.Equal(t, 1, result.BlockCount)
	assert.Equal(t, 1, q.Len()) // just tx remaining

	// No more blocks, should return nil
	assert.Nil(t, q.DrainBlocks(1))
}

func TestEventQueueDrainBlocksNotEnoughBlocks(t *testing.T) {
	initDocIDPrefix()
	cols := DefaultCollectionConfig()
	q := NewEventQueue(cols)

	// Only transactions, no blocks
	q.Push("Ethereum__Mainnet__Transaction", "bae-550e8400-e29b-41d4-a716-446655440000")
	q.Push("Ethereum__Mainnet__Transaction", "bae-660e8400-e29b-41d4-a716-446655440001")

	// blockCount is 0 so should return nil
	assert.Nil(t, q.DrainBlocks(1))
}

func TestEventQueueSaveLoad(t *testing.T) {
	initDocIDPrefix()
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "event_queue.gob")

	cols := DefaultCollectionConfig()
	q := NewEventQueue(cols)

	// Set file path (file doesn't exist yet)
	count, err := q.LoadFromFile(filePath)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Push entries
	q.Push("Ethereum__Mainnet__Block", "bae-550e8400-e29b-41d4-a716-446655440000")
	q.Push("Ethereum__Mainnet__Transaction", "bae-660e8400-e29b-41d4-a716-446655440001")

	// Save
	err = q.Save()
	require.NoError(t, err)

	// Load into new queue
	q2 := NewEventQueue(cols)
	count, err = q2.LoadFromFile(filePath)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
	assert.Equal(t, 2, q2.Len())
}

func TestEventQueueSaveNoFilePath(t *testing.T) {
	cols := DefaultCollectionConfig()
	q := NewEventQueue(cols)
	err := q.Save()
	require.NoError(t, err) // no-op
}

func TestEventQueueSaveEmptyRemovesFile(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "event_queue.gob")

	os.WriteFile(filePath, []byte("data"), 0644)

	cols := DefaultCollectionConfig()
	q := NewEventQueue(cols)
	q.LoadFromFile(filePath)

	err := q.Save()
	require.NoError(t, err)

	_, err = os.Stat(filePath)
	assert.True(t, os.IsNotExist(err))
}

func TestEventQueueLoadFromFileInvalidData(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "bad_queue.gob")

	os.WriteFile(filePath, []byte("not valid gob data"), 0644)

	cols := DefaultCollectionConfig()
	q := NewEventQueue(cols)
	_, err := q.LoadFromFile(filePath)
	assert.Error(t, err)
}

func TestIndexerQueueLoadFromFileInvalidData(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "bad_queue.gob")

	os.WriteFile(filePath, []byte("not valid gob data"), 0644)

	q := NewIndexerQueue()
	_, err := q.LoadFromFile(filePath)
	assert.Error(t, err)
}

// ─── Additional queue edge case tests ────────────────────────────────────────

func TestIndexerQueueSave_WithEntries(t *testing.T) {
	initDocIDPrefix()
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "save_test.gob")

	q := NewIndexerQueue()
	q.LoadFromFile(filePath)

	// Add entries with various doc types
	err := q.TrackBlockDocIDs(1,
		"bae-550e8400-e29b-41d4-a716-446655440000",
		map[string][]string{
			"Ethereum__Mainnet__Transaction":     {"bae-660e8400-e29b-41d4-a716-446655440001"},
			"Ethereum__Mainnet__Log":             {"bae-770e8400-e29b-41d4-a716-446655440002"},
			"Ethereum__Mainnet__AccessListEntry": {"bae-880e8400-e29b-41d4-a716-446655440003"},
		},
		"bae-990e8400-e29b-41d4-a716-446655440004",
	)
	require.NoError(t, err)

	// Save
	err = q.Save()
	require.NoError(t, err)

	// Verify file exists
	_, err = os.Stat(filePath)
	assert.NoError(t, err)

	// Load and verify
	q2 := NewIndexerQueue()
	count, err := q2.LoadFromFile(filePath)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
	assert.Equal(t, 5, q2.DocCount())
}

func TestEventQueueSave_WithEntries(t *testing.T) {
	initDocIDPrefix()
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "event_save.gob")

	cols := DefaultCollectionConfig()
	q := NewEventQueue(cols)
	q.LoadFromFile(filePath)

	// Push entries
	q.Push("Ethereum__Mainnet__Block", "bae-550e8400-e29b-41d4-a716-446655440000")
	q.Push("Ethereum__Mainnet__Transaction", "bae-660e8400-e29b-41d4-a716-446655440001")
	q.Push("Ethereum__Mainnet__Log", "bae-770e8400-e29b-41d4-a716-446655440002")

	// Save
	err := q.Save()
	require.NoError(t, err)

	// Verify file exists
	_, err = os.Stat(filePath)
	assert.NoError(t, err)

	// Load and verify
	q2 := NewEventQueue(cols)
	count, err := q2.LoadFromFile(filePath)
	require.NoError(t, err)
	assert.Equal(t, 3, count)
	assert.Equal(t, 1, q2.BlockCount())
}

func TestEventQueueDrainDocs_UnknownCollectionEntry(t *testing.T) {
	initDocIDPrefix()
	cols := DefaultCollectionConfig()
	q := NewEventQueue(cols)

	// Push entries
	q.Push("Ethereum__Mainnet__Block", "bae-550e8400-e29b-41d4-a716-446655440000")
	q.Push("Ethereum__Mainnet__Transaction", "bae-660e8400-e29b-41d4-a716-446655440001")

	// Manually insert an entry with unknown collection type
	q.mu.Lock()
	q.entries = append(q.entries, eventEntry{Collection: 99, UUID: [uuidSize]byte{1, 2, 3}})
	q.mu.Unlock()

	// DrainDocs should skip the unknown entry
	result := q.DrainDocs(3)
	require.NotNil(t, result)
	// Should only have entries from known collections
	totalDocs := 0
	for _, docs := range result.DocIDsByCollection {
		totalDocs += len(docs)
	}
	assert.Equal(t, 2, totalDocs) // block + tx, unknown skipped
}

func TestEventQueueDrainBlocks_UnknownCollectionEntry(t *testing.T) {
	initDocIDPrefix()
	cols := DefaultCollectionConfig()
	q := NewEventQueue(cols)

	q.Push("Ethereum__Mainnet__Block", "bae-550e8400-e29b-41d4-a716-446655440000")

	// Manually insert an entry with unknown collection type before the block
	q.mu.Lock()
	oldEntries := q.entries
	newEntries := make([]eventEntry, 0)
	newEntries = append(newEntries, eventEntry{Collection: 99, UUID: [uuidSize]byte{1, 2, 3}})
	newEntries = append(newEntries, oldEntries...)
	q.entries = newEntries
	q.mu.Unlock()

	result := q.DrainBlocks(1)
	require.NotNil(t, result)
	assert.Equal(t, 1, result.BlockCount)
}

func TestEventQueueDrainBlocks_RequestMoreThanAvailable(t *testing.T) {
	initDocIDPrefix()
	cols := DefaultCollectionConfig()
	q := NewEventQueue(cols)

	q.Push("Ethereum__Mainnet__Block", "bae-550e8400-e29b-41d4-a716-446655440000")
	q.Push("Ethereum__Mainnet__Transaction", "bae-660e8400-e29b-41d4-a716-446655440001")

	// Try to drain 5 blocks but only 1 exists
	// cutoff never reaches count, blocksSeen < count, cutoff stays 0 => returns nil
	result := q.DrainBlocks(5)
	assert.Nil(t, result)
}

func TestIndexerQueueDrain_WithAllDocTypes(t *testing.T) {
	initDocIDPrefix()
	q := NewIndexerQueue()
	cols := DefaultCollectionConfig()

	// Track block with all doc types
	err := q.TrackBlockDocIDs(1,
		"bae-550e8400-e29b-41d4-a716-446655440000",
		map[string][]string{
			"Ethereum__Mainnet__Transaction":     {"bae-110e8400-e29b-41d4-a716-446655440001"},
			"Ethereum__Mainnet__Log":             {"bae-220e8400-e29b-41d4-a716-446655440002"},
			"Ethereum__Mainnet__AccessListEntry": {"bae-330e8400-e29b-41d4-a716-446655440003"},
		},
		"bae-440e8400-e29b-41d4-a716-446655440004",
	)
	require.NoError(t, err)

	// Track a second block to keep
	err = q.TrackBlockDocIDs(2,
		"bae-aa0e8400-e29b-41d4-a716-446655440005",
		nil, "",
	)
	require.NoError(t, err)

	// Drain keeping 1 (drains block 1 with all its docs)
	result := q.Drain(1, cols)
	require.NotNil(t, result)
	assert.Equal(t, 1, result.BlockCount)

	// Verify all collections are in the result
	assert.Contains(t, result.DocIDsByCollection, "Ethereum__Mainnet__Block")
	assert.Contains(t, result.DocIDsByCollection, "Ethereum__Mainnet__Transaction")
	assert.Contains(t, result.DocIDsByCollection, "Ethereum__Mainnet__Log")
	assert.Contains(t, result.DocIDsByCollection, "Ethereum__Mainnet__AccessListEntry")
	assert.Contains(t, result.DocIDsByCollection, "Ethereum__Mainnet__BlockSignature")
}

func TestIndexerQueueDrainByDocCount_NotEnoughDocs(t *testing.T) {
	initDocIDPrefix()
	q := NewIndexerQueue()
	cols := DefaultCollectionConfig()

	// Track a single block with 1 doc
	err := q.TrackBlockDocIDs(1, "bae-550e8400-e29b-41d4-a716-446655440000", nil, "")
	require.NoError(t, err)

	// Request excess of 100 but only 1 doc exists
	// cutoff stays 0 after loop ends because docsAccumulated (1) < excess (100)
	// Then cutoff = len(q.entries) = 1, drains everything
	result := q.DrainByDocCount(100, cols)
	require.NotNil(t, result)
	assert.Equal(t, 1, result.BlockCount)
	assert.Equal(t, 0, q.Len())
}

func TestIndexerQueueTrackBlockDocIDs_EmptyBlockDocID(t *testing.T) {
	initDocIDPrefix()
	q := NewIndexerQueue()

	// Track with empty block docID - should succeed (blockDocID == "")
	err := q.TrackBlockDocIDs(1, "", nil, "")
	require.NoError(t, err)
	assert.Equal(t, 1, q.Len())
}

func TestEventQueuePush_AllCollectionTypes(t *testing.T) {
	initDocIDPrefix()
	cols := DefaultCollectionConfig()
	q := NewEventQueue(cols)

	q.Push("Ethereum__Mainnet__Block", "bae-110e8400-e29b-41d4-a716-446655440001")
	q.Push("Ethereum__Mainnet__Transaction", "bae-220e8400-e29b-41d4-a716-446655440002")
	q.Push("Ethereum__Mainnet__Log", "bae-330e8400-e29b-41d4-a716-446655440003")
	q.Push("Ethereum__Mainnet__AccessListEntry", "bae-440e8400-e29b-41d4-a716-446655440004")
	q.Push("Ethereum__Mainnet__BlockSignature", "bae-550e8400-e29b-41d4-a716-446655440005")

	assert.Equal(t, 5, q.Len())
	assert.Equal(t, 1, q.BlockCount())

	// Drain all and verify grouping
	result := q.DrainDocs(5)
	require.NotNil(t, result)
	assert.Equal(t, 1, result.BlockCount)
	assert.Len(t, result.DocIDsByCollection, 5)
}

func TestEventQueueLoadFromFile_WithPrefix(t *testing.T) {
	initDocIDPrefix()
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "prefix_test.gob")

	cols := DefaultCollectionConfig()
	q := NewEventQueue(cols)
	q.LoadFromFile(filePath)

	// Push so that docIDPrefix gets set
	q.Push("Ethereum__Mainnet__Block", "bae-550e8400-e29b-41d4-a716-446655440000")

	// Save
	err := q.Save()
	require.NoError(t, err)

	// Reset prefix and load
	initDocIDPrefix()
	q2 := NewEventQueue(cols)
	count, err := q2.LoadFromFile(filePath)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
	// The prefix should have been restored from the snapshot
	assert.Equal(t, "bae", docIDPrefix)
}

func TestIndexerQueueLoadFromFile_WithPrefix(t *testing.T) {
	initDocIDPrefix()
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "prefix_test.gob")

	q := NewIndexerQueue()
	q.LoadFromFile(filePath)

	// Track so that docIDPrefix gets set
	err := q.TrackBlockDocIDs(1, "bae-550e8400-e29b-41d4-a716-446655440000", nil, "")
	require.NoError(t, err)

	// Save
	err = q.Save()
	require.NoError(t, err)

	// Reset prefix and load
	initDocIDPrefix()
	q2 := NewIndexerQueue()
	count, err := q2.LoadFromFile(filePath)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
	assert.Equal(t, "bae", docIDPrefix)
}
