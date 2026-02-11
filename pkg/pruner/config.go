package pruner

// Config represents pruner configuration for removing old documents.
type Config struct {
	Enabled         bool  `yaml:"enabled"`
	MaxBlocks       int64 `yaml:"max_blocks"`
	PruneThreshold  int64 `yaml:"prune_threshold"`
	IntervalSeconds int   `yaml:"interval_seconds"`
	PruneHistory    bool  `yaml:"prune_history"`
}

// CollectionConfig defines which collections to prune and how.
type CollectionConfig struct {
	// BlockCollection is the name of the block collection (e.g. "Ethereum__Mainnet__Block").
	BlockCollection string
	// BlockNumberField is the field name for block number in the block collection (e.g. "number").
	BlockNumberField string
	// DependentCollections are collections that reference blocks via "blockNumber" field,
	// listed in deletion order (deleted before the block collection).
	DependentCollections []string
}

// DefaultCollectionConfig returns the default Ethereum mainnet collection config.
func DefaultCollectionConfig() CollectionConfig {
	return CollectionConfig{
		BlockCollection:  "Ethereum__Mainnet__Block",
		BlockNumberField: "number",
		DependentCollections: []string{
			"Ethereum__Mainnet__BatchSignature",
			"Ethereum__Mainnet__AccessListEntry",
			"Ethereum__Mainnet__Log",
			"Ethereum__Mainnet__Transaction",
		},
	}
}

// SetDefaults fills in zero-value fields with sensible defaults.
func (c *Config) SetDefaults() {
	if c.MaxBlocks <= 0 {
		c.MaxBlocks = 10000
	}
	if c.PruneThreshold <= 0 {
		c.PruneThreshold = 100
	}
	if c.IntervalSeconds <= 0 {
		c.IntervalSeconds = 60
	}
}
