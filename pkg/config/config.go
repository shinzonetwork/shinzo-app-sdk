package config

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
	"gopkg.in/yaml.v3"
)

const CollectionName = "shinzo"

type Config struct {
	DefraDB DefraDBConfig `yaml:"defradb"`
	Shinzo  ShinzoConfig  `yaml:"shinzo"`
	Logger  LoggerConfig  `yaml:"logger"`
}

type DefraDBConfig struct {
	Url           string             `yaml:"url"`
	KeyringSecret string             `yaml:"keyring_secret"`
	P2P           DefraP2PConfig     `yaml:"p2p"`
	Store         DefraStoreConfig   `yaml:"store"`
	Optimization  OptimizationConfig `yaml:"optimization"`
}

type OptimizationConfig struct {
	EnableEventManager   bool `yaml:"enable_event_manager"`
	EnableConnectionPool bool `yaml:"enable_connection_pool"`
	MaxConnections       int  `yaml:"max_connections"`
	DefaultBufferSize    int  `yaml:"default_buffer_size"`
	EnableBackpressure   bool `yaml:"enable_backpressure"`
	MemoryThresholdMB    int  `yaml:"memory_threshold_mb"`
}

type DefraP2PConfig struct {
	Enabled             bool     `yaml:"enabled"` // Toggle P2P networking on/off
	BootstrapPeers      []string `yaml:"bootstrap_peers"`
	ListenAddr          string   `yaml:"listen_addr"`
	MaxRetries          int      `yaml:"max_retries"`
	RetryBaseDelayMs    int      `yaml:"retry_base_delay_ms"`
	ReconnectIntervalMs int      `yaml:"reconnect_interval_ms"`
	EnableAutoReconnect bool     `yaml:"enable_auto_reconnect"`
}

type DefraStoreConfig struct {
	Path string `yaml:"path"`
}

type ShinzoConfig struct {
	MinimumAttestations string `yaml:"minimum_attestations"`
}

type LoggerConfig struct {
	Development bool `yaml:"development"`
}

// LoadConfig loads configuration from a YAML file and environment variables
func LoadConfig(path string) (*Config, error) {
	// Load .env file if it exists
	_ = godotenv.Load()

	// Load YAML config
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Override with environment variables
	if keyringSecret := os.Getenv("DEFRA_KEYRING_SECRET"); keyringSecret != "" {
		cfg.DefraDB.KeyringSecret = keyringSecret
	}

	if url := os.Getenv("DEFRA_URL"); url != "" {
		cfg.DefraDB.Url = url
	}

	return &cfg, nil
}
