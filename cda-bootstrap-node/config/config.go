package config

import (
	"encoding/json"
	"os"
)

type Config struct {
	APIPort       int    `json:"api_port"`
	ColumnID      int    `json:"column_id"`
	StoreNodeAddr string `json:"store_node_addr"`
	PublisherAddr string `json:"publisher_addr"`
	K             int    `json:"k"`
	KPiece        int    `json:"k_piece"`
	PruneEnable   bool   `json:"prune_enable"`
	PruneTTL      string `json:"prune_ttl"`
}

func DefaultConfig() *Config {
	return &Config{
		APIPort:       8081,
		ColumnID:      0,
		StoreNodeAddr: "http://localhost:8082",
		PublisherAddr: "http://localhost:8080",
		K:             4,
		KPiece:        4,
		PruneEnable:   false,
		PruneTTL:      "5m",
	}
}

func LoadConfig(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	cfg := DefaultConfig()
	if err := json.NewDecoder(file).Decode(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
