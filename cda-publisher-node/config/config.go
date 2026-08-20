package config

import (
	"encoding/json"
	"os"
)

type Config struct {
	APIPort            int            `json:"api_port"`
	K                  int            `json:"k"`
	KPiece             int            `json:"k_piece"`
	ActiveCols         int            `json:"active_cols,omitempty"`
	BootstrapPeers     map[int]string `json:"bootstrap_peers"` // maps colIdx to bootstrap node address (HTTP/P2P address)
	SequencerPublicKey string         `json:"sequencer_public_key"`
}

func DefaultConfig() *Config {
	return &Config{
		APIPort: 8080,
		K:       4,
		KPiece:  4,
		BootstrapPeers: map[int]string{
			0: "http://localhost:8081", // Default address for column 0 (for other columns, fallback or config map)
		},
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
