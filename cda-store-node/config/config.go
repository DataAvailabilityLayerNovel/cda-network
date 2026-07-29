package config

import (
	"flag"
	"fmt"
	"strings"
)

type Config struct {
	Port          int
	RowIdx        int
	ColIdx        int
	PublisherAddr string
	BootstrapAddr string
	K             int
	Peers         []string
	MyAddr        string
	CrashOnFail   bool
}

func LoadConfig() (*Config, error) {
	port := flag.Int("port", 8082, "Store node listening port")
	rowIdx := flag.Int("row", 0, "Store node custody row coordinate")
	colIdx := flag.Int("col", 0, "Store node custody column coordinate")
	pubAddr := flag.String("publisher", "http://localhost:8080", "Publisher node URL")
	bootAddr := flag.String("bootstrap", "http://localhost:8081", "Bootstrap node URL")
	k := flag.Int("k", 8, "ODS dimension parameter k")
	peersStr := flag.String("peers", "", "Comma-separated list of peer Store Node URLs")
	myAddrFlag := flag.String("myaddr", "", "My own accessible address URL (e.g. http://localhost:8082 or http://store-1:8080)")
	crashOnFail := flag.Bool("crash-on-fail", false, "Crash the node if verification fails")

	flag.Parse()

	if *rowIdx < 0 || *colIdx < 0 {
		return nil, fmt.Errorf("coordinates row and col must be non-negative")
	}

	var peers []string
	if *peersStr != "" {
		parts := strings.Split(*peersStr, ",")
		for _, p := range parts {
			trimmed := strings.TrimSpace(p)
			if trimmed != "" {
				peers = append(peers, trimmed)
			}
		}
	}

	myAddr := *myAddrFlag
	if myAddr == "" {
		myAddr = fmt.Sprintf("http://localhost:%d", *port)
	}

	return &Config{
		Port:          *port,
		RowIdx:        *rowIdx,
		ColIdx:        *colIdx,
		PublisherAddr: *pubAddr,
		BootstrapAddr: *bootAddr,
		K:             *k,
		Peers:         peers,
		MyAddr:        myAddr,
		CrashOnFail:   *crashOnFail,
	}, nil
}
