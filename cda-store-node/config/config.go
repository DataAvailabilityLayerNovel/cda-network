package config

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Port          int
	RowIdx        int
	ColIdx        int
	PublisherAddr string
	BootstrapAddr string
	SeedAddr      string
	K             int
	KPiece        int
	NumCols       int
	StoresPerCol  int
	MyAddr        string
	CrashOnFail   bool
	PruneEnable   bool
	PruneTTL      time.Duration
}

func envIntOrDefault(key string, def int) int {
	if val := strings.TrimSpace(os.Getenv(key)); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func LoadConfig() (*Config, error) {
	port := flag.Int("port", 8082, "Store node listening port")
	rowIdx := flag.Int("row", -1, "Store node custody row coordinate (-1 for auto-derived from keypair)")
	colIdx := flag.Int("col", -1, "Store node custody column coordinate (-1 for auto-derived from keypair)")
	pubAddr := flag.String("publisher", "http://localhost:8080", "Publisher node URL")
	bootAddr := flag.String("bootstrap", "http://localhost:8081", "Bootstrap node URL (or seed node)")
	seedAddr := flag.String("seed", "", "Seed Bootstrap node URL for dynamic matrix discovery")
	k := flag.Int("k", 8, "ODS dimension parameter k")
	kPiece := flag.Int("k-piece", 0, "RLNC piece parameter k-piece")
	numCols := flag.Int("num-cols", envIntOrDefault("CDA_NET_COLS", 8), "Number of network column groups")
	netCols := flag.Int("net-cols", 0, "Alias for -num-cols: number of network matrix columns")
	storesPerCol := flag.Int("stores-per-col", envIntOrDefault("CDA_NET_ROWS", 8), "Number of network matrix rows per column")
	netRows := flag.Int("net-rows", 0, "Alias for -stores-per-col: number of network matrix rows")
	myAddrFlag := flag.String("myaddr", "", "My own accessible address URL (e.g. http://localhost:8082 or http://store-1:8080)")
	crashOnFail := flag.Bool("crash-on-fail", false, "Crash the node if verification fails")
	pruneEnable := flag.Bool("prune-enable", false, "Enable pruning of non-custody raw pieces")
	pruneTTLStr := flag.String("prune-ttl", "5m", "TTL duration before pruning non-custody cells")

	flag.Parse()

	myAddr := *myAddrFlag
	if myAddr == "" {
		myAddr = fmt.Sprintf("http://localhost:%d", *port)
	}

	effectiveSeed := *seedAddr
	if effectiveSeed == "" {
		effectiveSeed = *bootAddr
	}

	kPieceVal := *kPiece
	if kPieceVal == 0 {
		kPieceVal = *k
	}

	effectiveNumCols := *numCols
	if *netCols > 0 {
		effectiveNumCols = *netCols
	}
	if effectiveNumCols <= 0 {
		effectiveNumCols = 8
	}

	effectiveStoresPerCol := *storesPerCol
	if *netRows > 0 {
		effectiveStoresPerCol = *netRows
	}
	if effectiveStoresPerCol <= 0 {
		effectiveStoresPerCol = 8
	}

	pruneTTL, err := time.ParseDuration(*pruneTTLStr)
	if err != nil {
		return nil, fmt.Errorf("invalid prune-ttl: %w", err)
	}

	return &Config{
		Port:          *port,
		RowIdx:        *rowIdx,
		ColIdx:        *colIdx,
		PublisherAddr: *pubAddr,
		BootstrapAddr: *bootAddr,
		SeedAddr:      effectiveSeed,
		K:             *k,
		KPiece:        kPieceVal,
		NumCols:       effectiveNumCols,
		StoresPerCol:  effectiveStoresPerCol,
		MyAddr:        myAddr,
		CrashOnFail:   *crashOnFail,
		PruneEnable:   *pruneEnable,
		PruneTTL:      pruneTTL,
	}, nil
}
