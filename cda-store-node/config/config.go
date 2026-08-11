package config

import (
	"flag"
	"fmt"
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

func LoadConfig() (*Config, error) {
	port := flag.Int("port", 8082, "Store node listening port")
	rowIdx := flag.Int("row", -1, "Store node custody row coordinate (-1 for auto-derived from keypair)")
	colIdx := flag.Int("col", -1, "Store node custody column coordinate (-1 for auto-derived from keypair)")
	pubAddr := flag.String("publisher", "http://localhost:8080", "Publisher node URL")
	bootAddr := flag.String("bootstrap", "http://localhost:8081", "Bootstrap node URL (or seed node)")
	seedAddr := flag.String("seed", "", "Seed Bootstrap node URL for dynamic matrix discovery")
	k := flag.Int("k", 8, "ODS dimension parameter k")
	kPiece := flag.Int("k-piece", 0, "RLNC piece parameter k-piece")
	numCols := flag.Int("num-cols", 8, "Number of network column groups")
	storesPerCol := flag.Int("stores-per-col", 8, "Number of store nodes per column")
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
		NumCols:       *numCols,
		StoresPerCol:  *storesPerCol,
		MyAddr:        myAddr,
		CrashOnFail:   *crashOnFail,
		PruneEnable:   *pruneEnable,
		PruneTTL:      pruneTTL,
	}, nil
}
