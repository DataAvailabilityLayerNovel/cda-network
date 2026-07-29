package config

import (
	"flag"
	"strconv"
	"strings"
)

type Config struct {
	Port          int
	PublisherAddr string
	BootstrapsMap map[int]string
	K             int
	CrashOnFail   bool
}

func LoadConfig() *Config {
	port := flag.Int("port", 8085, "API port for light node")
	publisher := flag.String("publisher", "http://localhost:8080", "Publisher URL")
	bootstrapsStr := flag.String("bootstraps", "0:http://localhost:8090;1:http://localhost:8091;2:http://localhost:8092;3:http://localhost:8093", "Semicolon-separated mapping of networkColumnIdx:bootstrapURL")
	k := flag.Int("k", 4, "Number of chunks K")
	crashOnFail := flag.Bool("crash-on-fail", false, "Crash the node if verification fails")
	flag.Parse()

	bootstrapsMap := make(map[int]string)
	if *bootstrapsStr != "" {
		for _, colGroup := range strings.Split(*bootstrapsStr, ";") {
			parts := strings.SplitN(colGroup, ":", 2)
			if len(parts) == 2 {
				netColIdx, err := strconv.Atoi(parts[0])
				if err != nil {
					continue
				}
				bootstrapsMap[netColIdx] = strings.TrimSpace(parts[1])
			}
		}
	}

	return &Config{
		Port:          *port,
		PublisherAddr: *publisher,
		BootstrapsMap: bootstrapsMap,
		K:             *k,
		CrashOnFail:   *crashOnFail,
	}
}
