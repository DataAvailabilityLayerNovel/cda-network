package service

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	RSEncodeDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "cda_publisher_rs_encode_duration_seconds",
		Help:    "Duration of 2D Reed-Solomon encoding in publisher in seconds.",
		Buckets: prometheus.DefBuckets,
	})

	ThroughputBytesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cda_publisher_throughput_bytes_total",
		Help: "Total bytes published by the publisher.",
	})

	ThroughputBytesRate = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cda_publisher_throughput_bytes_per_second",
		Help: "Instantaneous throughput of the latest publish in bytes/second.",
	})

	LatestPublishedHeight = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cda_publisher_latest_published_height",
		Help: "Block height of the latest published/consensus block admitted to the CDA publisher pipeline.",
	})

	CometBFTConsensusHeight = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cometbft_consensus_height",
		Help: "Latest consensus block height observed by the CDA publisher or CometBFT cluster.",
	})

	LatestCompletedHeight = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cda_publisher_latest_completed_height",
		Help: "Block height of the latest completed block on CDA network.",
	})

	LatestK = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cda_publisher_latest_k",
		Help: "Dimension K of the latest completed block (ODS matrix size KxK).",
	})

	InFlightBlocks = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cda_publisher_in_flight_blocks",
		Help: "Number of blocks currently in-flight in the publisher pipeline.",
	})

	BlockDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "cda_publisher_block_duration_seconds",
		Help:    "End-to-end duration from block ingestion to BlockReady in seconds.",
		Buckets: []float64{0.5, 1.0, 2.0, 5.0, 10.0, 15.0, 30.0, 60.0},
	})

	BlocksCompletedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cda_publisher_blocks_completed_total",
		Help: "Total completed blocks on CDA network grouped by dynamic dimension K.",
	}, []string{"k"})
)

func init() {
	prometheus.MustRegister(
		RSEncodeDuration,
		ThroughputBytesTotal,
		ThroughputBytesRate,
		LatestPublishedHeight,
		CometBFTConsensusHeight,
		LatestCompletedHeight,
		LatestK,
		InFlightBlocks,
		BlockDuration,
		BlocksCompletedTotal,
	)
}
