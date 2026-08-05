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
)

func init() {
	prometheus.MustRegister(
		RSEncodeDuration,
		ThroughputBytesTotal,
		ThroughputBytesRate,
	)
}
