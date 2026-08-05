package p2p

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	KZGProofDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "cda_bootstrap_kzg_proof_duration_seconds",
		Help:    "Duration of KZG opening proof generation on bootstrap in seconds.",
		Buckets: prometheus.DefBuckets,
	})
)

func init() {
	prometheus.MustRegister(KZGProofDuration)
}
