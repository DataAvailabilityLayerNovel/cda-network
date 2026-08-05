package service

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	DASSampleLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "cda_light_das_sample_latency_seconds",
		Help:    "Duration of a DAS sample query in seconds.",
		Buckets: prometheus.DefBuckets,
	})

	DASAttemptsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cda_light_das_attempts_total",
		Help: "Total DAS sample attempts.",
	})

	DASSuccessTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cda_light_das_success_total",
		Help: "Total successful DAS samples.",
	})

	DASSuccessRateGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cda_light_das_success_rate",
		Help: "Running success rate of DAS queries as a percentage.",
	})

	DialAttemptsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cda_p2p_dial_attempts_total",
		Help: "Total P2P connection dial attempts.",
	})

	DialTimeoutsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cda_p2p_dial_timeouts_total",
		Help: "Total P2P connection dial timeouts/failures.",
	})

	DialTimeoutRateGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cda_p2p_dial_timeout_rate",
		Help: "Running dial timeout/failure rate as a percentage.",
	})

	ByzantineDetectionsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cda_store_byzantine_detection_count",
		Help: "Total number of Byzantine/forged pieces detected and blocked by the light node.",
	})
)

var (
	dasAttempts int64
	dasSuccess  int64
	dasMu       sync.Mutex

	dialAttempts int64
	dialTimeouts int64
	dialMu       sync.Mutex
)

func init() {
	prometheus.MustRegister(
		DASSampleLatency,
		DASAttemptsTotal,
		DASSuccessTotal,
		DASSuccessRateGauge,
		DialAttemptsTotal,
		DialTimeoutsTotal,
		DialTimeoutRateGauge,
		ByzantineDetectionsTotal,
	)
}

func recordDASAttempt(success bool) {
	dasMu.Lock()
	dasAttempts++
	if success {
		dasSuccess++
	}
	rate := float64(dasSuccess) / float64(dasAttempts) * 100.0
	dasMu.Unlock()

	DASAttemptsTotal.Inc()
	if success {
		DASSuccessTotal.Inc()
	}
	DASSuccessRateGauge.Set(rate)
}

func recordDialAttempt(success bool) {
	dialMu.Lock()
	dialAttempts++
	if !success {
		dialTimeouts++
	}
	rate := float64(dialTimeouts) / float64(dialAttempts) * 100.0
	dialMu.Unlock()

	DialAttemptsTotal.Inc()
	if !success {
		DialTimeoutsTotal.Inc()
	}
	DialTimeoutRateGauge.Set(rate)
}
