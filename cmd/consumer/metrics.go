package main

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	e2eLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "e2e_latency_seconds",
		Help:    "Time from produced_at until Braze accepted the event",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 12),
	})
	eventsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "events_total",
		Help: "Events handled by the consumer",
	}, []string{"result"})
	brazeRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "braze_requests_total",
		Help: "Requests sent to Braze by response status",
	}, []string{"status"})
	consumerLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "consumer_lag_ms",
		Help: "How far behind the tip of the stream the consumer is",
	})
	inflightBatches = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "inflight_batches",
		Help: "Batches currently being sent to Braze",
	})
	suppressedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "suppressed_total",
		Help: "Events dropped before reaching Braze by privacy reason",
	}, []string{"reason"})
)

func startMetricsServer(addr string) {
	http.Handle("/metrics", promhttp.Handler())
	if err := http.ListenAndServe(addr, nil); err != nil {
		fmt.Printf("Metrics server stopped. Here's why %v\n", err)
	}
}

func recordBrazeRequest(status int, err error) {
	label := strconv.Itoa(status)
	if err != nil {
		label = "error"
	}
	brazeRequestsTotal.WithLabelValues(label).Inc()
}

func recordSent(data []UserEvent) {
	eventsTotal.WithLabelValues("sent").Add(float64(len(data)))
	for _, event := range data {
		e2eLatency.Observe(time.Since(time.UnixMilli(event.ProducedAt)).Seconds())
	}
}

func recordFailed(data []UserEvent) {
	eventsTotal.WithLabelValues("failed").Add(float64(len(data)))
}

func recordSuppressed(reason string) {
	suppressedTotal.WithLabelValues(reason).Inc()
}
