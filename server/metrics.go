package server

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
)

// Metrics for attributing the Knative cold-start tail. The hypotheses each one
// tests:
//
//   - rpcDuration            — is any single RPC class slow? (write path)
//   - watchDelivery          — how long from event-write to client-delivery? (watch lag)
//   - watchSendMutexWait     — are watches blocking each other on the shared
//                              per-stream sender? (head-of-line blocking)
//   - watchSendWrite         — is the gRPC write itself slow? (flow control)
//   - activeWatches          — how many watches are live? (pool pressure context)
//   - watchCatchupEvents     — how many events does a watch scan before reaching
//                              its start revision? (the O(N) lastID="0" scan)
//   - redisPool*             — is the go-redis connection pool exhausted?
var (
	rpcDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "retcd_rpc_duration_seconds",
		Help:    "Duration of unary gRPC RPCs by method.",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 18), // 100µs … ~13s
	}, []string{"method"})

	watchDelivery = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "retcd_watch_delivery_seconds",
		Help:    "Latency from event stream-entry timestamp to delivery to a watch client.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 18), // 0.5ms … ~65s
	})

	watchSendMutexWait = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "retcd_watch_send_mutex_wait_seconds",
		Help:    "Time spent waiting for the per-stream sender mutex (head-of-line blocking signal).",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 18),
	})

	watchSendWrite = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "retcd_watch_send_write_seconds",
		Help:    "Time spent inside the gRPC stream Send call.",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 18),
	})

	activeWatches = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "retcd_active_watches",
		Help: "Number of active watch goroutines.",
	})

	watchReadErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "retcd_watch_read_errors_total",
		Help: "Transient BlockReadEvents errors that were retried (connection blips / pool timeouts).",
	})

	watchCancels = promauto.NewCounter(prometheus.CounterOpts{
		Name: "retcd_watch_cancels_total",
		Help: "Watches cancelled-and-notified to the client due to persistent backend failure (instead of dying silently).",
	})

	watchCatchupEvents = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "retcd_watch_catchup_events",
		Help:    "Stream events scanned by a watch before its first delivery (O(N) start-revision scan).",
		Buckets: prometheus.ExponentialBuckets(1, 2, 20), // 1 … ~1M
	})

	// watchCreates attributes watch-establishment churn: which resource prefix
	// (e.g. /registry/pods) and whether it asked to catch up from a historical
	// revision ("catchup") or start from now ("fromnow"). High catchup churn on a
	// resource = reflectors re-listing/re-watching that resource against REtcd.
	watchCreates = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "retcd_watch_creates_total",
		Help: "Watch CreateRequests by resource prefix and mode (catchup|fromnow).",
	}, []string{"prefix", "mode"})

	redisPoolTotalConns = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "retcd_redis_pool_total_conns",
		Help: "go-redis pool: total connections.",
	})
	redisPoolIdleConns = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "retcd_redis_pool_idle_conns",
		Help: "go-redis pool: idle connections.",
	})
	redisPoolTimeouts = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "retcd_redis_pool_timeouts_total",
		Help: "go-redis pool: cumulative wait-timeouts (connection exhaustion signal).",
	})
	redisPoolMisses = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "retcd_redis_pool_misses_total",
		Help: "go-redis pool: cumulative pool misses (a new connection had to be opened).",
	})
)

// UnaryMetricsInterceptor records per-method RPC latency. It only observes a
// histogram — no logging — so it is cheap on the hot path.
func UnaryMetricsInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	t0 := time.Now()
	resp, err := handler(ctx, req)
	rpcDuration.WithLabelValues(info.FullMethod).Observe(time.Since(t0).Seconds())
	return resp, err
}

// RecordRedisPool publishes a snapshot of the go-redis connection pool. main
// calls this periodically with the values from RedisStore.PoolStats.
func RecordRedisPool(totalConns, idleConns, timeouts, misses uint32) {
	redisPoolTotalConns.Set(float64(totalConns))
	redisPoolIdleConns.Set(float64(idleConns))
	redisPoolTimeouts.Set(float64(timeouts))
	redisPoolMisses.Set(float64(misses))
}
