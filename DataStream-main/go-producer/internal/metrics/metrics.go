package metrics

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	OrdersPublishedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "orders_published_total",
		Help: "Total number of orders successfully published to Kafka",
	})

	OrdersFailedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "orders_failed_total",
		Help: "Total number of orders that failed to publish",
	})

	OrdersDLQTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "orders_dlq_total",
		Help: "Total number of orders sent to dead letter queue",
	})

	OrdersRevenue = promauto.NewCounter(prometheus.CounterOpts{
		Name: "orders_revenue_total",
		Help: "Cumulative revenue from all published orders",
	})
)

var (
	EventsSinceLast int64
	eventsPerSecond int64
)

func ResetSecondCounter() {
	eps := atomic.SwapInt64(&EventsSinceLast, 0)
	atomic.StoreInt64(&eventsPerSecond, eps)
}

func GetEventsPerSecond() int64 {
	return atomic.LoadInt64(&eventsPerSecond)
}
