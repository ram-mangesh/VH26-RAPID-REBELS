package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"go-producer/internal/clickhouse"
	"go-producer/internal/generator"
	kfk "go-producer/internal/kafka"
	"go-producer/internal/metrics"
	"go-producer/internal/models"
)

type Handler struct {
	producer   *kfk.Producer
	ch         *clickhouse.Client
	topicEvents string
	topicDLQ    string
	gen        *generator.Generator
}

func New(producer *kfk.Producer, gen *generator.Generator) *Handler {
	return &Handler{
		producer:    producer,
		ch:          clickhouse.NewClient(),
		topicEvents: getEnv("KAFKA_TOPIC_EVENTS", "events"),
		topicDLQ:    getEnv("KAFKA_TOPIC_DLQ", "events-dlq"),
		gen:         gen,
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func (h *Handler) CreateEvent(c *gin.Context) {
	var event models.Event
	if err := c.ShouldBindJSON(&event); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if event.EventID == "" {
		event.EventID = "evt_" + uuid.New().String()[:8]
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}
	if event.Type == "" {
		event.Type = models.EventOrder
	}

	payload, err := json.Marshal(event)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "serialization failed"})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	topic := h.topicEvents
	if err := h.producer.Publish(ctx, topic, event.EventID, payload); err != nil {
		metrics.OrdersFailedTotal.Inc()
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to publish event"})
		return
	}

	metrics.OrdersPublishedTotal.Inc()
	c.JSON(http.StatusCreated, event)
}

func (h *Handler) GetStats(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":    "running",
		"timestamp": time.Now().UTC(),
	})
}

func (h *Handler) HealthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":    "healthy",
		"timestamp": time.Now().UTC(),
	})
}

func (h *Handler) GetOrdersPerMinute(c *gin.Context) {
	rows, err := h.ch.Query(`
		SELECT
			bucket AS minute,
			order_count,
			total_revenue,
			failed_count
		FROM ecommerce.orders_per_15s
		WHERE bucket >= now() - INTERVAL 30 MINUTE
		ORDER BY bucket ASC
	`)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if rows == nil {
		rows = []map[string]interface{}{}
	}
	c.JSON(http.StatusOK, rows)
}

func (h *Handler) GetRevenueByRegion(c *gin.Context) {
	rows, err := h.ch.Query(`
		SELECT
			region,
			sum(total_revenue) AS revenue,
			sum(order_count)   AS orders
		FROM ecommerce.revenue_by_region
		WHERE window_start >= now() - INTERVAL 1 HOUR
		GROUP BY region
		ORDER BY revenue DESC
	`)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if rows == nil {
		rows = []map[string]interface{}{}
	}
	c.JSON(http.StatusOK, rows)
}

func (h *Handler) GetTopProducts(c *gin.Context) {
	rows, err := h.ch.Query(`
		SELECT
			product,
			category,
			sum(quantity_sold) AS quantity,
			sum(total_revenue) AS revenue
		FROM ecommerce.top_products
		WHERE window_start >= now() - INTERVAL 1 HOUR
		GROUP BY product, category
		ORDER BY revenue DESC
		LIMIT 10
	`)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if rows == nil {
		rows = []map[string]interface{}{}
	}
	c.JSON(http.StatusOK, rows)
}

func (h *Handler) GetErrorRate(c *gin.Context) {
	rows, err := h.ch.Query(`
		SELECT
			count(*)                                        AS total,
			countIf(status = 'failed')                      AS failed,
			round(countIf(status = 'failed') * 100.0 / count(*), 2) AS error_rate
		FROM ecommerce.orders
		WHERE timestamp >= now() - INTERVAL 5 MINUTE
	`)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if len(rows) == 0 {
		c.JSON(http.StatusOK, gin.H{"total": 0, "failed": 0, "error_rate": 0})
		return
	}
	c.JSON(http.StatusOK, rows[0])
}

func (h *Handler) GetGeneratorRate(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"events_per_minute": h.gen.GetRate(),
		"events_per_second": h.gen.GetRate() / 60,
		"current_eps":      metrics.GetEventsPerSecond(),
	})
}

func (h *Handler) GetTotalEvents(c *gin.Context) {
	rows, err := h.ch.Query(`SELECT total_count FROM ecommerce.total_events ORDER BY timestamp DESC LIMIT 1`)
	if err != nil || len(rows) == 0 {
		c.JSON(http.StatusOK, gin.H{"total_events": 0})
		return
	}
	c.JSON(http.StatusOK, gin.H{"total_events": int(rows[0]["total_count"].(int64))})
}

func (h *Handler) GetRealtimeRate(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"events_per_minute": metrics.GetEventsPerMinute(),
		"events_per_second": metrics.GetEventsPerSecond(),
		"generator_rate":    h.gen.GetRate(),
	})
}

func (h *Handler) SetGeneratorRate(c *gin.Context) {
	var req struct {
		EventsPerMinute int `json:"events_per_minute" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	h.gen.SetRate(req.EventsPerMinute)
	c.JSON(http.StatusOK, gin.H{
		"events_per_minute": h.gen.GetRate(),
		"events_per_second": h.gen.GetRate() / 60,
		"message":           "Rate updated successfully",
	})
}

func (h *Handler) ClearPipeline(c *gin.Context) {
	// Truncate ClickHouse tables
	tables := []string{
		"ecommerce.orders_per_minute",
		"ecommerce.orders",
		"ecommerce.revenue_by_region",
		"ecommerce.top_products",
	}
	for _, table := range tables {
		h.ch.Execute(`TRUNCATE TABLE IF EXISTS ` + table)
	}

	

	// Reset metrics
	metrics.OrdersPublishedTotal.Add(0)
	metrics.OrdersDLQTotal.Add(0)
	metrics.OrdersFailedTotal.Add(0)
	metrics.OrdersRevenue.Add(0)
	atomic.StoreInt64(&metrics.EventsSinceLast, 0)
	atomic.StoreInt64(&metrics.EventsSinceLast, 0)

	// Reset generator counter
	h.gen.ResetCounter()

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "Pipeline cleared successfully",
	})
}
