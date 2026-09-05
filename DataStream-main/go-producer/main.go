package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"go-producer/internal/generator"
	"go-producer/internal/handlers"
	kfk "go-producer/internal/kafka"
)

func main() {
	brokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")

	// Ensure Kafka topics exist
	topics := []string{
		getEnv("KAFKA_TOPIC_EVENTS", "events"),
		getEnv("KAFKA_TOPIC_DLQ", "events-dlq"),
	}
	for retry := 0; retry < 10; retry++ {
		if err := kfk.EnsureTopics(brokers, topics); err != nil {
			log.Printf("Waiting for Kafka (%d/10): %v", retry+1, err)
			time.Sleep(3 * time.Second)
			continue
		}
		log.Println("Kafka topics ready")
		break
	}

	producer, err := kfk.NewProducer(brokers)
	if err != nil {
		log.Fatalf("create kafka producer: %v", err)
	}
	defer producer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gen := generator.New(producer)
	go gen.Start(ctx)

	r := gin.Default()

	// CORS for local frontend development
	r.Use(func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	})

	h := handlers.New(producer, gen)

	api := r.Group("/api")
	{
		api.POST("/events", h.CreateEvent)
		api.GET("/events/stats", h.GetStats)
		api.GET("/health", h.HealthCheck)

		api.GET("/generator/rate", h.GetGeneratorRate)
		api.POST("/generator/rate", h.SetGeneratorRate)

		api.GET("/total-events", h.GetTotalEvents)

		api.POST("/pipeline/clear", h.ClearPipeline)

		metrics := api.Group("/metrics")
		{
			metrics.GET("/events-per-minute", h.GetOrdersPerMinute)
			metrics.GET("/revenue-by-region", h.GetRevenueByRegion)
			metrics.GET("/top-products", h.GetTopProducts)
			metrics.GET("/error-rate", h.GetErrorRate)
			metrics.GET("/realtime-rate", h.GetRealtimeRate)
		}
	}

	r.GET("/metrics", gin.WrapH(promhttp.Handler()))
	r.GET("/health", h.HealthCheck)

	srv := &http.Server{
		Addr:    ":8080",
		Handler: r,
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Println("Go producer API listening on :8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server: %v", err)
		}
	}()

	<-quit
	log.Println("Shutting down...")
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = srv.Shutdown(shutdownCtx)
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
