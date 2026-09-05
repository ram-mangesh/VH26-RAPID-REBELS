package generator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go-producer/internal/kafka"
	"go-producer/internal/metrics"
	"go-producer/internal/models"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/google/uuid"
)

type Generator struct {
	producer        *kafka.Producer
	faker           *gofakeit.Faker
	eventsPerMinute int
	topicEvents     string
	topicDLQ        string
	wg              sync.WaitGroup
	eventCount      int64
	rateMu          sync.RWMutex
}

func New(producer *kafka.Producer) *Generator {
	rate := 60000
	if v := os.Getenv("EVENTS_PER_MINUTE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			rate = n
		}
	}
	return &Generator{
		producer:        producer,
		faker:           gofakeit.New(0),
		eventsPerMinute: rate,
		topicEvents:     getEnv("KAFKA_TOPIC_EVENTS", "events"),
		topicDLQ:        getEnv("KAFKA_TOPIC_DLQ", "events-dlq"),
	}
}

func (g *Generator) Start(ctx context.Context) {
	batchSize := 100
	if v := os.Getenv("BATCH_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			batchSize = n
		}
	}

	workers := 8
	if v := os.Getenv("WORKERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			workers = n
		}
	}

	eventsPerSec := g.eventsPerMinute / 60
	if eventsPerSec < 1 {
		eventsPerSec = 1
	}

	log.Printf("Event simulator started: %d events/min (%d events/sec), batch=%d, workers=%d",
		g.eventsPerMinute, eventsPerSec, batchSize, workers)

  eventChan := make(chan models.Event, 10000)

	for i := 0; i < workers; i++ {
		g.wg.Add(1)
		go g.worker(ctx, eventChan, i)
	}

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				currentRate := g.GetRate()
				remaining := currentRate / 60
				for remaining > 0 {
					toSend := batchSize
					if remaining < batchSize {
						toSend = remaining
					}
					select {
					case <-ctx.Done():
						return
					default:
					}
					for i := 0; i < toSend; i++ {
						event := g.generateEvent()
						select {
						case eventChan <- event:
						case <-ctx.Done():
							return
						}
					}
					remaining -= toSend
				}
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				count := atomic.LoadInt64(&g.eventCount)
				eps := metrics.GetEventsPerSecond()
				log.Printf("Events published so far: %d, current_rate: %d eps", count, eps)
				metrics.ResetSecondCounter()
			}
		}
	}()

	<-ctx.Done()
	log.Println("Event simulator stopping...")
	close(eventChan)
	g.wg.Wait()
	log.Printf("Event simulator stopped. Total events published: %d", atomic.LoadInt64(&g.eventCount))
}

func (g *Generator) worker(ctx context.Context, eventChan <-chan models.Event, id int) {
	defer g.wg.Done()
	for event := range eventChan {
		g.publishEvent(ctx, event)
	}
}

func (g *Generator) publishEvent(ctx context.Context, event models.Event) {
	payload, err := json.Marshal(event)
	if err != nil {
		log.Printf("marshal event: %v", err)
		return
	}

	topic := g.topicEvents
	metrics.OrdersPublishedTotal.Inc()

	g.producer.PublishAsync(topic, event.EventID, payload)

	atomic.AddInt64(&g.eventCount, 1)
	atomic.AddInt64(&metrics.EventsSinceLast, 1)
}

func (g *Generator) generateEvent() models.Event {
	eventType := g.randomEventType()

	var payload interface{}

	switch eventType {
	case models.EventPayment:
		payload = g.generatePayment()
	case models.EventOrder:
		payload = g.generateOrder()
	case models.EventClick:
		payload = g.generateClick()
	}

	return models.Event{
		EventID:   "evt_" + uuid.New().String()[:8],
		Type:      eventType,
		Timestamp: time.Now(),
		Payload:   payload,
	}
}

func (g *Generator) randomEventType() models.EventType {
	r := rand.Float64()
	if r < 0.40 {
		return models.EventPayment
	} else if r < 0.75 {
		return models.EventOrder
	}
	return models.EventClick
}

func (g *Generator) generatePayment() models.PaymentPayload {
	customerID := uuid.New().String()[:8]
	method := g.randomPaymentMethod()
	region := randomElement(models.Regions)
	amount := roundFloat(10+rand.Float64()*9990, 2)

	status := models.PaymentSuccess
	r := rand.Float64()
	switch {
	case r < 0.01:
		status = models.PaymentFailed
	case r < 0.02:
		status = models.PaymentPending
	case r < 0.03:
		status = models.PaymentRefunded
	case r < 0.04:
		status = models.PaymentDeclined
	}

	return models.PaymentPayload{
		TransactionID: uuid.New().String(),
		CustomerID:    customerID,
		Amount:        amount,
		Currency:      "INR",
		Method:        method,
		Status:        status,
		Gateway:       randomElement(models.PaymentGateways),
		Region:        region,
		CardLast4:     fmt.Sprintf("%04d", rand.Intn(10000)),
	}
}

func (g *Generator) generateOrder() models.OrderPayload {
	category := randomElement(models.Categories)
	products := models.Products[category]
	product := randomElement(products)
	region := randomElement(models.Regions)
	customerID := uuid.New().String()[:8]

	quantity := rand.Intn(5) + 1
	price := roundFloat(50+rand.Float64()*950, 2)
	total := roundFloat(price*float64(quantity), 2)

	status := models.StatusCompleted
	r := rand.Float64()
	switch {
	case r < 0.02:
		status = models.StatusFailed
	case r < 0.04:
		status = models.StatusPending
	}

	return models.OrderPayload{
		OrderID:     uuid.New().String(),
		CustomerID:  customerID,
		Product:     product,
		Category:    category,
		Quantity:    quantity,
		Price:       price,
		TotalAmount: total,
		Region:      region,
		Status:      status,
	}
}

func (g *Generator) generateClick() models.ClickPayload {
	sessionID := uuid.New().String()[:12]
	customerID := uuid.New().String()[:8]
	region := randomElement(models.Regions)

	action := g.randomClickAction()
	page := randomElement(models.Pages)
	device := randomElement(models.Devices)
	browser := randomElement(models.Browsers)
	referrer := randomElement(models.Referrers)

	payload := models.ClickPayload{
		SessionID:  sessionID,
		CustomerID: customerID,
		Page:       page,
		Action:     action,
		Device:     device,
		Browser:    browser,
		Referrer:   referrer,
		DurationMs: rand.Intn(30000),
		Region:     region,
	}

	if action == models.ClickSearch {
		payload.SearchQuery = strings.ToLower(randomElement([]string{
			"laptop", "headphones", "shoes", "phone case", "yoga mat",
			"coffee maker", "skincare", "lego", "dumbbells", "watch",
		}))
	}

	if action == models.ClickView || action == models.ClickAdd || action == models.ClickBuy {
		category := randomElement(models.Categories)
		products := models.Products[category]
		payload.ProductID = "prod_" + strings.ToLower(strings.ReplaceAll(randomElement(products), " ", "_"))
	}

	return payload
}

func (g *Generator) randomPaymentMethod() models.PaymentMethod {
	methods := []models.PaymentMethod{
		models.PaymentCreditCard,
		models.PaymentDebitCard,
		models.PaymentUPI,
		models.PaymentNetBanking,
		models.PaymentWallet,
		models.PaymentCrypto,
	}
	weights := []float64{0.30, 0.20, 0.25, 0.10, 0.10, 0.05}
	r := rand.Float64()
	cumulative := 0.0
	for i, w := range weights {
		cumulative += w
		if r < cumulative {
			return methods[i]
		}
	}
	return methods[0]
}

func (g *Generator) randomClickAction() models.ClickAction {
	actions := []models.ClickAction{
		models.ClickView, models.ClickAdd, models.ClickRemove,
		models.ClickBuy, models.ClickSearch, models.ClickFilter,
		models.ClickShare, models.ClickWish, models.ClickReview,
		models.ClickScroll, models.ClickHover, models.ClickBanner,
	}
	weights := []float64{0.30, 0.12, 0.03, 0.05, 0.15, 0.10, 0.03, 0.05, 0.02, 0.10, 0.03, 0.02}
	r := rand.Float64()
	cumulative := 0.0
	for i, w := range weights {
		cumulative += w
		if r < cumulative {
			return actions[i]
		}
	}
	return actions[0]
}

func randomElement(s []string) string {
	return s[rand.Intn(len(s))]
}

func roundFloat(val float64, precision int) float64 {
	pow := 1.0
	for i := 0; i < precision; i++ {
		pow *= 10
	}
	return float64(int(val*pow+0.5)) / pow
}

func (g *Generator) GetRate() int {
	g.rateMu.RLock()
	defer g.rateMu.RUnlock()
	return g.eventsPerMinute
}

func (g *Generator) SetRate(rate int) {
	if rate < 1000 {
		rate = 1000
	}
	if rate > 100000 {
		rate = 100000
	}
	g.rateMu.Lock()
	g.eventsPerMinute = rate
	g.rateMu.Unlock()
	log.Printf("Event rate updated to %d events/min (%d events/sec)", rate, rate/60)
}

func (g *Generator) ResetCounter() {
	atomic.StoreInt64(&g.eventCount, 0)
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
