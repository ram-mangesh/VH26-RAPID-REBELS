package generator

import (
	"encoding/json"
	"testing"

	"go-producer/internal/models"
)

func TestGenerateEventFormat(t *testing.T) {
	g := &Generator{}
	event := g.generateEvent()

	if event.EventID == "" {
		t.Error("EventID should not be empty")
	}
	if event.Type == "" {
		t.Error("Type should not be empty")
	}
	if event.Timestamp.IsZero() {
		t.Error("Timestamp should not be zero")
	}
	if event.Payload == nil {
		t.Error("Payload should not be nil")
	}

	raw, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	for _, key := range []string{"event_id", "type", "timestamp", "payload"} {
		if _, ok := m[key]; !ok {
			t.Errorf("missing key: %s", key)
		}
	}

	switch event.Type {
	case models.EventPayment:
		if _, ok := event.Payload.(models.PaymentPayload); !ok {
			t.Error("payment payload type mismatch")
		}
	case models.EventOrder:
		if _, ok := event.Payload.(models.OrderPayload); !ok {
			t.Error("order payload type mismatch")
		}
	case models.EventClick:
		if _, ok := event.Payload.(models.ClickPayload); !ok {
			t.Error("click payload type mismatch")
		}
	}
}

func TestGenerateAllTypes(t *testing.T) {
	g := &Generator{}
	seen := map[models.EventType]bool{}

	for i := 0; i < 1000; i++ {
		ev := g.generateEvent()
		seen[ev.Type] = true
	}

	for _, typ := range []models.EventType{models.EventPayment, models.EventOrder, models.EventClick} {
		if !seen[typ] {
			t.Errorf("event type %s never generated", typ)
		}
	}
}
