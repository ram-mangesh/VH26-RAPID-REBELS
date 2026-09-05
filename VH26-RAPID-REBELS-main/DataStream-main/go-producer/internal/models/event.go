package models

import "time"

type EventType string

const (
	EventPayment EventType = "payment"
	EventOrder   EventType = "order"
	EventClick   EventType = "log"
)

type Event struct {
	EventID   string      `json:"event_id"`
	Type      EventType   `json:"type"`
	Timestamp time.Time   `json:"timestamp"`
	Payload   interface{} `json:"payload"`
}
