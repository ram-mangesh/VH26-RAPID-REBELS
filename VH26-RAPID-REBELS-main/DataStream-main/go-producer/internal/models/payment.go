package models

type PaymentMethod string

const (
	PaymentCreditCard  PaymentMethod = "credit_card"
	PaymentDebitCard   PaymentMethod = "debit_card"
	PaymentUPI         PaymentMethod = "upi"
	PaymentNetBanking  PaymentMethod = "net_banking"
	PaymentWallet      PaymentMethod = "wallet"
	PaymentCrypto      PaymentMethod = "crypto"
)

type PaymentStatus string

const (
	PaymentSuccess      PaymentStatus = "success"
	PaymentFailed       PaymentStatus = "failed"
	PaymentPending      PaymentStatus = "pending"
	PaymentRefunded     PaymentStatus = "refunded"
	PaymentDeclined     PaymentStatus = "declined"
)

var PaymentGateways = []string{
	"Stripe", "Razorpay", "PayU", "CcAvenue", "PayPal", "Square", "Braintree",
}

var Currencies = []string{
	"INR", "USD", "EUR", "GBP", "JPY", "AED", "AUD",
}

type PaymentPayload struct {
	TransactionID string        `json:"transaction_id"`
	CustomerID    string        `json:"customer_id"`
	Amount        float64       `json:"amount"`
	Currency      string        `json:"currency"`
	Method        PaymentMethod `json:"method"`
	Status        PaymentStatus `json:"status"`
	Gateway       string        `json:"gateway"`
	Region        string        `json:"region"`
	CardLast4     string        `json:"card_last4,omitempty"`
}
