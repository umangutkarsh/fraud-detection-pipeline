package domain

import "time"

type Transaction struct {
	ID        string    `json:"id"`
	UserID    string    `json:"user_id"`
	Amount    float64   `json:"amount"`
	Currency  string    `json:"currency"`
	Location  string    `json:"location"`
	Country   string    `json:"country"`
	Timestamp time.Time `json:"timestamp"`
}

type FraudResult struct {
	Transaction  Transaction `json:"transaction"`
	IsFraudulent bool        `json:"is_fraudulent"`
	RiskScore    int         `json:"risk_score"`
	Reasons      []string    `json:"reasons"`
}