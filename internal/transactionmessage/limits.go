package transactionmessage

// MessageLimits bounds opaque message payloads. An all-zero struct selects defaults.
type MessageLimits struct {
	MaxMessageBytes     int
	MaxTransactionBytes int
	MaxMessages         int
}
