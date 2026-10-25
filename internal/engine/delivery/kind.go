// Package delivery owns bounded per-consumer queues, client-window membership,
// and transaction-preserving fan-out. Subject-specific visibility belongs to
// the outer connection delivery layer.
package delivery

// DeliveryKind distinguishes source commits from later fenced hydration of an
// entity that was explicitly marked pending in an earlier commit.
type DeliveryKind uint8

const (
	DeliveryCommit DeliveryKind = iota + 1
	DeliveryHydration
	// DeliveryWindow is a connection-ordered, subscription-scoped snapshot
	// transition. It participates in the predecessor chain but retains its
	// request acknowledgement and is not a source transaction.
	DeliveryWindow
	// DeliveryProgress is an authoritative source watermark produced by a
	// decoded logical barrier. It carries no subscription effects.
	DeliveryProgress
)
