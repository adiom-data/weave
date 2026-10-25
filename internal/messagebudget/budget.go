// Package messagebudget bounds opaque transaction metadata before payloads are
// retained. It does not interpret routing keys or mutation identifiers.
package messagebudget

import "errors"

// MessageOverhead accounts conservatively for an empty message and its framing.
const MessageOverhead = 32

type Limits struct {
	MaxMessageBytes     int
	MaxTransactionBytes int
	MaxMessages         int
}

func Defaults() Limits {
	return Limits{MaxMessageBytes: 256 << 10, MaxTransactionBytes: 512 << 10, MaxMessages: 1024}
}

func (l Limits) Validate() error {
	if l.MaxMessageBytes < 1 || l.MaxTransactionBytes < 2*MessageOverhead || l.MaxMessages < 1 {
		return errors.New("message byte/count limits must be positive and transaction budget must fit two empty messages")
	}
	return nil
}

type Decision uint8

const (
	Keep Decision = iota
	Empty
	Skip
)

// Budget reserves one empty tail marker. When count or aggregate byte limits
// are exhausted, that marker represents all remaining omissions, preventing
// an attacker from allocating an unbounded list of empty placeholders.
type Budget struct {
	Limits  Limits
	Bytes   int
	Count   int
	Dropped int
	tail    bool
}

func (b *Budget) Add(kindBytes, payloadBytes int) Decision {
	if b.tail {
		b.Dropped++
		return Skip
	}
	remaining := b.Limits.MaxTransactionBytes - b.Bytes - MessageOverhead
	if b.Count >= b.Limits.MaxMessages || remaining < MessageOverhead {
		b.tail = true
		b.Bytes += MessageOverhead
		b.Dropped++
		return Empty
	}
	b.Count++
	if kindBytes > b.Limits.MaxMessageBytes || payloadBytes > b.Limits.MaxMessageBytes-kindBytes || kindBytes > remaining-MessageOverhead || payloadBytes > remaining-MessageOverhead-kindBytes {
		b.Bytes += MessageOverhead
		b.Dropped++
		return Empty
	}
	b.Bytes += MessageOverhead + kindBytes + payloadBytes
	return Keep
}
