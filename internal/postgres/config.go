package postgres

import (
	"github.com/adiom-data/weave/internal/messagebudget"
	"github.com/adiom-data/weave/internal/postgres/cdc"
)

// ReplicationOptions is data-only public configuration. Decoder validation and
// normalization helpers stay private to the CDC implementation.
type ReplicationOptions cdc.ReplicationOptions

// DefaultOptions supplies operational defaults, not application identity.
// Publication and Slot must still be provided.
func DefaultOptions() Options {
	return Options{
		QueryLimits: DefaultQueryLimits(), MessageLimits: MessageLimits(messagebudget.Defaults()),
		MessageLogInterval: cdc.DefaultReplicatorOptions().MessageLogInterval,
		Replication:        ReplicationOptions(cdc.DefaultReplicationOptions()),
		CaptureBufferLimit: cdc.DefaultBrokerOptions().ConsumerBufferLimit,
	}
}

func DefaultQueryLimits() QueryLimits {
	return QueryLimits{MaxOrderTerms: 16, MaxPredicateNodes: 64, MaxPredicateDepth: 8, MaxPredicateValueBytes: 4096}
}
