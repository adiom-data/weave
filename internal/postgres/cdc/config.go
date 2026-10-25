package cdc

import (
	"errors"
	"time"

	"github.com/adiom-data/weave/internal/messagebudget"
)

// ReplicationOptions controls retry and standby-status scheduling, not WAL
// ordering or acknowledgment semantics. Zero fields select defaults.
type ReplicationOptions struct {
	RetryMinDelay  time.Duration
	RetryMaxDelay  time.Duration
	StatusInterval time.Duration
}

func DefaultReplicationOptions() ReplicationOptions {
	return ReplicationOptions{RetryMinDelay: time.Second, RetryMaxDelay: 30 * time.Second, StatusInterval: 10 * time.Second}
}

func (o ReplicationOptions) WithDefaults() ReplicationOptions {
	d := DefaultReplicationOptions()
	if o.RetryMinDelay == 0 {
		o.RetryMinDelay = d.RetryMinDelay
	}
	if o.RetryMaxDelay == 0 {
		o.RetryMaxDelay = d.RetryMaxDelay
	}
	if o.StatusInterval == 0 {
		o.StatusInterval = d.StatusInterval
	}
	return o
}

func (o ReplicationOptions) Validate() error {
	o = o.WithDefaults()
	if o.RetryMinDelay <= 0 || o.RetryMaxDelay < o.RetryMinDelay || o.StatusInterval <= 0 {
		return errors.New("replication delays must be positive and retry maximum must not be smaller than minimum")
	}
	return nil
}

func (o ReplicationOptions) nextRetryDelay(current time.Duration) time.Duration {
	if current >= o.RetryMaxDelay-current {
		return o.RetryMaxDelay
	}
	return current * 2
}

func DefaultReplicatorOptions() ReplicatorOptions {
	return ReplicatorOptions{MessageLimits: messagebudget.Defaults(), MessageLogInterval: time.Minute, Replication: DefaultReplicationOptions()}
}

type BrokerOptions struct{ ConsumerBufferLimit int }

func DefaultBrokerOptions() BrokerOptions { return BrokerOptions{ConsumerBufferLimit: 1024} }
