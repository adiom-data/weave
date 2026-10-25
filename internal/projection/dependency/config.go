package dependency

import "time"

func DefaultOptions() Options {
	return Options{
		UpstreamBufferLimit: 4096, MaxSubscriptions: 10000,
		IdleTTL: 30 * time.Second, SweepInterval: time.Second,
		EvictionBatchSize: 128, CleanupBatchSize: 128,
	}
}

// WithDefaults fills optional eviction batch sizes. Other fields are explicit
// at this internal boundary, including IdleTTL=0 for immediate retirement.
func (o Options) WithDefaults() Options {
	d := DefaultOptions()
	if o.EvictionBatchSize == 0 {
		o.EvictionBatchSize = d.EvictionBatchSize
	}
	if o.CleanupBatchSize == 0 {
		o.CleanupBatchSize = d.CleanupBatchSize
	}
	return o
}
