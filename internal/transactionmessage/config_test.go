package transactionmessage

import "testing"

func TestMessageConfigurationDefaultsAndOverrides(t *testing.T) {
	d := DefaultOptions()
	if got, err := (Options{}).normalized(); err != nil || got != d {
		t.Fatalf("unexpected defaults: %+v %v", got, err)
	}
	custom := d
	custom.MaxQueuedTransactions++
	custom.MaxQueuedBytes++
	custom.MaxBatchTransactions++
	custom.MaxBatchBytes++
	custom.MessageLimits.MaxMessageBytes++
	if got, err := custom.normalized(); err != nil || got != custom {
		t.Fatalf("overrides changed: %+v %v", got, err)
	}
}
