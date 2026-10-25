package querygrpc

import "testing"

func TestTransportConfigurationDefaultsAndOverrides(t *testing.T) {
	if got := (TransportOptions{}).withDefaults(); got != DefaultTransportOptions() {
		t.Fatalf("unexpected defaults: %+v", got)
	}
	custom := TransportOptions{ChunkBytes: 17, MaxPayloadBytes: 101, ControlQueueLimit: 3, MaxInFlightPayloads: 7}
	if got := custom.withDefaults(); got != custom {
		t.Fatalf("overrides changed: %+v", got)
	}
}
