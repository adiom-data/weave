package websocket

import (
	"testing"
	"time"
)

func TestSocketDefaultsAndOverrides(t *testing.T) {
	if got := (SocketConfig{}).withDefaults(); got != DefaultSocketConfig() {
		t.Fatalf("zero configuration: %+v", got)
	}
	got := (SocketConfig{WriteTimeout: time.Minute}).withDefaults()
	if got.WriteTimeout != time.Minute || got.IdleTimeout != DefaultSocketConfig().IdleTimeout || got.MaxMessageBytes != DefaultSocketConfig().MaxMessageBytes || got.MaxMessageDepth != DefaultSocketConfig().MaxMessageDepth {
		t.Fatalf("partial configuration: %+v", got)
	}
	if got := (SocketConfig{IdleTimeout: 2 * time.Minute}).withDefaults(); got.IdleTimeout != 2*time.Minute {
		t.Fatalf("idle timeout override: %+v", got)
	}
}
