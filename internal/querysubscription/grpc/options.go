package querygrpc

const ProtocolVersion uint32 = 1

// TransportOptions bound transport framing independently from the logical
// subscription queue. ChunkBytes must fit beneath the configured gRPC message
// limit; MaxPayloadBytes bounds one reconstructed snapshot or delivery.
type TransportOptions struct {
	ChunkBytes          int
	MaxPayloadBytes     int
	ControlQueueLimit   int
	MaxInFlightPayloads int
}

func (options TransportOptions) withDefaults() TransportOptions {
	d := DefaultTransportOptions()
	if options.ChunkBytes <= 0 {
		options.ChunkBytes = d.ChunkBytes
	}
	if options.MaxPayloadBytes <= 0 {
		options.MaxPayloadBytes = d.MaxPayloadBytes
	}
	if options.ChunkBytes > options.MaxPayloadBytes {
		options.ChunkBytes = options.MaxPayloadBytes
	}
	if options.ControlQueueLimit <= 0 {
		options.ControlQueueLimit = d.ControlQueueLimit
	}
	if options.MaxInFlightPayloads <= 0 {
		options.MaxInFlightPayloads = d.MaxInFlightPayloads
	}
	return options
}

func DefaultTransportOptions() TransportOptions {
	return TransportOptions{ChunkBytes: 256 << 10, MaxPayloadBytes: 64 << 20,
		ControlQueueLimit: 256, MaxInFlightPayloads: 1024}
}
