package querygrpc

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	querysubscriptionv1 "github.com/adiom-data/weave/pkg/gen/querysubscription/v1"
)

var errPayloadTooLarge = errors.New("query subscription payload exceeds the configured transport limit")

type payloadAssembly struct {
	kind           querysubscriptionv1.PayloadChunk_Kind
	subscriptionID string
	payloadID      uint64
	totalSize      uint64
	nextIndex      uint32
	data           []byte
}

func newPayloadAssembly(chunk *querysubscriptionv1.PayloadChunk, maximum int) (*payloadAssembly, error) {
	if chunk == nil || chunk.Kind == querysubscriptionv1.PayloadChunk_KIND_UNSPECIFIED || chunk.PayloadId == 0 || chunk.Index != 0 {
		return nil, errors.New("invalid initial payload chunk")
	}
	if chunk.TotalSize > uint64(maximum) {
		return nil, errPayloadTooLarge
	}
	return &payloadAssembly{
		kind: chunk.Kind, subscriptionID: chunk.SubscriptionId, payloadID: chunk.PayloadId,
		totalSize: chunk.TotalSize, data: make([]byte, 0, int(chunk.TotalSize)),
	}, nil
}

func (assembly *payloadAssembly) append(chunk *querysubscriptionv1.PayloadChunk) (bool, error) {
	if chunk == nil || chunk.Kind != assembly.kind || chunk.SubscriptionId != assembly.subscriptionID || chunk.PayloadId != assembly.payloadID || chunk.TotalSize != assembly.totalSize || chunk.Index != assembly.nextIndex {
		return false, errors.New("payload chunks are inconsistent or out of order")
	}
	if uint64(len(assembly.data))+uint64(len(chunk.Data)) > assembly.totalSize {
		return false, errors.New("payload chunks exceed declared size")
	}
	assembly.data = append(assembly.data, chunk.Data...)
	assembly.nextIndex++
	if !chunk.Final {
		if uint64(len(assembly.data)) == assembly.totalSize {
			return false, errors.New("payload reached declared size before its final chunk")
		}
		return false, nil
	}
	if uint64(len(assembly.data)) != assembly.totalSize {
		return false, errors.New("final payload size does not match its declaration")
	}
	return true, nil
}

func marshalPayload(message proto.Message, maximum int) ([]byte, error) {
	data, err := proto.Marshal(message)
	if err != nil {
		return nil, err
	}
	if len(data) > maximum {
		return nil, fmt.Errorf("%w: %d bytes exceeds %d", errPayloadTooLarge, len(data), maximum)
	}
	return data, nil
}
