import { create, fromBinary, fromJson, toBinary } from '@bufbuild/protobuf'
import type { JsonValue } from '@bufbuild/protobuf'
import { ClientMessageSchema } from './gen/reactive/v1/reactive_pb.js'
import { WebSocketClientFrameSchema, WebSocketServerFrameSchema } from './gen/reactive/v1/websocket_pb.js'
import { normalizeServerMessage, ProtocolError } from './protocol.js'
import type { ClientMessage, DecodeLimits, ServerMessage } from './protocol.js'

type SocketMessage = { readonly type: 'pong'; readonly id: string } | { readonly type: 'model'; readonly message: ServerMessage }

// Only reactive messages reach the normalized model or the gRPC service.
export function encodeSocketMessage(message: ClientMessage | { readonly ping: string }): Uint8Array<ArrayBuffer> {
  const frame = 'ping' in message
    ? create(WebSocketClientFrameSchema, { payload: { case: 'ping', value: { id: message.ping } } })
    : create(WebSocketClientFrameSchema, { payload: { case: 'message', value:
      // Readonly inputs are validated by the generated codec. No JSON text is
      // created or transmitted; generated JSON types are just input ergonomics.
      fromJson(ClientMessageSchema, message as JsonValue),
    } })
  return toBinary(WebSocketClientFrameSchema, frame)
}
export function decodeSocketMessage(input: Uint8Array, limits: DecodeLimits): SocketMessage {
  const frame = fromBinary(WebSocketServerFrameSchema, input, {
    recursionLimit: limits.maxDepth ?? 64, readUnknownFields: false,
  })
  switch (frame.payload.case) {
    case 'pong': {
      const id = frame.payload.value.id
      if (!id || id.length > 128) throw new ProtocolError('Invalid heartbeat response')
      return { type: 'pong', id }
    }
    case 'message': return { type: 'model', message: normalizeServerMessage(frame.payload.value, limits) }
    default: throw new ProtocolError('Missing socket frame payload')
  }
}
