import { create, fromBinary, fromJson, toBinary } from '@bufbuild/protobuf'
import type { JsonValue } from '@bufbuild/protobuf'
import { ClientMessageSchema } from './gen/reactive/v1/reactive_pb.js'
import { WebSocketAuthStatus_Status, WebSocketClientFrameSchema, WebSocketServerFrameSchema } from './gen/reactive/v1/websocket_pb.js'
import { normalizeServerMessage, ProtocolError } from './protocol.js'
import type { ClientMessage, DecodeLimits, ServerMessage } from './protocol.js'

export type AuthStatus = 'unknown' | 'rejected' | 'reconnect'
export type SocketRequest = ClientMessage | { readonly ping: string } | { readonly authToken: { readonly token: string } }
type SocketMessage = { readonly type: 'pong'; readonly id: string } | { readonly type: 'model'; readonly message: ServerMessage }
  | { readonly type: 'authStatus'; readonly status: AuthStatus }

// Only reactive messages reach the normalized model or the gRPC service.
export function encodeSocketMessage(message: SocketRequest): Uint8Array<ArrayBuffer> {
  const frame = 'ping' in message
    ? create(WebSocketClientFrameSchema, { payload: { case: 'ping', value: { id: message.ping } } })
    : 'authToken' in message
    ? create(WebSocketClientFrameSchema, { payload: { case: 'authToken', value: message.authToken } })
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
    case 'authStatus': return { type: 'authStatus', status:
      frame.payload.value.status === WebSocketAuthStatus_Status.REJECTED ? 'rejected'
        : frame.payload.value.status === WebSocketAuthStatus_Status.RECONNECT ? 'reconnect' : 'unknown' }
    case 'pong': {
      const id = frame.payload.value.id
      if (!id || id.length > 128) throw new ProtocolError('Invalid heartbeat response')
      return { type: 'pong', id }
    }
    case 'message': return { type: 'model', message: normalizeServerMessage(frame.payload.value, limits) }
    default: throw new ProtocolError('Missing socket frame payload')
  }
}
