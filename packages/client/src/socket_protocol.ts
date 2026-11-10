import { create, fromBinary, fromJson, toBinary } from '@bufbuild/protobuf'
import type { JsonValue } from '@bufbuild/protobuf'
import { ClientMessageSchema } from './gen/reactive/v1/reactive_pb.js'
import { WebSocketAuthStatus_Status, WebSocketClientVersionStatus_Status, WebSocketClientFrameSchema, WebSocketServerFrameSchema } from './gen/reactive/v1/websocket_pb.js'
import { normalizeServerMessage, ProtocolError } from './protocol.js'
import type { ClientMessage, DecodeLimits, ServerMessage } from './protocol.js'

export type AuthStatus = 'unknown' | 'rejected' | 'reconnect'
export interface ClientVersionStatus { readonly status: 'unknown' | 'deprecated' | 'upgrade-required'; readonly minimumRevision: bigint; readonly recommendedRevision: bigint }
export type SocketRequest = ClientMessage | { readonly ping: string } | { readonly authToken: { readonly token: string } } | { readonly clientInfo: { readonly revision: bigint } }
type SocketMessage = { readonly type: 'pong'; readonly id: string } | { readonly type: 'model'; readonly message: ServerMessage }
  | { readonly type: 'authStatus'; readonly status: AuthStatus }
  | { readonly type: 'clientVersion'; readonly notice: ClientVersionStatus }

// Only reactive messages reach the normalized model or the gRPC service.
export function encodeSocketMessage(message: SocketRequest): Uint8Array<ArrayBuffer> {
  const frame = 'ping' in message
    ? create(WebSocketClientFrameSchema, { payload: { case: 'ping', value: { id: message.ping } } })
    : 'authToken' in message
    ? create(WebSocketClientFrameSchema, { payload: { case: 'authToken', value: message.authToken } })
    : 'clientInfo' in message
    ? create(WebSocketClientFrameSchema, { payload: { case: 'clientInfo', value: message.clientInfo } })
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
    case 'clientVersionStatus': return { type: 'clientVersion', notice: Object.freeze({
      status: frame.payload.value.status === WebSocketClientVersionStatus_Status.DEPRECATED ? 'deprecated' : frame.payload.value.status === WebSocketClientVersionStatus_Status.UPGRADE_REQUIRED ? 'upgrade-required' : 'unknown',
      minimumRevision: frame.payload.value.minimumRevision, recommendedRevision: frame.payload.value.recommendedRevision,
    }) }
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
