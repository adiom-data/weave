import { decodeServerMessage } from '../dist/index.js'
import { fromBinary, fromJson, toBinary, toJson } from '@bufbuild/protobuf'
import { ServerMessageSchema } from '../dist/gen/reactive/v1/reactive_pb.js'
import { WebSocketClientFrameSchema, WebSocketServerFrameSchema } from '../dist/gen/reactive/v1/websocket_pb.js'

export const modelBytes = value => toBinary(ServerMessageSchema, fromJson(ServerMessageSchema, value))
export const serverBytes = value => toBinary(WebSocketServerFrameSchema, fromJson(WebSocketServerFrameSchema,
  'pong' in value ? { pong: { id: value.pong } } : 'authStatus' in value || 'clientVersionStatus' in value ? value : { message: value }))
export const clientFrame = bytes => {
  const frame = toJson(WebSocketClientFrameSchema, fromBinary(WebSocketClientFrameSchema, bytes))
  return frame.ping ? { ping: frame.ping.id } : frame.authToken ? { authToken: frame.authToken } : frame.clientInfo ? { clientInfo: frame.clientInfo } : frame.message
}
export class FakeSocket {
  readyState = 0
  binaryType = 'blob'
  sent = []
  onopen = null; onmessage = null; onclose = null; onerror = null
  open() { this.readyState = 1; this.onopen?.({}) }
  send(bytes) {
    if (this.readyState !== 1) throw new Error('closed')
    if (!(bytes instanceof Uint8Array)) throw new Error('Expected binary protobuf')
    this.sent.push(clientFrame(bytes))
  }
  message(value) { this.onmessage?.({ data: serverBytes(value).buffer }) }
  close() { this.readyState = 3; this.onclose?.({}) }
}

export const entity = (entityId, title, extra = {}) => ({ entityId, upsert: { values: { title: { textValue: title }, ...extra } } })
export const attach = (occurrenceId, nodeId, target, parentOccurrenceId = '', parentEntityId = '', subscriptionId = 'p') => ({
  occurrenceId, nodeId, attached: target, parentOccurrenceId, parentEntityId, subscriptionId,
})
export const reset = (querySubscriptionId, entityIds, version = '1') => ({ querySubscriptionId, reset: {
  entityIds, version, left: { sentinel: 'SENTINEL_START' }, right: { sentinel: 'SENTINEL_END' },
} })
export const batch = (modelBatch, envelope = {}) => decodeServerMessage(modelBytes({ modelBatch, ...envelope }))
export const ready = (subscriptionId, sequence) => decodeServerMessage(modelBytes({ subscriptionId, sequence, ready: {} }))
export const request = {
  root: { nodeId: 'project', entity: { query: { table: 'projects' } }, children: [{
    nodeId: 'memberships', list: { query: { table: 'memberships' }, window: { from: 'FROM_START', count: 20 } },
    children: [{ nodeId: 'task', entity: { query: { table: 'tasks' } } }],
  }] },
}
export const initial = () => ({
  kind: 'KIND_SNAPSHOT',
  entities: [entity('project', 'Project'), entity('m1', 'Link 1'), entity('m2', 'Link 2'), entity('shared', 'Shared task')],
  queries: [reset('list', ['m1', 'm2'])],
  projections: [attach('root', 'project', { entityId: 'project' }),
    attach('members', 'memberships', { querySubscriptionId: 'list' }, 'root', 'project'),
    attach('task1', 'task', { entityId: 'shared' }, 'members', 'm1'),
    attach('task2', 'task', { entityId: 'shared' }, 'members', 'm2')],
})
