import test from 'node:test'
import assert from 'node:assert/strict'
import { create, fromBinary, fromJson, toBinary } from '@bufbuild/protobuf'
import { decodeServerMessage } from '../dist/index.js'
import { BoundarySchema, BoundaryUpdateSchema, ClientMessageSchema, ComparisonSchema, InitialWindowSchema, JunctionSchema, ModelBatchSchema, ServerMessageSchema } from '../dist/gen/reactive/v1/reactive_pb.js'
import { WebSocketServerFrameSchema } from '../dist/gen/reactive/v1/websocket_pb.js'
import { decodeSocketMessage, encodeSocketMessage } from '../dist/socket_protocol.js'
import { clientFrame, entity, FakeSocket, modelBytes, request, serverBytes } from './helpers.mjs'
import { ReactiveClient } from '../dist/index.js'

test('prefixed protobuf JSON enum names retain their original binary numbers', () => {
  const cases = [
    [ComparisonSchema, { operator: 'OPERATOR_EQUAL' }, { operator: 'EQUAL' }, [16, 1]],
    [JunctionSchema, { operator: 'OPERATOR_AND' }, { operator: 'AND' }, [8, 1]],
    [InitialWindowSchema, { from: 'FROM_END' }, { from: 'END' }, [8, 2]],
    [BoundaryUpdateSchema, { side: 'SIDE_RIGHT', operation: 'OPERATION_EXTEND' }, { side: 'RIGHT', operation: 'EXTEND' }, [8, 2, 16, 1]],
    [BoundarySchema, { sentinel: 'SENTINEL_END' }, { sentinel: 'END' }, [8, 2]],
    [ModelBatchSchema, { kind: 'KIND_COMMIT' }, { kind: 'COMMIT' }, [40, 2]],
  ]
  for (const [schema, json, legacy, bytes] of cases) {
    assert.deepEqual([...toBinary(schema, fromJson(schema, json))], bytes)
    assert.throws(() => fromJson(schema, legacy), /enum/)
  }
})

test('connection-opened events normalize through the binary WebSocket path', () => {
  const opened = decodeSocketMessage(serverBytes({ connectionOpened: { connectionContext: 'generation-context' } }), {}).message
  assert.deepEqual(opened.event, { type: 'connectionOpened', context: 'generation-context' })
  assert.equal(opened.connectionSequence, 0n)
  assert.ok(Object.isFrozen(opened.event))
  assert.throws(() => decodeServerMessage(modelBytes({ connectionSequence: '1', connectionOpened: { connectionContext: 'invalid' } })), /connection-opened envelope/)
})

test('committed transaction messages survive normalization without entering entity data', () => {
  const output = decodeServerMessage(modelBytes({ connectionSequence: '1', modelBatch: {
    kind: 'KIND_COMMIT', atomic: true, checkpoint: '10', messages: [
      { kind: 'mutation.receipt', payload: 'AP+A' }, { kind: 'empty-payload' },
    ],
  } }))
  assert.equal(output.event.batch.kind, 'COMMIT', 'normalized runtime kinds are not protobuf JSON names')
  assert.deepEqual(output.event.batch.messages, [
    { kind: 'mutation.receipt', payload: 'AP+A' }, { kind: 'empty-payload', payload: '' },
  ])
  assert.deepEqual(output.event.batch.entities, [])
  assert.ok(Object.isFrozen(output.event.batch.messages[0]))
  assert.throws(() => decodeServerMessage(modelBytes({ modelBatch: { kind: 'KIND_SNAPSHOT', messages: [{ kind: 'receipt' }] } })), /commit batch/)
})

test('binary protobuf preserves uint64, exact numeric strings, bytes, and nanosecond timestamps', () => {
  const expected = {
    integer: { integerValue: '-9223372036854775808' },
    decimal: { decimalValue: '123456789012345678901234567890.123456789' },
    float: { floatValue: 'NaN' },
    bytes: { bytesValue: 'AAEC//4=' },
    datetime: { datetimeValue: '2026-10-27T12:34:56.123456789Z' },
    ancient: { datetimeValue: '0001-01-01T00:00:00Z' },
    object: { objectValue: { fields: { number: { decimalValue: '1e1000' } } } },
  }
  const input = { sequence: '18446744073709551615', connectionSequence: '9007199254740993',
    modelBatch: { kind: 'KIND_COMMIT', atomic: true, entities: [entity('a', 'Exact', expected)] } }
  const output = decodeSocketMessage(serverBytes(input), {}).message
  assert.equal(output.sequence, 18446744073709551615n)
  assert.equal(output.connectionSequence, 9007199254740993n)
  const { title, ...values } = output.event.batch.entities[0].values
  assert.equal(title.textValue, 'Exact')
  assert.deepEqual(JSON.parse(JSON.stringify(values)), expected)
  assert.equal(Object.getPrototypeOf(values.object.objectValue.fields), null)
  assert.ok(Object.isFrozen(output.event.batch.entities[0].values))
})

test('outgoing projection requests and window cursors use generated binary bindings', () => {
  const bytes = encodeSocketMessage({ subscriptionId: 'p', requestId: 'r', subscribeProjection: request })
  assert.ok(bytes instanceof Uint8Array)
  assert.deepEqual(clientFrame(bytes), { subscriptionId: 'p', requestId: 'r', subscribeProjection: request })
  const window = encodeSocketMessage({ subscriptionId: 'p', requestId: 'w', updateProjectionWindow: {
    instanceId: 'list', updates: [{ side: 'SIDE_RIGHT', operation: 'OPERATION_MOVE', cursor: 'AAEC//4=' }],
  } })
  assert.equal(clientFrame(window).updateProjectionWindow.updates[0].cursor, 'AAEC//4=')
  assert.throws(() => encodeSocketMessage({ subscribeProjection: { root: { nodeId: 'root', entity: {}, list: {} } } }))
})

test('binary envelope rejects absent payloads, malformed data, invalid model events, and resource excess', () => {
  assert.throws(() => decodeSocketMessage(new Uint8Array(), {}), /payload/)
  assert.throws(() => decodeSocketMessage(new Uint8Array([0xff]), {}))
  assert.throws(() => decodeSocketMessage(toBinary(WebSocketServerFrameSchema, create(WebSocketServerFrameSchema,
    { payload: { case: 'message', value: {} } })), {}), /server event/)
  assert.throws(() => decodeSocketMessage(serverBytes({ pong: '' }), {}), /heartbeat/)
  assert.throws(() => decodeSocketMessage(serverBytes({ modelBatch: { entities: [entity('a', 'A')] } }), { maxNodes: 2 }), /limit/)
  const missingKind = create(ServerMessageSchema, { event: { case: 'modelBatch', value: {
    entities: [{ entityId: 'a', event: { case: 'upsert', value: { values: { bad: {} } } } }],
  } } })
  assert.throws(() => decodeServerMessage(toBinary(ServerMessageSchema, missingKind)), /value kind/)
  const input = modelBytes({ ready: {}, sequence: '1' })
  assert.equal(fromBinary(ServerMessageSchema, input).sequence, 1n)
  assert.throws(() => fromJson(ClientMessageSchema, { subscribeProjection: { parameters: { n: { integerValue: 9007199254740992 } } } }))
})

test('legacy text WebSocket frames are rejected rather than retained as a second protocol path', () => {
  let socket
  const client = new ReactiveClient({ url: 'ws://test', createSocket: () => (socket = new FakeSocket()) })
  client.connect(); socket.open()
  assert.equal(socket.binaryType, 'arraybuffer')
  socket.onmessage({ data: '{"ready":{}}' })
  assert.match(client.store.getConnection().error, /binary/)
  assert.equal(socket.readyState, 3)
  client.clear()
})
