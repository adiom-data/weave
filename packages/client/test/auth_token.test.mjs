import test from 'node:test'
import assert from 'node:assert/strict'
import { setTimeout as delay } from 'node:timers/promises'
import { ReactiveClient } from '../dist/index.js'
import { FakeSocket, initial, request } from './helpers.mjs'

function setup(t, options = {}) {
  const sockets = []
  const client = new ReactiveClient({ url: 'ws://example.test', heartbeatIntervalMs: 0,
    requestTimeoutMs: 10, reconnectDelayMs: 5,
    createSocket: () => { const socket = new FakeSocket(); sockets.push(socket); return socket }, ...options })
  t.after(() => client.disconnect())
  client.connect()
  sockets[0].open()
  return { client, sockets, socket: sockets[0] }
}

test('auth tokens are fire-and-forget with no acceptance, timeout, or projection changes', async t => {
  const statuses = []
  const { client, socket } = setup(t, { onAuthStatus: status => statuses.push(status) })
  assert.equal(socket.sent.length, 0, 'no auth submissions by default')
  const handle = client.subscribeProjection(request)
  const image = initial()
  image.projections.forEach(ref => { ref.subscriptionId = handle.id })
  socket.message({ subscriptionId: handle.id, sequence: '1', modelBatch: image })
  socket.message({ subscriptionId: handle.id, sequence: '2', ready: {} })
  const before = handle.data.getSnapshot()
  assert.equal(client.updateAuthToken('fresh-token'), undefined)
  client.updateAuthToken('newer-token')
  assert.deepEqual(socket.sent.slice(-2), [{ authToken: { token: 'fresh-token' } }, { authToken: { token: 'newer-token' } }])
  await delay(30)
  assert.deepEqual(statuses, [])
  assert.equal(client.getDiagnostics().failures, 0)
  assert.equal(handle.data.getSnapshot(), before)
})

test('connection-level auth notices need no matching request and do not mutate projections', t => {
  const statuses = []
  const { client, socket } = setup(t, { onAuthStatus: status => statuses.push(status) })
  for (const status of ['STATUS_REJECTED', 'STATUS_RECONNECT', 'STATUS_UNSPECIFIED', 999]) socket.message({ authStatus: { status } })
  assert.deepEqual(statuses, ['rejected', 'reconnect', 'unknown', 'unknown'])
  assert.equal(client.getDiagnostics().failures, 0)
  assert.equal(socket.readyState, 1, 'application/server owns reconnect scheduling')
})

test('token updates are not retained or replayed and obsolete connections cannot notify', async t => {
  const statuses = []
  const { client, socket, sockets } = setup(t, { onAuthStatus: status => statuses.push(status) })
  client.updateAuthToken('old')
  socket.close()
  await delay(20)
  const replacement = sockets[1]
  replacement.open()
  assert.deepEqual(replacement.sent, [], 'obsolete credential replayed')
  socket.message({ authStatus: { status: 'STATUS_RECONNECT' } })
  replacement.message({ authStatus: { status: 'STATUS_REJECTED' } })
  assert.deepEqual(statuses, ['rejected'])
  client.updateAuthToken('latest')
  assert.deepEqual(replacement.sent, [{ authToken: { token: 'latest' } }])
  client.disconnect()
  assert.throws(() => client.updateAuthToken('offline'), /not open/)
})

test('failed token sends use the normal disconnect path and invalid local inputs are not sent', t => {
  const { client, socket } = setup(t, { failurePolicy: () => 'pause' })
  assert.throws(() => client.updateAuthToken(''), /required/)
  assert.deepEqual(socket.sent, [])
  socket.send = () => { throw new Error('write failed') }
  assert.throws(() => client.updateAuthToken('token'), /write failed/)
  assert.equal(client.getDiagnostics().lastFailure.kind, 'send')
})

test('auth notice listener errors are isolated from transport and model messages', t => {
  const errors = []
  const { client, socket } = setup(t, { onAuthStatus: () => { throw new Error('listener failed') }, onListenerError: error => errors.push(error.message) })
  socket.message({ authStatus: { status: 'STATUS_REJECTED' } })
  socket.message({ connectionOpened: { connectionContext: 'still-live' } })
  assert.deepEqual(errors, ['listener failed'])
  assert.equal(client.connectionContext, 'still-live')
  assert.equal(client.getDiagnostics().failures, 0)
})
