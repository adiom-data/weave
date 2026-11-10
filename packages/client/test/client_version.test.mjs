import test from 'node:test'
import assert from 'node:assert/strict'
import { setTimeout as delay } from 'node:timers/promises'
import { ReactiveClient } from '../dist/index.js'
import { FakeSocket, request } from './helpers.mjs'

test('application revision is announced before subscriptions, exactly once per connection', t => {
  const socket = new FakeSocket()
  const client = new ReactiveClient({ url: 'ws://test', clientRevision: (1n << 64n) - 1n, heartbeatIntervalMs: 0, createSocket: () => socket })
  t.after(() => client.clear())
  const handle = client.subscribeProjection(request)
  client.connect(); socket.open()
  assert.equal(socket.sent[0].clientInfo.revision, '18446744073709551615')
  assert.equal(socket.sent[1].subscriptionId, handle.id)
  assert.equal(socket.sent.filter(value => value.clientInfo).length, 1)
})

test('deprecation is advisory; upgrade-required pauses retries and leaves reload to the application', async t => {
  const sockets = [], notices = []
  const client = new ReactiveClient({ url: 'ws://test', clientRevision: 3n, reconnectDelayMs: 1, heartbeatIntervalMs: 0, onClientVersionStatus: value => notices.push(value), createSocket: () => { const s = new FakeSocket(); sockets.push(s); return s } })
  t.after(() => client.clear())
  client.connect(); const socket = sockets[0]; socket.open()
  socket.message({ clientVersionStatus: { status: 'STATUS_DEPRECATED', minimumRevision: '2', recommendedRevision: '5' } })
  assert.equal(socket.readyState, 1)
  assert.deepEqual(notices[0], { status: 'deprecated', minimumRevision: 2n, recommendedRevision: 5n })
  socket.message({ clientVersionStatus: { status: 'STATUS_UPGRADE_REQUIRED', minimumRevision: '4', recommendedRevision: '5' } })
  assert.equal(socket.readyState, 3)
  assert.equal(client.store.getConnection().status, 'disconnected')
  assert.equal(notices[1].status, 'upgrade-required')
  await delay(10)
  assert.equal(sockets.length, 1, 'must not retry forever with the same obsolete bundle')
  socket.message({ clientVersionStatus: { status: 'STATUS_DEPRECATED' } })
  assert.equal(notices.length, 2, 'old generation cannot notify')
})

test('invalid revisions fail locally, and notice callback errors do not break the transport', t => {
  for (const revision of [-1n, 1n << 64n, 123, '1']) assert.throws(() => new ReactiveClient({ url: 'ws://test', clientRevision: revision }), /revision/)
  const socket = new FakeSocket(), errors = []
  const client = new ReactiveClient({ url: 'ws://test', heartbeatIntervalMs: 0, onClientVersionStatus: () => { throw new Error('callback') }, onListenerError: error => errors.push(error.message), createSocket: () => socket })
  t.after(() => client.clear())
  client.connect(); socket.open()
  socket.message({ clientVersionStatus: { status: 'STATUS_DEPRECATED', minimumRevision: '0', recommendedRevision: '10' } })
  assert.equal(socket.readyState, 1)
  assert.deepEqual(errors, ['callback'])
})
