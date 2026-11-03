import test from 'node:test'
import assert from 'node:assert/strict'
import { setTimeout as delay } from 'node:timers/promises'
import { ReactiveClient } from '../dist/index.js'
import { ProjectionPublication } from '../dist/publication.js'
import { initial, request, entity, FakeSocket } from './helpers.mjs'
function setup(t, options = {}) {
  const sockets = []
  const client = new ReactiveClient({ url: () => 'ws://example.test', reconnectDelayMs: 5, requestTimeoutMs: 100,
    createSocket: () => { const socket = new FakeSocket(); sockets.push(socket); return socket }, ...options })
  t.after(() => client.disconnect())
  return { client, sockets }
}
function snapshot(handle) {
  const image = initial()
  image.projections.forEach(ref => { ref.subscriptionId = handle.id })
  return { subscriptionId: handle.id, sequence: '1', modelBatch: image }
}
function markReady(socket, handle, sequence = '2') { socket.message({ subscriptionId: handle.id, sequence, ready: {} }) }

test('connection context is generation-scoped and receipts run after model publication', async t => {
  const contexts = [], receipts = []
  let handle
  const { client, sockets } = setup(t, {
    onConnectionOpened: context => contexts.push(context),
    onTransaction: transaction => receipts.push({ transaction, title: client.store.getEntity('project').values.title.textValue }),
  })
  handle = client.subscribeProjection(request)
  client.connect()
  const first = sockets[0]; first.open()
  assert.equal(client.connectionContext, undefined)
  first.message({ connectionOpened: { connectionContext: 'first-context' } })
  assert.equal(client.connectionContext, 'first-context')
  first.message(snapshot(handle)); markReady(first, handle)
  first.message({ connectionSequence: '1', modelBatch: { kind: 'KIND_COMMIT', atomic: true, checkpoint: '10',
    entities: [entity('project', 'Changed')], messages: [{ kind: 'receipt', payload: 'AP8=' }] } })
  assert.equal(receipts.length, 1)
  assert.equal(receipts[0].title, 'Changed')
  assert.equal(receipts[0].transaction.messages[0].payload, 'AP8=')
  assert.equal(Object.isFrozen(receipts[0].transaction.messages), true)
  const published = handle.data.getSnapshot()
  first.message({ connectionSequence: '2', modelBatch: { kind: 'KIND_COMMIT', atomic: true, checkpoint: '20', messages: [{ kind: 'receipt', payload: '' }] } })
  assert.equal(handle.data.getSnapshot(), published)
  first.close()
  assert.equal(client.connectionContext, undefined)
  await delay(15)
  const next = sockets[1]; next.open()
  next.message({ connectionOpened: { connectionContext: 'next-context' } })
  first.message({ connectionOpened: { connectionContext: 'obsolete' } })
  first.message({ connectionSequence: '3', modelBatch: { kind: 'KIND_COMMIT', messages: [{ kind: 'receipt' }] } })
  assert.deepEqual(contexts, ['first-context', 'next-context'])
  assert.equal(client.connectionContext, 'next-context')
  assert.equal(receipts.length, 2)
})

test('multiplexes projections and owns immutable definitions', t => {
  const { client, sockets } = setup(t)
  const definition = structuredClone(request)
  const first = client.subscribeProjection(definition)
  definition.root.nodeId = 'caller mutation'
  const second = client.subscribeProjection(request)
  assert.equal(sockets.length, 0)
  client.connect(); client.connect()
  assert.equal(sockets.length, 1)
  const socket = sockets[0]
  socket.open()
  assert.equal(socket.sent.length, 2)
  assert.equal(socket.sent[0].subscribeProjection.root.nodeId, 'project')
  assert.notEqual(first.id, second.id)
  assert.notEqual(socket.sent[0].requestId, socket.sent[1].requestId)
  socket.message(snapshot(first))
  markReady(socket, first)
  assert.equal(first.view.getSnapshot().status, 'ready')
  assert.equal(second.view.getSnapshot().status, 'loading')
})

test('reconnect starts a fresh model and ignores callbacks from the previous socket', async t => {
  const { client, sockets } = setup(t)
  const handle = client.subscribeProjection(request)
  client.connect()
  const old = sockets[0]
  old.open(); old.message(snapshot(handle)); markReady(old, handle)
  const firstRequestId = old.sent[0].requestId
  const published = handle.view.getSnapshot().root
  old.close()
  assert.equal(handle.view.getSnapshot().root, published)
  assert.equal(handle.view.getSnapshot().stale, true)
  assert.equal(client.store.getEntity('shared').status, 'unavailable')
  await delay(15)
  assert.equal(sockets.length, 2)
  const current = sockets[1]
  current.open()
  assert.notEqual(current.sent[0].requestId, firstRequestId)
  old.message({ connectionSequence: '1', modelBatch: { entities: [entity('old', 'stale')] } })
  assert.equal(client.store.getEntity('old').status, 'unavailable')
  current.message(snapshot(handle)); markReady(current, handle)
  assert.equal(handle.view.getSnapshot().status, 'ready')
  assert.equal(handle.view.getSnapshot().root.row.children.memberships.rows.length, 2)
})

test('reconnect publishes privately and reuses identical display data across new server IDs', async t => {
  const { client, sockets } = setup(t)
  const handle = client.subscribeProjection(request)
  client.connect(); const first = sockets[0]; first.open()
  first.message(snapshot(handle)); markReady(first, handle)
  const data = handle.data.getSnapshot()
  const root = handle.view.getSnapshot().root
  let changes = 0
  const stop = handle.data.subscribe(() => { changes++ })
  t.after(stop)
  first.close()
  assert.equal(handle.data.getSnapshot(), data)
  await assert.rejects(handle.updateWindow('members', []), /not ready/)
  await delay(15)
  const second = sockets[1]; second.open()
  const next = snapshot(handle)
  next.modelBatch.projections.forEach(ref => {
    ref.occurrenceId += '-new'
    if (ref.parentOccurrenceId) ref.parentOccurrenceId += '-new'
    if (ref.attached.querySubscriptionId) ref.attached.querySubscriptionId = 'new-list'
  })
  next.modelBatch.queries[0].querySubscriptionId = 'new-list'
  second.message(next)
  assert.equal(handle.view.getSnapshot().root, root)
  assert.equal(handle.view.getSnapshot().stale, true)
  markReady(second, handle)
  assert.equal(handle.view.getSnapshot().stale, false)
  assert.equal(handle.data.getSnapshot(), data)
  assert.equal(changes, 0)
  await assert.rejects(handle.updateWindow('members', []), /current projection/)
  const command = handle.updateWindow('members-new', [])
  second.message({ requestId: second.sent.at(-1).requestId, connectionSequence: '1', modelBatch: {} })
  await command
})

test('replacement is authoritative, shares unchanged rows, and clears on logout', t => {
  const { client, sockets } = setup(t)
  const handle = client.subscribeProjection(request)
  client.connect(); sockets[0].open()
  const original = snapshot(handle)
  original.modelBatch.entities[0] = entity('project', 'Project', { obsolete: { textValue: 'remove me' } })
  sockets[0].message(original); markReady(sockets[0], handle)
  const before = handle.data.getSnapshot()
  const rows = before.row.children.memberships.rows
  client.disconnect(); client.connect(); sockets[1].open()
  const next = snapshot(handle)
  next.modelBatch.queries[0].reset.entityIds = ['m2']
  next.modelBatch.entities = next.modelBatch.entities.map(e => e.entityId === 'project' ? entity('project', 'Changed') : e)
  sockets[1].message(next)
  assert.equal(handle.data.getSnapshot(), before)
  markReady(sockets[1], handle)
  const after = handle.data.getSnapshot()
  assert.equal(after.row.entity.values.title.textValue, 'Changed')
  assert.equal(after.row.entity.values.obsolete, undefined)
  assert.equal(after.row.children.memberships.rows.length, 1)
  assert.equal(after.row.children.memberships.rows[0], rows[1])
  client.clear()
  assert.equal(handle.data.getSnapshot(), undefined)
  assert.equal(handle.view.getSnapshot().status, 'closed')
  client.connect(); sockets[2].open()
  assert.equal(sockets[2].sent.length, 0)
})

test('replacement pending and subscription failure never retain stale payloads', t => {
  const { client, sockets } = setup(t)
  const handle = client.subscribeProjection(request)
  client.connect(); sockets[0].open()
  sockets[0].message(snapshot(handle)); markReady(sockets[0], handle)
  client.disconnect(); client.connect(); sockets[1].open()
  const next = snapshot(handle)
  next.modelBatch.entities = next.modelBatch.entities.map(e => e.entityId === 'shared' ? { entityId: 'shared', pending: {} } : e)
  sockets[1].message(next); markReady(sockets[1], handle)
  assert.equal(handle.data.getSnapshot().row.children.memberships.rows[0].children.task.row.entity.status, 'pending')
  sockets[1].message({ subscriptionId: handle.id, sequence: '3', error: { code: 'RESYNC_REQUIRED', message: 'denied' } })
  assert.equal(handle.data.getSnapshot(), undefined)
  assert.equal(handle.view.getSnapshot().status, 'error')
})

test('publication listeners see all ready projections updated by the same commit', t => {
  const { client, sockets } = setup(t)
  const a = client.subscribeProjection(request), b = client.subscribeProjection(request)
  client.connect(); sockets[0].open()
  sockets[0].message(snapshot(a)); markReady(sockets[0], a)
  const next = snapshot(b)
  next.modelBatch.projections.forEach(ref => {
    ref.occurrenceId += '-b'
    if (ref.parentOccurrenceId) ref.parentOccurrenceId += '-b'
    if (ref.attached.querySubscriptionId) ref.attached.querySubscriptionId = 'list-b'
  })
  next.modelBatch.queries[0].querySubscriptionId = 'list-b'
  sockets[0].message(next); markReady(sockets[0], b)
  let notifications = 0
  const stop = a.data.subscribe(() => {
    notifications++
    assert.equal(b.data.getSnapshot().row.entity.values.title.textValue, 'Atomic')
  })
  const raw = client.store.observe(store => store.getEntity('project'))
  let rawNotifications = 0
  const stopRaw = raw.subscribe(() => {
    rawNotifications++
    assert.equal(a.data.getSnapshot().row.entity.values.title.textValue, 'Atomic')
    assert.equal(b.data.getSnapshot().row.entity.values.title.textValue, 'Atomic')
  })
  sockets[0].message({ connectionSequence: '1', modelBatch: { entities: [entity('project', 'Atomic')] } })
  assert.equal(notifications, 1)
  assert.equal(rawNotifications, 1)
  stopRaw()
  stop()
  sockets[0].message({ connectionSequence: '2', modelBatch: { entities: [entity('shared', 'Changed shared task')] } })
  const rows = a.data.getSnapshot().row.children.memberships.rows
  assert.equal(rows[0].children.task.row.entity, rows[1].children.task.row.entity)
})

test('publication coordination visits each handle once when a commit affects many projections', t => {
  const { client, sockets } = setup(t)
  const handles = Array.from({ length: 12 }, () => client.subscribeProjection(request))
  client.connect(); sockets[0].open()
  handles.forEach((handle, i) => {
    const image = snapshot(handle)
    image.modelBatch.projections.forEach(ref => {
      ref.occurrenceId += `-${i}`
      if (ref.parentOccurrenceId) ref.parentOccurrenceId += `-${i}`
      if (ref.attached.querySubscriptionId) ref.attached.querySubscriptionId += `-${i}`
    })
    image.modelBatch.queries[0].querySubscriptionId += `-${i}`
    sockets[0].message(image); markReady(sockets[0], handle)
  })
  let visits = 0, notifications = 0
  const original = ProjectionPublication.prototype.stage
  ProjectionPublication.prototype.stage = function (candidate) {
    visits++
    return original.call(this, candidate)
  }
  const stops = handles.map(handle => handle.data.subscribe(() => { notifications++ }))
  try {
    sockets[0].message({ connectionSequence: '1', modelBatch: { entities: [entity('project', 'One commit')] } })
    assert.equal(visits, handles.length)
    assert.equal(notifications, handles.length)
    assert.ok(handles.every(handle => handle.data.getSnapshot().row.entity.values.title.textValue === 'One commit'))
    assert.deepEqual(Object.keys(handles[0]).sort(), ['data', 'dispose', 'id', 'request', 'target', 'updateWindow', 'view'])
    visits = 0
    sockets[0].message({ connectionSequence: '2', modelBatch: { entities: [entity('unrelated', 'No projection uses this')] } })
    assert.equal(visits, 0, 'unrelated changes do not visit registered projections')
  } finally {
    ProjectionPublication.prototype.stage = original
    stops.forEach(stop => stop())
  }
})

test('clear is an exclusive lifecycle boundary, including synchronous listener reentry', t => {
  const { client, sockets } = setup(t)
  const a = client.subscribeProjection(request), b = client.subscribeProjection(request)
  client.connect(); sockets[0].open()
  sockets[0].message(snapshot(a)); markReady(sockets[0], a)
  let attempts = 0
  const stop = a.view.subscribe(() => {
    attempts++
    assert.throws(() => client.subscribeProjection(request), /clearing/)
    client.connect()
    client.clear()
  })
  const connection = client.store.observe(s => s.getConnection())
  const stopConnection = connection.subscribe(() => {
    if (connection.getSnapshot().status === 'disconnected') client.connect()
  })
  client.clear()
  stop(); stopConnection()
  assert.ok(attempts > 0)
  assert.equal(a.data.getSnapshot(), undefined)
  assert.equal(b.view.getSnapshot().status, 'closed')
  assert.equal(client.store.getConnection().status, 'disconnected')
  assert.equal(sockets.length, 1)
  client.connect(); sockets[1].open()
  assert.equal(sockets[1].sent.length, 0)
  const fresh = client.subscribeProjection(request)
  assert.equal(sockets[1].sent[0].subscriptionId, fresh.id)
})

test('connection watchdog recovers a socket that never opens', t => {
  t.mock.timers.enable({ apis: ['setTimeout'] })
  const { client, sockets } = setup(t, { connectionTimeoutMs: 20 })
  client.connect()
  t.mock.timers.tick(20)
  assert.equal(sockets[0].readyState, 3)
  assert.match(client.store.getConnection().error, /Connection timed out/)
  t.mock.timers.tick(5)
  assert.equal(sockets.length, 2)
  sockets[1].open()
  assert.equal(client.store.getConnection().status, 'connected')
})

test('heartbeat detects a silent server, preserves the view, and retires old timers and replies', t => {
  t.mock.timers.enable({ apis: ['setTimeout'] })
  const { client, sockets } = setup(t, { heartbeatIntervalMs: 20, heartbeatTimeoutMs: 10 })
  const handle = client.subscribeProjection(request)
  client.connect(); const socket = sockets[0]; socket.open(); socket.open()
  socket.message(snapshot(handle)); markReady(socket, handle)
  const retained = handle.data.getSnapshot()
  t.mock.timers.tick(20)
  const ping = socket.sent.at(-1)
  assert.equal(typeof ping.ping, 'string')
  assert.equal(socket.sent.filter(message => message.ping).length, 1)
  socket.message({ pong: 'wrong-id' })
  t.mock.timers.tick(10)
  assert.match(client.store.getConnection().error, /Heartbeat timed out/)
  assert.equal(handle.data.getSnapshot(), retained)
  assert.equal(handle.view.getSnapshot().stale, true)
  t.mock.timers.tick(5); sockets[1].open()
  socket.message({ pong: ping.ping })
  assert.equal(client.store.getConnection().status, 'connected')
  client.disconnect()
  t.mock.timers.tick(100000)
  assert.equal(sockets.length, 2)
})

test('matching heartbeat replies maintain idle connections without changing model sequences', t => {
  t.mock.timers.enable({ apis: ['setTimeout'] })
  const { client, sockets } = setup(t, { heartbeatIntervalMs: 20, heartbeatTimeoutMs: 10 })
  const handle = client.subscribeProjection(request)
  client.connect(); const socket = sockets[0]; socket.open()
  socket.message(snapshot(handle)); markReady(socket, handle)
  const retained = handle.data.getSnapshot()
  for (let i = 0; i < 3; i++) {
    t.mock.timers.tick(20)
    socket.message({ pong: socket.sent.at(-1).ping })
  }
  socket.message({ connectionSequence: '1', modelBatch: { entities: [entity('project', 'After idle')] } })
  assert.equal(client.store.getConnection().status, 'connected')
  assert.notEqual(handle.data.getSnapshot(), retained)
  assert.equal(handle.data.getSnapshot().row.entity.values.title.textValue, 'After idle')
})

test('an empty replacement removes the retained root only when ready', t => {
  const { client, sockets } = setup(t)
  const handle = client.subscribeProjection(request)
  client.connect(); sockets[0].open()
  sockets[0].message(snapshot(handle)); markReady(sockets[0], handle)
  const retained = handle.data.getSnapshot()
  client.disconnect(); client.connect(); sockets[1].open()
  assert.equal(handle.data.getSnapshot(), retained)
  markReady(sockets[1], handle, '1')
  assert.equal(handle.data.getSnapshot(), undefined)
  assert.equal(handle.view.getSnapshot().stale, false)
})

test('invalid batches resynchronize instead of exposing partial state', t => {
  const { client, sockets } = setup(t)
  const handle = client.subscribeProjection(request)
  client.connect(); sockets[0].open()
  sockets[0].message(snapshot(handle)); markReady(sockets[0], handle)
  let sawPartial = false
  const view = client.store.observe(s => s.getEntity('partial'))
  const stop = view.subscribe(() => { if (view.getSnapshot().status === 'available') sawPartial = true })
  sockets[0].message({ connectionSequence: '1', modelBatch: { entities: [entity('partial', 'bad')], queries: [{ querySubscriptionId: 'list', delta: { baseVersion: '99', resultVersion: '100' } }] } })
  assert.equal(sawPartial, false)
  assert.equal(handle.view.getSnapshot().status, 'disconnected')
  assert.match(handle.view.getSnapshot().error, /version mismatch/)
  assert.equal(sockets[0].readyState, 3)
  stop()
})

test('window updates resolve only on their own acknowledgment; rejection does not fail projection', async t => {
  const { client, sockets } = setup(t)
  const handle = client.subscribeProjection(request)
  client.connect(); const socket = sockets[0]; socket.open()
  socket.message(snapshot(handle)); markReady(socket, handle)
  let settled = false
  const first = handle.updateWindow('members', [{ side: 'SIDE_RIGHT', operation: 'OPERATION_EXTEND', count: 10 }]).then(() => { settled = true })
  const command = socket.sent.at(-1)
  assert.equal(command.updateProjectionWindow.instanceId, 'members')
  socket.message({ connectionSequence: '1', modelBatch: {} })
  await Promise.resolve()
  assert.equal(settled, false)
  socket.message({ subscriptionId: handle.id, requestId: command.requestId, sequence: '3', modelBatch: {} })
  await first
  const second = handle.updateWindow('members', [{ side: 'SIDE_LEFT', operation: 'OPERATION_CONTRACT', count: 10 }])
  const rejection = assert.rejects(second, /WINDOW_FAILED/)
  socket.message({ subscriptionId: handle.id, requestId: socket.sent.at(-1).requestId, sequence: '4', error: { code: 'WINDOW_FAILED', message: 'obsolete' } })
  await rejection
  assert.equal(handle.view.getSnapshot().status, 'ready')
})

test('a rejected projection does not fail another projection or reconnect the socket', t => {
  const { client, sockets } = setup(t)
  const rejected = client.subscribeProjection(request)
  const healthy = client.subscribeProjection(request)
  client.connect(); const socket = sockets[0]; socket.open()
  socket.message({ subscriptionId: rejected.id, requestId: socket.sent[0].requestId, error: { code: 'INVALID_PROJECTION', message: 'bad definition' } })
  markReady(socket, healthy, '1')
  assert.equal(rejected.view.getSnapshot().status, 'error')
  assert.match(rejected.view.getSnapshot().error, /bad definition/)
  assert.equal(healthy.view.getSnapshot().status, 'ready')
  assert.equal(client.store.getConnection().status, 'connected')
  assert.equal(sockets.length, 1)
})

test('disconnect, timeout, and scope failure settle outstanding requests', async t => {
  const { client, sockets } = setup(t, { requestTimeoutMs: 5 })
  const handle = client.subscribeProjection(request)
  client.connect(); const socket = sockets[0]; socket.open()
  socket.message(snapshot(handle)); markReady(socket, handle)
  await assert.rejects(handle.updateWindow('members', []), /timed out/)
  const operation = handle.updateWindow('members', [])
  const rejected = assert.rejects(operation, /RESYNC_REQUIRED/)
  socket.message({ subscriptionId: handle.id, sequence: '3', error: { code: 'RESYNC_REQUIRED', message: 'failed' } })
  await rejected
  assert.equal(handle.view.getSnapshot().status, 'error')
  await assert.rejects(handle.updateWindow('members', []), /not ready/)
  const healthy = client.subscribeProjection(request)
  const healthyImage = snapshot(healthy)
  healthyImage.modelBatch.projections.forEach(ref => {
    ref.occurrenceId += '-healthy'
    if (ref.parentOccurrenceId) ref.parentOccurrenceId += '-healthy'
    if (ref.attached.querySubscriptionId) ref.attached.querySubscriptionId = 'list-healthy'
  })
  healthyImage.modelBatch.queries[0].querySubscriptionId = 'list-healthy'
  socket.message(healthyImage); markReady(socket, healthy)
  const interrupted = healthy.updateWindow('members-healthy', [])
  const closed = assert.rejects(interrupted, /Connection closed/)
  client.disconnect()
  await closed
})

test('disposal is idempotent, cleans acknowledged scope state, and never replays on reconnect', async t => {
  const { client, sockets } = setup(t)
  const handle = client.subscribeProjection(request)
  client.connect(); const socket = sockets[0]; socket.open()
  socket.message(snapshot(handle)); markReady(socket, handle)
  handle.dispose(); handle.dispose()
  assert.equal(socket.sent.filter(message => message.unsubscribe).length, 1)
  const unsubscribe = socket.sent.at(-1)
  socket.message({ subscriptionId: handle.id, requestId: unsubscribe.requestId, sequence: '3', modelBatch: {
    projections: initial().projections.map(ref => ({ occurrenceId: ref.occurrenceId, detached: {} })),
    queries: [{ querySubscriptionId: 'list', closed: {} }],
    entities: ['project', 'm1', 'm2', 'shared'].map(entityId => ({ entityId, released: {} })),
  } })
  assert.equal(client.store.getEntity('shared').status, 'unavailable')
  assert.equal(client.store.statuses.size, 0)
  assert.equal(client.store.sequences.size, 0)
  await assert.rejects(handle.updateWindow('members', []), /disposed/)
  socket.close(); await delay(15)
  sockets[1].open()
  assert.equal(sockets[1].sent.length, 0)
})

test('disconnect/connect is reusable and does not revive a disposed projection', t => {
  const { client, sockets } = setup(t)
  const first = client.subscribeProjection(request)
  client.connect(); sockets[0].open()
  first.dispose(); client.disconnect()
  const second = client.subscribeProjection(request)
  client.connect(); sockets[1].open()
  assert.equal(sockets[1].sent.length, 1)
  assert.equal(sockets[1].sent[0].subscriptionId, second.id)
  assert.notEqual(first.id, second.id)
})

test('decoder and message-size failures close the generation safely', t => {
  const { client, sockets } = setup(t, { maxMessageBytes: 20 })
  client.connect(); sockets[0].open()
  sockets[0].message({ modelBatch: { entities: [entity('a', 'oversized')] } })
  assert.equal(client.store.getConnection().status, 'disconnected')
  assert.match(client.store.getConnection().error, /oversized/)
})

test('generated input validation rejects local mistakes without resetting healthy projections', async t => {
  const { client, sockets } = setup(t)
  const handle = client.subscribeProjection(request)
  client.connect(); const socket = sockets[0]; socket.open()
  socket.message(snapshot(handle)); markReady(socket, handle)
  assert.throws(() => client.subscribeProjection({ root: { nodeId: 'bad', entity: {}, list: {} } }))
  assert.throws(() => client.subscribeProjection({ root: { nodeId: '', entity: {} } }))
  await assert.rejects(handle.updateWindow('members', [{ side: 'SIDE_RIGHT', operation: 'OPERATION_EXTEND', count: -1 }]))
  assert.equal(client.store.getConnection().status, 'connected')
  assert.equal(handle.view.getSnapshot().status, 'ready')
  assert.equal(socket.sent.length, 1)
  assert.equal(sockets.length, 1)
})

test('connected observers and startup share one installation per generation', async t => {
  const { client, sockets } = setup(t)
  const connection = client.store.observe(s => s.getConnection())
  let handle
  const stop = connection.subscribe(() => {
    if (connection.getSnapshot().status === 'connected' && !handle) handle = client.subscribeProjection(request)
  })
  t.after(stop)
  client.connect(); sockets[0].open(); sockets[0].open()
  assert.equal(sockets[0].sent.length, 1)
  assert.equal(sockets[0].sent[0].subscriptionId, handle.id)
  sockets[0].message(snapshot(handle)); markReady(sockets[0], handle)
  assert.equal(handle.view.getSnapshot().status, 'ready')
  sockets[0].close(); await delay(15)
  sockets[1].open()
  assert.equal(sockets[1].sent.length, 1)
  assert.equal(sockets[1].sent[0].subscriptionId, handle.id)
  assert.notEqual(sockets[1].sent[0].requestId, sockets[0].sent[0].requestId)
})

test('disposing an unsent projection from a connected observer cannot subscribe it afterward', t => {
  const { client, sockets } = setup(t)
  const handle = client.subscribeProjection(request)
  const connection = client.store.observe(s => s.getConnection())
  const stop = connection.subscribe(() => {
    if (connection.getSnapshot().status === 'connected') handle.dispose()
  })
  t.after(stop)
  client.connect(); sockets[0].open()
  assert.equal(sockets[0].sent.length, 0, 'neither subscribe nor unsubscribe is needed for an unsent handle')
  assert.equal(handle.view.getSnapshot().status, 'closed')
})

test('disconnecting from a connecting observer cancels before socket creation', t => {
  const { client, sockets } = setup(t)
  const connection = client.store.observe(s => s.getConnection())
  const stop = connection.subscribe(() => {
    if (connection.getSnapshot().status === 'connecting') client.disconnect()
  })
  t.after(stop)
  client.connect()
  assert.equal(sockets.length, 0)
  assert.equal(client.store.getConnection().status, 'disconnected')
})

test('reconnecting from a connected observer cannot let the retired startup send on the new socket', t => {
  const { client, sockets } = setup(t)
  const handle = client.subscribeProjection(request)
  const connection = client.store.observe(s => s.getConnection())
  let replaced = false
  const stop = connection.subscribe(() => {
    if (connection.getSnapshot().status === 'connected' && !replaced) {
      replaced = true
      client.disconnect(); client.connect()
    }
  })
  t.after(stop)
  client.connect(); sockets[0].open()
  assert.equal(sockets.length, 2)
  assert.equal(sockets[0].sent.length, 0)
  sockets[0].open()
  sockets[0].message({ connectionSequence: '1', modelBatch: { entities: [entity('stale', 'Stale')] } })
  assert.equal(client.store.getEntity('stale').status, 'unavailable')
  sockets[1].open()
  assert.equal(sockets[1].sent.length, 1)
  assert.equal(sockets[1].sent[0].subscriptionId, handle.id)
})

test('socket construction that retires its generation closes the returned obsolete socket', t => {
  const sockets = []
  let client
  client = new ReactiveClient({ url: 'ws://test', createSocket: () => {
    const socket = new FakeSocket()
    sockets.push(socket)
    if (sockets.length === 1) { client.disconnect(); client.connect() }
    return socket
  } })
  t.after(() => client.disconnect())
  const handle = client.subscribeProjection(request)
  client.connect()
  assert.equal(sockets.length, 2)
  assert.equal(sockets[0].readyState, 3)
  sockets[1].open()
  assert.equal(sockets[1].sent.length, 1)
  assert.equal(sockets[1].sent[0].subscriptionId, handle.id)
})

test('listener errors are reported without interrupting connected observers or causing reconnect', t => {
  const errors = []
  const { client, sockets } = setup(t, { onListenerError: error => errors.push(error) })
  const connection = client.store.observe(s => s.getConnection())
  let handle
  const failure = new Error('connected listener failed')
  const stopThrowing = connection.subscribe(() => {
    if (connection.getSnapshot().status === 'connected') throw failure
  })
  const stopHealthy = connection.subscribe(() => {
    if (connection.getSnapshot().status === 'connected') handle = client.subscribeProjection(request)
  })
  t.after(stopThrowing); t.after(stopHealthy)
  client.connect(); sockets[0].open()
  assert.deepEqual(errors, [failure])
  assert.equal(sockets.length, 1)
  assert.equal(sockets[0].sent.length, 1)
  sockets[0].message(snapshot(handle)); markReady(sockets[0], handle)
  assert.equal(handle.view.getSnapshot().status, 'ready')
})

test('send failure cannot let a retired startup loop install on its replacement generation', t => {
  const { client, sockets } = setup(t)
  client.subscribeProjection(request)
  client.subscribeProjection(request)
  const connection = client.store.observe(s => s.getConnection())
  const stop = connection.subscribe(() => {
    if (connection.getSnapshot().status === 'disconnected') client.connect()
  })
  t.after(() => { stop(); client.disconnect() })
  client.connect()
  sockets[0].send = () => { throw new Error('send failed') }
  sockets[0].open()
  assert.equal(sockets.length, 2)
  assert.equal(sockets[0].readyState, 3)
  assert.equal(sockets[1].sent.length, 0)
  sockets[1].open()
  assert.equal(sockets[1].sent.length, 2)
  assert.equal(new Set(sockets[1].sent.map(message => message.subscriptionId)).size, 2)
})

test('disposing a terminally failed scope does not unsubscribe it again or reset healthy scopes', t => {
  const { client, sockets } = setup(t)
  const failed = client.subscribeProjection(request)
  const healthy = client.subscribeProjection(request)
  client.connect(); const socket = sockets[0]; socket.open()
  markReady(socket, failed, '1'); markReady(socket, healthy, '1')
  socket.message({ subscriptionId: failed.id, sequence: '2', error: { code: 'RESYNC_REQUIRED', message: 'scope closed by server' } })
  failed.dispose()
  assert.equal(socket.sent.filter(message => message.unsubscribe).length, 0)
  assert.equal(client.store.getConnection().status, 'connected')
  assert.equal(healthy.view.getSnapshot().status, 'ready')
})

test('a terminal server close racing Unsubscribe retires the scope sequence before its acknowledgment', t => {
  const { client, sockets } = setup(t)
  const failed = client.subscribeProjection(request)
  const healthy = client.subscribeProjection(request)
  client.connect(); const socket = sockets[0]; socket.open()
  markReady(socket, failed, '1'); markReady(socket, healthy, '1')
  failed.dispose()
  const unsubscribe = socket.sent.at(-1)
  socket.message({ subscriptionId: failed.id, sequence: '2', error: { code: 'RESYNC_REQUIRED', message: 'scope closed by server' } })
  socket.message({ subscriptionId: failed.id, requestId: unsubscribe.requestId, sequence: '1', modelBatch: {} })
  assert.equal(client.store.getConnection().status, 'connected')
  assert.equal(healthy.view.getSnapshot().status, 'ready')
  assert.equal(sockets.length, 1)
})
