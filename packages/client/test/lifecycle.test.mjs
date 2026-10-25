import test from 'node:test'
import assert from 'node:assert/strict'
import { ModelStore, ReactiveClient } from '../dist/index.js'
import { FakeSocket, batch, entity, initial, request } from './helpers.mjs'

function setup(t, options = {}) {
  const sockets = [], errors = []
  const client = new ReactiveClient({ url: 'ws://test?access_token=SECRET', heartbeatIntervalMs: 0,
    onListenerError: error => errors.push(error), createSocket: () => { const socket = new FakeSocket(); sockets.push(socket); return socket }, ...options })
  t.after(() => client.clear())
  const handle = client.subscribeProjection(request)
  client.connect(); sockets[0].open()
  sockets[0].message({ connectionOpened: { connectionContext: 'context' } })
  const image = initial(); image.projections.forEach(value => { value.subscriptionId = handle.id })
  sockets[0].message({ subscriptionId: handle.id, sequence: '1', modelBatch: image })
  sockets[0].message({ subscriptionId: handle.id, sequence: '2', ready: {} })
  return { client, handle, sockets, errors }
}

test('failure policy receives close details; clear is terminal and drops retained state', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] })
  const events = [], failures = []
  const { client, handle, sockets } = setup(t, { onDiagnostic: event => events.push(event), failurePolicy: failure => { failures.push(failure); return 'clear' } })
  client.beginOptimistic({ mutationId: 'm', patches: [{ entityId: 'project', set: { title: { textValue: 'Prediction' } } }] })
  const pending = handle.updateWindow('members', [{ side: 'RIGHT', operation: 'EXTEND', count: 1 }])
  const rejected = assert.rejects(pending, /Connection closed/)
  sockets[0].onclose({ code: 4401, reason: 'SECRET reason', wasClean: true })
  await rejected
  assert.equal(failures.length, 1)
  assert.deepEqual(failures[0].close, { code: 4401, reason: 'SECRET reason', wasClean: true })
  assert.equal(handle.view.getSnapshot().status, 'closed')
  assert.equal(handle.data.getSnapshot(), undefined)
  const diagnostics = client.getDiagnostics()
  assert.equal(diagnostics.activeProjections, 0)
  assert.equal(diagnostics.pendingWindowRequests, 0)
  assert.equal(diagnostics.store.optimistic.layers, 0)
  assert.equal(diagnostics.store.trackedKeys, 0)
  assert.ok(Object.isFrozen(diagnostics.store.entities))
  await Promise.resolve()
  assert.equal(JSON.stringify([diagnostics, events]).includes('SECRET'), false, 'diagnostics never copy URLs, arbitrary errors, or close reasons')
  assert.ok(events.some(event => event.type === 'failure' && event.action === 'clear'))
  t.mock.timers.tick(100000)
  assert.equal(sockets.length, 1)
})

test('pause retains only authoritative data, rejects old requests, and permits explicit reconnect', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] })
  const { client, handle, sockets } = setup(t, { failurePolicy: () => 'pause' })
  client.beginOptimistic({ mutationId: 'm', patches: [{ entityId: 'project', set: { title: { textValue: 'Prediction' } } }] })
  sockets[0].close()
  assert.equal(handle.view.getSnapshot().stale, true)
  assert.equal(handle.data.getSnapshot().row.entity.values.title.textValue, 'Project')
  t.mock.timers.tick(100000)
  assert.equal(sockets.length, 1)
  client.connect()
  assert.equal(sockets.length, 2)
  assert.equal(client.getDiagnostics().connectionAttempts, 2)
})

test('failure-policy reentry cannot clear a replacement connection; diagnostic callbacks are isolated', async t => {
  let current
  const { client, sockets, errors } = setup(t, { failurePolicy: () => { current.connect(); return 'clear' }, onDiagnostic: () => { throw new Error('diagnostic observer') } })
  current = client
  sockets[0].close()
  assert.equal(sockets.length, 2)
  assert.equal(client.getDiagnostics().activeProjections, 1)
  assert.equal(client.store.getConnection().status, 'connecting')
  await Promise.resolve()
  assert.ok(errors.length > 0)
  assert.ok(errors.every(error => error.message === 'diagnostic observer'))
})

test('policy errors pause safely; unknown transport errors do not fabricate authentication details', t => {
  const failures = []
  const { client, sockets, errors } = setup(t, { failurePolicy: failure => { failures.push(failure); throw new Error('bad policy') } })
  sockets[0].onerror({})
  assert.equal(failures[0].kind, 'transport')
  assert.equal(failures[0].close, undefined)
  assert.equal(client.getDiagnostics().retriesScheduled, 0)
  assert.equal(client.store.getConnection().status, 'disconnected')
  assert.equal(errors[0].message, 'bad policy')
})

test('an installed policy returning no action pauses rather than falling back to retry', t => {
  const { client, sockets, errors } = setup(t, { failurePolicy: () => undefined })
  sockets[0].close()
  assert.equal(client.getDiagnostics().retriesScheduled, 0)
  assert.equal(client.getDiagnostics().connection, 'disconnected')
  assert.match(errors[0].message, /Invalid connection failure action/)
})

test('nested connection failures retain independent diagnostic summaries', async t => {
  const events = []
  let client, sockets
  const current = setup(t, { onDiagnostic: event => events.push(event), failurePolicy: failure => {
    if (failure.kind === 'close') { client.connect(); sockets.at(-1).onerror({}) }
    return 'pause'
  } })
  ;({ client, sockets } = current)
  sockets[0].onclose({ code: 4401, reason: 'Expired', wasClean: true })
  await Promise.resolve()
  const failures = events.filter(event => event.type === 'failure')
  assert.deepEqual(failures.map(event => event.failure.kind), ['transport', 'close'])
  assert.equal(failures[1].failure.closeCode, 4401)
  assert.equal(client.getDiagnostics().lastFailure.kind, 'transport', 'latest failure is not overwritten by the returning policy')
})

test('diagnostics count committed store state without scanning or changing it', () => {
  const store = new ModelStore()
  store.apply(batch({ entities: [entity('a', 'A'), { entityId: 'b', pending: {} }] }))
  assert.deepEqual(store.getDiagnostics().entities, { available: 1, pending: 1 })
  assert.throws(() => store.apply(batch({ entities: [entity('b', 'B')], queries: [{ querySubscriptionId: 'missing', delta: { baseVersion: '1', resultVersion: '2' } }] })))
  assert.deepEqual(store.getDiagnostics().entities, { available: 1, pending: 1 })
  store.apply(batch({ entities: [{ entityId: 'a', released: {} }, entity('b', 'B')] }))
  assert.deepEqual(store.getDiagnostics().entities, { available: 1, pending: 0 })
  store.reset()
  assert.deepEqual(store.getDiagnostics().entities, { available: 0, pending: 0 })
})

test('repeated subscription disposal and reconnect release dependency state and ignore obsolete callbacks', t => {
  const { client, handle, sockets } = setup(t)
  handle.dispose()
  for (let round = 0; round < 60; round++) {
    const old = sockets.at(-1)
    const late = old.onmessage
    const readers = Array.from({ length: 8 }, () => client.subscribeProjection(request))
    const stops = readers.map(reader => reader.data.subscribe(() => {}))
    readers.forEach(reader => reader.dispose())
    stops.forEach(stop => stop())
    client.disconnect()
    const counts = client.getDiagnostics()
    assert.equal(counts.activeProjections, 0)
    assert.equal(counts.store.trackedKeys, 0)
    assert.equal(counts.store.occurrences, 0)
    assert.equal(counts.store.queries, 0)
    assert.equal(counts.store.subscriptions, 0)
    client.connect(); sockets.at(-1).open()
    sockets.at(-1).message({ connectionOpened: { connectionContext: `round-${round}` } })
    late({ data: new ArrayBuffer(0) })
    assert.equal(client.connectionContext, `round-${round}`)
    assert.equal(client.getDiagnostics().failures, 0)
  }
})

test('watchdog failures are classified without treating them as authentication failures', t => {
  t.mock.timers.enable({ apis: ['setTimeout'] })
  const failures = []
  const sockets = []
  const client = new ReactiveClient({ url: 'ws://test', connectionTimeoutMs: 5, heartbeatIntervalMs: 5, heartbeatTimeoutMs: 7,
    failurePolicy: failure => { failures.push(failure.kind); return 'pause' },
    createSocket: () => { const socket = new FakeSocket(); sockets.push(socket); return socket } })
  t.after(() => client.clear())
  client.connect()
  t.mock.timers.tick(5)
  assert.deepEqual(failures, ['connection-timeout'])
  client.connect(); sockets[1].open()
  t.mock.timers.tick(5)
  assert.ok(sockets[1].sent.at(-1).ping)
  t.mock.timers.tick(7)
  assert.deepEqual(failures, ['connection-timeout', 'heartbeat-timeout'])
  assert.equal(client.getDiagnostics().retriesScheduled, 0)
})
