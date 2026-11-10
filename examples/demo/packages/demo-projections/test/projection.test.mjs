import test from 'node:test'
import assert from 'node:assert/strict'
import { ReactiveClient } from '@adiom-data/weave'
import { ProjectDetails } from '../dist/index.js'
import { FakeSocket, attach, reset } from '../../../../../packages/client/test/helpers.mjs'

const projectId = '10000000-0000-4000-8000-000000000001'
const taskId = '20000000-0000-4000-8000-000000000001'
const instant = '2026-10-27T12:00:00.123456Z'
function image(prefix = '') {
  const entity = (entityId, values) => ({ entityId, upsert: { values } })
  return {
    kind: 'KIND_SNAPSHOT',
    entities: [
      entity('project', { id: { uuidValue: projectId }, title: { textValue: 'Project' } }),
      entity('m1', { position: { decimalValue: '12345678901234567890.123456789' } }),
      entity('m2', { position: { decimalValue: '2' } }),
      entity('task', { id: { uuidValue: taskId }, title: { textValue: 'Task' }, completed: { boolValue: false }, created_at: { datetimeValue: instant }, updated_at: { datetimeValue: instant } }),
    ],
    queries: [reset(prefix + 'list', ['m1', 'm2'])],
    projections: [
      attach(prefix + 'root', 'project', { entityId: 'project' }),
      attach(prefix + 'links', 'project.taskLinks', { querySubscriptionId: prefix + 'list' }, prefix + 'root', 'project'),
      attach(prefix + 'task1', 'project.taskLinks.task', { entityId: 'task' }, prefix + 'links', 'm1'),
      attach(prefix + 'task2', 'project.taskLinks.task', { entityId: 'task' }, prefix + 'links', 'm2'),
    ],
  }
}
function setup() {
  const sockets = []
  const client = new ReactiveClient({ url: 'ws://test', heartbeatIntervalMs: 0, createSocket: () => { const socket = new FakeSocket(); sockets.push(socket); return socket } })
  const handle = ProjectDetails.subscribe(client, { projectId })
  client.connect(); sockets[0].open()
  return { client, handle, sockets }
}
function install(socket, prefix = '') {
  const scope = socket.sent.find(message => message.subscribeProjection).subscriptionId
  const snapshot = image(prefix)
  snapshot.projections.forEach(occurrence => { occurrence.subscriptionId = scope })
  socket.message({ subscriptionId: scope, sequence: '1', modelBatch: snapshot })
  socket.message({ subscriptionId: scope, sequence: '2', ready: {} })
  return scope
}

test('generated bindings encode parameters, decode aliases and retain decimal precision', () => {
  const { client, handle, sockets } = setup()
  try {
    assert.equal(handle.data.getSnapshot(), undefined)
    const request = sockets[0].sent[0].subscribeProjection
    assert.equal(request.parameters.projectId.uuidValue, projectId)
    assert.deepEqual(request.root.children[0].list.bindings.map(binding => binding.parentColumn.segments), [['organization_id'], ['id']])
    install(sockets[0])
    const data = handle.data.getSnapshot()
    assert.equal(data.status, 'available')
    assert.equal(data.value.title, 'Project')
    const link = data.value.taskLinks[0]
    assert.equal(link.value.position, '12345678901234567890.123456789')
    assert.equal(link.value.task.value.completed, false)
    assert.equal(link.value.task.value.createdAt, instant)
    assert.ok(Object.isFrozen(data.value.taskLinks))
    assert.ok(Object.isFrozen(link.value.task.value))
    assert.equal(handle.view.getSnapshot().status, 'ready')
  } finally { handle.dispose(); client.disconnect() }
})

test('typed diamond readers share optimism and preserve their references on matching confirmation', () => {
  const { client, handle, sockets } = setup()
  try {
    install(sockets[0])
    sockets[0].message({ connectionOpened: { connectionContext: 'context' } })
    client.registerOptimisticReceiptDecoder(() => ['edit'])
    let calls = 0
    const stop = handle.data.subscribe(() => { calls++ })
    const optimistic = client.beginOptimistic({ mutationId: 'edit', patches: [{ entityId: 'task', set: { title: { textValue: 'Predicted' } } }] })
    const predicted = handle.data.getSnapshot()
    for (const link of predicted.value.taskLinks) assert.equal(link.value.task.value.title, 'Predicted')
    assert.equal(calls, 1)
    const values = { ...image().entities.find(entity => entity.entityId === 'task').upsert.values, title: { textValue: 'Predicted' } }
    sockets[0].message({ connectionSequence: '1', modelBatch: { kind: 'KIND_COMMIT', atomic: true,
      entities: [{ entityId: 'task', upsert: { values } }], messages: [{ kind: 'confirm' }] } })
    optimistic.rollback()
    assert.equal(handle.data.getSnapshot(), predicted)
    assert.equal(calls, 1, 'matching confirmation does not invalidate typed readers')
    stop()
  } finally { handle.dispose(); client.disconnect() }
})

test('window controls target the list occurrence and follow replacement IDs after reconnect', async () => {
  const { client, handle, sockets } = setup()
  try {
    const scope = install(sockets[0])
    const before = handle.data.getSnapshot()
    const control = handle.window(before.value.taskLinks)
    assert.equal(control.bounds.getSnapshot().left.sentinel, 'SENTINEL_START')
    assert.equal(control.bounds.getSnapshot().right.sentinel, 'SENTINEL_END')
    let boundsChanges = 0
    const stopBounds = control.bounds.subscribe(() => { boundsChanges++ })
    const update = control.extend('SIDE_LEFT', 20)
    const sent = sockets[0].sent.at(-1)
    assert.equal(sent.updateProjectionWindow.instanceId, 'links')
    sockets[0].message({ subscriptionId: scope, requestId: sent.requestId, sequence: '3', modelBatch: { kind: 'KIND_WINDOW', queries: [
      { querySubscriptionId: 'list', windowChanged: { left: { cursor: 'AQI=' }, right: { sentinel: 'SENTINEL_END' } } },
    ] } })
    await update
    assert.equal(control.bounds.getSnapshot().left.cursor, 'AQI=')
    assert.equal(boundsChanges, 1)
    const move = control.move('SIDE_LEFT', control.bounds.getSnapshot().left.cursor)
    const moved = sockets[0].sent.at(-1)
    assert.equal(moved.updateProjectionWindow.updates[0].cursor, 'AQI=')
    sockets[0].message({ subscriptionId: scope, requestId: moved.requestId, sequence: '4', modelBatch: { kind: 'KIND_WINDOW' } })
    await move
    assert.equal(boundsChanges, 1, 'unchanged bounds must not notify')
    client.disconnect()
    assert.equal(control.bounds.getSnapshot(), undefined)
    assert.equal(boundsChanges, 2)
    await assert.rejects(control.extend('SIDE_LEFT', 1), /no active server window/)
    assert.equal(handle.data.getSnapshot(), before)
    client.connect(); sockets[1].open(); const nextScope = install(sockets[1], 'new-')
    assert.equal(handle.data.getSnapshot(), before, 'identical reconnect should retain typed display references')
    assert.equal(control.bounds.getSnapshot().left.sentinel, 'SENTINEL_START')
    const next = control.extend('SIDE_RIGHT', 5)
    const message = sockets[1].sent.at(-1)
    assert.equal(message.updateProjectionWindow.instanceId, 'new-links')
    sockets[1].message({ subscriptionId: nextScope, requestId: message.requestId, sequence: '3', modelBatch: { kind: 'KIND_WINDOW' } })
    await next
    assert.throws(() => handle.window([]), /does not belong/)
    stopBounds()
  } finally { handle.dispose(); client.disconnect() }
})

test('unselected changes preserve typed data, row identities and view notifications', () => {
  const { client, handle, sockets } = setup()
  try {
    install(sockets[0])
    const before = handle.data.getSnapshot(), view = handle.view.getSnapshot()
    let dataChanges = 0, viewChanges = 0
    const stopData = handle.data.subscribe(() => { dataChanges++ })
    const stopView = handle.view.subscribe(() => { viewChanges++ })
    const task = image().entities[3]
    task.upsert.values.extra = { textValue: 'selected only by some other projection' }
    sockets[0].message({ connectionSequence: '1', modelBatch: { kind: 'KIND_COMMIT', entities: [task] } })
    assert.equal(handle.data.getSnapshot(), before)
    assert.equal(handle.view.getSnapshot(), view)
    assert.equal(dataChanges, 0); assert.equal(viewChanges, 0)
    task.upsert.values.title = { textValue: 'Changed' }
    sockets[0].message({ connectionSequence: '2', modelBatch: { kind: 'KIND_COMMIT', entities: [task] } })
    assert.equal(handle.data.getSnapshot().value.taskLinks[0].value.task.value.title, 'Changed')
    assert.equal(dataChanges, 1); assert.equal(viewChanges, 1)
    const rows = handle.data.getSnapshot().value.taskLinks
    sockets[0].message({ connectionSequence: '3', modelBatch: { kind: 'KIND_COMMIT', queries: [{ querySubscriptionId: 'list', delta: {
      baseVersion: '1', resultVersion: '2', removedEntityIds: ['m2'], insertions: [{ entityId: 'm2', finalIndex: 0 }],
    } }] } })
    const reordered = handle.data.getSnapshot().value.taskLinks
    assert.equal(reordered[0], rows[1]); assert.equal(reordered[1], rows[0])
    assert.doesNotThrow(() => handle.window(reordered))
    stopData(); stopView()
  } finally { handle.dispose(); client.disconnect() }
})

test('pending hydration, shared entities and terminal removal retain honest availability', () => {
  const { client, handle, sockets } = setup()
  try {
    install(sockets[0])
    const before = handle.data.getSnapshot()
    sockets[0].message({ connectionSequence: '1', modelBatch: { kind: 'KIND_COMMIT', entities: [{ entityId: 'task', pending: {} }] } })
    const pending = handle.data.getSnapshot()
    assert.equal(pending.value.taskLinks[0].value.task.status, 'pending')
    assert.equal(pending.value.taskLinks[1].value.task.status, 'pending')
    assert.equal(pending.value.taskLinks[0].value.task.value, undefined)
    assert.equal(before.value.taskLinks[0].value.task.value.title, 'Task')
    sockets[0].message({ connectionSequence: '2', modelBatch: { kind: 'KIND_COMMIT', entities: [image().entities[3]] } })
    assert.equal(handle.data.getSnapshot().value.taskLinks[0].value.task.value.title, 'Task')
    handle.dispose()
    assert.equal(handle.data.getSnapshot(), undefined)
  } finally { handle.dispose(); client.disconnect() }
})
