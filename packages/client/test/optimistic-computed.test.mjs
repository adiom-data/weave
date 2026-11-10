import test from 'node:test'
import assert from 'node:assert/strict'
import { ReactiveClient, defineProjection, unionOptimisticShapes, invalidateComputed } from '../dist/index.js'
import { FakeSocket, initial, request, entity, attach, reset } from './helpers.mjs'

const root = structuredClone(request.root)
root.children[0].children[0].computed = ['canEdit', 'role']
root.children[0].computed = ['canEdit']
root.children.push({ nodeId: 'shortcut', entity: { query: { table: 'tasks' } }, computed: ['canEdit'] })
const field = (name, path, kind = 'text', computed = false) => ({ name, path, type: { kind }, computed })
const definition = defineProjection(root, { nodeId: 'project', kind: 'entity', fields: [], children: {
  shortcut: { nodeId: 'shortcut', kind: 'entity', fields: [field('editable', ['canEdit'], 'bool', true)], children: {} },
  memberships: { nodeId: 'memberships', kind: 'list', fields: [field('editable', ['canEdit'], 'bool', true)], children: {
    task: { nodeId: 'task', kind: 'entity', fields: [field('title', ['title']), field('alias', ['title']), field('label', ['data', 'label']), field('editable', ['canEdit'], 'bool', true), field('role', ['role'], 'integer', true)], children: {} },
  } },
} }, [])
function setup(t, count = 1, diamond = true) {
  const sockets = []
  const client = new ReactiveClient({ url: 'ws://test', heartbeatIntervalMs: 0, createSocket: () => { const socket = new FakeSocket(); sockets.push(socket); return socket } })
  t.after(() => client.clear())
  client.connect(); sockets[0].open()
  sockets[0].message({ connectionOpened: { connectionContext: 'one' } })
  const handles = Array.from({ length: count }, (_, index) => {
    const handle = definition.subscribe(client, {})
    const snapshot = initial()
    if (!diamond) { snapshot.projections[3].attached.entityId = 'other'; snapshot.entities.push(entity('other', 'Other', { data: { objectValue: { fields: { label: { textValue: 'Other' } } } } })) }
    for (const value of snapshot.projections) {
      value.subscriptionId = handle.id
      value.occurrenceId = `${index}-${value.occurrenceId}`
      if (value.parentOccurrenceId) value.parentOccurrenceId = `${index}-${value.parentOccurrenceId}`
      if (value.attached.querySubscriptionId) value.attached.querySubscriptionId = `${index}-list`
    }
    snapshot.queries[0].querySubscriptionId = `${index}-list`
    snapshot.entities[3] = entity('shared', 'Shared task', { data: { objectValue: { fields: { label: { textValue: 'Label' }, other: { textValue: 'Keep' } } } } })
    snapshot.computed = [1, 2].map(n => ({ occurrenceId: `${index}-task${n}`, entityId: !diamond && n === 2 ? 'other' : 'shared', upsert: { fields: { canEdit: { boolValue: n === 2 }, role: { integerValue: String(n) } } } }))
    snapshot.computed.push(...[1, 2].map(n => ({ occurrenceId: `${index}-members`, entityId: `m${n}`, upsert: { fields: { canEdit: { boolValue: n === 2 } } } })))
    sockets[0].message({ subscriptionId: handle.id, sequence: '1', modelBatch: snapshot })
    sockets[0].message({ subscriptionId: handle.id, sequence: '2', ready: {} })
    return handle
  })
  return { client, handles, socket: sockets[0] }
}
const tasks = handle => handle.data.getSnapshot().value.memberships.map(row => row.value.task)
const fill = fields => ({ memberships: { task: fields } })
const shape = (handle, entities = tasks(handle)) => unionOptimisticShapes(entities, { project: handle })

test('one draft patches two entities, inserts stored data, and fills distinct named projection computations', t => {
  const { client, handles: [a, b] } = setup(t, 2, false)
  const [taskA, taskB] = tasks(a)
  const draft = client.optimistic({ mutationId: 'combined' })
  draft.patch(taskA, { title: 'A' }); draft.patch(taskB, { title: 'B' })
  const inserted = draft.insert(a.data.getSnapshot().value.memberships, { task: { title: 'New', label: 'New label' } })
  const affected = unionOptimisticShapes([taskA, taskB, inserted], { project: a, assigned: b })
  let calls = 0
  draft.fill(affected, {
    project: fill({ editable: row => { calls++; return row.current.title === 'A' }, role: () => 10n }),
    assigned: fill({ editable: row => { calls++; return row.current.title !== 'A' } }),
  })
  assert.equal(calls, 5)
  assert.deepEqual(tasks(a).map(task => task.value.title), ['Shared task', 'Other'], 'draft is private')
  const layer = draft.commit()
  assert.deepEqual(tasks(a).map(task => [task.value.title, task.value.editable]), [['A', true], ['B', false], ['New', false]])
  assert.deepEqual(tasks(b).map(task => [task.value.title, task.value.editable]), [['A', false], ['B', true]])
  assert.equal(tasks(a)[2].value.role, 10n)
  assert.equal(client.store.getEntity(inserted.value.task.entityId).values.canEdit, undefined)
  layer.rollback()
  assert.equal(tasks(a).length, 2)
  assert.deepEqual(tasks(a).map(task => task.value.title), ['Shared task', 'Other'])
})

test('stored entity metadata survives its originating projection; fill still requires a live destination', t => {
  const { client, handles: [origin, active] } = setup(t, 2)
  const task = tasks(origin)[0]
  origin.dispose()
  const draft = client.optimistic({ mutationId: 'retained-reference' })
  draft.patch(task, { title: 'Predicted after disposal' })
  const affected = unionOptimisticShapes([task], { active })
  draft.fill(affected, { active: fill({ editable: () => false }) })
  assert.throws(() => unionOptimisticShapes([task], { origin }), /disposed/)
  const layer = draft.commit()
  assert.equal(tasks(active)[0].value.title, 'Predicted after disposal')
  assert.equal(tasks(active)[0].value.editable, false)
  layer.rollback()
  assert.equal(tasks(active)[0].value.title, 'Shared task')

  const doomed = client.optimistic({ mutationId: 'destination-closed' })
  doomed.fill(affected, { active: fill({ editable: () => true }) })
  active.dispose()
  assert.throws(() => doomed.commit(), /disposed/)
})

test('one-row fill reads only its payload in a large list and reuses decoded immutable fields', t => {
  const socket = new FakeSocket()
  const client = new ReactiveClient({ url: 'ws://test', createSocket: () => socket, heartbeatIntervalMs: 0 })
  t.after(() => client.clear())
  client.connect(); socket.open(); socket.message({ connectionOpened: { connectionContext: 'large' } })
  const definition = defineProjection({ nodeId: 'tasks', list: { query: { table: 'tasks' }, window: { count: 1000 } }, computed: ['canEdit'] }, {
    nodeId: 'tasks', kind: 'list', children: {}, fields: [
      field('title', ['title']), field('canEdit', ['canEdit'], 'bool', true),
      { name: 'meta', path: ['meta'], type: { kind: 'object', fields: [field('label', ['label'])] } },
    ],
  }, [])
  const handle = definition.subscribe(client, {})
  const ids = Array.from({ length: 1000 }, (_, index) => `task-${index}`)
  socket.message({ subscriptionId: handle.id, sequence: '1', modelBatch: { kind: 'KIND_SNAPSHOT',
    entities: ids.map(id => entity(id, id, { meta: { objectValue: { fields: { label: { textValue: 'Label' } } } } })),
    queries: [reset('large-query', ids)], projections: [attach('large-list', 'tasks', { querySubscriptionId: 'large-query' }, '', '', handle.id)],
    computed: ids.map(entityId => ({ occurrenceId: 'large-list', entityId, upsert: { fields: { canEdit: { boolValue: true } } } })),
  } })
  socket.message({ subscriptionId: handle.id, sequence: '2', ready: {} })
  const task = handle.data.getSnapshot()[500]
  const affected = unionOptimisticShapes([task], { list: handle })
  const reads = []
  // Trace the authoritative source, shared by public reads and private staged
  // views, rather than depending on either view calling the other's facade.
  const entities = client.store.entities
  const getEntity = entities.get.bind(entities)
  entities.get = id => { reads.push(id); return getEntity(id) }
  const warm = client.optimistic({ mutationId: 'warm' })
  warm.fill(affected, { list: { canEdit: row => { assert.equal(row.current.meta, task.value.meta); return false } } })
  assert.deepEqual([...new Set(reads)], [task.entityId])
  const layer = warm.commit(); layer.rollback()
  reads.length = 0
  const staged = client.optimistic({ mutationId: 'staged' })
  staged.patch(task, { title: 'Staged title' })
  staged.fill(affected, { list: { canEdit: row => { assert.equal(row.current.title, 'Staged title'); return false } } })
  assert.deepEqual([...new Set(reads)], [task.entityId], 'staged reads must not materialize unrelated payloads')
  const predicted = staged.commit()
  assert.equal(handle.data.getSnapshot()[500].value.title, 'Staged title')
  predicted.rollback()
})

function collectionSetup(t, missing = false) {
  const socket = new FakeSocket()
  const client = new ReactiveClient({ url: 'ws://test', createSocket: () => socket, heartbeatIntervalMs: 0 })
  t.after(() => client.clear())
  const labels = { kind: 'object', fields: [{ name: 'labels', path: ['labels'], type: { kind: 'list', element: { kind: 'text' } } }] }
  const definition = defineProjection({ nodeId: 'task', entity: { query: { table: 'tasks' } }, computed: ['canEdit', 'details'] }, {
    nodeId: 'task', kind: 'entity', children: {}, fields: [
      { name: 'meta', path: ['meta'], type: labels }, field('editable', ['canEdit'], 'bool', true),
      { name: 'details', path: ['details'], type: labels, computed: true },
    ],
  }, [])
  const value = (...labels) => ({ objectValue: { fields: { labels: { listValue: { values: labels.map(textValue => ({ textValue })) } } } } })
  client.connect(); socket.open(); socket.message({ connectionOpened: { connectionContext: 'collections' } })
  const handle = definition.subscribe(client, {})
  const update = (fields, sequence = '1') => socket.message({ connectionSequence: sequence, modelBatch: {
    kind: 'KIND_COMMIT', atomic: true, computed: [{ occurrenceId: 'task-occurrence', entityId: 'task', upsert: { fields } }],
  } })
  socket.message({ subscriptionId: handle.id, sequence: '1', modelBatch: {
    kind: 'KIND_SNAPSHOT', entities: [{ entityId: 'task', upsert: { values: missing ? {} : { meta: value('Stored') } } }],
    projections: [attach('task-occurrence', 'task', { entityId: 'task' }, '', '', handle.id)],
    computed: [{ occurrenceId: 'task-occurrence', entityId: 'task', upsert: { fields: { ...(missing ? {} : { canEdit: { boolValue: true } }), details: value('Computed') } } }],
  } })
  socket.message({ subscriptionId: handle.id, sequence: '2', ready: {} })
  return { client, handle, socket, value, update }
}

for (const prediction of [false, invalidateComputed]) test(`computed ${typeof prediction === 'symbol' ? 'invalidation' : 'prediction'} does not re-decode unchanged stored collections`, t => {
  const { client, handle, socket, value, update } = collectionSetup(t)
  const original = handle.data.getSnapshot().value.meta
  const affected = unionOptimisticShapes([handle.data.getSnapshot()], { task: handle })
  const prior = client.optimistic({ mutationId: 'prior' })
  prior.fill(affected, { task: { editable: () => prediction } })
  const installed = prior.commit()
  for (let index = 0; index < 100; index++) {
    const draft = client.optimistic({ mutationId: `fill-${index}` })
    draft.fill(affected, { task: { editable: row => { assert.equal(row.current.meta, original); return undefined } } })
  }
  const staged = client.optimistic({ mutationId: 'staged' })
  staged.fill(affected, { task: { details: () => ({ labels: ['Predicted'] }) } })
  for (let index = 0; index < 3; index++) staged.fill(affected, { task: { editable: row => {
    assert.equal(row.current.meta, original)
    assert.deepEqual(row.current.details.labels, ['Predicted'])
    return undefined
  } } })
  update({ canEdit: { boolValue: false }, details: value('New computed') })
  assert.equal(handle.data.getSnapshot().value.meta, original)
  assert.deepEqual(handle.data.getSnapshot().value.details.labels, ['New computed'])
  socket.message({ connectionSequence: '2', modelBatch: { kind: 'KIND_COMMIT', atomic: true, entities: [
    { entityId: 'task', upsert: { values: { meta: value('New stored') } } },
  ] } })
  const changed = handle.data.getSnapshot().value.meta
  assert.notEqual(changed, original)
  assert.deepEqual(changed.labels, ['New stored'])
  installed.rollback()
  assert.equal(handle.data.getSnapshot().value.meta, changed)
})

test('stored updates reuse decoded computed collections', t => {
  const { client, handle, socket, value } = collectionSetup(t)
  const original = handle.data.getSnapshot().value.details
  const affected = unionOptimisticShapes([handle.data.getSnapshot()], { task: handle })
  const staged = client.optimistic({ mutationId: 'stored-patch' })
  staged.patch(handle.data.getSnapshot(), { meta: { labels: ['Changed'] } })
  staged.fill(affected, { task: { editable: row => { assert.equal(row.current.details, original); return undefined } } })
  socket.message({ connectionSequence: '1', modelBatch: { kind: 'KIND_COMMIT', atomic: true, entities: [
    { entityId: 'task', upsert: { values: { meta: value('Server') } } },
  ] } })
  assert.equal(handle.data.getSnapshot().value.details, original)
})

test('strict reads never reuse provisional field decoding', t => {
  const missing = collectionSetup(t, true)
  const layer = missing.client.beginOptimistic({ mutationId: 'missing-computed', computed: [{
    target: missing.handle.target(), entityId: 'task', invalidate: ['canEdit'],
  }] })
  assert.equal(missing.handle.data.getSnapshot().status, 'optimistic')
  assert.equal(missing.handle.data.getSnapshot().value.meta, undefined)
  missing.update({ canEdit: { boolValue: true }, details: missing.value('Available') })
  assert.throws(() => missing.handle.data.getSnapshot(), /task.meta: required field/)
  layer.rollback()
})

test('nested fills read only matching paths and do not cross pending ancestors', t => {
  const { client, handles: [handle], socket } = setup(t, 1, false)
  const task = tasks(handle)[0]
  const affected = shape(handle, [task])
  const reads = []
  const get = client.store.getEntity.bind(client.store)
  client.store.getEntity = id => { reads.push(id); return get(id) }
  const draft = client.optimistic({ mutationId: 'nested-sparse' })
  draft.fill(affected, { project: fill({ editable: () => false }) })
  assert.deepEqual(new Set(reads), new Set(['project', 'm1', 'shared']))
  const prediction = draft.commit(); prediction.rollback()

  socket.message({ connectionSequence: '1', modelBatch: { kind: 'KIND_HYDRATION', entities: [{ entityId: 'm1', pending: {} }] } })
  let callbacks = 0
  const pending = client.optimistic({ mutationId: 'pending-ancestor' })
  pending.fill(affected, { project: fill({ editable: () => { callbacks++; return true } }) })
  assert.equal(callbacks, 0)
  assert.equal(pending.hasChanges, false)
})

test('a selected parent callback still receives its full staged child context', t => {
  const { client, handles: [handle] } = setup(t)
  const member = handle.data.getSnapshot().value.memberships[0]
  const task = member.value.task
  const draft = client.optimistic({ mutationId: 'parent-context' })
  draft.patch(task, { title: 'Predicted child' })
  draft.fill(shape(handle, [member]), { project: { memberships: { editable: row => row.current.task.value.title === 'Predicted child' } } })
  const layer = draft.commit()
  assert.equal(handle.data.getSnapshot().value.memberships[0].value.editable, true)
  layer.rollback()
  assert.equal(handle.data.getSnapshot().value.memberships[0].value.editable, false)
})

for (const backing of ['authoritative', 'temporary', 'pending', 'retired-temporary']) test(`staged and committed child slots agree for ${backing} backing`, t => {
  const { client, handles: [handle], socket } = setup(t)
  let task = tasks(handle)[0], creation
  if (backing.includes('temporary')) {
    const creator = client.optimistic({ mutationId: 'creator' })
    task = creator.entity({ title: { textValue: 'Temporary' } })
    creation = creator.commit()
    if (backing === 'retired-temporary') creation.rollback()
  }
  let deletion
  if (backing === 'authoritative' || backing === 'temporary') deletion = client.beginOptimistic({ mutationId: 'delete', delete: [task.entityId] })
  if (backing === 'pending') socket.message({ connectionSequence: '1', modelBatch: { kind: 'KIND_HYDRATION', entities: [{ entityId: task.entityId, pending: {} }] } })
  const draft = client.optimistic({ mutationId: 'association' })
  const inserted = draft.insert(handle.data.getSnapshot().value.memberships, { task })
  let preview
  draft.fill(shape(handle, [inserted]), { project: { memberships: { editable: row => {
    preview = row.current.task?.status ?? 'missing'
    return preview === 'unavailable'
  } } } })
  const predicted = draft.commit()
  const committed = handle.data.getSnapshot().value.memberships.at(-1)
  assert.equal(committed.value.task?.status ?? 'missing', preview)
  assert.equal(preview, backing === 'authoritative' || backing === 'temporary' ? 'unavailable' : 'missing')
  assert.equal(committed.value.editable, preview === 'unavailable')
  predicted.rollback(); deletion?.rollback(); creation?.rollback()
})

for (const kind of ['slot', 'list']) test(`staged creation activates an earlier dormant ${kind} just as committing does`, t => {
  const { client, handles: [handle] } = setup(t)
  const creator = client.optimistic({ mutationId: 'creator' })
  const reference = creator.entity(kind === 'slot' ? { title: { textValue: 'New task' } } : {})
  const association = client.optimistic({ mutationId: 'association' })
  const inserted = association.insert(handle.data.getSnapshot().value.memberships, kind === 'slot' ? { task: reference } : reference)
  const membership = association.commit()
  if (kind === 'slot') assert.equal(handle.data.getSnapshot().value.memberships.at(-1).value.task, undefined)
  else assert.equal(handle.data.getSnapshot().value.memberships.length, 2)
  let preview
  creator.fill(shape(handle, [inserted]), { project: { memberships: { editable: row => {
    preview = kind === 'slot' ? row.current.task.value.title : row.entityId
    return true
  } } } })
  assert.equal(preview, kind === 'slot' ? 'New task' : reference.entityId)
  const creation = creator.commit()
  const committed = handle.data.getSnapshot().value.memberships.at(-1)
  assert.equal(kind === 'slot' ? committed.value.task.value.title : committed.entityId, preview)
  assert.equal(committed.value.editable, true)
  creation.rollback(); membership.rollback()
})

for (const scenario of [
  { name: 'inherited prediction', base: 'predicted' },
  { name: 'inherited invalidation', base: 'stale' },
  { name: 'new prediction over invalidation', base: 'stale', staged: 'predicted' },
  { name: 'new invalidation over prediction', base: 'predicted', staged: 'stale' },
]) test(`staged computed context preserves ${scenario.name} and unrelated field metadata`, t => {
  const { client, handles: [handle] } = setup(t)
  const prior = client.beginOptimistic({ mutationId: 'prior-computed', computed: [{
    target: handle.target('task', ['project', 'm1']), entityId: 'shared',
    set: { role: { integerValue: '9' }, ...(scenario.base === 'predicted' ? { canEdit: { boolValue: true } } : {}) },
    ...(scenario.base === 'stale' ? { invalidate: ['canEdit'] } : {}),
  }] })
  const member = handle.data.getSnapshot().value.memberships[0], task = member.value.task
  const draft = client.optimistic({ mutationId: 'next-computed' })
  draft.patch(task, { title: 'Staged title' })
  if (scenario.staged) draft.fill(shape(handle, [task]), { project: fill({ editable: () => scenario.staged === 'stale' ? invalidateComputed : false }) })
  let preview
  draft.fill(shape(handle, [member]), { project: { memberships: { editable: row => {
    preview = { states: row.current.task.computedState, title: row.current.task.value.title, role: row.current.task.value.role, editable: row.current.task.value.editable }
    return preview.states?.editable !== 'stale'
  } } } })
  assert.deepEqual({ ...preview.states }, { editable: scenario.staged ?? scenario.base, role: 'predicted' })
  assert.equal(preview.title, 'Staged title')
  assert.equal(preview.role, 9n)
  const prediction = draft.commit()
  const committed = handle.data.getSnapshot().value.memberships[0]
  assert.deepEqual(committed.value.task.computedState, preview.states)
  assert.equal(committed.value.task.value.editable, preview.editable)
  assert.equal(committed.value.editable, preview.states.editable !== 'stale')
  prediction.rollback()
  assert.equal(tasks(handle)[0].computedState.editable, scenario.base)
  prior.rollback()
  assert.equal(tasks(handle)[0].computedState, undefined)
})

test('duplicate entities coalesce, while diamonds and distinct handles retain separate computed slots', t => {
  const { client, handles: [a, b] } = setup(t, 2)
  const affected = unionOptimisticShapes([...tasks(a), ...tasks(a)], { first: a, second: b })
  let calls = 0
  const draft = client.optimistic({ mutationId: 'diamond' })
  draft.fill(affected, { first: fill({ editable: row => { calls++; return row.ancestors.at(-1) === 'm1' } }), second: fill({ editable: () => { calls++; return false } }) })
  const layer = draft.commit()
  assert.equal(calls, 4)
  assert.deepEqual(tasks(a).map(task => task.value.editable), [true, false])
  assert.deepEqual(tasks(b).map(task => task.value.editable), [false, false])
  layer.rollback()
})

test('same computed name at different paths in one projection has separate fillers', t => {
  const { client, handles: [handle], socket } = setup(t)
  socket.message({ connectionSequence: '1', modelBatch: { kind: 'KIND_HYDRATION',
    projections: [attach('shortcut', 'shortcut', { entityId: 'shared' }, '0-root', 'project', handle.id)],
    computed: [{ occurrenceId: 'shortcut', entityId: 'shared', upsert: { fields: { canEdit: { boolValue: false } } } }],
  } })
  const draft = client.optimistic({ mutationId: 'paths' })
  draft.fill(shape(handle), { project: { ...fill({ editable: () => false }), shortcut: { editable: () => true } } })
  const layer = draft.commit()
  assert.equal(tasks(handle)[0].value.editable, false)
  assert.equal(handle.data.getSnapshot().value.shortcut.value.editable, true)
  layer.rollback()
  assert.equal(handle.data.getSnapshot().value.shortcut.value.editable, false)
})

test('fill structure is owned before callbacks; invalid later projections invoke no callbacks', t => {
  const { client, handles: [a, b] } = setup(t, 2)
  const draft = client.optimistic({ mutationId: 'owned' })
  const affected = unionOptimisticShapes(tasks(a), { a, b })
  let calls = 0
  assert.throws(() => draft.fill(affected, { a: fill({ editable: () => { calls++; return true } }), b: fill({ typo: () => true }) }), /Unknown computed/)
  assert.equal(calls, 0)
  const input = { a: fill({ editable: () => { calls++; input.a.memberships.task.editable = () => false; return true } }) }
  draft.fill(affected, input)
  const layer = draft.commit()
  assert.equal(calls, 2)
  assert.deepEqual(tasks(a).map(row => row.value.editable), [true, true])
  layer.rollback()
})

test('blank predictions are no-ops; explicit invalidation retains live authoritative values', t => {
  const { client, handles: [a, b], socket } = setup(t, 2)
  const draft = client.optimistic({ mutationId: 'stale' })
  draft.fill(shape(a), { project: fill({ editable: () => undefined }) })
  assert.equal(draft.hasChanges, false)
  draft.fill(shape(a), { project: fill({ editable: () => invalidateComputed }) })
  const layer = draft.commit()
  assert.equal(tasks(a)[0].computedState.editable, 'stale')
  assert.equal(tasks(b)[0].computedState, undefined)
  socket.message({ connectionSequence: '1', modelBatch: { kind: 'KIND_COMMIT', atomic: true, computed: [{ occurrenceId: '0-task1', entityId: 'shared', upsert: { fields: { canEdit: { boolValue: true }, role: { integerValue: '1' } } } }] } })
  assert.equal(tasks(a)[0].value.editable, true)
  assert.equal(tasks(a)[0].computedState.editable, 'stale')
  layer.rollback()
  assert.equal(tasks(a)[0].computedState, undefined)
})

test('insertion rejects computed values and computed omissions stay optional', t => {
  const { client, handles: [handle] } = setup(t)
  const draft = client.optimistic({ mutationId: 'insert' })
  const list = handle.data.getSnapshot().value.memberships
  assert.throws(() => draft.insert(list, { task: { title: 'Invalid', editable: false } }), /computed field/)
  const inserted = draft.insert(list, { task: { title: 'No prediction' } })
  assert.ok(inserted.value.task.entityId)
  const layer = draft.commit()
  assert.equal(tasks(handle)[2].status, 'optimistic')
  assert.equal(tasks(handle)[2].value.editable, undefined)
  assert.equal(tasks(handle)[2].computedState, undefined)
  layer.rollback()
})

test('typed nested patches preserve unmentioned object fields and authoritative updates', t => {
  const { client, handles: [handle], socket } = setup(t)
  const draft = client.optimistic({ mutationId: 'nested' })
  draft.patch(tasks(handle)[0], { label: 'Predicted' })
  const layer = draft.commit()
  socket.message({ connectionSequence: '1', modelBatch: { kind: 'KIND_COMMIT', atomic: true, entities: [entity('shared', 'Updated', { data: { objectValue: { fields: { label: { textValue: 'Server' }, other: { textValue: 'New other' } } } } })] } })
  assert.equal(tasks(handle)[0].value.label, 'Predicted')
  assert.equal(client.store.getEntity('shared').values.data.objectValue.fields.other.textValue, 'New other')
  layer.rollback()
  assert.equal(tasks(handle)[0].value.label, 'Server')
})

test('receipt retirement combines entity and computed updates into one publication', t => {
  const { client, handles: [a, b], socket } = setup(t, 2)
  const older = client.optimistic({ mutationId: 'older' })
  older.fill(shape(a), { project: fill({ editable: () => true }) })
  const old = older.commit()
  const newer = client.optimistic({ mutationId: 'newer' })
  newer.patch(tasks(a)[0], { title: 'Prediction' })
  newer.fill(unionOptimisticShapes(tasks(a), { a, b }), { a: fill({ editable: () => false }), b: fill({ editable: () => false }) })
  newer.commit()
  client.registerOptimisticReceiptDecoder(() => ['newer'])
  const seen = []
  const stop = a.data.subscribe(() => seen.push([tasks(a)[0].value.editable, tasks(b)[0].value.editable, tasks(a)[0].value.title]))
  socket.message({ connectionSequence: '1', modelBatch: { kind: 'KIND_COMMIT', atomic: true, messages: [{ kind: 'receipt', payload: '' }], entities: [entity('shared', 'Confirmed', { data: { objectValue: { fields: { label: { textValue: 'Label' } } } } })], computed: [{ occurrenceId: '1-task1', entityId: 'shared', upsert: { fields: { canEdit: { boolValue: true }, role: { integerValue: '1' } } } }] } })
  assert.deepEqual(seen, [[true, true, 'Confirmed']])
  assert.equal(tasks(a)[0].computedState.editable, 'predicted')
  assert.equal(tasks(b)[0].computedState, undefined)
  stop(); old.rollback()
})

test('invalid shapes, callbacks and values cannot partially stage predictions', t => {
  const one = setup(t), two = setup(t)
  const [handle] = one.handles
  assert.throws(() => unionOptimisticShapes(tasks(handle), { a: handle, b: handle }), /Duplicate concrete/)
  assert.throws(() => unionOptimisticShapes(tasks(handle), { a: handle, b: two.handles[0] }), /same client/)
  const draft = one.client.optimistic({ mutationId: 'invalid' })
  assert.throws(() => draft.fill(shape(handle), { project: fill({ editable: () => true, role: () => { throw new Error('callback') } }) }), /callback/)
  assert.equal(draft.hasChanges, false)
  assert.throws(() => draft.fill(shape(handle), { project: fill({ editable: () => 'wrong type' }) }), /expected bool/)
  assert.throws(() => draft.fill(shape(handle), { project: fill({ editable: async () => { throw new Error('async failure') } }) }), /synchronous/)
  assert.throws(() => draft.fill(shape(handle), { project: fill({ editable: () => { draft.commit(); return true } }) }), /fill callback/)
  assert.throws(() => draft.fill(shape(handle), { project: fill({ typo: () => true }) }), /Unknown computed/)
  assert.throws(() => draft.fill(shape(two.handles[0]), {}), /does not belong/)
  const saved = shape(handle)
  handle.dispose()
  assert.throws(() => draft.fill(saved, {}), /disposed/)
  assert.equal(draft.hasChanges, false)
})

test('partial errors and disconnect retain authoritative computations, never predictions', t => {
  const { client, handles: [handle], socket } = setup(t)
  const draft = client.optimistic({ mutationId: 'partial' })
  draft.fill(shape(handle), { project: fill({ editable: () => true }) })
  draft.commit()
  const expired = client.optimistic({ mutationId: 'expired' })
  const saved = shape(handle)
  socket.message({ connectionSequence: '1', modelBatch: { kind: 'KIND_COMMIT', atomic: true, projections: [{ occurrenceId: '0-task1', detached: {} }], branches: [{ subscriptionId: handle.id, occurrenceId: '0-task1', nodeId: 'task', parentOccurrenceId: '0-members', parentEntityId: 'm1', entityId: 'shared', error: { code: 'EVALUATION_FAILED', message: 'failed' } }] } })
  assert.equal(tasks(handle)[0].value.editable, false)
  assert.equal(tasks(handle)[0].computedState, undefined)
  assert.equal(tasks(handle)[1].computedState.editable, 'predicted')
  client.disconnect()
  assert.equal(tasks(handle)[1].computedState, undefined)
  assert.throws(() => expired.fill(saved, {}), /expired connection/)
})
