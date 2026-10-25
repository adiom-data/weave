import test from 'node:test'
import assert from 'node:assert/strict'
import { ModelStore, ReactiveClient, defineProjection } from '../dist/index.js'
import { readProjection } from '../dist/projection.js'
import { FakeSocket, batch, entity, initial, request, reset, attach } from './helpers.mjs'

const text = value => ({ textValue: value })
const title = (store, id = 'a') => store.getEntity(id).values.title.textValue
const patch = (entityId, title) => ({ entityId, set: { title: text(title) } })

test('one layer creates a nested graph, shares temporary records, and retires atomically', () => {
  const store = new ModelStore()
  store.apply(batch(initial()))
  const task = store.temporaryEntity(), link = store.temporaryEntity()
  const target = store.projectionTarget('p', 'memberships', ['project'], 'list')
  const slot = store.projectionTarget('p', 'task', ['project', link.entityId], 'entity')
  const secondSlot = store.projectionTarget('p', 'task', ['project', 'm1'], 'entity')
  const values = { title: text('New task') }
  const read = () => readProjection(store, 'p', request).root.row.children.memberships
  const prediction = store.beginOptimistic({ mutationId: 'create',
    create: [{ entity: task, values }, { entity: link, values: {} }],
    lists: [{ target, type: 'insert', entityId: link.entityId, position: 'end' }],
    slots: [{ target: slot, entityId: task.entityId }, { target: secondSlot, entityId: task.entityId }],
  })
  values.title.textValue = 'Mutated input'
  const rows = read().rows
  assert.deepEqual(rows.map(row => row.entityId), ['m1', 'm2', link.entityId])
  assert.equal(rows[2].entity.provisional, true)
  assert.equal(rows[2].children.task.occurrenceId, undefined)
  assert.equal(rows[2].children.task.row.entity, rows[0].children.task.row.entity)
  assert.equal(rows[2].children.task.row.entity.values.title.textValue, 'New task')
  assert.equal(store.getAuthoritativeEntity(task.entityId).status, 'unavailable')
  assert.deepEqual(read().query.ids, ['m1', 'm2'], 'authoritative query state stays untouched')
  const seen = []
  const selection = store.observe(read)
  selection.subscribe(() => seen.push(selection.getSnapshot()))
  store.apply(batch({ kind: 'COMMIT', atomic: true,
    entities: [entity('real-link', 'Link'), entity('real-task', 'Actual task')],
    queries: [reset('list', ['m1', 'm2', 'real-link'], '2')],
    projections: [attach('real-child', 'task', { entityId: 'real-task' }, 'members', 'real-link')],
  }), { retireOptimistic: ['create'] })
  assert.equal(seen.length, 1)
  assert.deepEqual(seen[0].rows.map(row => row.entityId), ['m1', 'm2', 'real-link'])
  assert.equal(seen[0].rows[0].children.task.row.entityId, 'shared')
  assert.equal(seen[0].rows[2].children.task.row.entity.values.title.textValue, 'Actual task')
  assert.equal(store.getEntity(task.entityId).status, 'unavailable')
  prediction.rollback()
  assert.equal(seen.length, 1)
})

test('list operations compose explicitly; missing backing and anchors stay dormant', () => {
  const store = new ModelStore()
  const target = store.projectionTarget('p', 'items', [], 'list')
  const read = () => store.projectionSelection('p', 'items', [], ['a', 'b', 'c']).ids
  store.beginOptimistic({ mutationId: 'pending', patches: [patch('x', 'Predicted')],
    lists: [{ target, type: 'insert', entityId: 'x', position: { after: 'b' } }] })
  assert.deepEqual(read(), ['a', 'b', 'c'])
  store.apply(batch({ entities: [entity('x', 'Loaded')] }))
  assert.deepEqual(read(), ['a', 'b', 'x', 'c'])
  assert.equal(title(store, 'x'), 'Predicted')
  const layer = store.beginOptimistic({ mutationId: 'edit-list', lists: [
    { target, type: 'move', entityId: 'a', position: 'end' },
    { target, type: 'remove', entityId: 'b' },
    { target, type: 'insert', entityId: 'x', position: { before: 'c' } },
  ] })
  assert.deepEqual(read(), ['x', 'c', 'a'], 'inserting an existing reference does not duplicate it')
  layer.rollback()
  assert.deepEqual(read(), ['a', 'b', 'x', 'c'])
  const anchor = store.beginOptimistic({ mutationId: 'anchor', lists: [{ target, type: 'move', entityId: 'c', position: { before: 'missing' } }] })
  assert.deepEqual(read(), ['a', 'b', 'x', 'c'])
  anchor.rollback()
  store.apply(batch({ kind: 'COMMIT', atomic: true }), { retireOptimistic: ['pending'] })
  assert.deepEqual(read(), ['a', 'b', 'c'])
  assert.equal(title(store, 'x'), 'Loaded')
})

test('deletion hides all list references and clears child expansions without touching base membership', () => {
  const store = new ModelStore()
  store.apply(batch(initial()))
  const read = () => readProjection(store, 'p', request).root
  const deletion = store.beginOptimistic({ mutationId: 'delete', delete: ['shared', 'm2', 'unloaded'] })
  const rows = read().row.children.memberships.rows
  assert.deepEqual(rows.map(row => row.entityId), ['m1'])
  assert.equal(rows[0].children.task.row.entity.status, 'unavailable')
  assert.deepEqual(Object.keys(rows[0].children.task.row.children), [])
  assert.deepEqual(store.getQuery('list').ids, ['m1', 'm2'])
  store.apply(batch({ entities: [entity('unloaded', 'Arrived')] }))
  assert.equal(store.getEntity('unloaded').status, 'unavailable')
  deletion.rollback()
  assert.equal(title(store, 'unloaded'), 'Arrived')
  assert.equal(read().row.children.memberships.rows.length, 2)
})

test('slot assignments retain deleted references instead of falling back to the previous entity', () => {
  for (const temporary of [false, true]) for (const deleteFirst of [false, true]) {
    const store = new ModelStore()
    const request = { root: { nodeId: 'root', entity: { query: { table: 'items' } } } }
    store.apply(batch({ entities: [entity('a', 'A'), entity('b', 'B')], projections: [attach('r', 'root', { entityId: 'a' })] }))
    const target = store.projectionTarget('p', 'root', [], 'entity')
    let id = 'b'
    if (temporary) {
      const reference = store.temporaryEntity()
      id = reference.entityId
      store.beginOptimistic({ mutationId: 'create', create: [{ entity: reference, values: { title: text('B') } }] })
    }
    const read = () => readProjection(store, 'p', request).root.row
    let deletion
    if (deleteFirst) deletion = store.beginOptimistic({ mutationId: 'delete', delete: [id] })
    const assignment = store.beginOptimistic({ mutationId: 'select', slots: [{ target, entityId: id }] })
    if (!deleteFirst) {
      assert.equal(read().entityId, id)
      assert.equal(read().entity.status, 'available')
      deletion = store.beginOptimistic({ mutationId: 'delete', delete: [id] })
    }
    assert.equal(read().entityId, id, `retain reference: temporary=${temporary}, deleteFirst=${deleteFirst}`)
    assert.equal(read().entity.status, 'unavailable')
    deletion.rollback()
    assert.equal(read().entityId, id)
    assert.equal(read().entity.values.title.textValue, 'B')
    assignment.rollback()
    assert.equal(read().entityId, 'a')
  }
})

test('unbacked slot changes remain dormant and become unavailable when deleted backing arrives', () => {
  const store = new ModelStore()
  const request = { root: { nodeId: 'root', entity: { query: { table: 'items' } } } }
  store.apply(batch({ entities: [entity('a', 'A')], projections: [attach('r', 'root', { entityId: 'a' })] }))
  const target = store.projectionTarget('p', 'root', [], 'entity')
  const read = () => readProjection(store, 'p', request).root.row
  const selection = store.observe(read), seen = []
  selection.subscribe(() => seen.push(selection.getSnapshot()))
  store.beginOptimistic({ mutationId: 'm', delete: ['b'], slots: [{ target, entityId: 'b' }] })
  assert.equal(read().entityId, 'a', 'an unbacked delete does not invent backing')
  seen.length = 0
  store.apply(batch({ entities: [entity('b', 'B')] }))
  assert.equal(seen.length, 1, 'backing arrival invalidates a dormant slot')
  assert.equal(seen[0].entityId, 'b')
  assert.equal(seen[0].entity.status, 'unavailable')
  seen.length = 0
  store.apply(batch({ kind: 'COMMIT', atomic: true }), { retireOptimistic: ['m'] })
  assert.equal(seen.length, 1)
  assert.equal(seen[0].entityId, 'a', 'whole-layer retirement restores the authoritative slot')
})

test('targets and temporary refs are client-owned, and invalid layers never partially apply', () => {
  const store = new ModelStore(), other = new ModelStore()
  const temp = store.temporaryEntity(), foreign = other.temporaryEntity()
  const target = other.projectionTarget('p', 'items', [], 'list')
  assert.throws(() => store.beginOptimistic({ mutationId: 'bad', create: [{ entity: temp, values: {} }], lists: [{ target, type: 'insert', entityId: temp.entityId, position: 'end' }] }), /target from this client/)
  assert.equal(store.getEntity(temp.entityId).status, 'unavailable')
  assert.throws(() => store.beginOptimistic({ mutationId: 'bad', create: [{ entity: foreign, values: {} }] }), /reference from this client/)
  const layer = store.beginOptimistic({ mutationId: 'good', create: [{ entity: temp, values: {} }] })
  assert.throws(() => store.beginOptimistic({ mutationId: 'duplicate', create: [{ entity: temp, values: {} }] }), /unused/)
  layer.rollback()
  assert.equal(store.getEntity(temp.entityId).status, 'unavailable')
})

test('optimistic values are owned, field-level, and separate from authoritative state', () => {
  const store = new ModelStore()
  store.apply(batch({ entities: [entity('a', 'Base', { extra: text('kept'), 'odd.name': text('literal'), nested: { objectValue: { fields: { old: text('old') } } } })], queries: [reset('q', ['a'])] }))
  const before = store.getAuthoritativeEntity('a'), query = store.getQuery('q')
  const set = { title: text('Predicted'), nested: { objectValue: { fields: { next: text('next') } } } }
  const handle = store.beginOptimistic({ mutationId: 'm1', patches: [{ entityId: 'a', set, unset: ['odd.name'] }] })
  set.title.textValue = 'caller mutation'
  set.nested.objectValue.fields.next.textValue = 'caller mutation'
  assert.equal(title(store), 'Predicted')
  assert.equal(store.getAuthoritativeEntity('a'), before)
  assert.equal(store.getQuery('q'), query, 'optimism does not infer list membership or order')
  assert.equal(store.getEntity('a').values.extra.textValue, 'kept')
  assert.equal(store.getEntity('a').values['odd.name'], undefined)
  assert.equal(store.getEntity('a').values.nested.objectValue.fields.old, undefined, 'structured values replace whole')
  assert.equal(store.getEntity('a').values.nested.objectValue.fields.next.textValue, 'next')
  assert.ok(Object.isFrozen(store.getEntity('a').values.nested.objectValue.fields))
  handle.rollback(); handle.rollback()
  assert.equal(title(store), 'Base')
})

test('ordered overlapping layers rebase on CDC and retire independently', () => {
  const store = new ModelStore()
  store.apply(batch({ entities: [entity('a', 'Base', { completed: { boolValue: false } })] }))
  const first = store.beginOptimistic({ mutationId: 'm1', patches: [patch('a', 'First')] })
  const second = store.beginOptimistic({ mutationId: 'm2', patches: [{ entityId: 'a', set: { title: text('Second'), completed: { boolValue: true } } }] })
  store.apply(batch({ kind: 'COMMIT', atomic: true, entities: [entity('a', 'Remote', { completed: { boolValue: false }, extra: text('new') })] }))
  assert.equal(title(store), 'Second')
  assert.equal(store.getEntity('a').values.extra.textValue, 'new')
  store.apply(batch({ kind: 'COMMIT', atomic: true }), { retireOptimistic: ['m1'] })
  assert.equal(title(store), 'Second')
  first.rollback()
  second.rollback()
  assert.equal(title(store), 'Remote')
})

test('matching predictions retain references and never notify effective-value readers', () => {
  const store = new ModelStore()
  store.apply(batch({ entities: [entity('a', 'Base')] }))
  const selection = store.observe(store => store.getEntity('a'))
  const authoritative = store.observe(store => store.getAuthoritativeEntity('a'))
  let visibleCalls = 0, baseCalls = 0
  selection.subscribe(() => { visibleCalls++ })
  authoritative.subscribe(() => { baseCalls++ })
  const original = selection.getSnapshot()
  store.beginOptimistic({ mutationId: 'same', patches: [patch('a', 'Base')] })
  assert.equal(selection.getSnapshot(), original)
  assert.equal(visibleCalls, 0)
  const handle = store.beginOptimistic({ mutationId: 'change', patches: [patch('a', 'Predicted')] })
  const predicted = selection.getSnapshot()
  assert.equal(visibleCalls, 1)
  store.apply(batch({ kind: 'COMMIT', atomic: true, entities: [entity('a', 'Predicted')] }), { retireOptimistic: ['same', 'change'] })
  handle.rollback()
  assert.equal(selection.getSnapshot(), predicted)
  assert.equal(visibleCalls, 1)
  assert.equal(baseCalls, 1)
})

test('authoritative changes, query membership, and overlay retirement publish atomically', () => {
  const store = new ModelStore()
  store.apply(batch({ entities: [entity('a', 'Base'), entity('b', 'Other')], queries: [reset('q', ['a'])] }))
  store.beginOptimistic({ mutationId: 'm', patches: [patch('a', 'Predicted')] })
  const seen = []
  const selection = store.observe(store => ({ title: title(store), ids: store.getQuery('q').ids }))
  selection.subscribe(() => { seen.push(selection.getSnapshot()) })
  store.apply(batch({ kind: 'COMMIT', atomic: true, entities: [entity('a', 'Actual')], queries: [reset('q', ['b', 'a'], '2')] }), { retireOptimistic: ['m'] })
  assert.deepEqual(seen, [{ title: 'Actual', ids: ['b', 'a'] }])
})

test('validation and invalid CDC leave optimistic and authoritative state untouched', () => {
  const store = new ModelStore()
  store.apply(batch({ entities: [entity('a', 'Base')], queries: [reset('q', ['a'])] }))
  const handle = store.beginOptimistic({ mutationId: 'm', patches: [patch('a', 'Predicted')] })
  assert.throws(() => store.beginOptimistic({ mutationId: 'm', patches: [patch('a', 'Duplicate')] }), /unique/)
  assert.throws(() => store.beginOptimistic({ mutationId: 'bad', patches: [{ entityId: 'a', set: { title: {} } }] }))
  assert.throws(() => store.beginOptimistic({ mutationId: 'bad', patches: [{ entityId: 'a', set: { title: text('Bad') }, unset: ['title'] }] }), /disjoint/)
  assert.throws(() => store.apply(batch({ kind: 'COMMIT', atomic: true, entities: [entity('a', 'Bad')], queries: [{ querySubscriptionId: 'q', delta: { baseVersion: '5', resultVersion: '6' } }] }), { retireOptimistic: ['m'] }), /version/)
  assert.throws(() => store.apply(batch({ kind: 'SNAPSHOT', entities: [entity('a', 'Bad')] }), { retireOptimistic: ['m'] }), /atomic commit/)
  assert.equal(title(store), 'Predicted')
  assert.equal(store.getAuthoritativeEntity('a').values.title.textValue, 'Base')
  handle.rollback()
  assert.equal(title(store), 'Base')
})

test('unbacked predictions are dormant until backing arrives; old handles cannot remove new layers', () => {
  for (const event of [{ pending: {} }, { released: {} }]) {
    const store = new ModelStore()
    store.apply(batch({ entities: [entity('a', 'Base')] }))
    store.beginOptimistic({ mutationId: 'old', patches: [patch('a', 'Still pending')] })
    store.apply(batch({ entities: [{ entityId: 'a', ...event }] }))
    assert.notEqual(store.getEntity('a').status, 'available')
    store.apply(batch({ entities: [entity('a', 'Reloaded')] }))
    assert.equal(title(store), 'Still pending')
    store.apply(batch({ kind: 'COMMIT', atomic: true }), { retireOptimistic: ['old'] })
    assert.equal(title(store), 'Reloaded')
  }
  const store = new ModelStore()
  store.apply(batch({ entities: [entity('a', 'Base')] }))
  const old = store.beginOptimistic({ mutationId: 'm', patches: [patch('a', 'Old')] })
  store.reset()
  store.apply(batch({ entities: [entity('a', 'New base')] }))
  store.beginOptimistic({ mutationId: 'm', patches: [patch('a', 'New layer')] })
  old.rollback()
  assert.equal(title(store), 'New layer')
})

function setup(t) {
  const sockets = [], errors = []
  const client = new ReactiveClient({ url: 'ws://test', heartbeatIntervalMs: 0, reconnectDelayMs: 100000,
    onListenerError: error => errors.push(error), createSocket: () => { const socket = new FakeSocket(); sockets.push(socket); return socket } })
  const handle = client.subscribeProjection(request)
  client.connect(); sockets[0].open()
  sockets[0].message({ connectionOpened: { connectionContext: 'context' } })
  sockets[0].message({ subscriptionId: handle.id, sequence: '1', modelBatch: { ...initial(), projections: initial().projections.map(value => ({ ...value, subscriptionId: handle.id })) } })
  sockets[0].message({ subscriptionId: handle.id, sequence: '2', ready: {} })
  t.after(() => { handle.dispose(); client.disconnect() })
  return { client, handle, sockets, errors }
}

test('disconnect rolls back graph membership and temporary slots across projections', t => {
  const { client, handle, sockets } = setup(t)
  const second = client.subscribeProjection(request)
  sockets[0].message({ subscriptionId: second.id, sequence: '1', modelBatch: {
    kind: 'SNAPSHOT', projections: [attach('second-root', 'project', { entityId: 'project' }, '', '', second.id)],
  } })
  sockets[0].message({ subscriptionId: second.id, sequence: '2', ready: {} })
  const task = client.temporaryEntity(), link = client.temporaryEntity()
  client.beginOptimistic({ mutationId: 'graph',
    create: [{ entity: task, values: { title: text('Temporary') } }, { entity: link, values: {} }],
    delete: ['m1'],
    lists: [{ target: handle.target('memberships', ['project']), type: 'insert', entityId: link.entityId, position: 'start' }],
    slots: [{ target: handle.target('task', ['project', link.entityId]), entityId: task.entityId },
      { target: second.target(), entityId: task.entityId }],
  })
  assert.deepEqual(handle.data.getSnapshot().row.children.memberships.rows.map(row => row.entityId), [link.entityId, 'm2'])
  assert.equal(client.store.getEntity(task.entityId).provisional, true)
  assert.equal(second.data.getSnapshot().row.entityId, task.entityId)
  assert.equal(second.data.getSnapshot().row.entity.values.title.textValue, 'Temporary')
  assert.throws(() => handle.target('task', ['wrong-depth']), /ancestor/)
  client.disconnect()
  assert.deepEqual(handle.data.getSnapshot().row.children.memberships.rows.map(row => row.entityId), ['m1', 'm2'])
  assert.equal(client.store.getEntity(task.entityId).status, 'unavailable')
  assert.equal(second.data.getSnapshot().row.entityId, 'project')
  second.dispose()
})

test('synthetic nested lists are displayable, targetable, and never pretend to have server windows', async t => {
  const { client, sockets } = setup(t)
  const definition = defineProjection(request.root, {
    nodeId: 'project', kind: 'entity', fields: [], children: {
      members: { nodeId: 'memberships', kind: 'list', fields: [], children: {} },
    },
  }, [])
  const handle = definition.subscribe(client, {})
  t.after(() => handle.dispose())
  sockets[0].message({ subscriptionId: handle.id, sequence: '1', ready: {} })
  const parent = client.temporaryEntity(), child = client.temporaryEntity()
  const layer = client.beginOptimistic({ mutationId: 'nested',
    create: [{ entity: parent, values: {} }, { entity: child, values: {} }],
    slots: [{ target: handle.target(), entityId: parent.entityId }],
    lists: [{ target: handle.target('memberships', [parent.entityId]), type: 'insert', entityId: child.entityId, position: 'end' }],
  })
  const children = handle.data.getSnapshot().value.members
  assert.equal(children[0].entityId, child.entityId)
  await assert.rejects(handle.window(children).extend('RIGHT', 1), /no active server window/)
  const removal = client.beginOptimistic({ mutationId: 'remove-child', lists: [{ target: handle.target(children), type: 'remove', entityId: child.entityId }] })
  assert.equal(handle.data.getSnapshot().value.members.length, 0)
  removal.rollback()
  assert.equal(handle.data.getSnapshot().value.members.length, 1)
  layer.rollback()
  assert.equal(handle.data.getSnapshot(), undefined)
  client.beginOptimistic({ mutationId: 'again', create: [{ entity: parent, values: {} }], slots: [{ target: handle.target(), entityId: parent.entityId }] })
  assert.equal(handle.data.getSnapshot().status, 'optimistic')
  client.disconnect()
  assert.equal(handle.data.getSnapshot(), undefined, 'no speculative empty-root fallback survives disconnect')
})

test('typed provisional records permit missing fields but keep authoritative decoding strict', t => {
  const { client, sockets } = setup(t)
  const definition = defineProjection({ nodeId: 'item', entity: { query: { table: 'tasks' } } }, {
    nodeId: 'item', kind: 'entity', children: {}, fields: [
      { name: 'title', path: ['title'], type: { kind: 'text' } },
      { name: 'id', path: ['id'], type: { kind: 'uuid' } },
      { name: 'entityId', path: ['entityId'], type: { kind: 'text' } },
      { name: 'status', path: ['status'], type: { kind: 'text' } },
      { name: 'metadata', path: ['metadata'], type: { kind: 'object', fields: [
        { name: 'entityId', path: ['entityId'], type: { kind: 'text' } },
        { name: 'status', path: ['status'], type: { kind: 'text' } },
        { name: 'required', path: ['required'], type: { kind: 'text' } },
      ] } },
    ],
  }, [])
  const handle = definition.subscribe(client, {})
  t.after(() => handle.dispose())
  sockets[0].message({ subscriptionId: handle.id, sequence: '1', ready: {} })
  const temporary = client.temporaryEntity()
  const layer = client.beginOptimistic({ mutationId: 'provisional', create: [{ entity: temporary, values: {
    title: text('Title'), metadata: { objectValue: { fields: { entityId: text('ordinary stored field') } } },
  } }],
    slots: [{ target: handle.target(), entityId: temporary.entityId }],
  })
  const value = handle.data.getSnapshot()
  assert.equal(value.status, 'optimistic')
  assert.equal(value.value.title, 'Title')
  assert.equal(value.value.id, undefined)
  assert.equal(value.value.entityId, undefined)
  assert.equal(value.value.status, undefined)
  assert.equal(value.value.metadata.entityId, 'ordinary stored field')
  assert.equal(value.value.metadata.status, undefined)
  assert.equal(value.value.metadata.required, undefined)
  assert.equal(value.entityId, temporary.entityId)
  layer.rollback()
  assert.equal(handle.data.getSnapshot(), undefined)
  sockets[0].message({ connectionSequence: '1', modelBatch: { kind: 'COMMIT',
    entities: [entity('real', 'Title')], projections: [attach('real-root', 'item', { entityId: 'real' }, '', '', handle.id)],
  } })
  assert.throws(() => handle.data.getSnapshot(), /id/)
})

test('application receipt decoding retires overlays before projection/transaction observers', t => {
  const { client, handle, sockets, errors } = setup(t)
  client.registerOptimisticReceiptDecoder(() => { throw new Error('bad decoder') })
  client.registerOptimisticReceiptDecoder(transaction => transaction.messages.filter(message => message.kind === 'confirm').map(message => atob(message.payload)))
  client.beginOptimistic({ mutationId: 'm', patches: [patch('project', 'Predicted')] })
  const seen = []
  handle.data.subscribe(() => { seen.push(handle.data.getSnapshot().row.entity.values.title.textValue) })
  let receiptTitle
  client.subscribeTransactions(() => { receiptTitle = handle.data.getSnapshot().row.entity.values.title.textValue })
  sockets[0].message({ connectionSequence: '1', modelBatch: { kind: 'COMMIT', atomic: true,
    entities: [entity('project', 'Actual')], messages: [{ kind: 'confirm', payload: btoa('m') }] } })
  assert.deepEqual(seen, ['Actual'])
  assert.equal(receiptTitle, 'Actual')
  assert.equal(errors.length, 1)
  assert.equal(sockets[0].readyState, 1)
})

test('disconnect retains the authoritative view, never its optimistic prediction', async t => {
  for (const close of ['manual', 'network']) {
    await t.test(close, t => {
      const { client, handle, sockets } = setup(t)
      client.beginOptimistic({ mutationId: 'm', patches: [patch('project', 'Predicted'), patch('shared', 'Predicted task')] })
      sockets[0].message({ connectionSequence: '1', modelBatch: { kind: 'COMMIT', entities: [entity('project', 'Latest confirmed')] } })
      assert.equal(handle.data.getSnapshot().row.entity.values.title.textValue, 'Predicted')
      if (close === 'manual') client.disconnect(); else sockets[0].close()
      const view = handle.view.getSnapshot()
      assert.equal(view.stale, true)
      assert.equal(view.root.row.entity.values.title.textValue, 'Latest confirmed')
      const rows = view.root.row.children.memberships.rows
      assert.equal(rows[0].children.task.row.entity.values.title.textValue, 'Shared task')
      assert.equal(rows[1].children.task.row.entity.values.title.textValue, 'Shared task', 'diamonds roll back together')
    })
  }
})

test('result rejection rolls back against the latest base; a late old rejection cannot touch a new generation', async t => {
  const { client, sockets } = setup(t)
  let reject
  const result = new Promise((_, fail) => { reject = fail })
  client.beginOptimistic({ mutationId: 'm', patches: [patch('project', 'Predicted')], result })
  sockets[0].message({ connectionSequence: '1', modelBatch: { kind: 'COMMIT', entities: [entity('project', 'Latest confirmed')] } })
  reject(new Error('rejected'))
  await Promise.resolve()
  assert.equal(title(client.store, 'project'), 'Latest confirmed')

  let rejectOld
  const old = new Promise((_, fail) => { rejectOld = fail })
  client.beginOptimistic({ mutationId: 'same-id', patches: [patch('project', 'Old')], result: old })
  client.disconnect(); client.connect(); sockets[1].open()
  sockets[1].message({ connectionOpened: { connectionContext: 'new-context' } })
  // Load a base without requiring a replacement projection to become ready.
  sockets[1].message({ connectionSequence: '1', modelBatch: { kind: 'COMMIT', entities: [entity('project', 'New base')] } })
  client.beginOptimistic({ mutationId: 'same-id', patches: [patch('project', 'New prediction')] })
  rejectOld(new Error('late rejection'))
  await Promise.resolve()
  assert.equal(title(client.store, 'project'), 'New prediction')
})

test('RPC success and non-atomic metadata do not retire a prediction', async t => {
  const { client, sockets } = setup(t)
  const stop = client.registerOptimisticReceiptDecoder(() => ['m'])
  client.beginOptimistic({ mutationId: 'm', patches: [patch('project', 'Predicted')], result: Promise.resolve() })
  await Promise.resolve()
  assert.equal(title(client.store, 'project'), 'Predicted')
  sockets[0].message({ connectionSequence: '1', modelBatch: { kind: 'COMMIT', atomic: false, messages: [{ kind: 'confirm' }] } })
  assert.equal(title(client.store, 'project'), 'Predicted')
  stop()
  sockets[0].message({ connectionSequence: '2', modelBatch: { kind: 'COMMIT', atomic: true, messages: [{ kind: 'confirm' }] } })
  assert.equal(title(client.store, 'project'), 'Predicted')
  client.registerOptimisticReceiptDecoder(() => ['m', 'm', 'unknown'])
  sockets[0].message({ connectionSequence: '3', modelBatch: { kind: 'COMMIT', atomic: true, messages: [{ kind: 'confirm' }] } })
  assert.equal(title(client.store, 'project'), 'Project')
})

test('mixed layer operations match a simple ordered reference model', () => {
  const store = new ModelStore()
  const base = new Map(['a', 'b'].map(id => [id, { title: text(id) }]))
  const layers = new Map(), handles = new Map()
  store.apply(batch({ entities: [...base].map(([entityId, values]) => ({ entityId, upsert: { values } })) }))
  let state = 17, nextID = 0
  const random = n => { state = (Math.imul(state, 1664525) + 1013904223) >>> 0; return Math.floor(state / 4294967296 * n) }
  const actions = new Set()
  for (let i = 0; i < 300; i++) {
    const entityId = random(2) ? 'a' : 'b', action = random(4)
    actions.add(action)
    if (action === 0) {
      const id = `mutation-${nextID++}`
      const value = random(3) ? { set: { title: text(`prediction-${i}`) } } : { unset: ['title'] }
      handles.set(id, store.beginOptimistic({ mutationId: id, patches: [{ entityId, ...value }] }))
      layers.set(id, { entityId, ...value })
    } else if (action === 1) {
      const values = { title: text(`confirmed-${i}`), other: text(`field-${i}`) }
      base.set(entityId, values)
      store.apply(batch({ kind: 'COMMIT', atomic: true, entities: [{ entityId, upsert: { values } }] }))
    } else if (layers.size) {
      const id = [...layers.keys()][random(layers.size)]
      if (action === 2) handles.get(id).rollback()
      else store.apply(batch({ kind: 'COMMIT', atomic: true }), { retireOptimistic: [id] })
      handles.delete(id); layers.delete(id)
    }
    for (const [id, authoritative] of base) {
      const expected = { ...authoritative }
      for (const layer of layers.values()) {
        if (layer.entityId !== id) continue
        for (const field of layer.unset ?? []) delete expected[field]
        Object.assign(expected, layer.set)
      }
      assert.deepEqual({ ...store.getEntity(id).values }, expected, `effective entity ${id} after step ${i}`)
      assert.deepEqual({ ...store.getAuthoritativeEntity(id).values }, authoritative)
    }
  }
  assert.equal(actions.size, 4)
})
