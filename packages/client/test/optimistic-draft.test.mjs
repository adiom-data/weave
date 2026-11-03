import test from 'node:test'
import assert from 'node:assert/strict'
import { ReactiveClient, defineProjection } from '../dist/index.js'
import { FakeSocket, attach, reset, entity, batch } from './helpers.mjs'

const field = (name, kind = 'text', path = [name]) => ({ name, path, type: { kind } })
const taskShape = { nodeId: 'task', kind: 'entity', fields: [field('title'), field('completed', 'bool'), field('renamed', 'text', ['data', 'odd.name'])], children: {
  subtasks: { nodeId: 'subtasks', kind: 'list', fields: [field('title')], children: {} },
} }
const definition = defineProjection({ nodeId: 'project', entity: { query: { table: 'projects' } }, children: [
  { nodeId: 'links', list: { query: { table: 'links' }, window: { from: 'FROM_START', count: 10 } }, children: [
    { nodeId: 'task', entity: { query: { table: 'tasks' } }, children: [{ nodeId: 'subtasks', list: { query: { table: 'tasks' }, window: { from: 'FROM_START', count: 10 } } }] },
  ] },
] }, { nodeId: 'project', kind: 'entity', fields: [], children: { links: { nodeId: 'links', kind: 'list', fields: [], children: { task: taskShape } } } }, [])

function setup(t, count = 1) {
  const sockets = []
  const client = new ReactiveClient({ url: 'ws://test', heartbeatIntervalMs: 0, createSocket: () => { const socket = new FakeSocket(); sockets.push(socket); return socket } })
  t.after(() => client.clear())
  client.connect(); sockets[0].open()
  sockets[0].message({ connectionOpened: { connectionContext: 'generation-1' } })
  const handles = Array.from({ length: count }, (_, i) => {
    const handle = definition.subscribe(client, {})
    sockets[0].message({ subscriptionId: handle.id, sequence: '1', modelBatch: { kind: 'KIND_SNAPSHOT',
      entities: [entity(`root${i}`, 'Project')], queries: [reset(`q${i}`, [])],
      projections: [attach(`r${i}`, 'project', { entityId: `root${i}` }, '', '', handle.id), attach(`l${i}`, 'links', { querySubscriptionId: `q${i}` }, `r${i}`, `root${i}`, handle.id)],
    } })
    sockets[0].message({ subscriptionId: handle.id, sequence: '2', ready: {} })
    return handle
  })
  return { client, handles, sockets }
}
const list = handle => handle.data.getSnapshot().value.links

test('typed nested insertions infer paths, encode aliases, and remain private until commit', t => {
  const { client, handles: [handle] } = setup(t)
  const input = { task: { title: 'New task', completed: false, renamed: 'Literal field', subtasks: [{ title: 'Child' }] } }
  const draft = client.optimistic({ mutationId: 'm' })
  draft.insert(list(handle), input)
  input.task.title = 'Caller mutation'
  assert.equal(list(handle).length, 0)
  const layer = draft.commit()
  const task = list(handle)[0].value.task
  assert.equal(task.status, 'optimistic')
  assert.equal(task.value.title, 'New task')
  assert.equal(task.value.renamed, 'Literal field')
  assert.equal(task.value.subtasks[0].value.title, 'Child')
  assert.equal(client.store.getEntity(task.entityId).values.data.objectValue.fields['odd.name'].textValue, 'Literal field')
  assert.throws(() => draft.commit(), /already committed/)
  layer.rollback()
  assert.equal(list(handle).length, 0)
  assert.equal(client.getDiagnostics().store.optimistic.layers, 0)
})

test('one draft targets several projections and shares one canonical temporary entity', t => {
  const { client, handles: [a, b], sockets } = setup(t, 2)
  const draft = client.optimistic({ mutationId: 'shared' })
  const task = draft.entity({ title: { textValue: 'Shared' }, completed: { boolValue: false }, data: { objectValue: { fields: { 'odd.name': { textValue: 'Field' } } } } })
  draft.insert(list(a), { task }); draft.insert(list(b), { task })
  draft.commit()
  assert.equal(list(a)[0].value.task.entityId, task.entityId)
  assert.equal(list(b)[0].value.task.entityId, task.entityId)
  assert.equal(client.getDiagnostics().store.optimistic.entities, 3, 'one task and two association records')
  client.registerOptimisticReceiptDecoder(() => ['shared'])
  const seen = []
  const stop = a.data.subscribe(() => seen.push([list(a).length, list(b).length]))
  sockets[0].message({ connectionSequence: '1', modelBatch: { kind: 'KIND_COMMIT', atomic: true, messages: [{ kind: 'receipt', payload: '' }] } })
  assert.deepEqual(seen, [[0, 0]], 'whole-layer retirement publishes across projections atomically')
  stop()
})

test('empty nested lists are real local lists; failed graph encodes never partially enter the draft', t => {
  const { client, handles: [handle] } = setup(t)
  const draft = client.optimistic({ mutationId: 'm' })
  assert.throws(() => draft.insert(list(handle), { task: { title: 'Partial', subtasks: [{ title: 123 }] } }), /expected text/)
  assert.throws(() => draft.insert(list(handle), { typo: true }), /unknown field/)
  draft.insert(list(handle), { task: { title: 'Valid', subtasks: [] } })
  draft.commit()
  assert.equal(list(handle).length, 1)
  assert.equal(list(handle)[0].value.task.value.subtasks.length, 0)
  assert.equal(client.getDiagnostics().store.optimistic.entities, 2)
})

test('invalid positions leave drafts usable; anchor positions are owned and ignore inherited properties', t => {
  const { client, handles: [handle] } = setup(t)
  const draft = client.optimistic({ mutationId: 'positions' })
  assert.throws(() => draft.insert(list(handle), { task: { title: 'Invalid' } }, { before: '' }), /position requires/)
  const first = draft.insert(list(handle), { task: { title: 'First' } })
  const third = draft.insert(list(handle), { task: { title: 'Third' } })
  const position = Object.assign(Object.create({ before: 'wrong-inherited-anchor' }), { after: first.entityId })
  draft.insert(list(handle), { task: { title: 'Second' } }, position)
  position.after = third.entityId
  draft.commit()
  assert.deepEqual(list(handle).map(row => row.value.task.value.title), ['First', 'Second', 'Third'])
  assert.equal(client.getDiagnostics().store.optimistic.entities, 6, 'failed operation leaves no temporary records behind')
})

test('invalid patches and duplicate entity operations are rejected before changing the draft', t => {
  const { client, handles: [handle] } = setup(t)
  const draft = client.optimistic({ mutationId: 'validation' })
  const root = handle.data.getSnapshot()
  assert.throws(() => draft.patch(root, { title: { textValue: 'Invalid' } }, ['title']), /disjoint/)
  assert.throws(() => draft.patch(root, {}), /at least one field/)
  draft.patch(root, { title: { textValue: 'Valid' } })
  assert.throws(() => draft.delete(root), /unique within a mutation/)
  const link = draft.insert(list(handle), { task: { title: 'Temporary' } })
  assert.throws(() => draft.patch(link, { title: { textValue: 'Duplicate' } }), /unique within a mutation/)
  draft.commit()
  assert.equal(client.store.getEntity(root.entityId).values.title.textValue, 'Valid')
  assert.equal(list(handle)[0].value.task.value.title, 'Temporary')
})

test('drafts reject foreign lists, disposed handles, and changed connection generations', t => {
  const one = setup(t), two = setup(t)
  const draft = one.client.optimistic({ mutationId: 'm' })
  assert.throws(() => draft.insert(list(two.handles[0]), {}), /does not belong/)
  draft.insert(list(one.handles[0]), { task: { title: 'New' } })
  one.handles[0].dispose()
  assert.throws(() => draft.commit(), /disposed/)
  assert.equal(one.client.getDiagnostics().store.optimistic.layers, 0)
  const obsolete = two.client.optimistic({ mutationId: 'old' })
  obsolete.insert(list(two.handles[0]), {})
  two.client.disconnect(); two.client.connect(); two.sockets[1].open()
  two.sockets[1].message({ connectionOpened: { connectionContext: 'generation-2' } })
  assert.throws(() => obsolete.commit(), /expired connection/)
})

test('seeded optimistic graph churn matches layer order and releases temporary materializations', t => {
  const { client, handles: [handle] } = setup(t)
  let seed = 9321, next = 0
  const random = n => { seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0; return Math.floor(seed / 4294967296 * n) }
  const active = new Map()
  for (let step = 0; step < 600; step++) {
    if (!active.size || random(3) !== 0) {
      const id = `draft-${next++}`
      const draft = client.optimistic({ mutationId: id })
      const title = `Title ${step}`
      draft.insert(list(handle), { task: { title, subtasks: [] } })
      active.set(id, { handle: draft.commit(), title })
    } else {
      const id = [...active.keys()][random(active.size)]
      if (random(2)) active.get(id).handle.rollback()
      else client.store.apply(batch({ kind: 'KIND_COMMIT', atomic: true }), { retireOptimistic: [id] })
      active.delete(id)
    }
    assert.deepEqual(list(handle).map(row => row.value.task.value.title), [...active.values()].map(layer => layer.title), `step ${step}`)
  }
  for (const value of active.values()) value.handle.rollback()
  assert.equal(list(handle).length, 0)
  assert.deepEqual(client.getDiagnostics().store.optimistic, { layers: 0, entities: 0, targets: 0, materializedEntities: 0 })
})

test('retiring temporary backing leaves later patches dormant without retaining its payload', t => {
  const { client, handles: [handle] } = setup(t)
  const draft = client.optimistic({ mutationId: 'creator' })
  const link = draft.insert(list(handle), { task: { title: 'Temporary' } })
  const creation = draft.commit()
  const patch = client.optimistic({ mutationId: 'patch' })
  patch.patch(link, { title: { textValue: 'Patched' } })
  const layer = patch.commit()
  assert.equal(list(handle).length, 1)
  creation.rollback()
  assert.equal(list(handle).length, 0)
  assert.equal(client.store.getEntity(link.entityId).status, 'unavailable')
  assert.equal(client.getDiagnostics().store.optimistic.materializedEntities, 0)
  assert.equal(client.getDiagnostics().store.optimistic.layers, 1)
  layer.rollback()
  assert.deepEqual(client.getDiagnostics().store.optimistic, { layers: 0, entities: 0, targets: 0, materializedEntities: 0 })
})
