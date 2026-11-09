import test from 'node:test'
import assert from 'node:assert/strict'
import { ReactiveClient, ModelStore, scalar, defineProjection, readProjection } from '../dist/index.js'
import { BranchPublications } from '../dist/partial.js'
import { FakeSocket, initial, request, entity, attach, reset, batch } from './helpers.mjs'
const recovered = ({ error, ...address }) => ({ ...address, recovered: {} })

function setup(t) {
  const socket = new FakeSocket()
  const client = new ReactiveClient({ url: 'ws://test', createSocket: () => socket, heartbeatIntervalMs: 0 })
  t.after(() => client.clear())
  const handle = client.subscribeProjection({ ...request, allowPartialErrors: true })
  client.connect(); socket.open()
  socket.message({ connectionOpened: { connectionContext: 'connection' } })
  const image = initial()
  for (const edge of image.projections) edge.subscriptionId = handle.id
  socket.message({ subscriptionId: handle.id, sequence: '1', modelBatch: image })
  socket.message({ subscriptionId: handle.id, sequence: '2', ready: {} })
  const failure = (overrides = {}) => {
    const value = { subscriptionId: handle.id, occurrenceId: 'task1', nodeId: 'task', parentOccurrenceId: 'members', parentEntityId: 'm1', entityId: 'shared', error: { code: 'EVALUATION_FAILED', message: 'failed' }, ...overrides }
    if (value.recovered) delete value.error
    return value
  }
  const task = (index = 0) => handle.view.getSnapshot().root.row.children.memberships.rows[index].children.task
  return { client, socket, handle, failure, task }
}

test('one failed diamond path retains an immutable authoritative view while another updates the shared entity', t => {
  const { client, socket, handle, failure, task } = setup(t)
  let notices = 0
  handle.view.subscribe(() => notices++)
  socket.message({ connectionSequence: '1', modelBatch: { kind: 'KIND_COMMIT', atomic: true,
    entities: [entity('shared', 'New shared task')],
    projections: [{ occurrenceId: 'task1', detached: {} }], branches: [failure()],
  } })
  assert.equal(notices, 1)
  assert.equal(handle.view.getSnapshot().status, 'ready')
  assert.equal(handle.view.getSnapshot().errors.length, 1)
  assert.equal(handle.view.getSnapshot().stale, true)
  assert.equal(task().row.stale, true)
  assert.equal(scalar(task().row.entity.values, 'title'), 'Shared task')
  assert.equal(scalar(task(1).row.entity.values, 'title'), 'New shared task')
  assert.equal(scalar(client.store.getAuthoritativeEntity('shared').values, 'title'), 'New shared task')
  const healthy = task(1)
  socket.message({ connectionSequence: '2', modelBatch: { kind: 'KIND_HYDRATION',
    projections: [attach('task1', 'task', { entityId: 'shared' }, 'members', 'm1', handle.id)],
    branches: [failure({ error: undefined, recovered: {} })],
  } })
  assert.equal(notices, 2)
  assert.equal(task().row.error, undefined)
  assert.equal(task().row.stale, undefined)
  assert.equal(scalar(task().row.entity.values, 'title'), 'New shared task')
  assert.equal(task(1), healthy)
  assert.equal(handle.view.getSnapshot().errors, undefined)
  assert.equal(socket.sent.filter(message => message.subscribeProjection).length, 1)
})

test('an errored list is not live cache coverage and its recovery is an authoritative replacement', async t => {
  const { client, socket, handle, failure } = setup(t)
  const list = () => handle.view.getSnapshot().root.row.children.memberships
  socket.message({ connectionSequence: '1', modelBatch: { kind: 'KIND_COMMIT', atomic: true,
    entities: ['m1','m2','shared'].map(entityId => ({ entityId, released: {} })),
    queries: [{ querySubscriptionId: 'list', closed: {} }],
    projections: ['task1','task2','members'].map(occurrenceId => ({ occurrenceId, detached: {} })),
    branches: [failure({ occurrenceId: 'members', nodeId: 'memberships', parentOccurrenceId: 'root', parentEntityId: 'project', entityId: '', error: { code: 'QUERY_FAILED', message: 'failed' } })],
  } })
  assert.equal(client.store.getAuthoritativeEntity('shared').status, 'unavailable')
  assert.equal(list().rows.length, 2)
  assert.equal(list().error.code, 'QUERY_FAILED')
  assert.equal(list().stale, true)
  assert.equal(scalar(list().rows[0].children.task.row.entity.values, 'title'), 'Shared task')
  await assert.rejects(handle.updateWindow('members', [{ side: 'SIDE_RIGHT', moveBy: 1 }]), /window|available|visible/i)
  socket.message({ connectionSequence: '2', modelBatch: { kind: 'KIND_HYDRATION',
    entities: [entity('m2','Remaining link')], queries: [reset('replacement-list', ['m2'])],
    projections: [attach('new-members', 'memberships', { querySubscriptionId: 'replacement-list' }, 'root', 'project', handle.id)],
    branches: [failure({ occurrenceId: 'members', nodeId: 'memberships', parentOccurrenceId: 'root', parentEntityId: 'project', entityId: '', error: undefined, recovered: {} })],
  } })
  assert.equal(list().rows.length, 1)
  assert.equal(list().rows[0].entityId, 'm2')
  assert.equal(list().rows[0].children.task, undefined)
  assert.equal(list().error, undefined)
  assert.equal(client.store.getAuthoritativeEntity('m1').status, 'unavailable')
})

for (const failed of ['list', 'ancestor-node', 'ancestor-row']) test(`typed pagination bounds disappear for a failed ${failed} and return on replacement`, async t => {
  const socket = new FakeSocket()
  const client = new ReactiveClient({ url: 'ws://test', createSocket: () => socket, heartbeatIntervalMs: 0 })
  t.after(() => client.clear())
  const definition = defineProjection(request.root, {
    nodeId: 'project', kind: 'entity', fields: [], children: {
      memberships: { nodeId: 'memberships', kind: 'list', fields: [], children: {} },
    },
  }, [])
  const handle = definition.subscribe(client, {}, { allowPartialErrors: true })
  client.connect(); socket.open()
  const image = initial()
  for (const edge of image.projections) edge.subscriptionId = handle.id
  socket.message({ subscriptionId: handle.id, sequence: '1', modelBatch: image })
  socket.message({ subscriptionId: handle.id, sequence: '2', ready: {} })
  const control = handle.window(handle.data.getSnapshot().value.memberships)
  assert.ok(control.bounds.getSnapshot())
  let notifications = 0
  control.bounds.subscribe(() => notifications++)
  const ancestor = failed !== 'list'
  const error = { subscriptionId: handle.id, occurrenceId: ancestor ? 'root' : 'members', nodeId: ancestor ? 'project' : 'memberships',
    parentOccurrenceId: ancestor ? '' : 'root', parentEntityId: ancestor ? '' : 'project', entityId: failed === 'ancestor-row' ? 'project' : '',
    error: { code: 'QUERY_FAILED', message: 'failed' } }
  socket.message({ connectionSequence: '1', modelBatch: { kind: 'KIND_HYDRATION',
    queries: [{ querySubscriptionId: 'list', closed: {} }],
    projections: [...['members', 'task1', 'task2'], ...(ancestor ? ['root'] : [])].map(occurrenceId => ({ occurrenceId, detached: {} })),
    branches: [error],
  } })
  assert.ok(handle.data.getSnapshot().value.memberships, 'stale display remains available')
  assert.equal(control.bounds.getSnapshot(), undefined)
  assert.equal(handle.window(handle.data.getSnapshot().value.memberships).bounds.getSnapshot(), undefined)
  assert.equal(notifications, 1)
  const sent = socket.sent.length
  await assert.rejects(control.extend('SIDE_RIGHT', 1), /active.*window/i)
  assert.equal(socket.sent.length, sent, 'an unavailable window must not send a request')
  socket.message({ connectionSequence: '2', modelBatch: { kind: 'KIND_HYDRATION',
    queries: [reset('replacement-list', ['m2'])],
    projections: [
      ...(ancestor ? [attach('new-root', 'project', { entityId: 'project' }, '', '', handle.id)] : []),
      attach('new-members', 'memberships', { querySubscriptionId: 'replacement-list' }, ancestor ? 'new-root' : 'root', 'project', handle.id),
    ], branches: [recovered(error)],
  } })
  assert.ok(control.bounds.getSnapshot(), 'the same control follows the live replacement occurrence')
  assert.equal(notifications, 2)
  assert.equal(handle.data.getSnapshot().value.memberships.error, undefined)
  client.disconnect()
  assert.equal(control.bounds.getSnapshot(), undefined, 'reconnect display is not live coverage')
})

test('an individual failed row does not disable its healthy list window', async t => {
  const socket = new FakeSocket()
  const client = new ReactiveClient({ url: 'ws://test', createSocket: () => socket, heartbeatIntervalMs: 0 })
  t.after(() => client.clear())
  const definition = defineProjection({ nodeId: 'items', list: { query: { table: 'items' }, window: { count: 20 } } }, {
    nodeId: 'items', kind: 'list', fields: [], children: {},
  }, [])
  const handle = definition.subscribe(client, {}, { allowPartialErrors: true })
  client.connect(); socket.open()
  socket.message({ subscriptionId: handle.id, sequence: '1', modelBatch: { kind: 'KIND_SNAPSHOT',
    entities: [entity('a', 'A'), entity('b', 'B')], queries: [reset('q', ['a', 'b'])],
    projections: [attach('items', 'items', { querySubscriptionId: 'q' }, '', '', handle.id)],
  } })
  socket.message({ subscriptionId: handle.id, sequence: '2', ready: {} })
  const control = handle.window(handle.data.getSnapshot())
  socket.message({ connectionSequence: '1', modelBatch: { kind: 'KIND_COMMIT',
    queries: [{ querySubscriptionId: 'q', delta: { baseVersion: '1', resultVersion: '2', removedEntityIds: ['a'] } }],
    branches: [{ subscriptionId: handle.id, occurrenceId: 'items', nodeId: 'items', entityId: 'a', error: { code: 'EVALUATION_FAILED', message: 'failed' } }],
  } })
  assert.equal(handle.data.getSnapshot()[0].error.code, 'EVALUATION_FAILED')
  assert.ok(control.bounds.getSnapshot())
  const promise = control.extend('SIDE_RIGHT', 1)
  const sent = socket.sent.at(-1)
  assert.equal(sent.updateProjectionWindow.instanceId, 'items')
  socket.message({ subscriptionId: handle.id, requestId: sent.requestId, sequence: '3', modelBatch: { kind: 'KIND_WINDOW' } })
  await promise
})

test('batch failure capture visits the projection once instead of scanning it for each row', () => {
  const size = 8000
  let reads = 0
  const rows = Array.from({ length: size }, (_, index) => ({
    get entityId() { reads++; return String(index) },
    entity: { status: 'available', values: {} }, children: {},
  }))
  const root = { occurrenceId: 'list', nodeId: 'items', type: 'list', rows }
  const errors = rows.map(row => ({ type: 'error', subscriptionId: 'p', occurrenceId: 'list', nodeId: 'items',
    parentOccurrenceId: '', parentEntityId: '', entityId: row.entityId, code: 'FAILED', message: 'failed' }))
  const branches = new BranchPublications()
  reads = 0
  branches.prepare(errors, root, root)
  assert.ok(reads <= 3 * size, `capture performed ${reads} entity reads for ${size} rows`)
  reads = 0
  new BranchPublications().prepare([errors[0]], root, root)
  assert.ok(reads <= 3, 'capture must stop once the requested address is found')
  const failed = { ...root, rows: errors.map(error => ({ entityId: error.entityId, entity: { status: 'unavailable' }, children: {}, error })) }
  const retained = branches.reconcile(failed, errors)
  assert.deepEqual(retained.rows.map(row => row.entityId), rows.map(row => row.entityId))
  assert.ok(retained.rows.every(row => row.stale && row.entity.status === 'available'))
})

test('detached error lookup does not rescan all failures for every child', () => {
  const size = 1000
  const ids = Array.from({ length: size }, (_, index) => String(index))
  let reads = 0
  const errors = new Proxy(ids.map(entityId => ({ subscriptionId: 'p', occurrenceId: `child-${entityId}`, nodeId: 'child',
    parentOccurrenceId: 'list', parentEntityId: entityId, entityId: `target-${entityId}`, code: 'FAILED', message: 'failed' })), {
    get(target, property, receiver) { if (/^\d+$/.test(String(property))) reads++; return Reflect.get(target, property, receiver) },
  })
  const store = {
    getSubscription: () => ({ status: 'ready' }), getConnection: () => ({ status: 'connected' }), getBranchErrors: () => errors,
    getOccurrences: (_scope, _node, parent) => parent === '' ? [{ id: 'list', target: { type: 'query', id: 'q' } }] : [],
    getQuery: () => ({ ids }), getAuthoritativeEntity: () => ({ status: 'available', values: {} }),
  }
  const image = readProjection(store, 'p', { root: { nodeId: 'items', list: {}, children: [{ nodeId: 'child', entity: {} }] } }, { authoritative: true })
  assert.equal(image.root.rows.length, size)
  assert.ok(image.root.rows.every(row => row.children.child.row.error.code === 'FAILED'))
  assert.ok(reads <= 3 * size, `error lookup performed ${reads} reads for ${size} errors`)
})

test('mixed healthy and failed memberships merge without repeated membership scans', () => {
  const size = 1000
  let reads = 0
  const ids = new Proxy(Array.from({ length: size }, (_, index) => `healthy-${index}`), {
    get(target, property, receiver) { if (/^\d+$/.test(String(property))) reads++; return Reflect.get(target, property, receiver) },
  })
  const errors = Array.from({ length: size }, (_, index) => ({ subscriptionId: 'p', occurrenceId: 'list', nodeId: 'items',
    parentOccurrenceId: '', parentEntityId: '', entityId: index % 2 ? `failed-${index}` : `healthy-${index}`, code: 'FAILED', message: 'failed' }))
  const store = {
    getSubscription: () => ({ status: 'ready' }), getConnection: () => ({ status: 'connected' }), getBranchErrors: () => errors,
    getOccurrences: () => [{ id: 'list', target: { type: 'query', id: 'q' } }],
    getQuery: () => ({ ids }), getAuthoritativeEntity: () => ({ status: 'available', values: {} }),
  }
  const image = readProjection(store, 'p', { root: { nodeId: 'items', list: {} } }, { authoritative: true })
  assert.equal(image.root.rows.length, size * 1.5)
  assert.equal(new Set(image.root.rows.map(row => row.entityId)).size, image.root.rows.length)
  assert.equal(image.root.rows.filter(row => row.error).length, size)
  assert.ok(reads <= 3 * size, `membership merge performed ${reads} reads for ${size} healthy rows`)
})

test('streaming failed-row placement preserves ordered insertion behavior for changing positions', () => {
  let seed = 1147
  const random = max => { seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0; return seed % max }
  const row = entityId => ({ entityId, entity: { status: 'available', values: {} }, children: {} })
  const list = rows => ({ occurrenceId: 'list', nodeId: 'items', type: 'list', rows })
  for (let trial = 0; trial < 100; trial++) {
    const branches = new BranchPublications(), active = [], positions = new Map()
    const healthy = Array.from({ length: random(12) }, (_, index) => row(`healthy-${index}`))
    const placeholders = []
    for (let i = 0, count = 1 + random(12); i < count; i++) {
      const value = row(`failed-${i}`)
      const error = { type: 'error', subscriptionId: 'p', occurrenceId: 'list', nodeId: 'items',
        parentOccurrenceId: '', parentEntityId: '', entityId: value.entityId, code: 'FAILED', message: 'failed' }
      const index = random(20)
      const before = list([...Array.from({ length: index }, (_, j) => row(`filler-${j}`)), value])
      branches.prepare([error], before, before)
      active.push(error)
      branches.reconcile(undefined, active) // Commit this capture before the next failure arrives.
      positions.set(value.entityId, index)
      placeholders.push({ ...row(value.entityId), error })
    }
    // Failures can arrive in a different order than their prior list positions;
    // coincident positions and positions beyond the healthy suffix are valid.
    const failed = placeholders.map(value => ({ value, index: positions.get(value.entityId) })).sort((a, b) => a.index - b.index)
    const expected = [...healthy]
    for (const { value, index } of failed) expected.splice(Math.min(index, expected.length), 0, value)
    const result = branches.reconcile(list([...healthy, ...placeholders]), active)
    assert.deepEqual(result.rows.map(value => value.entityId), expected.map(value => value.entityId), `trial ${trial}`)
  }
})

test('errored row placeholders preserve list position and definitive removal clears retained data', t => {
  const { socket, handle, failure } = setup(t)
  const rows = () => handle.view.getSnapshot().root.row.children.memberships.rows
  const error = failure({ occurrenceId: 'members', nodeId: 'memberships', parentOccurrenceId: 'root', parentEntityId: 'project', entityId: 'm1' })
  socket.message({ connectionSequence: '1', modelBatch: { kind: 'KIND_COMMIT', atomic: true,
    entities: [{ entityId: 'm1', released: {} }],
    queries: [{ querySubscriptionId: 'list', delta: { baseVersion: '1', resultVersion: '2', removedEntityIds: ['m1'] } }],
    projections: [{ occurrenceId: 'task1', detached: {} }], branches: [error],
  } })
  assert.deepEqual(rows().map(row => row.entityId), ['m1','m2'])
  assert.equal(rows()[0].stale, true)
  socket.message({ connectionSequence: '2', modelBatch: { kind: 'KIND_HYDRATION', branches: [recovered(error)] } })
  assert.deepEqual(rows().map(row => row.entityId), ['m2'])
  assert.equal(handle.view.getSnapshot().stale, false)
})

test('optimistic predictions never become baked into retained stale branches, and receipts still retire them', t => {
  const { client, socket, handle, failure, task } = setup(t)
  const optimistic = client.beginOptimistic({ mutationId: 'mutation', patches: [{ entityId: 'shared', set: { title: { textValue: 'Prediction' } } }] })
  assert.equal(scalar(task().row.entity.values, 'title'), 'Prediction')
  client.registerOptimisticReceiptDecoder(() => ['mutation'])
  socket.message({ connectionSequence: '1', modelBatch: { kind: 'KIND_COMMIT', atomic: true,
    projections: [{ occurrenceId: 'task1', detached: {} }], branches: [failure()],
  } })
  assert.equal(scalar(task().row.entity.values, 'title'), 'Shared task')
  socket.message({ connectionSequence: '2', modelBatch: { kind: 'KIND_COMMIT', atomic: true, messages: [{ kind: 'receipt' }] } })
  assert.equal(scalar(task().row.entity.values, 'title'), 'Shared task')
  socket.message({ connectionSequence: '3', modelBatch: { kind: 'KIND_HYDRATION', entities: [entity('shared','Actual')],
    projections: [attach('task1','task',{ entityId:'shared' },'members','m1',handle.id)], branches:[failure({error:undefined,recovered:{}})],
  } })
  assert.equal(scalar(task().row.entity.values,'title'),'Actual')
  optimistic.rollback()
})

test('initial partial errors are explicit unavailable branches, not successful empty data', t => {
  const socket = new FakeSocket()
  const client = new ReactiveClient({ url:'ws://test',createSocket:()=>socket, heartbeatIntervalMs:0 })
  t.after(()=>client.clear())
  const definition = defineProjection({ nodeId:'project', entity:{ query:{table:'projects', columns:[{segments:['title']}]}} }, {
    nodeId:'project',kind:'entity',fields:[{name:'title',path:['title'],type:{kind:'text'}}],children:{},
  },[])
  const handle = definition.subscribe(client,{}, {allowPartialErrors:true})
  client.connect(); socket.open()
  assert.equal(socket.sent[0].subscribeProjection.allowPartialErrors,true)
  socket.message({subscriptionId:handle.id,sequence:'1',modelBatch:{ kind:'KIND_SNAPSHOT',branches:[{subscriptionId:handle.id,occurrenceId:'root',nodeId:'project',error:{code:'QUERY_FAILED',message:'failed'}}]}})
  socket.message({subscriptionId:handle.id,sequence:'2',ready:{}})
  assert.equal(handle.view.getSnapshot().status,'ready')
  assert.equal(handle.view.getSnapshot().errors[0].nodeId,'project')
  assert.equal(handle.data.getSnapshot().status,'unavailable')
})

test('branch updates and model validation commit together, and reset releases error state', () => {
  const store = new ModelStore()
  const error = {subscriptionId:'p',occurrenceId:'a',nodeId:'project',error:{code:'QUERY_FAILED',message:'failed'}}
  assert.throws(()=>store.apply(batch({branches:[error],queries:[{querySubscriptionId:'unknown',delta:{baseVersion:'1',resultVersion:'2'}}]})), /unknown/)
  assert.deepEqual(store.getBranchErrors('p'),[])
  store.apply(batch({branches:[error]}))
  assert.equal(store.getBranchErrors('p').length,1)
  store.reset()
  assert.deepEqual(store.getBranchErrors('p'),[])
})

test('typed entity recovery clears occurrence error metadata even with unchanged data', t => {
  const socket = new FakeSocket()
  const client = new ReactiveClient({url:'ws://test',createSocket:()=>socket,heartbeatIntervalMs:0})
  t.after(()=>client.clear())
  const definition = defineProjection({nodeId:'task',entity:{query:{table:'tasks'}}}, {
    nodeId:'task',kind:'entity',fields:[{name:'title',path:['title'],type:{kind:'text'}}],children:{},
  }, [])
  const handle = definition.subscribe(client,{}, {allowPartialErrors:true})
  client.connect(); socket.open()
  socket.message({subscriptionId:handle.id,sequence:'1',modelBatch:{kind:'KIND_SNAPSHOT',
    entities:[entity('t','Unchanged')], projections:[attach('task-instance','task',{entityId:'t'},'','',handle.id)],
  }})
  socket.message({subscriptionId:handle.id,sequence:'2',ready:{}})
  const before = handle.data.getSnapshot()
  const error = {subscriptionId:handle.id,occurrenceId:'task-instance',nodeId:'task',entityId:'t',error:{code:'EVALUATION_FAILED',message:'failed'}}
  socket.message({connectionSequence:'1',modelBatch:{kind:'KIND_COMMIT',
    projections:[{occurrenceId:'task-instance',detached:{}}], branches:[error],
  }})
  assert.equal(handle.data.getSnapshot().value, before.value)
  assert.equal(handle.data.getSnapshot().stale,true)
  assert.equal(handle.data.getSnapshot().error.code,'EVALUATION_FAILED')
  socket.message({connectionSequence:'2',modelBatch:{kind:'KIND_HYDRATION',
    projections:[attach('task-instance','task',{entityId:'t'},'','',handle.id)], branches:[recovered(error)],
  }})
  const after = handle.data.getSnapshot()
  assert.equal(after.value,before.value)
  assert.equal(after.error,undefined)
  assert.equal(after.stale,undefined)
})

test('disconnect retains authoritative failed-branch display without restoring predictions', t => {
  const {client,socket,handle,failure,task} = setup(t)
  socket.message({connectionSequence:'1',modelBatch:{kind:'KIND_COMMIT',
    projections:[{occurrenceId:'task1',detached:{}}], branches:[failure()],
  }})
  client.beginOptimistic({mutationId:'pending',patches:[{entityId:'shared',set:{title:{textValue:'Prediction'}}}]})
  assert.equal(scalar(task(1).row.entity.values,'title'),'Prediction')
  client.disconnect()
  assert.equal(handle.view.getSnapshot().stale,true)
  assert.equal(scalar(task().row.entity.values,'title'),'Shared task')
  assert.equal(scalar(task(1).row.entity.values,'title'),'Shared task')
})
