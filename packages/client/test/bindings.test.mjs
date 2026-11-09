import test from 'node:test'
import assert from 'node:assert/strict'
import { defineProjection, ReactiveClient, ProjectionDecodeError } from '../dist/index.js'
import { FakeSocket, attach, reset } from './helpers.mjs'

function setup(definition, parameters = {}) {
  const sockets = []
  const client = new ReactiveClient({ url: 'ws://test', heartbeatIntervalMs: 0, createSocket: () => { const socket = new FakeSocket(); sockets.push(socket); return socket } })
  const handle = definition.subscribe(client, parameters)
  client.connect(); sockets[0].open()
  return { client, handle, socket: sockets[0], scope: sockets[0].sent[0].subscriptionId }
}
const scalar = (name, kind, nullable = false) => ({ name, path: [name], type: { kind, nullable } })
const root = { nodeId: 'root', entity: { query: { schema: 'app', table: 'rows' } } }

test('computed selections decode through typed aliases without changing entity values', () => {
  const definition = defineProjection({ ...root, computed: ['role', 'canEdit'] }, {
    nodeId: 'root', kind: 'entity', children: {}, fields: [
      { name: 'permission', path: ['role'], type: { kind: 'integer' }, computed: true },
      { name: 'editable', path: ['canEdit'], type: { kind: 'bool' }, computed: true },
      scalar('title', 'text'),
    ],
  }, [])
  const { client, handle, socket, scope } = setup(definition)
  try {
    assert.deepEqual(socket.sent[0].subscribeProjection.root.computed, ['role', 'canEdit'])
    socket.message({ subscriptionId: scope, sequence: '1', modelBatch: { kind: 'KIND_SNAPSHOT',
      entities: [{ entityId: 'e', upsert: { values: { title: { textValue: 'Task' } } } }],
      projections: [attach('a', 'root', { entityId: 'e' }, '', '', scope)],
      computed: [{ occurrenceId: 'a', entityId: 'e', upsert: { fields: { role: { integerValue: '2' }, canEdit: { boolValue: true } } } }],
    } })
    socket.message({ subscriptionId: scope, sequence: '2', ready: {} })
    const before = handle.data.getSnapshot()
    assert.equal(before.value.permission, 2n)
    assert.equal(before.value.editable, true)
    assert.equal(before.value.title, 'Task')
    assert.equal(client.store.getEntity('e').values.role, undefined)
    socket.message({ connectionSequence: '1', modelBatch: { kind: 'KIND_COMMIT',
      computed: [{ occurrenceId: 'a', entityId: 'e', upsert: { fields: { role: { integerValue: '1' }, canEdit: { boolValue: false } } } }],
    } })
    assert.equal(handle.data.getSnapshot().value.permission, 1n)
    assert.equal(handle.data.getSnapshot().value.editable, false)
    assert.equal(before.value.permission, 2n)
  } finally { handle.dispose(); client.disconnect() }
})

test('computed floats preserve canonical values through binary snapshots and commits', () => {
  const definition = defineProjection({ ...root, computed: ['score'] }, {
    nodeId: 'root', kind: 'entity', children: {}, fields: [
      { name: 'score', path: ['score'], type: { kind: 'float' }, computed: true },
    ],
  }, [])
  const { client, handle, socket, scope } = setup(definition)
  const computed = spelling => [{ occurrenceId: 'a', entityId: 'e', upsert: { fields: { score: { floatValue: spelling } } } }]
  try {
    socket.message({ subscriptionId: scope, sequence: '1', modelBatch: { kind: 'KIND_SNAPSHOT',
      entities: [{ entityId: 'e', upsert: { values: {} } }],
      projections: [attach('a', 'root', { entityId: 'e' }, '', '', scope)],
      computed: computed('Infinity'),
    } })
    socket.message({ subscriptionId: scope, sequence: '2', ready: {} })
    const before = handle.data.getSnapshot()
    assert.equal(before.value.score, Infinity)
    let sequence = 0n
    for (const [spelling, expected] of [['-Infinity', -Infinity], ['NaN', NaN], ['0', 0], ['1.5', 1.5]]) {
      socket.message({ connectionSequence: String(++sequence), modelBatch: { kind: 'KIND_COMMIT', computed: computed(spelling) } })
      assert.equal(handle.data.getSnapshot().value.score, expected)
      assert.equal(client.store.getComputed('a', 'e').score.floatValue, spelling)
    }
    assert.equal(before.value.score, Infinity)
  } finally { handle.dispose(); client.disconnect() }
})

test('allFields publishes whole parents without widening typed readers or child selections', () => {
  const request = { ...root, allFields: true, children: [{ nodeId: 'root.owner', entity: { query: { schema: 'app', table: 'users', columns: [{ segments: ['name'] }] } } }] }
  const definition = defineProjection(request, { nodeId: 'root', kind: 'entity', fields: [], children: {
    owner: { nodeId: 'root.owner', kind: 'entity', fields: [scalar('name', 'text')], children: {} },
  } }, [])
  const { client, handle, socket, scope } = setup(definition)
  const taskValues = title => ({ id: { textValue: 'task-1' }, title: { textValue: title }, owner_id: { textValue: 'user-1' } })
  try {
    const sent = socket.sent[0].subscribeProjection.root
    assert.equal(sent.allFields, true)
    assert.equal(sent.entity.query.columns, undefined)
    assert.equal(sent.children[0].allFields, undefined)
    assert.deepEqual(sent.children[0].entity.query.columns, [{ segments: ['name'] }])
    socket.message({ subscriptionId: scope, sequence: '1', modelBatch: { kind: 'KIND_SNAPSHOT',
      entities: [{ entityId: 'task', upsert: { values: taskValues('Title') } }, { entityId: 'user', upsert: { values: { name: { textValue: 'Alice' } } } }],
      projections: [attach('task-instance', 'root', { entityId: 'task' }, '', '', scope), attach('owner-instance', 'root.owner', { entityId: 'user' }, 'task-instance', 'task', scope)],
    } })
    socket.message({ subscriptionId: scope, sequence: '2', ready: {} })
    const before = handle.data.getSnapshot()
    assert.deepEqual(Object.keys(before.value), ['owner'])
    assert.deepEqual(Object.keys(before.value.owner.value), ['name'])
    assert.equal(before.value.owner.value.name, 'Alice')
    assert.equal(client.store.getEntity('task').values.title.textValue, 'Title')
    socket.message({ connectionSequence: '1', modelBatch: { kind: 'KIND_COMMIT', entities: [{ entityId: 'task', upsert: { values: taskValues('Changed') } }] } })
    assert.equal(client.store.getEntity('task').values.title.textValue, 'Changed')
    assert.equal(handle.data.getSnapshot(), before)
  } finally { handle.dispose(); client.disconnect() }
})

test('nested output masks survive binary requests and shared selection shrink preserves typed views', () => {
  const field = (name, selection) => ({ path: { segments: [name] }, ...(selection ? { selection } : {}) })
  const ownerMask = { fields: [field('metadata', { object: { fields: [field('owner')] } })] }
  const membersMask = { fields: [field('metadata', { object: { fields: [field('members', { elements: { object: { fields: [field('name')] } } })] } })] }
  const definition = (mask, fields) => defineProjection({ ...root, fields: mask }, {
    nodeId: 'root', kind: 'entity', children: {}, fields: [{ name: 'meta', path: ['metadata'], type: { kind: 'object', fields } }],
  }, [])
  const owner = definition(ownerMask, [scalar('owner', 'text')])
  const members = definition(membersMask, [{ name: 'members', path: ['members'], type: { kind: 'list', element: { kind: 'object', nullable: true, fields: [scalar('name', 'text')] } } }])
  const { client, handle, socket, scope } = setup(owner)
  let other
  const values = includeMembers => ({ metadata: { objectValue: { fields: {
    owner: { textValue: 'Alice' },
    ...(includeMembers ? { members: { listValue: { values: [{ objectValue: { fields: { name: { textValue: 'Bob' } } } }, { nullValue: true }] } } } : {}),
  } } } })
  try {
    assert.deepEqual(socket.sent[0].subscribeProjection.root.fields, ownerMask)
    socket.message({ subscriptionId: scope, sequence: '1', modelBatch: { kind: 'KIND_SNAPSHOT', entities: [{ entityId: 'e', upsert: { values: values(false) } }], projections: [attach('a', 'root', { entityId: 'e' }, '', '', scope)] } })
    socket.message({ subscriptionId: scope, sequence: '2', ready: {} })
    const before = handle.data.getSnapshot()
    other = members.subscribe(client, {})
    const second = socket.sent.at(-1).subscriptionId
    assert.deepEqual(socket.sent.at(-1).subscribeProjection.root.fields, membersMask)
    socket.message({ subscriptionId: second, sequence: '1', modelBatch: { kind: 'KIND_SNAPSHOT', entities: [{ entityId: 'e', upsert: { values: values(true) } }], projections: [attach('b', 'root', { entityId: 'e' }, '', '', second)] } })
    socket.message({ subscriptionId: second, sequence: '2', ready: {} })
    assert.equal(handle.data.getSnapshot(), before)
    const items = other.data.getSnapshot().value.meta.members
    assert.deepEqual(items.map(item => item && item.name), ['Bob', null])
    assert.deepEqual(Object.keys(items[0]), ['name'])
    other.dispose()
    socket.message({ connectionSequence: '1', modelBatch: { kind: 'KIND_COMMIT', entities: [{ entityId: 'e', upsert: { values: values(false) } }] } })
    assert.equal(handle.data.getSnapshot(), before)
    assert.equal(client.store.getEntity('e').values.metadata.objectValue.fields.members, undefined)
  } finally { other?.dispose(); handle.dispose(); client.disconnect() }
})

test('prepared requests own input values before asynchronous subscription work', () => {
  const definition = defineProjection(root, { nodeId: 'root', kind: 'entity', fields: [], children: {} }, [{ name: 'value', type: { kind: 'structured' } }])
  const input = { value: { objectValue: { fields: { name: { textValue: 'before' } } } } }
  const prepared = definition.prepare(input)
  input.value.objectValue.fields.name.textValue = 'after'
  assert.equal(prepared.request.parameters.value.objectValue.fields.name.textValue, 'before')
  assert.ok(Object.isFrozen(prepared.request.parameters.value.objectValue.fields))
  assert.equal(prepared.key, definition.prepare({ value: prepared.request.parameters.value }).key)
})

test('compiled codecs preserve exact scalar values, structured data, lists and embedded field paths', () => {
  const definition = defineProjection(root, { nodeId: 'root', kind: 'entity', children: {}, fields: [
    scalar('integer', 'integer'), scalar('decimal', 'decimal'), scalar('float', 'float'), scalar('bytes', 'bytes'), scalar('timestamp', 'datetime'), scalar('arbitrary', 'structured'), scalar('absent', 'text', true), scalar('nil', 'text', true),
    { name: 'nested', path: ['nested'], type: { kind: 'object', fields: [{ name: 'renamed', path: ['odd.name'], type: { kind: 'text' } }, { name: 'list', path: ['list'], type: { kind: 'list', element: { kind: 'integer' } } }] } },
  ] }, [{ name: 'integer', type: { kind: 'integer' } }])
  const { client, handle, socket, scope } = setup(definition, { integer: 9223372036854775807n })
  try {
    assert.equal(socket.sent[0].subscribeProjection.parameters.integer.integerValue, '9223372036854775807')
    socket.message({ subscriptionId: scope, sequence: '1', modelBatch: { kind: 'KIND_SNAPSHOT',
      entities: [{ entityId: 'e', upsert: { values: {
        integer: { integerValue: '-9223372036854775808' }, decimal: { decimalValue: '1234567890.123456789' }, float: { floatValue: 'Infinity' }, bytes: { bytesValue: 'AP8=' }, timestamp: { datetimeValue: '2026-10-27T00:00:00.123456789Z' }, nil: { nullValue: true }, arbitrary: { objectValue: { fields: { x: { uuidValue: '10000000-0000-4000-8000-000000000001' } } } },
        nested: { objectValue: { fields: { 'odd.name': { textValue: 'value' }, list: { listValue: { values: [{ integerValue: '1' }, { integerValue: '2' }] } } } } },
      } } }], projections: [attach('r', 'root', { entityId: 'e' }, '', '', scope)],
    } })
    socket.message({ subscriptionId: scope, sequence: '2', ready: {} })
    const value = handle.data.getSnapshot().value
    assert.equal(value.integer, -9223372036854775808n)
    assert.equal(value.decimal, '1234567890.123456789')
    assert.equal(value.float, Infinity)
    assert.equal(value.bytes, 'AP8=')
    assert.equal(value.timestamp, '2026-10-27T00:00:00.123456789Z')
    assert.equal(value.nil, null); assert.equal(value.absent, undefined)
    assert.deepEqual(value.nested.list, [1n, 2n]); assert.equal(value.nested.renamed, 'value')
    assert.ok(Object.isFrozen(value.nested)); assert.ok(Object.isFrozen(value.nested.list))
    assert.throws(() => definition.request({ integer: 1n << 63n }), /expected integer/)
    assert.throws(() => definition.request({ integer: 1 }), /expected integer/)
  } finally { handle.dispose(); client.disconnect() }
})

test('nested window locators distinguish the same entity reached along diamond paths', async () => {
  const members = { nodeId: 'root.members', list: { query: { table: 'members' }, window: { from: 'FROM_START', count: 10 } }, children: [{ nodeId: 'root.members.entity', entity: { query: { table: 'items' } }, children: [{ nodeId: 'root.members.entity.children', list: { query: { table: 'items' }, window: { from: 'FROM_START', count: 10 } } }] }] }
  const definition = defineProjection({ ...root, children: [members] }, {
    nodeId: 'root', kind: 'entity', fields: [], children: { members: { nodeId: members.nodeId, kind: 'list', fields: [], children: { entity: { nodeId: 'root.members.entity', kind: 'entity', fields: [], children: { children: { nodeId: 'root.members.entity.children', kind: 'list', fields: [], children: {} } } } } } },
  }, [])
  const { client, handle, socket, scope } = setup(definition)
  try {
    socket.message({ subscriptionId: scope, sequence: '1', modelBatch: { kind: 'KIND_SNAPSHOT',
      entities: ['project', 'a', 'b', 'shared'].map(entityId => ({ entityId, upsert: { values: {} } })),
      queries: [reset('members', ['a', 'b']), reset('a-children', []), reset('b-children', [])],
      projections: [attach('r', 'root', { entityId: 'project' }, '', '', scope), attach('m', 'root.members', { querySubscriptionId: 'members' }, 'r', 'project', scope),
        attach('a-item', 'root.members.entity', { entityId: 'shared' }, 'm', 'a', scope), attach('b-item', 'root.members.entity', { entityId: 'shared' }, 'm', 'b', scope),
        attach('a-list', 'root.members.entity.children', { querySubscriptionId: 'a-children' }, 'a-item', 'shared', scope), attach('b-list', 'root.members.entity.children', { querySubscriptionId: 'b-children' }, 'b-item', 'shared', scope)],
    } })
    socket.message({ subscriptionId: scope, sequence: '2', ready: {} })
    const data = handle.data.getSnapshot().value.members
    const first = handle.window(data[0].value.entity.value.children)
    const second = handle.window(data[1].value.entity.value.children)
    for (const [control, target, sequence] of [[first, 'a-list', '3'], [second, 'b-list', '4']]) {
      const promise = control.extend('SIDE_RIGHT', 1)
      const sent = socket.sent.at(-1)
      assert.equal(sent.updateProjectionWindow.instanceId, target)
      socket.message({ subscriptionId: scope, requestId: sent.requestId, sequence, modelBatch: { kind: 'KIND_WINDOW' } })
      await promise
    }
    socket.message({ connectionSequence: '1', modelBatch: { kind: 'KIND_COMMIT', projections: [{ occurrenceId: 'a-list', detached: {} }], queries: [{ querySubscriptionId: 'a-children', closed: {} }] } })
    await assert.rejects(first.extend('SIDE_RIGHT', 1), /no longer present/)
  } finally { handle.dispose(); client.disconnect() }
})

test('schema mismatches fail typed reads without disconnecting a healthy stream', () => {
  const definition = defineProjection(root, { nodeId: 'root', kind: 'entity', fields: [scalar('title', 'text')], children: {} }, [])
  const { client, handle, socket, scope } = setup(definition)
  try {
    socket.message({ subscriptionId: scope, sequence: '1', modelBatch: { kind: 'KIND_SNAPSHOT', entities: [{ entityId: 'e', upsert: { values: { title: { boolValue: false } } } }], projections: [attach('r', 'root', { entityId: 'e' }, '', '', scope)] } })
    socket.message({ subscriptionId: scope, sequence: '2', ready: {} })
    assert.throws(() => handle.data.getSnapshot(), ProjectionDecodeError)
    assert.equal(socket.readyState, 1)
    socket.message({ connectionSequence: '1', modelBatch: { kind: 'KIND_COMMIT', entities: [{ entityId: 'e', upsert: { values: { title: { textValue: 'recovered' } } } }] } })
    assert.equal(handle.data.getSnapshot().value.title, 'recovered')
  } finally { handle.dispose(); client.disconnect() }
})

test('selected embedded values reconcile and subscribers keep independent notification baselines', () => {
  const definition = defineProjection(root, { nodeId: 'root', kind: 'entity', children: {}, fields: [scalar('title', 'text'),
    { name: 'metadata', path: ['metadata'], type: { kind: 'object', fields: [
      { name: 'names', path: ['names'], type: { kind: 'list', element: { kind: 'object', fields: [scalar('name', 'text')] } } },
    ] } },
  ] }, [])
  const { client, handle, socket, scope } = setup(definition)
  const values = (title, hidden) => ({ title: { textValue: title }, metadata: { objectValue: { fields: {
    names: { listValue: { values: [{ objectValue: { fields: { name: { textValue: 'same' }, hidden: { textValue: hidden } } } }] } },
  } } } })
  try {
    socket.message({ subscriptionId: scope, sequence: '1', modelBatch: { kind: 'KIND_SNAPSHOT', entities: [{ entityId: 'e', upsert: { values: values('Title', 'one') } }], projections: [attach('r', 'root', { entityId: 'e' }, '', '', scope)] } })
    socket.message({ subscriptionId: scope, sequence: '2', ready: {} })
    const before = handle.data.getSnapshot()
    const calls = [0, 0]
    const stopA = handle.data.subscribe(() => { calls[0]++; handle.data.getSnapshot() })
    const stopB = handle.data.subscribe(() => { calls[1]++ })
    socket.message({ connectionSequence: '1', modelBatch: { kind: 'KIND_COMMIT', entities: [{ entityId: 'e', upsert: { values: values('Title', 'two') } }] } })
    assert.equal(handle.data.getSnapshot(), before)
    assert.deepEqual(calls, [0, 0])
    socket.message({ connectionSequence: '2', modelBatch: { kind: 'KIND_COMMIT', entities: [{ entityId: 'e', upsert: { values: values('Changed', 'two') } }] } })
    assert.notEqual(handle.data.getSnapshot(), before)
    assert.equal(handle.data.getSnapshot().value.metadata, before.value.metadata)
    assert.deepEqual(calls, [1, 1], 'an earlier cached read cannot swallow another subscriber notification')
    stopA(); stopB()
  } finally { handle.dispose(); client.disconnect() }
})

test('mapped subscriptions report decode failures and notify recovery to an identical value', () => {
  const definition = defineProjection(root, { nodeId: 'root', kind: 'entity', fields: [scalar('title', 'text')], children: {} }, [])
  const { client, handle, socket, scope } = setup(definition)
  try {
    const upsert = value => ({ entityId: 'e', upsert: { values: { title: value } } })
    socket.message({ subscriptionId: scope, sequence: '1', modelBatch: { kind: 'KIND_SNAPSHOT', entities: [upsert({ textValue: 'same' })], projections: [attach('r', 'root', { entityId: 'e' }, '', '', scope)] } })
    socket.message({ subscriptionId: scope, sequence: '2', ready: {} })
    let errors = 0, successes = 0
    const stop = handle.data.subscribe(() => { try { handle.data.getSnapshot(); successes++ } catch (error) { assert.ok(error instanceof ProjectionDecodeError); errors++ } })
    socket.message({ connectionSequence: '1', modelBatch: { kind: 'KIND_COMMIT', entities: [upsert({ boolValue: true })] } })
    socket.message({ connectionSequence: '2', modelBatch: { kind: 'KIND_COMMIT', entities: [upsert({ textValue: 'same' })] } })
    assert.equal(errors, 1); assert.equal(successes, 1)
    stop()
  } finally { handle.dispose(); client.disconnect() }
})
