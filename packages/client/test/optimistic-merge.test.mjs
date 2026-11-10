import test from 'node:test'
import assert from 'node:assert/strict'
import { ReactiveClient, MutationHarness, defineProjection, unionOptimisticShapes, mergeValue, unsetValue } from '../dist/index.js'
import { FakeSocket, attach, reset } from './helpers.mjs'

const text = { kind: 'text', nullable: true }
const field = (name, path, type, computed = false) => ({ name, path, type, computed })
const shape = { nodeId: 'tasks', kind: 'list', children: {}, fields: [
  field('title', ['title'], { kind: 'text' }),
  field('metadata', ['metadata'], { kind: 'object', fields: [
    field('color', ['theme', 'color'], text), field('legacy', ['legacy'], text),
    field('nested', ['prefs'], { kind: 'object', fields: [field('dense', ['dense'], { kind: 'bool' }), field('locale', ['locale'], text)] }),
    field('items', ['items'], { kind: 'list', element: { kind: 'object', fields: [field('label', ['label'], text)] } }),
  ] }),
  field('colorAlias', ['metadata', 'theme', 'color'], text),
  field('payload', ['payload'], { kind: 'structured', nullable: true }),
  field('editable', ['canEdit'], { kind: 'bool' }, true),
] }
const definition = defineProjection({ nodeId: 'tasks', list: { query: { table: 'tasks' }, window: { count: 10 } }, computed: ['canEdit'] }, shape, [])
const value = input => input === null ? { nullValue: true } : typeof input === 'string' ? { textValue: input } :
  typeof input === 'boolean' ? { boolValue: input } : typeof input === 'bigint' ? { integerValue: input.toString() } :
  Array.isArray(input) ? { listValue: { values: input.map(value) } } : { objectValue: { fields: Object.fromEntries(Object.entries(input).map(([key, item]) => [key, value(item)])) } }
const native = input => 'objectValue' in input ? Object.fromEntries(Object.entries(input.objectValue.fields ?? {}).map(([key, item]) => [key, native(item)])) :
  'listValue' in input ? (input.listValue.values ?? []).map(native) : 'nullValue' in input ? null : 'integerValue' in input ? BigInt(input.integerValue) : 'textValue' in input ? input.textValue : input.boolValue
const base = () => ({ title: 'Task', metadata: {
  theme: { color: 'blue', hidden: 'Theme' }, prefs: { dense: false, locale: 'en', hidden: 'Preferences' },
  items: [{ label: 'First', hidden: 'Item' }], legacy: 'Old', hidden: 'Metadata',
}, payload: { color: 'blue', prefs: { dense: false, locale: 'en' }, legacy: 'Old', hidden: 'Payload' } })

function setup(t, source = base()) {
  const socket = new FakeSocket()
  const client = new ReactiveClient({ url: 'ws://test', heartbeatIntervalMs: 0, createSocket: () => socket })
  t.after(() => client.clear())
  client.connect(); socket.open(); socket.message({ connectionOpened: { connectionContext: 'merge' } })
  const entity = source => ({ entityId: 'task', upsert: { values: Object.fromEntries(Object.entries(source).map(([key, item]) => [key, value(item)])) } })
  const open = () => {
    const handle = definition.subscribe(client, {})
    socket.message({ subscriptionId: handle.id, sequence: '1', modelBatch: {
      kind: 'KIND_SNAPSHOT', entities: [entity(source)], queries: [reset(`query-${handle.id}`, ['task'])],
      projections: [attach(`occurrence-${handle.id}`, 'tasks', { querySubscriptionId: `query-${handle.id}` }, '', '', handle.id)],
      computed: [{ occurrenceId: `occurrence-${handle.id}`, entityId: 'task', upsert: { fields: { canEdit: { boolValue: true } } } }],
    } })
    socket.message({ subscriptionId: handle.id, sequence: '2', ready: {} })
    return handle
  }
  const handle = open()
  let sequence = 0
  const commit = batch => socket.message({ connectionSequence: String(++sequence), modelBatch: { kind: 'KIND_COMMIT', atomic: true, ...batch } })
  return { client, handle, open, commit, entity, row: () => handle.data.getSnapshot()[0], record: () => Object.fromEntries(Object.entries(client.store.getEntity('task').values).map(([key, item]) => [key, native(item)])) }
}

test('typed merges respect aliases, keep hidden siblings live, and share changes across projections', t => {
  const { client, handle, open, row, record, commit, entity } = setup(t)
  const other = open()
  const draft = client.optimistic({ mutationId: 'typed' })
  draft.patch(row(), { metadata: mergeValue({ color: 'red', nested: mergeValue({ dense: true }), legacy: unsetValue }) })
  draft.fill(unionOptimisticShapes([row()], { tasks: handle }), { tasks: { editable: occurrence => occurrence.current.metadata.nested.dense } })
  assert.equal(row().value.metadata.color, 'blue', 'staging does not publish')
  const layer = draft.commit()
  assert.equal(row().value.metadata.color, 'red')
  assert.equal(row().value.metadata.legacy, undefined)
  assert.equal(other.data.getSnapshot()[0].value.metadata.color, 'red')
  assert.equal(record().metadata.theme.hidden, 'Theme')
  const updated = base()
  updated.metadata.theme = { color: 'green', hidden: 'New theme' }
  updated.metadata.prefs.locale = 'fr'
  updated.metadata.hidden = 'New metadata'
  updated.metadata.legacy = 'Server legacy'
  commit({ entities: [entity(updated)] })
  assert.deepEqual(record().metadata.theme, { color: 'red', hidden: 'New theme' })
  assert.deepEqual(record().metadata.prefs, { dense: true, locale: 'fr', hidden: 'Preferences' })
  assert.equal(record().metadata.hidden, 'New metadata')
  assert.equal(row().value.metadata.legacy, undefined)
  layer.rollback()
  assert.deepEqual(record(), updated)
  assert.equal(other.data.getSnapshot()[0].value.metadata.color, 'green')
})

test('opaque merges use canonical member values, literal object keys, and explicit nested merges', t => {
  const { client, row, record, commit, entity } = setup(t)
  const input = { color: { textValue: 'red' }, prefs: mergeValue({ dense: { boolValue: true } }), legacy: unsetValue,
    'a.b': { integerValue: '9223372036854775807' }, '0': { textValue: 'zero' }, ['__proto__']: { textValue: 'ordinary key' }, objectValue: { textValue: 'also an ordinary key' } }
  const draft = client.optimistic({ mutationId: 'opaque' })
  draft.patch(row(), { payload: mergeValue(input) })
  input.color.textValue = 'Mutated'; input.extra = { textValue: 'Too late' }
  const layer = draft.commit()
  const expected = { ...base().payload, color: 'red', prefs: { dense: true, locale: 'en' }, 'a.b': 9223372036854775807n,
    '0': 'zero', ['__proto__']: 'ordinary key', objectValue: 'also an ordinary key' }
  delete expected.legacy
  assert.deepEqual(record().payload, expected)
  const updated = base()
  updated.payload.hidden = 'New payload'; updated.payload.prefs.locale = 'fr'
  commit({ entities: [entity(updated)] })
  assert.equal(record().payload.hidden, 'New payload')
  assert.deepEqual(record().payload.prefs, { dense: true, locale: 'fr' })
  assert.equal(record().payload.extra, undefined)
  layer.rollback()
  assert.deepEqual(record().payload, updated.payload)
})

test('unmarked subobjects and arrays replace, while null assigns and unset removes', t => {
  const { client, row, record } = setup(t)
  const draft = client.optimistic({ mutationId: 'replace-members' })
  draft.patch(row(), {
    metadata: mergeValue({ nested: { dense: true, locale: null }, items: [{ label: 'Replacement' }], legacy: null }),
    payload: mergeValue({ prefs: value({ dense: true }), hidden: { nullValue: true }, legacy: unsetValue }),
  })
  const layer = draft.commit()
  assert.deepEqual(record().metadata.prefs, { dense: true, locale: null })
  assert.deepEqual(record().metadata.items, [{ label: 'Replacement' }])
  assert.equal(record().metadata.legacy, null)
  assert.deepEqual(record().payload.prefs, { dense: true })
  assert.equal(record().payload.hidden, null)
  assert.equal(Object.hasOwn(record().payload, 'legacy'), false)
  layer.rollback()
  assert.deepEqual(record(), base())
})

test('empty merges are no-ops, but ordinary empty objects still replace', t => {
  const { client, row, record } = setup(t)
  const draft = client.optimistic({ mutationId: 'empty' })
  draft.patch(row(), { metadata: mergeValue({ nested: mergeValue({}) }), payload: mergeValue({ ignored: undefined }) })
  assert.equal(draft.hasChanges, false)
  draft.patch(row(), { title: 'Changed', payload: { objectValue: {} } })
  const layer = draft.commit()
  assert.equal(record().title, 'Changed')
  assert.deepEqual(record().payload, {})
  layer.rollback()
})

for (const initial of [null, 'scalar']) test(`opaque path sets create an object from ${initial}, while deletion alone leaves it untouched`, t => {
  const { client, row, record } = setup(t, { ...base(), payload: initial })
  const removal = client.optimistic({ mutationId: 'remove-only' })
  removal.patch(row(), { payload: mergeValue({ missing: mergeValue({ key: unsetValue }) }) })
  const deleted = removal.commit()
  assert.equal(record().payload, initial)
  deleted.rollback()
  const draft = client.optimistic({ mutationId: 'create-object' })
  draft.patch(row(), { payload: mergeValue({ nested: mergeValue({ key: { textValue: 'new' } }) }) })
  const layer = draft.commit()
  assert.deepEqual(record().payload, { nested: { key: 'new' } })
  layer.rollback()
  assert.equal(record().payload, initial)
})

test('merge validation is atomic, rejects unsupported contexts, and retains normal overlap checks', t => {
  const { client, handle, row, record } = setup(t)
  for (const fields of [null, [], 'invalid']) assert.throws(() => mergeValue(fields), /object-member map/)
  const draft = client.optimistic({ mutationId: 'validation' })
  for (const input of [
    { title: 'Must not stage', metadata: mergeValue({ color: 123 }) },
    { metadata: mergeValue({ unknown: undefined }) },
    { title: mergeValue({}) },
    { metadata: mergeValue({ items: mergeValue({}) }) },
    { payload: mergeValue({ invalid: 123 }) },
    { payload: mergeValue({ invalid: { textValue: 123 } }) },
    { metadata: mergeValue({ color: 'red' }), colorAlias: 'blue' },
    { metadata: row().value.metadata, colorAlias: 'red' },
    { metadata: { nested: mergeValue({ dense: true }) } },
  ]) {
    assert.throws(() => draft.patch(row(), input))
    assert.equal(draft.hasChanges, false)
  }
  const cyclic = {}
  const marker = mergeValue(cyclic)
  cyclic.self = marker
  assert.throws(() => draft.patch(row(), { payload: marker }), /cyclic/)
  assert.throws(() => draft.patch(row(), { payload: mergeValue({}) }, ['payload']), /disjoint/)
  assert.throws(() => draft.insert(handle.data.getSnapshot(), { payload: mergeValue({}) }), /only supported/)
  assert.throws(() => draft.insert(handle.data.getSnapshot(), mergeValue({})), /only supported/)
  assert.throws(() => draft.fill(unionOptimisticShapes([row()], { tasks: handle }), { tasks: { editable: () => mergeValue({}) } }), /only supported/)
  assert.equal(draft.hasChanges, false)
  draft.patch(row(), { metadata: mergeValue({ color: 'red' }), colorAlias: 'red' })
  const layer = draft.commit()
  assert.equal(record().metadata.theme.color, 'red')
  layer.rollback()
})

test('harness writers merge typed and opaque fields; CDC confirmation retires every leaf atomically', async t => {
  const { client, handle, row, record, commit, entity } = setup(t)
  const harness = new MutationHarness(client, { decodeReceipts: transaction => transaction.messages.map(message => message.kind) })
  t.after(() => harness.dispose())
  const operation = harness.run({
    optimistic: writer => writer.patch(row(), { metadata: mergeValue({ color: 'red' }), payload: mergeValue({ color: { textValue: 'red' } }) }),
    send: async () => 'ok',
  })
  await operation.response
  assert.equal(record().metadata.theme.color, 'red')
  assert.equal(record().payload.color, 'red')
  const seen = []
  const stop = handle.data.subscribe(() => seen.push([row().value.metadata.color, native(row().value.payload).color]))
  const confirmed = base()
  confirmed.metadata.theme.color = 'green'; confirmed.payload.color = 'green'
  commit({ entities: [entity(confirmed)], messages: [{ kind: operation.mutationId, payload: '' }] })
  await operation.applied
  stop()
  assert.deepEqual(seen, [['green', 'green']])
  assert.equal(client.getDiagnostics().store.optimistic.layers, 0)
})
