import test from 'node:test'
import assert from 'node:assert/strict'
import { ModelStore, decodeServerMessage, readProjection, scalar } from '../dist/index.js'
import { attach, batch, entity, initial, ready, request, reset, modelBytes } from './helpers.mjs'

test('commit observers run once after validation and before selector delivery', () => {
  const store = new ModelStore()
  const selected = store.observe(model => model.getEntity('a'))
  const order = []
  const stopSelector = selected.subscribe(() => order.push('selector'))
  const stopCommit = store.subscribeCommits(() => {
    order.push('commit')
    assert.equal(selected.getSnapshot().values.title.textValue, 'Updated')
    assert.deepEqual(store.getQuery('q').ids, ['a'])
  })
  assert.throws(() => store.apply(batch({ entities: [entity('a', 'Bad')], queries: [{ querySubscriptionId: 'unknown', delta: { baseVersion: '1', resultVersion: '2' } }] })), /unknown query/)
  assert.deepEqual(order, [])
  store.apply(batch({ entities: [entity('a', 'Updated')], queries: [reset('q', ['a'])] }))
  assert.deepEqual(order, ['commit', 'selector'])
  stopCommit(); stopSelector()
  store.reset()
  assert.deepEqual(order, ['commit', 'selector'])
})

test('materializes diamonds through exact occurrence edges with one entity image', () => {
  const store = new ModelStore()
  store.setConnection({ status: 'connected' })
  store.apply(batch(initial(), { subscriptionId: 'p', sequence: '1' }))
  store.apply(ready('p', '2'))
  const view = store.observe(s => readProjection(s, 'p', request))
  let notifications = 0
  const stop = view.subscribe(() => notifications++)
  const rows = view.getSnapshot().root.row.children.memberships.rows
  assert.equal(rows.length, 2)
  assert.equal(rows[0].children.task.row.entity, rows[1].children.task.row.entity)
  store.apply(batch({ entities: [entity('shared', 'Updated')] }, { connectionSequence: '1' }))
  const next = view.getSnapshot().root.row.children.memberships.rows
  assert.equal(scalar(next[0].children.task.row.entity.values, 'title'), 'Updated')
  assert.equal(next[0].children.task.row.entity, next[1].children.task.row.entity)
  assert.equal(notifications, 1)
  stop()
  assert.equal(store.listeners.size, 0)
})

test('stages the whole batch and sequence before committing anything', () => {
  const store = new ModelStore()
  store.apply(batch({ entities: [entity('a', 'before')], queries: [reset('q', ['a'])] }))
  const beforeEntity = store.getEntity('a'), beforeQuery = store.getQuery('q')
  const selected = store.observe(s => [s.getEntity('a'), s.getQuery('q')])
  let notifications = 0
  const stop = selected.subscribe(() => notifications++)
  assert.throws(() => store.apply(batch({ entities: [entity('a', 'wrong')], queries: [{ querySubscriptionId: 'q', delta: { baseVersion: '8', resultVersion: '9' } }] }, { connectionSequence: '1' })), /version mismatch/)
  assert.equal(store.getEntity('a'), beforeEntity)
  assert.equal(store.getQuery('q'), beforeQuery)
  assert.equal(notifications, 0)
  store.apply(batch({ entities: [entity('a', 'after')], queries: [{ querySubscriptionId: 'q', delta: { baseVersion: '1', resultVersion: '2', removedEntityIds: ['a'], insertions: [{ entityId: 'b', finalIndex: 0 }] } }] }, { connectionSequence: '1' }))
  assert.equal(notifications, 1)
  assert.deepEqual(store.getQuery('q').ids, ['b'])
  stop()
})

test('authoritative replacements remove absent fields and pending discards old payload', () => {
  const store = new ModelStore()
  store.apply(batch({ entities: [entity('a', 'one', { optional: { textValue: 'remove me' } })] }))
  store.apply(batch({ entities: [entity('a', 'two')] }))
  assert.equal(store.getEntity('a').values.optional, undefined)
  store.apply(batch({ entities: [{ entityId: 'a', pending: {} }] }))
  assert.deepEqual(store.getEntity('a'), { status: 'pending' })
  store.apply(batch({ entities: [entity('a', 'hydrated')] }))
  assert.equal(store.getEntity('a').status, 'available')
  store.apply(batch({ entities: [{ entityId: 'a', released: {} }] }))
  assert.equal(store.getEntity('a').status, 'unavailable')
})

test('list removals/moves do not release shared entities', () => {
  const store = new ModelStore()
  store.apply(batch({ entities: [entity('a', 'A'), entity('b', 'B')], queries: [reset('one', ['a', 'b']), reset('two', ['a'])] }))
  store.apply(batch({ queries: [{ querySubscriptionId: 'one', delta: { baseVersion: '1', resultVersion: '2', removedEntityIds: ['a'], insertions: [{ entityId: 'a', finalIndex: 1 }] } }] }))
  assert.deepEqual(store.getQuery('one').ids, ['b', 'a'])
  store.apply(batch({ queries: [{ querySubscriptionId: 'one', closed: {} }] }))
  assert.equal(store.getEntity('a').status, 'available')
  assert.deepEqual(store.getQuery('two').ids, ['a'])
})

test('rejects invalid list changes without changing the live query', () => {
  const badChanges = [
    { removedEntityIds: ['missing'] }, { removedEntityIds: ['a', 'a'] },
    { insertions: [{ entityId: 'a' }] }, { insertions: [{ entityId: 'b', finalIndex: 2 }] },
    { insertions: [{ entityId: 'b' }, { entityId: 'c' }] },
  ]
  for (const delta of badChanges) {
    const store = new ModelStore()
    store.apply(batch({ queries: [reset('q', ['a'])] }))
    const before = store.getQuery('q')
    assert.throws(() => store.apply(batch({ queries: [{ querySubscriptionId: 'q', delta: { baseVersion: '1', resultVersion: '2', ...delta } }] })))
    assert.equal(store.getQuery('q'), before)
  }
})

test('list deltas merge unsorted final-index insertions and moves in linear traversal', () => {
  const store = new ModelStore()
  store.apply(batch({ queries: [reset('q', ['a', 'b', 'c', 'd'])] }))
  store.apply(batch({ queries: [{ querySubscriptionId: 'q', delta: {
    baseVersion: '1', resultVersion: '2', removedEntityIds: ['a', 'c'],
    insertions: [{ entityId: 'a', finalIndex: 4 }, { entityId: 'new', finalIndex: 0 }, { entityId: 'c', finalIndex: 2 }],
  } }] }))
  assert.deepEqual(store.getQuery('q').ids, ['new', 'b', 'c', 'd', 'a'])
})

test('snapshot/ready/window and connection sequences are independent and lossless', () => {
  const store = new ModelStore()
  store.apply(batch({ queries: [reset('q', [], '9007199254740993')] }, { subscriptionId: 'p', sequence: '1' }))
  store.apply(ready('p', '2'))
  store.apply(batch({ queries: [{ querySubscriptionId: 'q', delta: { baseVersion: '9007199254740993', resultVersion: '9007199254740994', insertions: [{ entityId: 'a' }] } }] }, { connectionSequence: '1' }))
  assert.equal(store.getQuery('q').version, 9007199254740994n)
  store.apply(batch({ queries: [{ querySubscriptionId: 'q', windowChanged: { right: { cursor: 'opaque' } } }] }, { subscriptionId: 'p', sequence: '3' }))
  assert.throws(() => store.apply(batch({}, { connectionSequence: '3' })), /sequence gap/)
  store.apply(batch({}, { connectionSequence: '2' }))
  assert.throws(() => store.apply(ready('p', '3')), /sequence gap/)
  assert.equal(decodeServerMessage(modelBytes({ sequence: '18446744073709551615', ready: {} })).sequence, 18446744073709551615n)
})

test('occurrence indexes distinguish projections and separate paths to the same parent entity', () => {
  const store = new ModelStore()
  store.apply(batch({ projections: [
    attach('one', 'child', { entityId: 'a' }, 'parent-one', 'shared', 'p'),
    attach('two', 'child', { entityId: 'b' }, 'parent-two', 'shared', 'p'),
    attach('three', 'child', { entityId: 'c' }, 'parent-one', 'shared', 'other'),
  ] }))
  assert.equal(store.getOccurrences('p', 'child', 'parent-one', 'shared')[0].target.id, 'a')
  assert.equal(store.getOccurrences('p', 'child', 'parent-two', 'shared')[0].target.id, 'b')
  assert.equal(store.getOccurrences('other', 'child', 'parent-one', 'shared')[0].target.id, 'c')
  store.apply(batch({ projections: [{ occurrenceId: 'one', detached: {} }] }))
  assert.equal(store.getOccurrences('p', 'child', 'parent-one', 'shared').length, 0)
  assert.equal(store.getOccurrences('other', 'child', 'parent-one', 'shared').length, 1)
})

test('selectors are stable for unrelated commits and track changing dependencies', () => {
  const store = new ModelStore()
  store.apply(batch({ entities: [entity('a', 'A'), entity('b', 'B')], queries: [reset('q', ['a'])] }))
  const view = store.observe(s => ({ row: s.getEntity(s.getQuery('q').ids[0]) }))
  let notifications = 0
  const stop = view.subscribe(() => notifications++)
  const before = view.getSnapshot()
  store.apply(batch({ entities: [entity('b', 'Unrelated')] }))
  assert.equal(view.getSnapshot(), before)
  assert.equal(notifications, 0)
  store.apply(batch({ queries: [{ querySubscriptionId: 'q', delta: { baseVersion: '1', resultVersion: '2', removedEntityIds: ['a'], insertions: [{ entityId: 'b' }] } }] }))
  assert.equal(notifications, 1)
  store.apply(batch({ entities: [entity('a', 'No longer relevant')] }))
  assert.equal(notifications, 1)
  store.apply(batch({ entities: [entity('b', 'Now relevant')] }))
  assert.equal(notifications, 2)
  stop()
})

test('all selector reads inside a notification see the entire new batch', () => {
  const store = new ModelStore()
  const a = store.observe(s => s.getEntity('a')), b = store.observe(s => s.getEntity('b'))
  const stopA = a.subscribe(() => assert.equal(scalar(b.getSnapshot().values, 'title'), 'B'))
  const stopB = b.subscribe(() => {})
  store.apply(batch({ entities: [entity('a', 'A'), entity('b', 'B')] }))
  stopA(); stopB()
})

test('reading another selector before its notification cannot consume that notification', () => {
  for (const wrap of [false, true]) {
    const store = new ModelStore()
    const a = store.observe(s => s.getEntity('a'))
    let evaluations = 0
    const b = store.observe(s => {
      evaluations++
      const value = s.getEntity('b')
      return wrap ? { value } : value
    })
    let readDuringA, deliveredToB, notifications = 0
    const stopA = a.subscribe(() => { readDuringA = b.getSnapshot() })
    const stopB = b.subscribe(() => { notifications++; deliveredToB = b.getSnapshot() })
    store.apply(batch({ entities: [entity('a', 'A'), entity('b', 'B')] }))
    assert.equal(notifications, 1)
    assert.equal(deliveredToB, readDuringA)
    assert.equal(evaluations, 2, 'one bootstrap evaluation plus one evaluation for the batch')
    store.apply(batch({ entities: [entity('a', 'A2'), entity('b', 'B2')] }))
    assert.equal(notifications, 2)
    assert.equal(deliveredToB, readDuringA)
    stopA(); stopB()
  }
})

test('listener exceptions are isolated for subscribers sharing the same selector', () => {
  const errors = []
  const store = new ModelStore({ onListenerError: error => errors.push(error) })
  const shared = store.observe(s => s.getEntity('a'))
  const other = store.observe(s => s.getEntity('b'))
  let healthy = 0, unrelated = 0
  const failure = new Error('consumer failed')
  const stops = [
    shared.subscribe(() => { throw failure }),
    shared.subscribe(() => healthy++),
    other.subscribe(() => unrelated++),
  ]
  store.apply(batch({ entities: [entity('a', 'A'), entity('b', 'B')] }, { connectionSequence: '1' }))
  store.apply(batch({ entities: [entity('a', 'A2'), entity('b', 'B2')] }, { connectionSequence: '2' }))
  assert.equal(healthy, 2)
  assert.equal(unrelated, 2)
  assert.deepEqual(errors, [failure, failure])
  assert.equal(scalar(store.getEntity('a').values, 'title'), 'A2')
  stops.forEach(stop => stop())
})

test('a fresh subscription starts from the current snapshot, not an old notification baseline', () => {
  const store = new ModelStore()
  const view = store.observe(s => s.getEntity('a'))
  const stop = view.subscribe(() => {})
  stop()
  store.apply(batch({ entities: [entity('a', 'Unobserved')] }))
  view.getSnapshot()
  let notifications = 0
  const again = view.subscribe(() => notifications++)
  store.apply(batch({ entities: [entity('b', 'Unrelated')] }))
  assert.equal(notifications, 0)
  store.apply(batch({ entities: [entity('a', 'Next')] }))
  assert.equal(notifications, 1)
  again()
  assert.equal(store.listeners.size, 0)
})

test('reset removes stale models and sequences and notifies old selectors', () => {
  const store = new ModelStore()
  store.apply(batch(initial(), { subscriptionId: 'p', sequence: '1' }))
  const view = store.observe(s => readProjection(s, 'p', request))
  const stop = view.subscribe(() => {})
  store.reset({ status: 'connecting' })
  assert.equal(view.getSnapshot().root, undefined)
  assert.equal(store.getEntity('shared').status, 'unavailable')
  store.apply(batch(initial(), { subscriptionId: 'p', sequence: '1' }))
  assert.equal(view.getSnapshot().root.row.children.memberships.rows.length, 2)
  stop()
})

test('generated binary codec and model adapter validate events, limits, and immutable records', () => {
  assert.throws(() => decodeServerMessage(new Uint8Array([0xff])))
  assert.throws(() => decodeServerMessage(new Uint8Array()), /Missing server event/)
  assert.throws(() => batch({ entities: [{ entityId: 'a', upsert: {}, pending: {} }] }))
  assert.throws(() => batch({ entities: [entity('a', 'A', { bad: { boolValue: 'yes' } })] }))
  assert.throws(() => decodeServerMessage(modelBytes({ modelBatch: initial() }), { maxNodes: 2 }))
  assert.throws(() => decodeServerMessage(modelBytes({ modelBatch: initial() }), { maxDepth: 2 }))
  const message = batch({ entities: [entity('a', 'A', { object: { objectValue: { fields: { nested: { listValue: { values: [{ integerValue: '9223372036854775807' }] } } } } } })] })
  const store = new ModelStore()
  store.apply(message)
  assert.throws(() => { store.getEntity('a').values.title.textValue = 'mutated' })
  assert.throws(() => { store.getEntity('a').values.object.objectValue.fields.nested.listValue.values.push({ nullValue: true }) })
})
