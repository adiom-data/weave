import test from 'node:test'
import assert from 'node:assert/strict'
import { ModelStore, readProjection, scalar } from '../dist/index.js'
import { batch, initial, ready, request } from './helpers.mjs'

const computed = (occurrenceId, role) => ({ occurrenceId, entityId: 'shared', upsert: { fields: { role: { integerValue: String(role) } } } })
const definition = structuredClone(request)
definition.root.children[0].children[0].computed = ['role']

test('computed values belong to occurrences, not shared entities, and publish atomically', () => {
  const store = new ModelStore()
  store.apply(batch({ ...initial(), computed: [computed('task1', 1), computed('task2', 2)] }, { subscriptionId: 'p', sequence: '1' }))
  store.apply(ready('p', '2'))
  const view = store.observe(s => readProjection(s, 'p', definition))
  const rows = () => view.getSnapshot().root.row.children.memberships.rows.map(r => r.children.task.row)
  assert.equal(rows()[0].entity, rows()[1].entity)
  assert.equal(rows()[0].entity.values.role, undefined)
  assert.deepEqual(rows().map(r => scalar(r.computed, 'role')), ['1', '2'])
  let calls = 0
  const stop = view.subscribe(() => {
    calls++
    assert.deepEqual(rows().map(r => scalar(r.computed, 'role')), ['3', '4'])
  })
  store.apply(batch({ computed: [computed('task1', 3), computed('task2', 4)] }))
  assert.equal(calls, 1)
  stop()
})

test('computed updates validate atomically and are retired with their occurrence', () => {
  const store = new ModelStore()
  store.apply(batch({ ...initial(), computed: [computed('task1', 1)] }))
  const before = store.getComputed('task1', 'shared')
  assert.throws(() => store.apply(batch({ computed: [computed('task1', 2), computed('missing', 3)] })), /occurrence/)
  assert.equal(store.getComputed('task1', 'shared'), before)
  assert.throws(() => store.apply(batch({ computed: [{ ...computed('task1', 2), entityId: 'another' }] })), /entity|target/)
  store.apply(batch({ computed: [{ occurrenceId: 'task1', entityId: 'shared', released: {} }] }))
  assert.equal(Object.keys(store.getComputed('task1', 'shared')).length, 0)
  store.apply(batch({ computed: [computed('task1', 5)] }))
  store.apply(batch({ projections: [{ occurrenceId: 'task1', detached: {} }] }))
  assert.equal(Object.keys(store.getComputed('task1', 'shared')).length, 0)
  assert.equal(store.getEntity('shared').status, 'available')
})

test('list-row computations follow final membership and roll back invalid sparse batches', () => {
  const store = new ModelStore()
  const value = (entityId, flag) => ({ occurrenceId: 'members', entityId, upsert: { fields: { flag: { boolValue: flag } } } })
  store.apply(batch({ ...initial(), computed: [value('m1', true), value('m2', false)] }))
  const old = store.getComputed('members', 'm2')
  assert.throws(() => store.apply(batch({ computed: [value('m2', true), value('not-a-member', true)] })), /nonmember/)
  assert.equal(store.getComputed('members', 'm2'), old)
  store.apply(batch({
    queries: [{ querySubscriptionId: 'list', delta: { baseVersion: '1', resultVersion: '2', removedEntityIds: ['m1'] } }],
    computed: [{ occurrenceId: 'members', entityId: 'm1', released: {} }, value('m2', true)],
  }))
  assert.equal(Object.keys(store.getComputed('members', 'm1')).length, 0)
  assert.equal(store.getComputed('members', 'm2').flag.boolValue, true)
  assert.equal(old.flag.boolValue, false)
  store.reset()
  assert.equal(Object.keys(store.getComputed('members', 'm2')).length, 0)
})
