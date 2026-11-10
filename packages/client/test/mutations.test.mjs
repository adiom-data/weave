import test from 'node:test'
import assert from 'node:assert/strict'
import { MutationHarness, MutationOutcomeUnknownError, MutationUnconfirmedError, ReactiveClient } from '../dist/index.js'
import { FakeSocket, entity } from './helpers.mjs'
import { mutationReceiptKind } from '../dist/index.js'

const deferred = () => { let resolve, reject; const promise = new Promise((yes, no) => { resolve = yes; reject = no }); return { promise, resolve, reject } }
const flush = async () => { for (let i = 0; i < 8; i++) await Promise.resolve() }
function setup(t, options = {}) {
  const sockets = [], errors = []
  const reactive = new ReactiveClient({ url: 'ws://test', heartbeatIntervalMs: 0, onListenerError: error => errors.push(error),
    createSocket: () => { const socket = new FakeSocket(); sockets.push(socket); return socket } })
  reactive.connect(); sockets[0].open(); sockets[0].message({ connectionOpened: { connectionContext: 'context' } })
  let sequence = 0
  const commit = (ids = [], extra = {}) => sockets[0].message({ connectionSequence: String(++sequence), modelBatch: {
    kind: 'KIND_COMMIT', atomic: true, checkpoint: `checkpoint-${sequence}`, messages: ids.map(id => ({ kind: 'receipt', payload: btoa(id) })), ...extra,
  } })
  commit([], { entities: [entity('e', 'Base')] })
  const harness = new MutationHarness(reactive, { synchronizationTimeoutMs: 1000,
    decodeReceipts: transaction => transaction.messages.filter(message => message.kind === 'receipt').map(message => atob(message.payload)),
    onListenerError: error => errors.push(error), ...options })
  t.after(() => { harness.dispose(); reactive.clear() })
  const predict = draft => draft.patchCanonical({ entityId: 'e' }, { title: { textValue: 'Predicted' } })
  return { reactive, harness, sockets, errors, commit, predict }
}

test('harness uses the standard receipt decoder when none is supplied', async t => {
  const { harness, commit } = setup(t, { decodeReceipts: undefined })
  const op = harness.run({ send: async () => 'ok' })
  await op.response
  const payload = Buffer.from(op.mutationId, 'utf8').toString('base64')
  commit([], { messages: [{ kind: mutationReceiptKind, payload }] })
  assert.equal((await op.applied).mutationId, op.mutationId)
})

test('prediction precedes send; RPC success retains it until atomic receipt publication and cleanup hooks', async t => {
  const { reactive, harness, commit, predict } = setup(t)
  const hooks = []
  let sends = 0
  const op = harness.run({ optimistic: predict, send: async context => {
    sends++; assert.equal(reactive.store.getEntity('e').values.title.textValue, 'Predicted')
    assert.equal(context.mutationId, op.mutationId); assert.equal(context.connectionContext, 'context')
    return { id: 123 }
  }, onResponseSettled: result => hooks.push(result.kind),
  onOptimisticRetired: event => hooks.push(`retired:${event.reason}:${reactive.store.getEntity('e').values.title.textValue}`),
  onApplied: () => hooks.push('applied') })
  assert.equal(sends, 0)
  assert.equal((await op.response).id, 123)
  assert.equal(op.state.getSnapshot().reactive.kind, 'waiting')
  assert.equal(reactive.store.getEntity('e').values.title.textValue, 'Predicted')
  const seen = [], view = reactive.store.observe(store => store.getEntity('e').values.title.textValue)
  const stop = view.subscribe(() => seen.push(view.getSnapshot()))
  commit([op.mutationId], { entities: [entity('e', 'Actual')] })
  assert.equal((await op.applied).checkpoint, 'checkpoint-2')
  await flush()
  assert.deepEqual(seen, ['Actual'])
  assert.deepEqual(hooks, ['succeeded', 'retired:applied:Actual', 'applied'])
  assert.equal(reactive.getDiagnostics().store.optimistic.layers, 0)
  stop()
})

test('receipt before lost RPC response remains known applied; no second retirement or RPC retry', async t => {
  const { harness, commit, predict } = setup(t)
  const rpc = deferred(), events = []
  let sends = 0
  const op = harness.run({ send: () => { sends++; return rpc.promise }, optimistic: predict,
    onOptimisticRetired: event => events.push(event.reason), onApplied: () => events.push('observed') })
  await flush()
  commit([op.mutationId, op.mutationId])
  const receipt = await op.applied
  rpc.reject(new TypeError('Response lost'))
  await assert.rejects(op.response, MutationOutcomeUnknownError)
  await flush()
  assert.equal(op.state.getSnapshot().reactive.receipt, receipt)
  assert.equal(op.state.getSnapshot().rpc.kind, 'unknown')
  assert.deepEqual(events, ['applied', 'observed'])
  assert.equal(sends, 1)
})

test('definite rejection restores backing and reports independent terminal outcomes', async t => {
  const denied = new Error('Permission denied')
  const { harness, reactive, predict } = setup(t, { classifyError: () => 'rejected' })
  const events = []
  const op = harness.run({ send: async () => { throw denied }, optimistic: predict, onOptimisticRetired: event => events.push(event.reason) })
  await assert.rejects(op.response, error => error === denied)
  await assert.rejects(op.applied, error => error === denied)
  await flush()
  assert.equal(op.state.getSnapshot().rpc.kind, 'rejected')
  assert.equal(op.state.getSnapshot().reactive.kind, 'unconfirmed')
  assert.equal(reactive.store.getEntity('e').values.title.textValue, 'Base')
  assert.deepEqual(events, ['rejected'])
})

test('ambiguous RPC failure can still be followed by a committed receipt', async t => {
  const { harness, reactive, commit, predict } = setup(t)
  const op = harness.run({ send: async () => { throw new Error('Network lost') }, optimistic: predict })
  await assert.rejects(op.response, MutationOutcomeUnknownError)
  assert.equal(op.state.getSnapshot().reactive.kind, 'waiting')
  assert.equal(reactive.store.getEntity('e').values.title.textValue, 'Predicted')
  commit([op.mutationId])
  await op.applied
  assert.equal(op.state.getSnapshot().rpc.kind, 'unknown')
  assert.equal(reactive.store.getEntity('e').values.title.textValue, 'Base', 'receipt does not imply a visible or hydrated replacement')
})

test('application-only optimism retires once; manual rollback does not cancel or stop receipt tracking', async t => {
  const { harness, commit, reactive } = setup(t)
  const events = []
  let sends = 0
  const op = harness.run({ send: async () => { sends++; return 'ok' }, onApplied: () => events.push('applied'), onOptimisticRetired: event => events.push(event.reason) })
  const snapshots = []
  const stop = op.state.subscribe(() => snapshots.push(op.state.getSnapshot()))
  op.rollbackOptimistic(); op.rollbackOptimistic()
  await op.response
  commit([op.mutationId]); await op.applied; await flush()
  assert.deepEqual(events, ['rolled-back', 'applied'])
  assert.equal(sends, 1)
  assert.equal(reactive.getDiagnostics().store.optimistic.layers, 0)
  assert.ok(snapshots.length > 0)
  assert.ok(Object.isFrozen(op.state.getSnapshot().optimistic.event))
  stop()
})

test('invalid optimistic setup never sends; staging writers cannot escape or commit themselves', async t => {
  const { harness, predict, reactive } = setup(t)
  let writer, sends = 0
  const setupError = new Error('Bad prediction')
  const events = []
  const op = harness.run({ send: async () => { sends++ }, optimistic: draft => { writer = draft; predict(draft); throw setupError }, onOptimisticRetired: event => events.push(event.reason) })
  assert.equal(writer.commit, undefined)
  assert.throws(() => predict(writer), /only valid during/)
  await assert.rejects(op.response, error => error === setupError)
  await assert.rejects(op.applied, error => error === setupError)
  await flush()
  assert.equal(op.state.getSnapshot().rpc.kind, 'not-sent')
  assert.equal(sends, 0)
  assert.equal(reactive.getDiagnostics().store.optimistic.layers, 0)
  assert.deepEqual(events, ['not-sent'])
})

test('async optimistic callbacks are rejected without sending or installing a partial prediction', async t => {
  const { harness, predict, reactive } = setup(t)
  let sends = 0
  const op = harness.run({ send: async () => { sends++ }, optimistic: async draft => {
    predict(draft)
    await Promise.resolve()
    predict(draft)
  } })
  await assert.rejects(op.response, /must be synchronous/)
  await assert.rejects(op.applied, /must be synchronous/)
  assert.equal(sends, 0)
  assert.equal(reactive.getDiagnostics().store.optimistic.layers, 0)
})

test('disconnect before deferred send means not-sent; obsolete callbacks cannot affect a replacement context', async t => {
  const { harness, reactive, sockets } = setup(t)
  let sends = 0
  const old = harness.run({ send: async () => { sends++ } })
  reactive.disconnect()
  reactive.connect(); sockets[1].open(); sockets[1].message({ connectionOpened: { connectionContext: 'next' } })
  await assert.rejects(old.response, /no mutation was sent/)
  await assert.rejects(old.applied)
  assert.equal(old.state.getSnapshot().rpc.kind, 'not-sent')
  assert.equal(sends, 0)
  const next = harness.run({ send: async context => context.connectionContext })
  assert.equal(await next.response, 'next')
})

test('receipt wins over disconnect or harness disposal triggered during its publication', async t => {
  for (const action of ['disconnect', 'dispose']) await t.test(action, async t => {
    const { harness, reactive, commit, predict } = setup(t)
    const events = []
    const op = harness.run({ send: async () => 'ok', optimistic: predict, onOptimisticRetired: event => events.push(event.reason) })
    await op.response
    const view = reactive.store.observe(store => store.getEntity('e'))
    const stop = view.subscribe(() => {
      if (view.getSnapshot().values?.title.textValue === 'Actual') {
        if (action === 'dispose') harness.dispose()
        else reactive.disconnect()
      }
    })
    commit([op.mutationId], { entities: [entity('e', 'Actual')] })
    await op.applied; await flush()
    assert.equal(op.state.getSnapshot().reactive.kind, 'applied')
    assert.deepEqual(events, ['applied'])
    stop()
  })
})

test('a receipt batch settles all operations before application hooks can disconnect', async t => {
  const { harness, reactive, commit } = setup(t)
  const a = harness.run({ send: async () => 'a', onApplied: () => reactive.clear() })
  const b = harness.run({ send: async () => 'b' })
  await Promise.all([a.response, b.response])
  commit([a.mutationId, b.mutationId])
  await Promise.all([a.applied, b.applied])
  assert.equal(b.state.getSnapshot().reactive.kind, 'applied')
})

test('receipt timeout preserves successful RPC fact and bounds pending capacity until RPC completion', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] })
  const { harness, predict, reactive } = setup(t, { synchronizationTimeoutMs: 10, maxPending: 1 })
  const rpc = deferred()
  const a = harness.run({ send: () => rpc.promise, optimistic: predict })
  await flush()
  t.mock.timers.tick(10)
  await assert.rejects(a.applied, MutationUnconfirmedError)
  assert.throws(() => harness.run({ send: async () => 'blocked' }), /Too many pending/)
  assert.equal(reactive.store.getEntity('e').values.title.textValue, 'Base')
  rpc.resolve('ok'); await a.response
  assert.equal(a.state.getSnapshot().rpc.kind, 'succeeded')
  assert.equal(a.state.getSnapshot().reactive.kind, 'unconfirmed')
  const b = harness.run({ send: async () => 'next' }); await b.response
  t.mock.timers.tick(10)
  await assert.rejects(b.applied, MutationUnconfirmedError)
})

test('callback, selector and classifier failures are isolated from committed state', async t => {
  const { harness, commit, errors } = setup(t, { classifyError: () => { throw new Error('classifier') } })
  const op = harness.run({ send: async () => { throw new Error('network') }, onApplied: () => { throw new Error('applied observer') }, onOptimisticRetired: () => { throw new Error('cleanup observer') } })
  op.state.subscribe(() => { throw new Error('state observer') })
  await assert.rejects(op.response, MutationOutcomeUnknownError)
  commit([op.mutationId]); await op.applied; await flush()
  assert.equal(op.state.getSnapshot().reactive.kind, 'applied')
  assert.ok(errors.some(error => error.message === 'classifier'))
  assert.ok(errors.some(error => error.message === 'cleanup observer'))
  assert.ok(errors.some(error => error.message === 'applied observer'))
  assert.ok(errors.some(error => error.message === 'state observer'))
})

test('a decoded receipt never acknowledges a batch that fails model validation', async t => {
  const { harness, commit, errors } = setup(t)
  const op = harness.run({ send: async () => 'ok' })
  await op.response
  commit([op.mutationId], { queries: [{ querySubscriptionId: 'missing', delta: { baseVersion: '1', resultVersion: '2' } }] })
  await assert.rejects(op.applied, MutationUnconfirmedError)
  assert.equal(op.state.getSnapshot().reactive.kind, 'unconfirmed')
  assert.equal(errors.length, 0)
})

test('receipt decoder output is owned and completely validated before any retirement effects', async t => {
  for (const mode of ['sparse', 'throwing iterator']) await t.test(mode, t => {
    const { reactive, commit, errors } = setup(t)
    reactive.beginOptimistic({ mutationId: 'victim', patches: [{ entityId: 'e', set: { title: { textValue: 'Predicted' } } }] })
    let acknowledgments = 0
    const ids = ['victim']
    if (mode === 'sparse') ids.length = 2
    else ids[Symbol.iterator] = function* () { yield 'victim'; throw new Error('decoder iterator failed') }
    reactive.registerOptimisticReceiptDecoder(() => ids, () => acknowledgments++)
    commit(['unrelated'])
    assert.equal(reactive.store.getEntity('e').values.title.textValue, 'Predicted')
    assert.equal(reactive.getDiagnostics().failures, 0)
    assert.equal(acknowledgments, 0)
    assert.equal(errors.length, 1)
  })
})

test('receipt registrations own their subscriptions even when sharing the same decoder function', t => {
  const { reactive, commit } = setup(t)
  const events = []
  const decode = () => ['shared-id']
  const stopA = reactive.registerOptimisticReceiptDecoder(decode, () => events.push('a'))
  const stopB = reactive.registerOptimisticReceiptDecoder(decode, (_, ids) => {
    assert.ok(Object.isFrozen(ids)); events.push('b')
  })
  stopA(); stopA()
  commit(['unrelated'])
  assert.deepEqual(events, ['b'])
  stopB(); commit(['unrelated'])
  assert.deepEqual(events, ['b'])
})

test('state subscription churn cannot notify a new registration using an old notification baseline', async t => {
  const { harness, commit } = setup(t)
  const op = harness.run({ send: async () => 'ok' })
  let first = 0, second = 0, stopSecond
  op.state.subscribe(() => {
    if (++first === 1) { stopSecond(); stopSecond = op.state.subscribe(secondListener) }
  })
  const secondListener = () => { second++ }
  stopSecond = op.state.subscribe(secondListener)
  const obsoleteStop = stopSecond
  await op.response; await flush()
  assert.equal(second, 0, 'the replacement subscription starts at the current snapshot')
  obsoleteStop()
  commit([op.mutationId]); await op.applied; await flush()
  assert.equal(second, 1, 'obsolete cleanup cannot remove the replacement subscription')
  stopSecond()
})

test('multiple state subscriptions can share a callback without sharing cleanup ownership', async t => {
  const { harness, commit } = setup(t)
  const op = harness.run({ send: async () => 'ok' })
  await op.response
  let calls = 0
  const listener = () => { calls++ }
  const a = op.state.subscribe(listener), b = op.state.subscribe(listener)
  a()
  commit([op.mutationId]); await op.applied; await flush()
  assert.equal(calls, 1)
  b()
})

test('disposal inside optimistic setup cannot install predictions after termination', async t => {
  const { harness, reactive, predict } = setup(t)
  let sends = 0
  const seen = [], view = reactive.store.observe(store => store.getEntity('e').values.title.textValue)
  const stop = view.subscribe(() => seen.push(view.getSnapshot()))
  const op = harness.run({ send: async () => { sends++ }, optimistic: draft => { predict(draft); harness.dispose() } })
  assert.equal(reactive.store.getEntity('e').values.title.textValue, 'Base')
  assert.equal(reactive.getDiagnostics().store.optimistic.layers, 0)
  await assert.rejects(op.response, /disposed during optimistic setup/)
  await assert.rejects(op.applied, MutationUnconfirmedError)
  assert.equal(sends, 0)
  assert.deepEqual(seen, [])
  stop()
})

test('async lifecycle hooks and state listeners report rejections without blocking mutation completion', async t => {
  const { harness, commit, errors } = setup(t)
  const op = harness.run({ send: async () => 'ok',
    onResponseSettled: async () => { await Promise.resolve(); throw new Error('response hook') },
    onApplied: async () => { await Promise.resolve(); throw new Error('applied hook') },
    onOptimisticRetired: async () => { await Promise.resolve(); throw new Error('retirement hook') },
  })
  const stop = op.state.subscribe(async () => { await Promise.resolve(); throw new Error('async state listener') })
  await op.response
  commit([op.mutationId]); await op.applied; await flush()
  assert.equal(op.state.getSnapshot().reactive.kind, 'applied')
  for (const expected of ['response hook', 'applied hook', 'retirement hook', 'async state listener']) assert.ok(errors.some(error => error.message === expected), expected)
  stop()
})

test('async post-publication receipt observers are isolated without delaying other registrations', async t => {
  const { reactive, commit, errors } = setup(t)
  const events = []
  reactive.registerOptimisticReceiptDecoder(() => ['a'], async () => {
    await Promise.resolve(); throw new Error('receipt observer')
  })
  reactive.registerOptimisticReceiptDecoder(() => ['b'], () => events.push('b'))
  commit(['unrelated']); await flush()
  assert.deepEqual(events, ['b'])
  assert.equal(reactive.getDiagnostics().failures, 0)
  assert.ok(errors.some(error => error.message === 'receipt observer'))
})
