import test from 'node:test'
import assert from 'node:assert/strict'
import { create, fromBinary, toBinary } from '@bufbuild/protobuf'
import { Code } from '@connectrpc/connect'
import { ReactiveClient } from '@adiom-data/weave'
import { FakeSocket } from '../../../../../packages/client/test/helpers.mjs'
import { TaskMutations, MutationOutcomeUnknownError, CreateTaskRequestSchema, CreateTaskResponseSchema, UpdateTaskRequestSchema, UpdateTaskResponseSchema, DeleteTaskRequestSchema, DeleteTaskResponseSchema, receiptKind, mutationIdHeader, connectionContextHeader } from '../dist/index.js'

const context = Buffer.alloc(32, 1).toString('base64url')
const input = { projectId: '10000000-0000-4000-8000-000000000001', title: 'Test task' }
const reply = () => new Response(toBinary(CreateTaskResponseSchema, create(CreateTaskResponseSchema, { taskId: 'new-task' })), { headers: { 'Content-Type': 'application/proto' } })
const deferred = () => { let resolve; const promise = new Promise(r => { resolve = r }); return { promise, resolve } }
const completed = operation => Promise.all([operation.response, operation.applied]).then(([response]) => response)
function setup(t, fetch, options = {}) {
  const sockets = [], errors = []
  const reactive = new ReactiveClient({ url: 'ws://test', heartbeatIntervalMs: 0, onListenerError: error => errors.push(error),
    onTransaction: options.onTransaction, createSocket: () => { const socket = new FakeSocket(); sockets.push(socket); return socket } })
  const mutations = new TaskMutations(reactive, { baseUrl: 'http://test', timeoutMs: options.timeoutMs ?? 1000, maxPending: options.maxPending, fetch })
  t.after(() => { mutations.dispose(); reactive.disconnect() })
  reactive.connect(); sockets[0].open()
  if (options.handshake !== false) sockets[0].message({ connectionOpened: { connectionContext: context } })
  return { reactive, mutations, sockets, errors }
}
function receipt(socket, mutationId, sequence = '1', extra = {}) {
  const payload = Buffer.from(mutationId, 'utf8').toString('base64')
  socket.message({ connectionSequence: sequence, modelBatch: { kind: 'COMMIT', atomic: true, messages: [{ kind: receiptKind, payload }], ...extra } })
}

test('binary RPC carries correlation; receipt may precede response without subscriptions', async t => {
  const requested = deferred(), response = deferred()
  let requests = 0
  const { mutations, sockets, errors } = setup(t, async (input, init) => {
    requests++; requested.resolve(new Request(input, init))
    return response.promise
  }, { onTransaction: () => { throw new Error('application observer failure') } })
  const values = { ...input }
  const operation = mutations.createTask(values)
  values.title = 'mutated after call'
  const request = await requested.promise
  assert.equal(request.headers.get('Content-Type'), 'application/proto')
  assert.equal(request.headers.has('Authorization'), false)
  assert.equal(request.headers.get(connectionContextHeader), context)
  assert.equal(request.headers.get(mutationIdHeader), operation.mutationId)
  assert.equal(fromBinary(CreateTaskRequestSchema, new Uint8Array(await request.arrayBuffer())).title, input.title)
  let synchronized = false
  completed(operation).then(() => { synchronized = true })
  receipt(sockets[0], operation.mutationId)
  await Promise.resolve()
  assert.equal(synchronized, false)
  response.resolve(reply())
  assert.equal((await completed(operation)).taskId, 'new-task')
  assert.equal(requests, 1)
  assert.equal(errors.length, 1, 'one observer cannot prevent mutation receipt delivery')
})

test('update and delete use the same binary correlation and atomic overlay retirement', async t => {
  for (const remove of [false, true]) {
    await t.test(remove ? 'delete' : 'update', async t => {
      const requests = []
      const requestSchema = remove ? DeleteTaskRequestSchema : UpdateTaskRequestSchema
      const responseSchema = remove ? DeleteTaskResponseSchema : UpdateTaskResponseSchema
      const { mutations, reactive, sockets } = setup(t, async (input, init) => {
        requests.push(new Request(input, init))
        return new Response(toBinary(responseSchema, create(responseSchema)), { headers: { 'Content-Type': 'application/proto' } })
      })
      sockets[0].message({ connectionSequence: '1', modelBatch: { kind: 'COMMIT', entities: [{ entityId: 'opaque-task', upsert: { values: { completed: { boolValue: false } } } }] } })
      const hooks = { optimistic: draft => remove ? draft.delete({ entityId: 'opaque-task' }) : draft.patch({ entityId: 'opaque-task' }, { completed: { boolValue: true } }) }
      const operation = remove ? mutations.deleteTask({ projectId: input.projectId, taskId: 'application-id' }, hooks)
        : mutations.updateTask({ projectId: input.projectId, taskId: 'application-id', completed: true }, hooks)
      assert.equal(reactive.store.getEntity('opaque-task').status, remove ? 'unavailable' : 'available')
      await operation.response
      assert.equal(requests.length, 1)
      assert.ok(requests[0].url.endsWith(remove ? '/DeleteTask' : '/UpdateTask'))
      assert.equal(requests[0].headers.get(mutationIdHeader), operation.mutationId)
      assert.equal(requests[0].headers.get(connectionContextHeader), context)
      const decoded = fromBinary(requestSchema, new Uint8Array(await requests[0].arrayBuffer()))
      assert.equal(decoded.taskId, 'application-id', 'RPCs use application IDs, not opaque cache IDs')
      assert.equal(decoded.projectId, input.projectId)
      if (!remove) assert.equal(decoded.completed, true)
      receipt(sockets[0], operation.mutationId, '2', { entities: [{ entityId: 'opaque-task', upsert: { values: { completed: { boolValue: false } } } }] })
      await completed(operation)
      assert.equal(reactive.store.getEntity('opaque-task').values.completed.boolValue, false, 'confirmation retires the entire prediction, not the RPC response')
    })
  }
})

test('RPC success waits for its own valid atomic receipt, and observers can unsubscribe', async t => {
  const { mutations, reactive, sockets } = setup(t, async () => reply())
  const operation = mutations.createTask(input)
  assert.equal((await operation.response).taskId, 'new-task')
  let done = false, seen = 0
  const stop = reactive.subscribeTransactions(() => { seen++ })
  completed(operation).then(() => { done = true })
  receipt(sockets[0], crypto.randomUUID())
  receipt(sockets[0], operation.mutationId, '2', { atomic: false })
  sockets[0].message({ connectionSequence: '3', modelBatch: { kind: 'COMMIT', atomic: true, messages: [{ kind: receiptKind, payload: '/w==' }] } })
  await Promise.resolve()
  assert.equal(done, false)
  stop()
  receipt(sockets[0], operation.mutationId, '4')
  await completed(operation)
  assert.equal(seen, 3)
})

test('RPC rejection is surfaced without retrying', async t => {
  let requests = 0
  const { mutations } = setup(t, async () => {
    requests++
    return new Response(JSON.stringify({ code: 'permission_denied', message: 'project access required' }), { status: 403, headers: { 'Content-Type': 'application/json' } })
  })
  const operation = mutations.createTask(input)
  await assert.rejects(operation.response, error => error.code === Code.PermissionDenied)
  await assert.rejects(completed(operation), error => error.code === Code.PermissionDenied)
  assert.equal(requests, 1)
})

test('disconnect retires receipt waiters; replacement contexts cannot confirm old mutations', async t => {
  const { mutations, reactive, sockets } = setup(t, async () => reply())
  const old = mutations.createTask(input)
  await old.response
  const failed = assert.rejects(completed(old), MutationOutcomeUnknownError)
  reactive.disconnect()
  await failed
  reactive.connect(); sockets[1].open()
  sockets[1].message({ connectionOpened: { connectionContext: Buffer.alloc(32, 2).toString('base64url') } })
  const next = mutations.createTask(input)
  await next.response
  receipt(sockets[0], old.mutationId)
  receipt(sockets[1], old.mutationId)
  receipt(sockets[1], next.mutationId, '2')
  await completed(next)
})

test('transport failure reports an unknown outcome and never retries', async t => {
  let requests = 0
  const { mutations } = setup(t, async () => { requests++; throw new TypeError('connection lost') })
  const operation = mutations.createTask(input)
  await assert.rejects(operation.response, MutationOutcomeUnknownError)
  await assert.rejects(completed(operation), MutationOutcomeUnknownError)
  assert.equal(requests, 1)
})

test('invalid successful responses and ambiguous server errors report unknown outcomes', async t => {
  for (const code of [undefined, Code.Internal, Code.DataLoss]) {
    await t.test(code === undefined ? 'truncated successful response' : `ambiguous error ${code}`, async t => {
      let requests = 0
      const { mutations } = setup(t, async () => {
        requests++
        if (code === undefined) return new Response(new Uint8Array([255]), { headers: { 'Content-Type': 'application/proto' } })
        return new Response(JSON.stringify({ code: code === Code.Internal ? 'internal' : 'data_loss', message: 'ambiguous failure' }), {
          status: 500, headers: { 'Content-Type': 'application/json' },
        })
      })
      const operation = mutations.createTask(input)
      const unknown = error => error instanceof MutationOutcomeUnknownError && error.cause?.code === (code ?? Code.Internal)
      await assert.rejects(operation.response, unknown)
      await assert.rejects(completed(operation), unknown)
      assert.equal(requests, 1)
    })
  }
})

test('pending capacity remains reserved until both receipt and RPC settle', async t => {
  const requested = deferred(), response = deferred()
  let requests = 0
  const { mutations, sockets } = setup(t, async () => {
    requests++
    if (requests === 1) { requested.resolve(); return response.promise }
    return reply()
  }, { maxPending: 1 })
  const first = mutations.createTask(input)
  await requested.promise
  receipt(sockets[0], first.mutationId)
  assert.throws(() => mutations.createTask(input), /Too many pending mutations/)
  assert.equal(requests, 1)
  response.resolve(reply())
  await completed(first)

  const second = mutations.createTask(input)
  await second.response
  assert.throws(() => mutations.createTask(input), /Too many pending mutations/)
  receipt(sockets[0], second.mutationId, '2')
  await completed(second)

  const third = mutations.createTask(input)
  await third.response
  receipt(sockets[0], third.mutationId, '3')
  await completed(third)
  assert.equal(requests, 3)
})

test('disconnect does not release capacity while an RPC is still in flight', async t => {
  const requested = deferred(), response = deferred()
  let requests = 0
  const { mutations, reactive, sockets } = setup(t, async () => {
    requests++
    if (requests === 1) { requested.resolve(); return response.promise }
    return reply()
  }, { maxPending: 1 })
  const first = mutations.createTask(input)
  await requested.promise
  const failed = assert.rejects(completed(first), MutationOutcomeUnknownError)
  reactive.disconnect()
  await failed
  reactive.connect(); sockets[1].open()
  sockets[1].message({ connectionOpened: { connectionContext: Buffer.alloc(32, 2).toString('base64url') } })
  assert.throws(() => mutations.createTask(input), /Too many pending mutations/)
  response.resolve(reply())
  await first.response
  const next = mutations.createTask(input)
  await next.response
  receipt(sockets[1], next.mutationId)
  await completed(next)
})

test('a rejected RPC releases its operation capacity without waiting for a receipt', async t => {
  let requests = 0
  const { mutations, sockets } = setup(t, async () => {
    if (++requests === 1) return new Response(JSON.stringify({ code: 'permission_denied', message: 'denied' }), {
      status: 403, headers: { 'Content-Type': 'application/json' },
    })
    return reply()
  }, { maxPending: 1 })
  const first = mutations.createTask(input)
  await assert.rejects(completed(first), error => error.code === Code.PermissionDenied)
  const second = mutations.createTask(input)
  await second.response
  receipt(sockets[0], second.mutationId)
  await completed(second)
  assert.equal(requests, 2, 'second RPC was explicitly requested, never retried')
})

test('missing receipts time out as unknown; calls without a handshake never send', async t => {
  let requests = 0
  const { mutations, sockets } = setup(t, async () => { requests++; return reply() }, { timeoutMs: 25, handshake: false })
  assert.throws(() => mutations.createTask(input), /no mutation was sent/)
  assert.equal(requests, 0)
  sockets[0].message({ connectionOpened: { connectionContext: context } })
  const operation = mutations.createTask(input)
  await operation.response
  await assert.rejects(completed(operation), MutationOutcomeUnknownError)
  assert.equal(requests, 1)
})

test('demo receipt decoding joins authoritative updates with optimistic retirement', async t => {
  const { mutations, reactive, sockets } = setup(t, async () => reply())
  sockets[0].message({ connectionSequence: '1', modelBatch: { kind: 'COMMIT', entities: [
    { entityId: 'e', upsert: { values: { title: { textValue: 'Base' } } } },
  ] } })
  const operation = mutations.createTask(input, { optimistic: draft => draft.patch({ entityId: 'e' }, { title: { textValue: 'Predicted' } }) })
  await operation.response
  const seen = []
  const selection = reactive.store.observe(store => store.getEntity('e').values.title.textValue)
  const stop = selection.subscribe(() => seen.push(selection.getSnapshot()))
  receipt(sockets[0], operation.mutationId, '2', { entities: [
    { entityId: 'e', upsert: { values: { title: { textValue: 'Actual' } } } },
  ] })
  await completed(operation)
  assert.deepEqual(seen, ['Actual'])
  stop()
})

test('command rejection removes its bound overlay without modifying authoritative data', async t => {
  const { mutations, reactive, sockets } = setup(t, async () => new Response(JSON.stringify({ code: 'permission_denied' }), {
    status: 403, headers: { 'Content-Type': 'application/json' },
  }))
  sockets[0].message({ connectionSequence: '1', modelBatch: { kind: 'COMMIT', entities: [
    { entityId: 'e', upsert: { values: { title: { textValue: 'Base' } } } },
  ] } })
  const operation = mutations.createTask(input, { optimistic: draft => draft.patch({ entityId: 'e' }, { title: { textValue: 'Predicted' } }) })
  assert.equal(reactive.store.getEntity('e').values.title.textValue, 'Predicted')
  await assert.rejects(completed(operation))
  assert.equal(reactive.store.getEntity('e').values.title.textValue, 'Base')
  assert.equal(reactive.store.getAuthoritativeEntity('e').values.title.textValue, 'Base')
})
