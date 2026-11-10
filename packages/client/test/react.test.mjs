import test from 'node:test'
import assert from 'node:assert/strict'
import { act, createElement, StrictMode } from 'react'
import { renderToString } from 'react-dom/server'
import { createRoot } from 'react-dom/client'
import { JSDOM } from 'jsdom'
import { ReactiveClient, defineProjection, unionOptimisticShapes } from '../dist/index.js'
import { useConnection, useProjection, useSelection, useTypedProjection } from '../dist/react.js'
import { initial, request, entity, attach, FakeSocket } from './helpers.mjs'

test('React rendering/SSR does not open sockets or create live subscriptions', () => {
  const client = new ReactiveClient({ url: 'ws://example.test', createSocket: () => { throw new Error('Network work during render') } })
  const definition = defineProjection(request.root, { nodeId: 'project', kind: 'entity', fields: [], children: {} }, [])
  function View() {
    assert.equal(useConnection(client).status, 'disconnected')
    const result = useProjection(client, request)
    assert.equal(typeof result.updateWindow, 'function')
    assert.equal(useTypedProjection(client, definition, {}).handle, undefined)
    return createElement('div', null, result.status)
  }
  assert.match(renderToString(createElement(StrictMode, null, createElement(View))), /loading/)
  assert.equal(client.handles.size, 0)
})

function mounted(t, connect = true) {
  const dom = new JSDOM('<!doctype html><div id="root"></div>')
  const previous = new Map()
  for (const [key, value] of Object.entries({ window: dom.window, document: dom.window.document, IS_REACT_ACT_ENVIRONMENT: true })) {
    previous.set(key, Object.getOwnPropertyDescriptor(globalThis, key))
    Object.defineProperty(globalThis, key, { value, configurable: true, writable: true })
  }
  const sockets = []
  const client = new ReactiveClient({ url: 'ws://test', createSocket: () => {
    const socket = new FakeSocket(); sockets.push(socket); return socket
  } })
  const container = dom.window.document.getElementById('root')
  const root = createRoot(container)
  t.after(async () => {
    await act(async () => { root.unmount(); client.clear() })
    dom.window.close()
    for (const [key, descriptor] of previous) {
      if (descriptor) Object.defineProperty(globalThis, key, descriptor)
      else delete globalThis[key]
    }
  })
  if (connect) { client.connect(); sockets[0].open() }
  return { client, sockets, root, container }
}

test('connection hook owns effect lifecycle, survives StrictMode, and follows client replacement', async t => {
  const { client, sockets, root, container } = mounted(t, false)
  function View({ connection }) {
    return createElement('div', null, useConnection(connection).status)
  }
  const render = connection => root.render(createElement(StrictMode, null, createElement(View, { connection })))
  await act(async () => render(client))
  assert.equal(sockets.length, 2, 'StrictMode replays setup after cleanup')
  assert.equal(sockets[0].readyState, 3, 'replayed effect closes its old socket')
  assert.equal(container.textContent, 'connecting')
  await act(async () => sockets[1].open())
  assert.equal(container.textContent, 'connected')
  await act(async () => render(client))
  assert.equal(sockets.length, 2, 'rerender does not reconnect')

  const replacementSockets = []
  const replacement = new ReactiveClient({ url: 'ws://replacement', createSocket: () => {
    const socket = new FakeSocket(); replacementSockets.push(socket); return socket
  } })
  t.after(() => replacement.clear())
  await act(async () => render(replacement))
  assert.equal(sockets[1].readyState, 3, 'client replacement disconnects the old client')
  assert.equal(client.store.getConnection().status, 'disconnected')
  assert.equal(container.textContent, 'connecting', 'old connection status is not retained')
  await act(async () => replacementSockets[0].open())
  assert.equal(container.textContent, 'connected')
  await act(async () => root.render(null))
  assert.equal(replacementSockets[0].readyState, 3)
  assert.equal(replacement.store.getConnection().status, 'disconnected')
})
function image(scope, suffix = '') {
  const value = initial()
  value.projections.forEach(ref => {
    ref.subscriptionId = scope
    ref.occurrenceId += suffix
    if (ref.parentOccurrenceId) ref.parentOccurrenceId += suffix
    if (ref.attached.querySubscriptionId) ref.attached.querySubscriptionId += suffix
  })
  value.queries[0].querySubscriptionId += suffix
  return { subscriptionId: scope, sequence: '1', modelBatch: value }
}

test('typed hooks stabilize equal parameter values and replace scopes when parameters change', async t => {
  const { client, sockets, root, container } = mounted(t)
  const definition = defineProjection(request.root, {
    nodeId: 'project', kind: 'entity', fields: [{ name: 'title', path: ['title'], type: { kind: 'text' } }], children: {},
  }, [{ name: 'id', type: { kind: 'text' } }])
  let renders = 0
  let latest
  const seen = []
  function View({ id }) {
    renders++
    const view = useTypedProjection(client, definition, { id })
    latest = view
    seen.push({ id, handle: view.handle, data: view.data })
    return createElement('div', null, view.data?.status === 'available' ? view.data.value.title : view.status)
  }
  const subscriptions = () => sockets[0].sent.filter(message => message.subscribeProjection)
  await act(async () => root.render(createElement(StrictMode, null, createElement(View, { id: 'one' }))))
  const count = subscriptions().length
  const scope = subscriptions().at(-1).subscriptionId
  const original = latest.handle
  assert.equal(original.id, scope)
  await act(async () => {
    sockets[0].message(image(scope)); sockets[0].message({ subscriptionId: scope, sequence: '2', ready: {} })
  })
  assert.equal(container.textContent, 'Project')
  assert.equal(latest.handle, original)
  const beforeUnselected = renders
  await act(async () => sockets[0].message({ connectionSequence: '1', modelBatch: { kind: 'KIND_COMMIT', entities: [entity('project', 'Project', { unselected: { textValue: 'new' } })] } }))
  assert.equal(renders, beforeUnselected, 'unselected values must not rerender the combined typed hook')
  await act(async () => root.render(createElement(StrictMode, null, createElement(View, { id: 'one' }))))
  assert.equal(subscriptions().length, count, 'new argument object with equal values must not resubscribe')
  assert.equal(latest.handle, original)
  await act(async () => root.render(createElement(StrictMode, null, createElement(View, { id: 'two' }))))
  assert.equal(subscriptions().length, count + 1)
  assert.equal(container.textContent, 'loading', 'old parameters must not leak while the new scope loads')
  assert.ok(seen.some(value => value.id === 'two' && value.handle === undefined && value.data === undefined), 'input-change render hides the previous handle along with its snapshot')
  assert.notEqual(latest.handle, original)
  assert.equal(latest.handle.id, subscriptions().at(-1).subscriptionId)
  assert.throws(() => original.target(), /disposed/)
})

test('typed hook handles support computed fills and follow definition, connection, and mount lifetimes', async t => {
  const { client, sockets, root, container } = mounted(t)
  const node = { nodeId: 'project', entity: { query: { table: 'projects' } }, computed: ['canEdit'] }
  const shape = { nodeId: 'project', kind: 'entity', children: {}, fields: [
    { name: 'title', path: ['title'], type: { kind: 'text' } },
    { name: 'editable', path: ['canEdit'], type: { kind: 'bool' }, computed: true },
  ] }
  const definition = defineProjection(node, shape, [])
  const replacementDefinition = defineProjection(node, shape, [])
  let latest
  const seen = []
  function View({ connection, projection }) {
    latest = useTypedProjection(connection, projection, {})
    seen.push({ connection, projection, handle: latest.handle })
    return createElement('div', null, latest.data?.status === 'available' ? `${latest.data.value.title}:${latest.data.value.editable}` : latest.status)
  }
  const render = (connection = client, projection = definition) => root.render(createElement(StrictMode, null, createElement(View, { connection, projection })))
  const load = socket => {
    const scope = latest.handle.id
    socket.message({ subscriptionId: scope, sequence: '1', modelBatch: {
      kind: 'KIND_SNAPSHOT', entities: [entity('project', 'Project')],
      projections: [attach('project-occurrence', 'project', { entityId: 'project' }, '', '', scope)],
      computed: [{ occurrenceId: 'project-occurrence', entityId: 'project', upsert: { fields: { canEdit: { boolValue: true } } } }],
    } })
    socket.message({ subscriptionId: scope, sequence: '2', ready: {} })
  }
  sockets[0].message({ connectionOpened: { connectionContext: 'one' } })
  await act(async () => render())
  const original = latest.handle
  assert.equal(latest.status, 'loading', 'a mounted handle need not be ready yet')
  await act(async () => load(sockets[0]))
  assert.equal(container.textContent, 'Project:true')
  assert.equal(latest.handle, original)
  const count = sockets[0].sent.filter(message => message.subscribeProjection).length
  const draft = client.optimistic({ mutationId: 'react-computed' })
  draft.fill(unionOptimisticShapes([latest.data], { project: latest.handle }), { project: { editable: () => false } })
  let overlay
  await act(async () => { overlay = draft.commit() })
  assert.equal(container.textContent, 'Project:false')
  assert.equal(latest.data.computedState.editable, 'predicted')
  assert.equal(latest.handle, original)
  assert.equal(sockets[0].sent.filter(message => message.subscribeProjection).length, count, 'using the hook handle creates no duplicate subscription')
  await act(async () => overlay.rollback())
  assert.equal(container.textContent, 'Project:true')

  await act(async () => { client.disconnect(); client.connect(); sockets[1].open() })
  assert.equal(latest.handle, original, 'reconnect keeps the mounted projection identity')
  await act(async () => {
    sockets[1].message({ connectionOpened: { connectionContext: 'two' } })
    load(sockets[1])
  })
  assert.equal(latest.handle, original)
  assert.equal(container.textContent, 'Project:true')

  await act(async () => render(client, replacementDefinition))
  assert.ok(seen.some(value => value.projection === replacementDefinition && value.handle === undefined))
  assert.notEqual(latest.handle, original)
  assert.throws(() => original.target(), /disposed/)
  const priorClientHandle = latest.handle
  const replacement = new ReactiveClient({ url: 'ws://replacement', createSocket: () => new FakeSocket() })
  t.after(() => replacement.clear())
  await act(async () => render(replacement, replacementDefinition))
  assert.ok(seen.some(value => value.connection === replacement && value.handle === undefined))
  assert.notEqual(latest.handle, priorClientHandle)
  assert.throws(() => priorClientHandle.target(), /disposed/)
  const last = latest.handle
  await act(async () => root.render(null))
  assert.throws(() => last.target(), /disposed/)
  assert.equal(client.getDiagnostics().activeProjections, 0)
  assert.equal(replacement.getDiagnostics().activeProjections, 0)
})

test('typed hooks replace scopes when partial-error behavior changes but not for equivalent options', async t => {
  const { client, sockets, root, container } = mounted(t)
  const definition = defineProjection(request.root, {
    nodeId: 'project', kind: 'entity', fields: [{ name: 'title', path: ['title'], type: { kind: 'text' } }], children: {},
  }, [])
  function View({ partial }) {
    const view = useTypedProjection(client, definition, {}, { allowPartialErrors: partial })
    return createElement('div', null, view.data?.status === 'available' ? view.data.value.title : view.status)
  }
  const subscriptions = () => sockets[0].sent.filter(message => message.subscribeProjection)
  const render = partial => root.render(createElement(StrictMode, null, createElement(View, { partial })))
  await act(async () => render(undefined))
  const original = subscriptions().at(-1)
  const count = subscriptions().length
  await act(async () => {
    sockets[0].message(image(original.subscriptionId))
    sockets[0].message({ subscriptionId: original.subscriptionId, sequence: '2', ready: {} })
  })
  assert.equal(container.textContent, 'Project')
  await act(async () => render(false))
  assert.equal(subscriptions().length, count, 'omitted and explicit defaults are equivalent')
  await act(async () => render(true))
  assert.equal(subscriptions().length, count + 1)
  assert.equal(subscriptions().at(-1).subscribeProjection.allowPartialErrors, true)
  assert.notEqual(subscriptions().at(-1).subscriptionId, original.subscriptionId)
  assert.equal(container.textContent, 'loading', 'old behavior must not remain active while the new scope loads')
  await act(async () => render(true))
  assert.equal(subscriptions().length, count + 1, 'equal fresh option objects must not reopen the scope')
  await act(async () => render(false))
  assert.equal(subscriptions().length, count + 2)
  assert.equal(subscriptions().at(-1).subscribeProjection.allowPartialErrors ?? false, false)
})

test('mounted StrictMode and request changes dispose old subscriptions without displaying old parameters', async t => {
  const { client, sockets, root, container } = mounted(t)
  function View({ definition }) {
    const view = useProjection(client, definition)
    return createElement('div', null, view.root?.row.entity.values?.title.textValue ?? view.status)
  }
  await act(async () => root.render(createElement(StrictMode, null, createElement(View, { definition: request }))))
  const subscriptions = () => sockets[0].sent.filter(message => message.subscribeProjection)
  assert.equal(subscriptions().length, 2)
  assert.equal(sockets[0].sent.filter(message => message.unsubscribe).length, 1)
  const scope = subscriptions().at(-1).subscriptionId
  await act(async () => {
    sockets[0].message(image(scope))
    sockets[0].message({ subscriptionId: scope, sequence: '2', ready: {} })
  })
  assert.equal(container.textContent, 'Project')
  const nextRequest = { ...request, parameters: { id: { textValue: 'different' } } }
  await act(async () => root.render(createElement(StrictMode, null, createElement(View, { definition: nextRequest }))))
  assert.equal(container.textContent, 'loading')
  assert.equal(sockets[0].sent.filter(message => message.unsubscribe).length, 2)
  const nextScope = subscriptions().at(-1).subscriptionId
  assert.notEqual(nextScope, scope)
  await act(async () => {
    const next = image(nextScope, '-next')
    next.modelBatch.entities[0] = entity('project', 'New parameters')
    sockets[0].message(next)
    sockets[0].message({ subscriptionId: nextScope, sequence: '2', ready: {} })
  })
  assert.equal(container.textContent, 'New parameters')
  await act(async () => root.unmount())
  assert.equal(sockets[0].sent.filter(message => message.unsubscribe).length, 3)
})

test('mounted data-only readers retain their view and do not rerender for identical reconnect replacements', async t => {
  const { client, sockets, root, container } = mounted(t)
  const handle = client.subscribeProjection(request)
  sockets[0].message(image(handle.id))
  sockets[0].message({ subscriptionId: handle.id, sequence: '2', ready: {} })
  let renders = 0
  function Data() {
    renders++
    const value = useSelection(handle.data)
    return createElement('div', null, value?.row.entity.values?.title.textValue ?? 'empty')
  }
  await act(async () => root.render(createElement(Data)))
  const baseline = renders
  await act(async () => {
    client.disconnect(); client.connect(); sockets[1].open()
    sockets[1].message(image(handle.id, '-new'))
  })
  assert.equal(container.textContent, 'Project')
  assert.equal(handle.view.getSnapshot().stale, true)
  assert.equal(renders, baseline)
  await act(async () => sockets[1].message({ subscriptionId: handle.id, sequence: '2', ready: {} }))
  assert.equal(renders, baseline)
  assert.equal(handle.view.getSnapshot().stale, false)
  await act(async () => sockets[1].message({ connectionSequence: '1', modelBatch: { entities: [entity('project', 'Updated')] } }))
  assert.equal(container.textContent, 'Updated')
  assert.equal(renders, baseline + 1)
})
