import test from 'node:test'
import assert from 'node:assert/strict'
import { act, createElement, StrictMode } from 'react'
import { renderToString } from 'react-dom/server'
import { createRoot } from 'react-dom/client'
import { JSDOM } from 'jsdom'
import { ReactiveClient, defineProjection } from '../dist/index.js'
import { useConnection, useProjection, useSelection, useTypedProjection } from '../dist/react.js'
import { initial, request, entity, FakeSocket } from './helpers.mjs'

test('React rendering/SSR does not open sockets or create live subscriptions', () => {
  const client = new ReactiveClient({ url: 'ws://example.test', createSocket: () => { throw new Error('Network work during render') } })
  function View() {
    assert.equal(useConnection(client).status, 'disconnected')
    const result = useProjection(client, request)
    assert.equal(typeof result.updateWindow, 'function')
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
  function View({ id }) {
    renders++
    const view = useTypedProjection(client, definition, { id })
    return createElement('div', null, view.data?.status === 'available' ? view.data.value.title : view.status)
  }
  const subscriptions = () => sockets[0].sent.filter(message => message.subscribeProjection)
  await act(async () => root.render(createElement(StrictMode, null, createElement(View, { id: 'one' }))))
  const count = subscriptions().length
  const scope = subscriptions().at(-1).subscriptionId
  await act(async () => {
    sockets[0].message(image(scope)); sockets[0].message({ subscriptionId: scope, sequence: '2', ready: {} })
  })
  assert.equal(container.textContent, 'Project')
  const beforeUnselected = renders
  await act(async () => sockets[0].message({ connectionSequence: '1', modelBatch: { kind: 'KIND_COMMIT', entities: [entity('project', 'Project', { unselected: { textValue: 'new' } })] } }))
  assert.equal(renders, beforeUnselected, 'unselected values must not rerender the combined typed hook')
  await act(async () => root.render(createElement(StrictMode, null, createElement(View, { id: 'one' }))))
  assert.equal(subscriptions().length, count, 'new argument object with equal values must not resubscribe')
  await act(async () => root.render(createElement(StrictMode, null, createElement(View, { id: 'two' }))))
  assert.equal(subscriptions().length, count + 1)
  assert.equal(container.textContent, 'loading', 'old parameters must not leak while the new scope loads')
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
