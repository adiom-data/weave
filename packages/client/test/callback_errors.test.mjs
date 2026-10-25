import test from 'node:test'
import assert from 'node:assert/strict'
import { ReactiveClient, defineProjection } from '../dist/index.js'
import { FakeSocket, initial, request, entity } from './helpers.mjs'

const flush = () => new Promise(resolve => setImmediate(resolve))
const fail = name => async () => { await Promise.resolve(); throw new Error(name) }

test('async lifecycle and observer failures reach the error hook without blocking publication', async t => {
  const errors = [], sockets = [], stops = [], observed = []
  const client = new ReactiveClient({ url: 'ws://test', heartbeatIntervalMs: 0,
    createSocket: () => { const socket = new FakeSocket(); sockets.push(socket); return socket },
    onListenerError: async error => { await Promise.resolve(); errors.push(error.message) },
    onDiagnostic: async event => { if (event.type === 'handshake') { await Promise.resolve(); throw new Error('diagnostic') } },
    onConnectionOpened: fail('handshake'), onTransaction: fail('transaction hook'),
  })
  t.after(() => { stops.forEach(stop => stop()); client.clear() })
  const handle = client.subscribeProjection(request)
  client.connect(); sockets[0].open()
  sockets[0].message({ connectionOpened: { connectionContext: 'context' } })
  const image = initial(); image.projections.forEach(value => { value.subscriptionId = handle.id })
  sockets[0].message({ subscriptionId: handle.id, sequence: '1', modelBatch: image })
  sockets[0].message({ subscriptionId: handle.id, sequence: '2', ready: {} })

  stops.push(client.subscribeTransactions(fail('transaction listener')))
  stops.push(client.subscribeTransactions(() => observed.push(client.store.getEntity('project').values.title.textValue)))
  stops.push(client.store.observe(store => store.getEntity('project')).subscribe(fail('store listener')))
  stops.push(client.store.subscribeCommits(fail('commit listener')))
  stops.push(handle.data.subscribe(fail('projection data listener')))
  stops.push(handle.view.subscribe(fail('projection view listener')))
  sockets[0].message({ connectionSequence: '1', modelBatch: { kind: 'COMMIT', atomic: true, checkpoint: '10',
    entities: [entity('project', 'Changed')], messages: [{ kind: 'application', payload: '' }],
  } })
  assert.equal(client.store.getEntity('project').values.title.textValue, 'Changed')
  assert.deepEqual(observed, ['Changed'], 'healthy listeners run before async failures settle')
  assert.equal(handle.data.getSnapshot().row.entity.values.title.textValue, 'Changed')
  await flush()
  for (const name of ['diagnostic', 'handshake', 'transaction hook', 'transaction listener', 'store listener', 'commit listener', 'projection data listener', 'projection view listener']) {
    assert.ok(errors.includes(name), name)
  }
  assert.equal(sockets.length, 1, 'callback failures must not resynchronize the connection')
})

test('typed selection callbacks preserve rejected promises on normal and decode-error notifications', async t => {
  const errors = [], sockets = []
  const client = new ReactiveClient({ url: 'ws://test', heartbeatIntervalMs: 0,
    onListenerError: error => errors.push(error.message),
    createSocket: () => { const socket = new FakeSocket(); sockets.push(socket); return socket },
  })
  const definition = defineProjection(request.root, {
    nodeId: 'project', kind: 'entity', fields: [{ name: 'title', path: ['title'], type: { kind: 'text' } }], children: {},
  }, [])
  const handle = definition.subscribe(client, {})
  client.connect(); sockets[0].open()
  const scope = sockets[0].sent[0].subscriptionId
  const image = initial(); image.projections.forEach(value => { value.subscriptionId = scope })
  sockets[0].message({ subscriptionId: scope, sequence: '1', modelBatch: image })
  sockets[0].message({ subscriptionId: scope, sequence: '2', ready: {} })
  const stop = handle.data.subscribe(fail('typed listener'))
  t.after(() => { stop(); handle.dispose(); client.clear() })
  for (const [index, value] of [entity('project', 'Changed'), entity('project', '', { title: { integerValue: '123' } }), entity('project', 'Recovered')].entries()) {
    sockets[0].message({ connectionSequence: String(index + 1), modelBatch: { entities: [value] } })
    await flush()
  }
  assert.equal(errors.filter(error => error === 'typed listener').length, 3)
  assert.equal(handle.data.getSnapshot().value.title, 'Recovered')
  assert.equal(sockets.length, 1)
})
