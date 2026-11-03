import test from 'node:test'
import assert from 'node:assert/strict'
import { ReactiveClient, scalar } from '@adiom-data/weave'

// Opt-in: uses only a uniquely named task created by this test, soft-deletes it
// in finally, and leaves service/database lifecycle to the caller.
const url = process.env.WEAVE_TEST_URL
const column = name => ({ segments: [name] })
const columns = (...names) => names.map(column)
const active = { nullTest: { column: column('deleted_at'), isNull: true } }
const definition = {
  root: { nodeId: 'project', entity: { query: { schema: 'tenant_001', table: 'projects', columns: columns('id', 'organization_id', 'title'), filter: active }, bindings: [
    { column: column('id'), operator: 'OPERATOR_EQUAL', literal: { uuidValue: '10000000-0000-4000-8000-000000000001' } },
  ] }, children: [{ nodeId: 'memberships', list: {
    query: { schema: 'tenant_001', table: 'project_tasks', columns: columns('organization_id', 'project_id', 'task_id', 'position'), filter: active, order: ['position', 'task_id'].map(name => ({ column: column(name) })) },
    window: { from: 'FROM_END', count: 100 },
    bindings: [{ column: column('project_id'), operator: 'OPERATOR_EQUAL', parentColumn: column('id') }],
  }, children: [{ nodeId: 'task', entity: { query: { schema: 'tenant_001', table: 'tasks', columns: columns('id', 'organization_id', 'title', 'completed'), filter: active }, bindings: [
    { column: column('id'), operator: 'OPERATOR_EQUAL', parentColumn: column('task_id') },
  ] } }] }] },
}
function waitFor(handle, predicate) {
  return new Promise((resolve, reject) => {
    let stop = () => {}
    const timer = setTimeout(() => { stop(); reject(new Error(`Timed out: ${JSON.stringify(handle.view.getSnapshot(), (_, value) => typeof value === 'bigint' ? String(value) : value)}`)) }, 10000)
    const check = () => {
      const view = handle.view.getSnapshot()
      if (view.status === 'error' || view.status === 'closed') { clearTimeout(timer); stop(); reject(new Error(view.error ?? 'Projection closed')) }
      else if (predicate(view)) { clearTimeout(timer); stop(); resolve(view) }
    }
    stop = handle.view.subscribe(check)
    check()
  })
}
const tasks = view => (view.root?.row.children.memberships?.rows ?? []).flatMap(row => {
  const entity = row.children.task?.row.entity
  return entity?.status === 'available' ? [entity.values] : []
})

test('live WebSocket → projections → CDC: two viewers, updates, windows, reconnect, deletion', {
  skip: !url, timeout: 45000,
}, async () => {
  const options = { url: () => `${url.replace(/^http/, 'ws')}/api/reactive`,
    heartbeatIntervalMs: 25, heartbeatTimeoutMs: 1000 }
  const clients = [new ReactiveClient(options), new ReactiveClient(options)]
  const handles = clients.map(client => client.subscribeProjection(definition))
  let taskId
  const projectId = '10000000-0000-4000-8000-000000000001'
  const mutate = async (method, body) => {
    const response = await fetch(`${url}/demo.v1.TaskMutationService/${method}`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ projectId, ...body }) })
    assert.ok(response.ok, `${method}: ${response.status} ${await (!response.ok ? response.text() : '')}`)
    return response.json()
  }
  try {
    clients.forEach(client => client.connect())
    await Promise.all(handles.map(handle => waitFor(handle, view => view.status === 'ready')))
    const title = `Client runtime smoke ${Date.now()}`
    taskId = (await mutate('CreateTask', { title })).taskId
    await Promise.all(handles.map(handle => waitFor(handle, view => tasks(view).some(row => scalar(row, 'id') === taskId))))
    await mutate('UpdateTask', { taskId, completed: true })
    await Promise.all(handles.map(handle => waitFor(handle, view => tasks(view).some(row => scalar(row, 'id') === taskId && row.completed?.boolValue === true && scalar(row, 'title') === title))))
    const list = handles[0].view.getSnapshot().root.row.children.memberships
    await handles[0].updateWindow(list.occurrenceId, [{ side: 'SIDE_LEFT', operation: 'OPERATION_EXTEND', count: 1 }])
    clients[0].disconnect()
    assert.ok(handles[0].view.getSnapshot().root)
    assert.equal(handles[0].view.getSnapshot().stale, true)
    clients[0].connect()
    await waitFor(handles[0], view => view.status === 'ready' && tasks(view).some(row => scalar(row, 'id') === taskId))
    await mutate('DeleteTask', { taskId })
    await Promise.all(handles.map(handle => waitFor(handle, view => !tasks(view).some(row => scalar(row, 'id') === taskId))))
    taskId = undefined
  } finally {
    try { if (taskId) await mutate('DeleteTask', { taskId }) }
    finally { handles.forEach(handle => handle.dispose()); clients.forEach(client => client.disconnect()) }
  }
})
