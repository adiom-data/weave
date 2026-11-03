import test from 'node:test'
import assert from 'node:assert/strict'
import { ReactiveClient } from '@adiom-data/weave'
import { ProjectDetails } from '../dist/index.js'

const url = process.env.WEAVE_TEST_URL
function waitFor(handle, predicate) {
  return new Promise((resolve, reject) => {
    let stop = () => {}
    const timer = setTimeout(() => { stop(); reject(new Error('Timed out waiting for generated projection')) }, 10000)
    const check = () => {
      const view = handle.view.getSnapshot()
      if (view.status === 'error') { clearTimeout(timer); stop(); reject(new Error(view.error)) }
      else if (predicate(view)) { clearTimeout(timer); stop(); resolve(view) }
    }
    stop = handle.view.subscribe(check); check()
  })
}
const tasks = view => view.data?.status === 'available' ? (view.data.value.taskLinks ?? []).flatMap(link => link.status === 'available' && link.value.task?.status === 'available' ? [link.value.task.value] : []) : []
test('generated demo bindings work through the live WebSocket and CDC', { skip: !url, timeout: 45000 }, async () => {
  const clients = [0, 1].map(() => new ReactiveClient({ url: () => `${url.replace(/^http/, 'ws')}/api/reactive` }))
  const handles = clients.map(client => ProjectDetails.subscribe(client, { projectId: '10000000-0000-4000-8000-000000000001' }))
  let id
  const projectId = '10000000-0000-4000-8000-000000000001'
  const mutate = async (method, body) => {
    const response = await fetch(`${url}/demo.v1.TaskMutationService/${method}`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ projectId, ...body }) })
    assert.ok(response.ok, `${method}: ${response.status}`)
    return response.json()
  }
  try {
    clients.forEach(client => client.connect())
    await Promise.all(handles.map(handle => waitFor(handle, view => view.status === 'ready')))
    const title = `Generated projection smoke ${Date.now()}`
    id = (await mutate('CreateTask', { title })).taskId
    await Promise.all(handles.map(handle => waitFor(handle, view => tasks(view).some(task => task.id === id && task.title === title))))
    await mutate('UpdateTask', { taskId: id, completed: true })
    await Promise.all(handles.map(handle => waitFor(handle, view => tasks(view).some(task => task.id === id && task.completed && task.title === title))))
    const list = handles[0].data.getSnapshot().value.taskLinks
    await handles[0].window(list).extend('SIDE_LEFT', 1)
    clients[0].disconnect(); clients[0].connect()
    await waitFor(handles[0], view => view.status === 'ready' && tasks(view).some(task => task.id === id))
    await mutate('DeleteTask', { taskId: id })
    await Promise.all(handles.map(handle => waitFor(handle, view => !tasks(view).some(task => task.id === id))))
    id = undefined
  } finally {
    try { if (id) await mutate('DeleteTask', { taskId: id }) }
    finally { handles.forEach(handle => handle.dispose()); clients.forEach(client => client.disconnect()) }
  }
})
