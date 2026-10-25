import test from 'node:test'
import assert from 'node:assert/strict'
import { ReactiveClient } from '@adiom-data/weave'
import { ProjectDetails } from '@adiom/weave-demo-projections'
import { TaskMutations, receiptKind } from '../dist/index.js'

const completed = operation => Promise.all([operation.response, operation.applied]).then(([response]) => response)

const url = process.env.WEAVE_TEST_URL
async function cleanupTask(taskId) {
  const response = await fetch(`${url}/demo.v1.TaskMutationService/DeleteTask`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ projectId: '10000000-0000-4000-8000-000000000001', taskId }),
  })
  assert.ok([200, 404].includes(response.status), 'clean up only the task created by this test')
}
function waitFor(handle, predicate) {
  return new Promise((resolve, reject) => {
    let stop = () => {}
    const timer = setTimeout(() => { stop(); reject(new Error('Projection did not catch up')) }, 10000)
    const check = () => {
      const view = handle.view.getSnapshot()
      if (view.status === 'error') { clearTimeout(timer); stop(); reject(new Error(view.error)) }
      else if (predicate(view)) { clearTimeout(timer); stop(); resolve(view) }
    }
    stop = handle.view.subscribe(check); check()
  })
}
const hasTask = (view, id) => view.data?.status === 'available' && (view.data.value.taskLinks ?? []).some(link =>
  link.status === 'available' && link.value.task?.status === 'available' && link.value.task.value.id === id)

const taskLink = (view, id) => view.data?.status === 'available' && view.data.value.taskLinks?.find(link =>
  link.status === 'available' && link.value.task?.status === 'available' && link.value.task.value.id === id)

test('live optimistic create, rejected update rollback, completion, and deletion with two viewers', { skip: !url, timeout: 45000 }, async () => {
  const socketUrl = `${url.replace(/^http/, 'ws')}/api/reactive`
  const issuer = new ReactiveClient({ url: socketUrl }), viewer = new ReactiveClient({ url: socketUrl })
  const projectId = '10000000-0000-4000-8000-000000000001'
  const own = ProjectDetails.subscribe(issuer, { projectId }), other = ProjectDetails.subscribe(viewer, { projectId })
  const mutations = new TaskMutations(issuer, { baseUrl: url })
  let id, deleted = false
  try {
    issuer.connect(); viewer.connect()
    await Promise.all([waitFor(own, view => view.status === 'ready'), waitFor(other, view => view.status === 'ready')])
    const root = own.data.getSnapshot()
    const title = `Optimistic smoke ${crypto.randomUUID()}`
    let link
    const operation = mutations.createTask({ projectId, title }, {
      optimistic: draft => { link = draft.insert(root.value.taskLinks, { task: { title, completed: false } }) },
    })
    const predicted = own.data.getSnapshot().value.taskLinks.find(row => row.entityId === link.entityId)
    assert.equal(predicted.status, 'optimistic')
    assert.equal(predicted.value.task.value.title, title)
    assert.equal(other.data.getSnapshot().value.taskLinks.some(row => row.entityId === link.entityId), false)
    id = (await operation.response).taskId
    await completed(operation)
    await Promise.all([waitFor(own, view => hasTask(view, id)), waitFor(other, view => hasTask(view, id))])
    assert.equal(own.data.getSnapshot().value.taskLinks.some(row => row.entityId === link.entityId), false)
    const loaded = taskLink(own.view.getSnapshot(), id).value.task

    const denied = mutations.updateTask({ projectId: 'ffffffff-ffff-4fff-8fff-ffffffffffff', taskId: id, completed: true }, {
      optimistic: draft => draft.patch(loaded, { completed: { boolValue: true } }),
    })
    assert.equal(taskLink(own.view.getSnapshot(), id).value.task.value.completed, true)
    await assert.rejects(completed(denied))
    assert.equal(taskLink(own.view.getSnapshot(), id).value.task.value.completed, false, 'rejection restores confirmed data')

    const update = mutations.updateTask({ projectId, taskId: id, completed: true }, {
      optimistic: draft => draft.patch(loaded, { completed: { boolValue: true } }),
    })
    assert.equal(taskLink(own.view.getSnapshot(), id).value.task.value.completed, true)
    await completed(update)
    await Promise.all([waitFor(own, view => taskLink(view, id)?.value.task.value.completed), waitFor(other, view => taskLink(view, id)?.value.task.value.completed)])

    const membership = taskLink(own.view.getSnapshot(), id)
    const removal = mutations.deleteTask({ projectId, taskId: id }, {
      optimistic: draft => {
        draft.delete(loaded); draft.delete(membership)
        draft.remove(own.data.getSnapshot().value.taskLinks, membership)
      },
    })
    assert.equal(hasTask(own.view.getSnapshot(), id), false)
    await completed(removal)
    deleted = true
    await Promise.all([waitFor(own, view => !hasTask(view, id)), waitFor(other, view => !hasTask(view, id))])
  } finally {
    try {
      if (id && !deleted) {
        await cleanupTask(id)
      }
    } finally { mutations.dispose(); own.dispose(); other.dispose(); issuer.disconnect(); viewer.disconnect() }
  }
})

test('live binary mutation → PostgreSQL commit → routed receipt and another viewer', { skip: !url, timeout: 30000 }, async () => {
  const socketUrl = `${url.replace(/^http/, 'ws')}/api/reactive`
  let opened
  const handshake = new Promise(resolve => { opened = resolve })
  const issuerReceipts = [], viewerReceipts = []
  // The mutation issuer intentionally has no projections/subscriptions.
  const issuer = new ReactiveClient({ url: socketUrl, onConnectionOpened: opened, onTransaction: transaction => issuerReceipts.push(...transaction.messages) })
  const viewer = new ReactiveClient({ url: socketUrl, onTransaction: transaction => viewerReceipts.push(...transaction.messages) })
  const projection = ProjectDetails.subscribe(viewer, { projectId: '10000000-0000-4000-8000-000000000001' })
  const mutations = new TaskMutations(issuer, { baseUrl: url })
  let id
  try {
    issuer.connect(); viewer.connect()
    await Promise.all([handshake, waitFor(projection, view => view.status === 'ready')])
    const operation = mutations.createTask({ projectId: '10000000-0000-4000-8000-000000000001', title: `Mutation smoke ${crypto.randomUUID()}` })
    id = (await operation.response).taskId
    await completed(operation)
    await waitFor(projection, view => hasTask(view, id))
    assert.equal(issuerReceipts.filter(message => message.kind === receiptKind).length, 1)
    assert.equal(viewerReceipts.filter(message => message.kind === receiptKind).length, 0)
  } finally {
    try {
      if (id) {
        await cleanupTask(id)
      }
    } finally { mutations.dispose(); projection.dispose(); issuer.disconnect(); viewer.disconnect() }
  }
})
