import { type FormEvent, useEffect, useMemo, useState } from 'react'
import { ReactiveClient } from '@adiom-data/weave'
import type { EntityReference } from '@adiom-data/weave'
import { useConnection } from '@adiom-data/weave/react'
import { useProjectDetails } from '@adiom/weave-demo-projections/react'
import { TaskMutations } from '@adiom/weave-demo-mutations'

const projectId = '10000000-0000-4000-8000-000000000001'

export default function App() {
  const client = useMemo(() => new ReactiveClient({
    url: () => {
      const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:'
      return `${protocol}//${location.host}/api/reactive`
    },
  }), [])
  const projection = useProjectDetails(client, { projectId })
  const connection = useConnection(client)
  const connected = connection.status === 'connected'
  const ready = projection.status === 'ready'
  const [title, setTitle] = useState('')
  const [mutationError, setError] = useState('')
  const [creating, setCreating] = useState(false)
  const [pendingTasks, setPendingTasks] = useState<ReadonlySet<string>>(new Set())
  const [mutations, setMutations] = useState<TaskMutations>()
  useEffect(() => {
    const mutations = new TaskMutations(client, { baseUrl: location.origin })
    setMutations(mutations)
    return () => mutations.dispose()
  }, [client])
  const error = projection.error || mutationError

  const taskLinks = projection.data?.status === 'available' ? projection.data.value.taskLinks : undefined
  const taskRows = useMemo(() => {
    return (taskLinks ?? []).map(membership => {
      const task = membership.status === 'available' || membership.status === 'optimistic' ? membership.value.task : undefined
      return {
        id: membership.entityId,
        entityId: task?.entityId,
        reference: task,
        membership,
        optimistic: membership.status === 'optimistic' || task?.status === 'optimistic',
        task: task?.status === 'available' || task?.status === 'optimistic' ? task.value : undefined,
      }
    })
  }, [taskLinks])
  const completed = useMemo(() => taskRows.filter(row => row.task?.completed).length, [taskRows])

  async function addTask(event: FormEvent) {
    event.preventDefault()
    const submittedTitle = title
    const nextTitle = submittedTitle.trim()
    if (!nextTitle || !mutations || creating || !taskLinks || !projection.data) return
    setCreating(true)
    setError('')
    try {
      const operation = mutations.createTask({ projectId, title: nextTitle }, {
        optimistic: draft => draft.insert(taskLinks, { task: { title: nextTitle, completed: false } }),
      })
      await Promise.all([operation.response, operation.applied])
      setTitle(current => current === submittedTitle ? '' : current)
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : 'Could not create task')
    } finally {
      setCreating(false)
    }
  }

  async function toggle(entity: EntityReference, taskId: string, completed: boolean) {
    const entityId = entity.entityId
    if (!mutations || !ready || pendingTasks.has(entityId)) return
    setError('')
    setPendingTasks(current => new Set(current).add(entityId))
    try {
      const operation = mutations.updateTask({ projectId, taskId, completed }, {
        optimistic: draft => draft.patch(entity, { completed: { boolValue: completed } }),
      })
      await Promise.all([operation.response, operation.applied])
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : 'Could not update task')
    } finally {
      setPendingTasks(current => { const next = new Set(current); next.delete(entityId); return next })
    }
  }

  async function remove(entity: EntityReference, taskId: string) {
    const entityId = entity.entityId
    if (!mutations || !ready || pendingTasks.has(entityId) || !taskLinks) return
    setError('')
    setPendingTasks(current => new Set(current).add(entityId))
    try {
      // Deleting a task is global; remove every loaded membership for it too.
      // Other projections receive the entity tombstone without inferred joins.
      const memberships = taskRows.filter(row => row.entityId === entityId).map(row => row.membership)
      const operation = mutations.deleteTask({ projectId, taskId }, {
        optimistic: draft => {
          draft.delete(entity)
          for (const membership of memberships) { draft.delete(membership); draft.remove(taskLinks, membership) }
        },
      })
      await Promise.all([operation.response, operation.applied])
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : 'Could not delete task')
    } finally {
      setPendingTasks(current => { const next = new Set(current); next.delete(entityId); return next })
    }
  }

  return (
    <main>
      <header>
        <div>
          <p className="eyebrow">Live PostgreSQL query</p>
          <h1>Reactive Query Demo</h1>
          <p className="subtitle">A project shared by every connected viewer.</p>
        </div>
        <span className={connected && ready ? 'status connected' : 'status'}>
          <i /> {connected ? (ready ? 'Live' : 'Synchronizing') : 'Reconnecting'}
        </span>
      </header>

      {error && <div className="error" role="alert">{error}</div>}

      <section className="project">
        <div className="project-heading">
          <div>
            <p className="eyebrow">Project</p>
            <h2>{projection.data?.status === 'available' ? projection.data.value.title : 'Loading project…'}</h2>
          </div>
          <span>{completed}/{taskRows.length} complete</span>
        </div>

        <form onSubmit={addTask} className="add-task">
          <input
            aria-label="Task title"
            value={title}
            onChange={(event) => setTitle(event.target.value)}
            placeholder="Add a task…"
          />
          <button type="submit" disabled={!ready || !mutations || creating}>{creating ? 'Adding…' : 'Add task'}</button>
        </form>

        <div className="tasks">
          {taskRows.length === 0 && ready ? <p className="empty">No tasks yet.</p> : taskRows.map(({ id, entityId, reference, task, optimistic }) => task ? (
            <article className={task.completed ? 'task done' : 'task'} key={id}>
              <button className="check" disabled={!ready || !mutations || optimistic || !entityId || pendingTasks.has(entityId) || !task.id} aria-label={task.completed ? 'Mark incomplete' : 'Mark complete'} onClick={() => { if (task.id && reference) void toggle(reference, task.id, !task.completed) }}>
                {task.completed ? '✓' : ''}
              </button>
              <div>
                <h3>{task.title ?? 'New task'}</h3>
                <small>{optimistic || (entityId && pendingTasks.has(entityId)) ? 'Saving…' : task.id}</small>
              </div>
              <button className="remove" disabled={!ready || !mutations || optimistic || !entityId || pendingTasks.has(entityId) || !task.id} aria-label="Delete task" onClick={() => { if (task.id && reference) void remove(reference, task.id) }}>×</button>
            </article>
          ) : (
            <article className="task" key={id} aria-busy="true">
              <span className="check" />
              <div>
                <h3>Loading task…</h3>
                <small>{id}</small>
              </div>
            </article>
          ))}
        </div>
        <button disabled={!ready || !taskLinks} onClick={() => {
          if (taskLinks) void projection.window(taskLinks).extend('SIDE_LEFT', 20).catch(reason => setError(String(reason)))
        }}>Load earlier tasks</button>
      </section>
    </main>
  )
}
