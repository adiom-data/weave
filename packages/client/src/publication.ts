import type { NodeView, ProjectionRow, ProjectionSnapshot } from './projection.js'
import type { Listener, Selection } from './store.js'
import { invokeSafely } from './callbacks.js'

// Display data deliberately excludes connection-owned window/occurrence IDs.
export interface ProjectionDataRow {
  readonly entityId: string
  readonly entity: ProjectionRow['entity']
  readonly children: Readonly<Record<string, ProjectionDataNode | undefined>>
}
export type ProjectionDataNode = Readonly<{ nodeId: string }> & (
  | Readonly<{ type: 'entity'; row: ProjectionDataRow }>
  | Readonly<{ type: 'list'; rows: readonly ProjectionDataRow[] }>
)

class Cell<T> implements Selection<T> {
  private listeners = new Set<Listener>()
  private notified: T
  constructor(private value: T, private report: (error: unknown) => void) { this.notified = value }
  getSnapshot = (): T => this.value
  subscribe = (listener: Listener): (() => void) => {
    this.listeners.add(listener)
    return () => { this.listeners.delete(listener) }
  }
  stage(value: T): void { this.value = share(this.value, value) }
  flush(): void {
    if (this.notified === this.value) return
    this.notified = this.value
    for (const listener of [...this.listeners]) {
      invokeSafely(listener, error => this.report(error))
    }
  }
}

// Frozen tree reconciliation. List rows match by entity identity, not index,
// preserving unchanged row references when other rows move or disappear.
function share<T>(before: T, after: T, memo = new WeakMap<object, unknown>()): T {
  if (Object.is(before, after)) return before
  if (!before || !after || typeof before !== 'object' || typeof after !== 'object') return after
  if (memo.has(after)) return memo.get(after) as T
  if (Array.isArray(after)) {
    if (!Array.isArray(before)) return after
    const rows = new Map(before.filter(isRow).map(row => [row.entityId, row]))
    const next = after.map((value, i) => share(isRow(value) ? rows.get(value.entityId) : before[i], value, memo))
    const result = next.length === before.length && next.every((value, i) => value === before[i]) ? before : Object.freeze(next)
    memo.set(after, result)
    return result as T
  }
  if (Array.isArray(before)) return after
  const old = before as Record<string, unknown>, incoming = after as Record<string, unknown>
  const keys = Object.keys(incoming)
  const result: Record<string, unknown> = Object.create(null)
  let same = keys.length === Object.keys(old).length
  for (const key of keys) {
    result[key] = share(old[key], incoming[key], memo)
    same &&= Object.hasOwn(old, key) && result[key] === old[key]
  }
  const shared = same ? before : Object.freeze(result)
  memo.set(after, shared)
  return shared as T
}
function isRow(value: unknown): value is ProjectionDataRow {
  return !!value && typeof value === 'object' && 'entityId' in value && typeof value.entityId === 'string'
}
function data(node: NodeView | undefined): ProjectionDataNode | undefined {
  if (!node) return undefined
  const row = (value: ProjectionRow): ProjectionDataRow => Object.freeze({
    entityId: value.entityId, entity: value.entity,
    children: Object.freeze(Object.fromEntries(Object.entries(value.children).map(([key, child]) => [key, data(child)]))),
  })
  return node.type === 'entity'
    ? Object.freeze({ nodeId: node.nodeId, type: 'entity', row: row(node.row) })
    : Object.freeze({ nodeId: node.nodeId, type: 'list', rows: Object.freeze(node.rows.map(row)) })
}

export class ProjectionPublication {
  readonly view: Cell<ProjectionSnapshot>
  readonly data: Cell<ProjectionDataNode | undefined>
  private current = false
  private candidate?: ProjectionSnapshot
  private resetRoot?: { readonly root: NodeView | undefined }
  constructor(report: (error: unknown) => void) {
    this.view = new Cell<ProjectionSnapshot>(Object.freeze({ status: 'loading', stale: false }), report)
    this.data = new Cell<ProjectionDataNode | undefined>(undefined, report)
  }
  // Preserve the last authoritative view during reconnect, not predictions
  // whose generation has ended. This preparation publishes nothing by itself.
  prepareReset(root: NodeView | undefined): void { this.resetRoot = { root } }
  stage(candidate: ProjectionSnapshot): void {
    if (candidate === this.candidate) return
    this.candidate = candidate
    const previous = this.view.getSnapshot()
    const ready = candidate.status === 'ready'
    const terminal = candidate.status === 'error' || candidate.status === 'closed'
    this.current = ready
    // Terminal failures clear previously visible data, including authorization
    // failures. A transient transport outage retains the last publication.
    const root = ready ? candidate.root : terminal ? undefined : this.resetRoot ? this.resetRoot.root : previous.root
    this.resetRoot = undefined
    this.view.stage(Object.freeze({ ...candidate, root, stale: !ready && !terminal && root !== undefined }))
    if (root !== previous.root) this.data.stage(data(root))
  }
  hasWindow(id: string): boolean {
    const visit = (node: NodeView | undefined): boolean => !!node && (
      (node.type === 'list' && node.occurrenceId === id) ||
      (node.type === 'entity' ? [node.row] : node.rows).some(row => Object.values(row.children).some(visit)))
    return this.current && visit(this.view.getSnapshot().root)
  }
  clear(): void {
    this.resetRoot = undefined
    this.current = false
    this.view.stage(Object.freeze({ status: 'closed', stale: false }))
    this.data.stage(undefined)
  }
  flush(): void { this.view.flush(); this.data.flush() }
}
