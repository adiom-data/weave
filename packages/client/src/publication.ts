import type { NodeView, ProjectionRow, ProjectionSnapshot } from './projection.js'
import type { Listener, Selection } from './store.js'
import { invokeSafely } from './callbacks.js'
import { BranchPublications } from './partial.js'
import { findLiveWindow } from './windows.js'
import type { BranchChange, BranchFailure } from './protocol.js'

// Display data deliberately excludes connection-owned window/occurrence IDs.
export interface ProjectionDataRow {
  readonly error?: BranchFailure
  readonly stale?: boolean
  readonly computed?: ProjectionRow['computed']
  readonly entityId: string
  readonly entity: ProjectionRow['entity']
  readonly children: Readonly<Record<string, ProjectionDataNode | undefined>>
}
export type ProjectionDataNode = Readonly<{ nodeId: string; error?: BranchFailure; stale?: boolean }> & (
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
function share<T>(before: T, after: T, memo = new WeakMap<object, WeakMap<object, unknown>>()): T {
  if (Object.is(before, after)) return before
  if (!before || !after || typeof before !== 'object' || typeof after !== 'object') return after
  // One incoming entity may meet both a stale branch and a current diamond
  // branch. Reconciliation is a function of BOTH images, not just the incoming
  // object; reusing the stale path's result would invalidate the healthy path.
  if (memo.get(after)?.has(before)) return memo.get(after)!.get(before) as T
  const remember = (result: unknown): void => {
    let inputs = memo.get(after)
    if (!inputs) memo.set(after, inputs = new WeakMap())
    inputs.set(before, result)
  }
  if (Array.isArray(after)) {
    if (!Array.isArray(before)) return after
    const rows = new Map(before.filter(isRow).map(row => [row.entityId, row]))
    const next = after.map((value, i) => share(isRow(value) ? rows.get(value.entityId) : before[i], value, memo))
    const result = next.length === before.length && next.every((value, i) => value === before[i]) ? before : Object.freeze(next)
    remember(result)
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
  remember(shared)
  return shared as T
}
function isRow(value: unknown): value is ProjectionDataRow {
  return !!value && typeof value === 'object' && 'entityId' in value && typeof value.entityId === 'string'
}
function data(node: NodeView | undefined): ProjectionDataNode | undefined {
  if (!node) return undefined
  const row = (value: ProjectionRow): ProjectionDataRow => Object.freeze({
    entityId: value.entityId, entity: value.entity, computed: value.computed,
    ...(value.error ? { error: value.error, stale: value.stale } : {}),
    children: Object.freeze(Object.fromEntries(Object.entries(value.children).map(([key, child]) => [key, data(child)]))),
  })
  return node.type === 'entity'
    ? Object.freeze({ nodeId: node.nodeId, type: 'entity', row: row(node.error ? { ...node.row, error: node.error, stale: node.stale } : node.row), ...(node.error ? { error: node.error, stale: node.stale } : {}) })
    : Object.freeze({ nodeId: node.nodeId, type: 'list', rows: Object.freeze(node.rows.map(row)), ...(node.error ? { error: node.error, stale: node.stale } : {}) })
}

export class ProjectionPublication {
  private branches = new BranchPublications()
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
  prepareReset(root: NodeView | undefined): void {
    this.resetRoot = { root: this.branches.reconcile(root, this.candidate?.errors) }
    this.branches.reset()
  }
  prepareFailures(changes: readonly BranchChange[], authoritative: NodeView | undefined): void {
    // Compose already-retained child failures into an authoritative parent
    // capture without incorporating any currently pending predictions.
    const baseline = this.branches.reconcile(authoritative, this.candidate?.errors)
    this.branches.prepare(changes, baseline, this.view.getSnapshot().root)
  }
  stage(candidate: ProjectionSnapshot): void {
    if (candidate === this.candidate) return
    this.candidate = candidate
    const previous = this.view.getSnapshot()
    const ready = candidate.status === 'ready'
    const terminal = candidate.status === 'error' || candidate.status === 'closed'
    this.current = ready
    // Terminal failures clear previously visible data, including authorization
    // failures. A transient transport outage retains the last publication.
    const root = ready ? this.branches.reconcile(candidate.root, candidate.errors) : terminal ? undefined : this.resetRoot ? this.resetRoot.root : previous.root
    this.resetRoot = undefined
    this.view.stage(Object.freeze({ ...candidate, root, stale: !terminal && root !== undefined && (!ready || !!candidate.errors?.length) }))
    if (root !== previous.root) this.data.stage(data(root))
  }
  hasWindow(id: string): boolean {
    return this.current && !!findLiveWindow(this.view.getSnapshot().root, { occurrenceId: id })
  }
  clear(): void {
    this.resetRoot = undefined
    this.branches.reset()
    this.current = false
    this.view.stage(Object.freeze({ status: 'closed', stale: false }))
    this.data.stage(undefined)
  }
  flush(): void { this.view.flush(); this.data.flush() }
}
