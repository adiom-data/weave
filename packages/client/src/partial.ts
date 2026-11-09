import { branchKey } from './protocol.js'
import type { BranchChange, BranchFailure } from './protocol.js'
import type { NodeView, ProjectionRow } from './projection.js'

interface RetainedBranch { readonly node?: NodeView; readonly row?: ProjectionRow; readonly index: number }

const slotKey = (nodeId: string, parent: string, entity: string): string => JSON.stringify([nodeId, parent, entity])

// Index requested addresses, then visit each source tree once. Store only
// requested captures; the index and traversal do not survive this publication.
function capture(root: NodeView | undefined, requested: ReadonlyMap<string, BranchChange>): Map<string, RetainedBranch> {
  const slots = new Map<string, Map<string, string[]>>()
  for (const [key, change] of requested) {
    const slot = slotKey(change.nodeId, change.parentOccurrenceId, change.parentEntityId)
    let rows = slots.get(slot)
    if (!rows) slots.set(slot, rows = new Map())
    const keys = rows.get(change.entityId) ?? []
    keys.push(key); rows.set(change.entityId, keys)
  }
  const captured = new Map<string, RetainedBranch>()
  const visit = (node: NodeView | undefined, parent = '', entity = ''): void => {
    if (!node || captured.size === requested.size) return
    const wanted = slots.get(slotKey(node.nodeId, parent, entity))
    for (const key of wanted?.get('') ?? []) if (!captured.has(key)) captured.set(key, { node, index: 0 })
    const rows = node.type === 'entity' ? [node.row] : node.rows
    for (let index = 0; index < rows.length && captured.size < requested.size; index++) {
      const row = rows[index]
      for (const key of wanted?.get(row.entityId) ?? []) if (!captured.has(key)) captured.set(key, { row, index })
      for (const child of Object.values(row.children)) visit(child, node.occurrenceId, row.entityId)
    }
  }
  visit(root)
  return captured
}

interface FailedRow { readonly value: ProjectionRow; readonly index: number }

// Ordered insertions need not shift the remaining array for every failure.
// Finalize the prefix only when the next insertion position increases. A stack
// at the insertion point preserves splice semantics even for equal positions
// or positions beyond the remaining healthy list's end.
function restoreRows(healthy: readonly ProjectionRow[], failed: FailedRow[]): ProjectionRow[] {
  failed.sort((a, b) => a.index - b.index)
  const result: ProjectionRow[] = [], pending: ProjectionRow[] = []
  let next = 0
  for (const failure of failed) {
    while (result.length < failure.index && (pending.length || next < healthy.length)) result.push(pending.length ? pending.pop()! : healthy[next++])
    pending.push(failure.value)
  }
  while (pending.length) result.push(pending.pop()!)
  while (next < healthy.length) result.push(healthy[next++])
  return result
}

// Stale display is immutable and occurrence-local. It never owns live entity
// references or reads the shared cache after failure. Capture authoritative
// data before applying the error batch, so optimistic predictions cannot become
// permanently baked into the retained image after their mutation is retired.
export class BranchPublications {
  private retained = new Map<string, RetainedBranch | undefined>()
  private prepared = new Map<string, RetainedBranch | undefined>()
  prepare(changes: readonly BranchChange[], authoritative: NodeView | undefined, displayed: NodeView | undefined): void {
    const requested = new Map<string, BranchChange>()
    for (const change of changes) {
      if (change.type !== 'error' || this.retained.has(branchKey(change))) continue
      requested.set(branchKey(change), change)
    }
    if (!requested.size) return
    const previous = capture(authoritative, requested)
    const missing = new Map([...requested].filter(([key]) => !previous.has(key)))
    const stale = missing.size ? capture(displayed, missing) : undefined
    for (const key of requested.keys()) {
      const fallback = stale?.get(key)
      this.prepared.set(key, previous.get(key) ?? (fallback?.node?.stale || fallback?.row?.stale ? fallback : undefined))
    }
  }
  reconcile(root: NodeView | undefined, errors: readonly BranchFailure[] = []): NodeView | undefined {
    const active = new Set(errors.map(branchKey))
    for (const key of this.retained.keys()) if (!active.has(key)) this.retained.delete(key)
    for (const [key, image] of this.prepared) if (active.has(key)) this.retained.set(key, image)
    this.prepared.clear()
    const visit = (node: NodeView | undefined): NodeView | undefined => {
      if (!node) return undefined
      if (node.error) {
        const before = this.retained.get(branchKey(node.error))?.node
        return before ? Object.freeze({ ...before, occurrenceId: node.occurrenceId, error: node.error, stale: true }) : node
      }
      const row = (value: ProjectionRow): ProjectionRow => {
        if (value.error) {
          const before = this.retained.get(branchKey(value.error))?.row
          return before ? Object.freeze({ ...before, error: value.error, stale: true }) : value
        }
        const children: Record<string, NodeView | undefined> = Object.create(null)
        let changed = false
        for (const [name, child] of Object.entries(value.children)) { children[name] = visit(child); changed ||= children[name] !== child }
        return changed ? Object.freeze({ ...value, children: Object.freeze(children) }) : value
      }
      if (node.type === 'entity') {
        const next = row(node.row)
        return next === node.row ? node : Object.freeze({ ...node, row: next })
      }
      const healthy: ProjectionRow[] = [], failed: FailedRow[] = []
      node.rows.forEach((value, index) => {
        if (value.error) failed.push({ value: row(value), index: this.retained.get(branchKey(value.error))?.index ?? index })
        else healthy.push(row(value))
      })
      const rows = failed.length ? restoreRows(healthy, failed) : healthy
      return rows.length === node.rows.length && rows.every((value,i) => value === node.rows[i]) ? node : Object.freeze({ ...node, rows: Object.freeze(rows) })
    }
    return errors.length ? visit(root) : root
  }
  reset(): void { this.retained.clear(); this.prepared.clear() }
}
