import type { ProjectionNode, ProjectionRequest } from './protocol.js'
import type { EntitySnapshot, ModelStore, QuerySnapshot, SubscriptionSnapshot } from './store.js'
import type { RecordData, BranchFailure } from './protocol.js'

export interface ProjectionRow {
  readonly error?: BranchFailure
  readonly stale?: boolean
  readonly computed?: RecordData
  readonly entityId: string
  readonly entity: EntitySnapshot
  readonly children: Readonly<Record<string, NodeView | undefined>>
}
export type NodeView = Readonly<{ occurrenceId: string | undefined; nodeId: string; error?: BranchFailure; stale?: boolean }> & (
  | Readonly<{ type: 'entity'; row: ProjectionRow }>
  | Readonly<{ type: 'list'; query: QuerySnapshot | undefined; rows: readonly ProjectionRow[] }>
)
export interface ProjectionSnapshot {
  readonly errors?: readonly BranchFailure[]
  readonly status: SubscriptionSnapshot['status'] | 'disconnected'
  readonly error?: string
  readonly stale?: boolean
  readonly root?: NodeView
}

const emptyIDs: readonly string[] = Object.freeze([])

// Server edges and explicit local targets determine membership. No query
// evaluation, guessed joins, or synthetic server occurrence IDs.
export function readProjection(store: ModelStore, subscriptionId: string, request: ProjectionRequest, options: { readonly authoritative?: boolean } = {}): ProjectionSnapshot {
  const state = store.getSubscription(subscriptionId)
  const connection = store.getConnection()
  const authoritative = options.authoritative ?? false
  const errors = store.getBranchErrors(subscriptionId)
  const slotKey = (node: string, parent: string, entity: string) => JSON.stringify([node, parent, entity])
  const slots = new Map(errors.filter(error => !error.entityId).map(error => [slotKey(error.nodeId, error.parentOccurrenceId, error.parentEntityId), error]))
  const rowErrors = new Map<string, Map<string, BranchFailure>>()
  const detachedErrors = new Map<string, BranchFailure>()
  for (const error of errors) if (error.entityId) {
    const slot = slotKey(error.nodeId, error.parentOccurrenceId, error.parentEntityId)
    if (!detachedErrors.has(slot)) detachedErrors.set(slot, error)
    let rows = rowErrors.get(error.occurrenceId)
    if (!rows) rowErrors.set(error.occurrenceId, rows = new Map())
    rows.set(error.entityId, error)
  }
  const visit = (node: ProjectionNode, parent: string | undefined, parentEntity = '', ancestors: readonly string[] = []): NodeView | undefined => {
    const occurrence = parent === undefined ? undefined : store.getOccurrences(subscriptionId, node.nodeId, parent, parentEntity)[0]
    const slotError = slots.get(slotKey(node.nodeId, parent ?? '', parentEntity))
    if (slotError) return Object.freeze(node.entity
      ? { occurrenceId: slotError.occurrenceId, nodeId: node.nodeId, type: 'entity' as const, error: slotError, row: Object.freeze({ entityId: '', entity: Object.freeze({ status: 'unavailable' as const }), children: Object.freeze({}) }) }
      : { occurrenceId: slotError.occurrenceId, nodeId: node.nodeId, type: 'list' as const, error: slotError, query: undefined, rows: Object.freeze([]) })
    // Failed entity slots are detached from the live model, but their row-level
    // error still supplies the concrete occurrence address.
    const detachedError = !occurrence ? detachedErrors.get(slotKey(node.nodeId, parent ?? '', parentEntity)) : undefined
    const failed = rowErrors.get(occurrence?.id ?? detachedError?.occurrenceId ?? '')
    const query = occurrence?.target.type === 'query' ? store.getQuery(occurrence.target.id) : undefined
    const base = occurrence?.target.type === 'entity' ? [occurrence.target.id] : query?.ids ?? emptyIDs
    const selection = authoritative ? { ids: base, overridden: false } : store.projectionSelection(subscriptionId, node.nodeId, ancestors, base)
    let ids = selection.ids
    if (failed?.size) {
      const present = new Set(ids)
      ids = [...ids, ...[...failed.keys()].filter(id => !present.has(id))]
    }
    if (!occurrence && !ids.length && !selection.overridden) return undefined
    const row = (entityId: string): ProjectionRow => {
      const error = failed?.get(entityId)
      if (error) return Object.freeze({ entityId, entity: Object.freeze({ status: 'unavailable' as const }), children: Object.freeze({}), error })
      const children: Record<string, NodeView | undefined> = Object.create(null)
      if (authoritative || !store.isOptimisticallyDeleted(entityId)) {
        for (const child of node.children ?? []) children[child.nodeId] = visit(child, occurrence?.id, entityId, [...ancestors, entityId])
      }
      return Object.freeze({ entityId, entity: authoritative ? store.getAuthoritativeEntity(entityId) : store.getEntity(entityId), computed: node.computed?.length && occurrence ? store.getComputed(occurrence.id, entityId) : undefined, children: Object.freeze(children) })
    }
    if (node.entity) {
      if (!ids.length) return undefined
      return Object.freeze({ occurrenceId: occurrence?.id ?? detachedError?.occurrenceId, nodeId: node.nodeId, type: 'entity', row: row(ids[0]) })
    }
    const rows = ids.filter(id => failed?.has(id) || authoritative || !store.isOptimisticallyDeleted(id)).map(row)
    return Object.freeze({ occurrenceId: occurrence?.id, nodeId: node.nodeId, type: 'list', query, rows: Object.freeze(rows) })
  }
  const status = state.status === 'error' || state.status === 'closed' ? state.status : connection.status === 'disconnected' ? 'disconnected' : state.status
  return Object.freeze({ status, error: state.error ?? connection.error, errors: errors.length ? errors : undefined, root: visit(request.root, '') })
}
