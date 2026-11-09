import type { ProjectionNode, ProjectionRequest } from './protocol.js'
import type { EntitySnapshot, ModelStore, QuerySnapshot, SubscriptionSnapshot } from './store.js'
import type { RecordData } from './protocol.js'

export interface ProjectionRow {
  readonly computed?: RecordData
  readonly entityId: string
  readonly entity: EntitySnapshot
  readonly children: Readonly<Record<string, NodeView | undefined>>
}
export type NodeView = Readonly<{ occurrenceId: string | undefined; nodeId: string }> & (
  | Readonly<{ type: 'entity'; row: ProjectionRow }>
  | Readonly<{ type: 'list'; query: QuerySnapshot | undefined; rows: readonly ProjectionRow[] }>
)
export interface ProjectionSnapshot {
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
  const visit = (node: ProjectionNode, parent: string | undefined, parentEntity = '', ancestors: readonly string[] = []): NodeView | undefined => {
    const occurrence = parent === undefined ? undefined : store.getOccurrences(subscriptionId, node.nodeId, parent, parentEntity)[0]
    const query = occurrence?.target.type === 'query' ? store.getQuery(occurrence.target.id) : undefined
    const base = occurrence?.target.type === 'entity' ? [occurrence.target.id] : query?.ids ?? emptyIDs
    const selection = authoritative ? { ids: base, overridden: false } : store.projectionSelection(subscriptionId, node.nodeId, ancestors, base)
    const ids = selection.ids
    if (!occurrence && !ids.length && !selection.overridden) return undefined
    const row = (entityId: string): ProjectionRow => {
      const children: Record<string, NodeView | undefined> = Object.create(null)
      if (authoritative || !store.isOptimisticallyDeleted(entityId)) {
        for (const child of node.children ?? []) children[child.nodeId] = visit(child, occurrence?.id, entityId, [...ancestors, entityId])
      }
      return Object.freeze({ entityId, entity: authoritative ? store.getAuthoritativeEntity(entityId) : store.getEntity(entityId), computed: node.computed?.length && occurrence ? store.getComputed(occurrence.id, entityId) : undefined, children: Object.freeze(children) })
    }
    if (node.entity) {
      if (!ids.length) return undefined
      return Object.freeze({ occurrenceId: occurrence?.id, nodeId: node.nodeId, type: 'entity', row: row(ids[0]) })
    }
    const rows = ids.filter(id => authoritative || !store.isOptimisticallyDeleted(id)).map(row)
    return Object.freeze({ occurrenceId: occurrence?.id, nodeId: node.nodeId, type: 'list', query, rows: Object.freeze(rows) })
  }
  const status = state.status === 'error' || state.status === 'closed' ? state.status : connection.status === 'disconnected' ? 'disconnected' : state.status
  return Object.freeze({ status, error: state.error ?? connection.error, root: visit(request.root, '') })
}
