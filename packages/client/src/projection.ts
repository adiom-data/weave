import type { ProjectionRequest } from './protocol.js'
import type { EntitySnapshot, ModelStore, QuerySnapshot, SubscriptionSnapshot } from './store.js'
import type { RecordData, BranchFailure } from './protocol.js'
import type { ComputedPredictionStates } from './optimistic_computed.js'
import { ProjectionTraversal } from './projection_reader.js'

export interface ProjectionRow {
  readonly error?: BranchFailure
  readonly stale?: boolean
  readonly computed?: RecordData
  readonly computedState?: ComputedPredictionStates
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

export type ProjectionReader = Pick<ModelStore, 'getSubscription' | 'getConnection' | 'getBranchErrors' | 'getOccurrences' | 'getQuery' | 'projectionSelection' | 'isOptimisticallyDeleted' | 'getComputed' | 'projectionComputed' | 'getAuthoritativeEntity' | 'getEntity'>

// Publication and private draft reads share one membership/error resolver.
export function readProjection(store: ProjectionReader, subscriptionId: string, request: ProjectionRequest, options: { readonly authoritative?: boolean } = {}): ProjectionSnapshot {
  const state = store.getSubscription(subscriptionId)
  const connection = store.getConnection()
  const traversal = new ProjectionTraversal(store, subscriptionId, request, options.authoritative)
  const status = state.status === 'error' || state.status === 'closed' ? state.status : connection.status === 'disconnected' ? 'disconnected' : state.status
  return Object.freeze({ status, error: state.error ?? connection.error, errors: traversal.errors.length ? traversal.errors : undefined, root: traversal.materialize(traversal.root()) })
}
