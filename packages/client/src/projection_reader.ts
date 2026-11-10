import type { BranchFailure, ProjectionNode, ProjectionRequest } from './protocol.js'
import type { NodeView, ProjectionReader, ProjectionRow } from './projection.js'
import type { EntitySnapshot, QuerySnapshot } from './store.js'

const emptyIDs: readonly string[] = Object.freeze([])
const slotKey = (node: string, parent: string, entity: string) => JSON.stringify([node, parent, entity])

export interface ProjectionBranch {
  readonly node: ProjectionNode
  readonly ancestors: readonly string[]
  readonly occurrenceId?: string
  // A detached error may retain a display address, but cannot supply live edges
  // or computed backing. Those always require an actual model occurrence.
  readonly sourceOccurrenceId?: string
  readonly query?: QuerySnapshot
  readonly ids: readonly string[]
  readonly error?: BranchFailure
  readonly failed?: ReadonlyMap<string, BranchFailure>
  readonly original?: ReadonlySet<string>
}

/** Shared membership/edge resolution for publication and targeted draft reads.
 * Resolving a branch reads identities only; payloads/children are materialized
 * explicitly, so unrelated rows never need decoding for an optimistic fill. */
export class ProjectionTraversal {
  readonly errors: readonly BranchFailure[]
  private slots = new Map<string, BranchFailure>()
  private rowErrors = new Map<string, Map<string, BranchFailure>>()
  private detachedErrors = new Map<string, BranchFailure>()
  private entities = new Map<string, EntitySnapshot>()
  constructor(private store: ProjectionReader, private scope: string, private request: ProjectionRequest, private authoritative = false) {
    this.errors = store.getBranchErrors(scope)
    for (const error of this.errors) {
      const key = slotKey(error.nodeId, error.parentOccurrenceId, error.parentEntityId)
      if (!error.entityId) { this.slots.set(key, error); continue }
      if (!this.detachedErrors.has(key)) this.detachedErrors.set(key, error)
      let rows = this.rowErrors.get(error.occurrenceId)
      if (!rows) this.rowErrors.set(error.occurrenceId, rows = new Map())
      rows.set(error.entityId, error)
    }
  }
  root(): ProjectionBranch | undefined { return this.branch(this.request.root, '') }
  child(parent: ProjectionBranch, entity: string, node: ProjectionNode): ProjectionBranch | undefined {
    return this.branch(node, parent.sourceOccurrenceId, entity, [...parent.ancestors, entity])
  }
  private branch(node: ProjectionNode, parent: string | undefined, parentEntity = '', ancestors: readonly string[] = []): ProjectionBranch | undefined {
    const occurrence = parent === undefined ? undefined : this.store.getOccurrences(this.scope, node.nodeId, parent, parentEntity)[0]
    const key = slotKey(node.nodeId, parent ?? '', parentEntity)
    const error = this.slots.get(key)
    if (error) return { node, ancestors, occurrenceId: error.occurrenceId, ids: emptyIDs, error }
    const detached = !occurrence ? this.detachedErrors.get(key) : undefined
    const failed = this.rowErrors.get(occurrence?.id ?? detached?.occurrenceId ?? '')
    const query = occurrence?.target.type === 'query' ? this.store.getQuery(occurrence.target.id) : undefined
    const base = occurrence?.target.type === 'entity' ? [occurrence.target.id] : query?.ids ?? emptyIDs
    const selection = this.authoritative ? { ids: base, overridden: false } : this.store.projectionSelection(this.scope, node.nodeId, ancestors, base)
    let ids = selection.ids
    if (failed?.size) {
      const present = new Set(ids)
      ids = [...ids, ...[...failed.keys()].filter(id => !present.has(id))]
    }
    if (!occurrence && !ids.length && !selection.overridden) return undefined
    return { node, ancestors, occurrenceId: occurrence?.id ?? (node.entity ? detached?.occurrenceId : undefined), sourceOccurrenceId: occurrence?.id, query, ids, failed, original: selection.overridden ? new Set(base) : undefined }
  }
  canVisit(branch: ProjectionBranch, id: string): boolean {
    return !branch.error && !branch.failed?.has(id) && (this.authoritative || !this.store.isOptimisticallyDeleted(id))
  }
  entity(id: string): EntitySnapshot {
    let entity = this.entities.get(id)
    if (!entity) {
      entity = this.authoritative ? this.store.getAuthoritativeEntity(id) : this.store.getEntity(id)
      this.entities.set(id, entity)
    }
    return entity
  }
  row(branch: ProjectionBranch, entityId: string): ProjectionRow {
    const error = branch.failed?.get(entityId)
    if (error) return Object.freeze({ entityId, entity: Object.freeze({ status: 'unavailable' as const }), children: Object.freeze({}), error })
    const children: Record<string, NodeView | undefined> = Object.create(null)
    if (this.authoritative || !this.store.isOptimisticallyDeleted(entityId)) {
      for (const child of branch.node.children ?? []) children[child.nodeId] = this.materialize(this.child(branch, entityId, child))
    }
    const computed = branch.node.computed?.length ? (this.authoritative
      ? { values: branch.sourceOccurrenceId ? this.store.getComputed(branch.sourceOccurrenceId, entityId) : undefined, states: undefined }
      : this.store.projectionComputed(this.scope, branch.node.nodeId, branch.ancestors, entityId, branch.sourceOccurrenceId)) : undefined
    let entity = this.entity(entityId)
    if (!this.authoritative && entity.status === 'available' && !entity.provisional && branch.node.computed?.some(name => !computed?.values?.[name]) && (branch.original && !branch.original.has(entityId) || Object.keys(computed?.states ?? {}).length)) entity = Object.freeze({ ...entity, provisional: true })
    return Object.freeze({ entityId, entity, computed: computed?.values, computedState: computed?.states && Object.keys(computed.states).length ? computed.states : undefined, children: Object.freeze(children) })
  }
  materialize(branch: ProjectionBranch | undefined): NodeView | undefined {
    if (!branch) return undefined
    const { node, occurrenceId, error } = branch
    if (error) return Object.freeze(node.entity
      ? { occurrenceId, nodeId: node.nodeId, type: 'entity' as const, error, row: Object.freeze({ entityId: '', entity: Object.freeze({ status: 'unavailable' as const }), children: Object.freeze({}) }) }
      : { occurrenceId, nodeId: node.nodeId, type: 'list' as const, error, query: undefined, rows: Object.freeze([]) })
    if (node.entity) return branch.ids.length ? Object.freeze({ occurrenceId, nodeId: node.nodeId, type: 'entity', row: this.row(branch, branch.ids[0]) }) : undefined
    const rows = branch.ids.filter(id => branch.failed?.has(id) || this.authoritative || !this.store.isOptimisticallyDeleted(id)).map(id => this.row(branch, id))
    return Object.freeze({ occurrenceId, nodeId: node.nodeId, type: 'list', query: branch.query, rows: Object.freeze(rows) })
  }
}
