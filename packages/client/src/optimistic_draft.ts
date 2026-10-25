import type { ProjectedEntity, ProjectedList, ProjectionShape } from './bindings.js'
import type { OptimisticHandle, OptimisticMutation, OptimisticPosition, OptimisticEntityPatch, OptimisticListChange, OptimisticSlotChange, TemporaryEntity } from './optimistic.js'
import { temporaryBrand, prepareOptimisticPatch } from './optimistic.js'
import { entityBrand, listBinding } from './projection_metadata.js'
import { prepareOptimisticPosition } from './optimistic_selection.js'
import type { ListBinding, OptimisticHost } from './projection_metadata.js'
import { encodeFields } from './value_input.js'
import { prepareRecordData } from './protocol.js'
import type { RecordData } from './protocol.js'

export type EntityReference = TemporaryEntity | ProjectedEntity<unknown>
type EntityValue<T> = Extract<T, { status: 'available' }> extends { value: infer V } ? V : never
type InputField<T> = NonNullable<T> extends ProjectedList<infer R> ? readonly (OptimisticInput<R> | EntityReference)[] :
  NonNullable<T> extends { readonly [entityBrand]: true } ? OptimisticInput<EntityValue<NonNullable<T>>> | EntityReference | null :
  T extends readonly (infer E)[] ? readonly InputField<E>[] : T extends object ? OptimisticInput<T> : T
/** Partial stored fields plus explicit child expansions, derived from generated data types. */
export type OptimisticInput<T> = { readonly [K in keyof T]?: InputField<T[K]> }
export interface OptimisticDraftOptions { readonly mutationId: string; readonly result?: Promise<unknown> }
interface Changes {
  create: { entity: TemporaryEntity; values: RecordData }[]
  lists: OptimisticListChange[]
  slots: OptimisticSlotChange[]
}
function changes(): Changes { return { create: [], lists: [], slots: [] } }
function reference(value: unknown): value is EntityReference {
  return !!value && typeof value === 'object' && (entityBrand in value || temporaryBrand in value)
}

/** A private draft until commit: no queries, inferred joins, or network writes. */
export class OptimisticDraft {
  private changes = changes()
  private patches: OptimisticEntityPatch[] = []
  private deletions: string[] = []
  private bindings = new Set<ListBinding>()
  private changedEntities = new Set<string>()
  private closed = false
  private context: string | undefined
  private options: OptimisticDraftOptions
  constructor(private host: OptimisticHost & { readonly connectionContext?: string }, options: OptimisticDraftOptions) {
    this.context = host.connectionContext
    this.options = Object.freeze({ mutationId: options.mutationId, result: options.result })
  }
  /** Canonical stored-field escape hatch, reusable across projections/aliases. */
  entity(values: RecordData): TemporaryEntity {
    this.assertOpen()
    const owned = prepareRecordData(values)
    const entity = this.host.temporaryEntity()
    this.reserveEntities([entity.entityId])
    this.changes.create.push({ entity, values: owned })
    return entity
  }
  insert<R>(list: ProjectedList<R>, input: OptimisticInput<NoInfer<R>> | EntityReference, position: OptimisticPosition = 'end'): EntityReference {
    this.assertOpen()
    const binding = listBinding(this.host, list)
    // Stage each graph operation separately so a failed encoder cannot leave a
    // half-inserted graph in a draft the caller subsequently commits.
    const staged = changes()
    const target = binding.target(binding.shape.nodeId, binding.ancestors)
    const ownedPosition = prepareOptimisticPosition(position)
    const entity = this.build(binding, binding.shape, binding.ancestors, input, staged)
    staged.lists.push({ type: 'insert', target, entityId: entity.entityId, position: ownedPosition })
    this.reserveEntities(staged.create.map(value => value.entity.entityId))
    this.changes.create.push(...staged.create); this.changes.lists.push(...staged.lists); this.changes.slots.push(...staged.slots)
    this.bindings.add(binding)
    return entity
  }
  remove<R>(list: ProjectedList<R>, entity: EntityReference): void {
    this.assertOpen()
    const binding = listBinding(this.host, list)
    this.changes.lists.push({ type: 'remove', target: binding.target(binding.shape.nodeId, binding.ancestors), entityId: entity.entityId })
    this.bindings.add(binding)
  }
  patch(entity: EntityReference, set: RecordData, unset: readonly string[] = []): void {
    this.assertOpen()
    const patch = { entityId: entity.entityId, ...prepareOptimisticPatch({ entityId: entity.entityId, set, unset }) }
    this.reserveEntities([patch.entityId])
    this.patches.push(patch)
  }
  delete(entity: EntityReference): void {
    this.assertOpen()
    this.reserveEntities([entity.entityId])
    this.deletions.push(entity.entityId)
  }
  commit(): OptimisticHandle {
    this.assertOpen()
    if (this.host.connectionContext !== this.context) throw new Error('Optimistic draft belongs to an expired connection')
    for (const binding of this.bindings) binding.target(binding.shape.nodeId, binding.ancestors)
    this.closed = true
    try {
      const mutation: OptimisticMutation = { ...this.options, ...this.changes, patches: this.patches, delete: this.deletions }
      return this.host.beginOptimistic(mutation)
    } catch (error) { this.closed = false; throw error }
  }
  /** Useful for callers whose optimistic scope contains only application-owned state. */
  get hasChanges(): boolean { return this.changes.create.length + this.changes.lists.length + this.changes.slots.length + this.patches.length + this.deletions.length > 0 }
  private assertOpen(): void { if (this.closed) throw new Error('Optimistic draft is already committed') }
  private reserveEntities(ids: readonly string[]): void {
    if (new Set(ids).size !== ids.length || ids.some(id => typeof id !== 'string' || !id || this.changedEntities.has(id))) throw new Error('Optimistic entity IDs must be nonempty and unique within a mutation')
    for (const id of ids) this.changedEntities.add(id)
  }
  private build(binding: ListBinding, shape: ProjectionShape, ancestors: readonly string[], input: unknown, staged: Changes): EntityReference {
    if (reference(input)) return input
    if (!input || typeof input !== 'object' || Array.isArray(input)) throw new Error('Optimistic entity input must be an object or entity reference')
    const fields: Record<string, unknown> = Object.create(null)
    for (const [key, value] of Object.entries(input)) {
      if (!Object.hasOwn(shape.children, key)) fields[key] = value
    }
    const values = prepareRecordData(encodeFields(fields, shape.fields, shape.nodeId))
    const entity = this.host.temporaryEntity()
    staged.create.push({ entity, values })
    const path = [...ancestors, entity.entityId]
    for (const [key, child] of Object.entries(shape.children)) {
      if (!Object.hasOwn(input, key)) continue
      const value = (input as Record<string, unknown>)[key]
      if (value === undefined) continue
      const target = binding.target(child.nodeId, path)
      if (child.kind === 'entity') {
        staged.slots.push({ target, entityId: value === null ? null : this.build(binding, child, path, value, staged).entityId })
      } else {
        if (!Array.isArray(value)) throw new Error(`Optimistic list ${key} must be an array`)
        staged.lists.push({ type: 'clear', target })
        for (const item of value) staged.lists.push({ type: 'insert', target, entityId: this.build(binding, child, path, item, staged).entityId, position: 'end' })
      }
    }
    return entity
  }
}
