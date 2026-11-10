import type { ProjectedEntity, ProjectedList, ProjectionShape, StoredFields } from './bindings.js'
import { decodeFields } from './bindings.js'
import type { OptimisticHandle, OptimisticMutation, OptimisticPosition, OptimisticEntityPatch, OptimisticListChange, OptimisticSlotChange, OptimisticComputedChange, TemporaryEntity } from './optimistic.js'
import { temporaryBrand, prepareOptimisticPatch } from './optimistic.js'
import { entityBrand, listBinding, bindEntity, entityBinding, previewOptimistic, validateComputed } from './projection_metadata.js'
import { prepareOptimisticPosition } from './optimistic_selection.js'
import type { ListBinding, OptimisticHost } from './projection_metadata.js'
import { encodeFields } from './value_input.js'
import { prepareRecordData } from './protocol.js'
import type { RecordData } from './protocol.js'
import { fillOptimisticShape } from './optimistic_shape.js'
import type { OptimisticShape, OptimisticFill } from './optimistic_shape.js'
import { prepareProjectedPatch } from './optimistic_patch.js'
import { isMergeValue } from './optimistic_values.js'
import type { PatchField } from './optimistic_values.js'
import type { ModelStore } from './store.js'

export type EntityReference = TemporaryEntity | ProjectedEntity<unknown>
type EntityValue<T> = Extract<T, { status: 'available' }> extends { value: infer V } ? V : never
type InputField<T> = NonNullable<T> extends ProjectedList<infer R> ? readonly (OptimisticInput<R> | EntityReference)[] :
  NonNullable<T> extends { readonly [entityBrand]: true } ? OptimisticInput<EntityValue<NonNullable<T>>> | EntityReference | null :
  T extends readonly (infer E)[] ? readonly InputField<E>[] : T extends object ? OptimisticInput<T> : T
/** Partial stored fields plus explicit child expansions, derived from generated data types. */
export type OptimisticInput<T> = { readonly [K in keyof StoredFields<T>]?: InputField<StoredFields<T>[K]> }
type ChildKeys<T> = { [K in keyof T]-?: NonNullable<T[K]> extends ProjectedList<unknown> | ProjectedEntity<unknown> ? K : never }[keyof T]
export type OptimisticPatch<T> = { readonly [K in keyof Omit<StoredFields<T>, ChildKeys<StoredFields<T>>>]?: PatchField<StoredFields<T>[K]> }
export type InsertedEntity<R> = Readonly<{ entityId: string; [entityBrand]: true; status: 'optimistic'; value: {
  readonly [K in keyof StoredFields<R>]?: NonNullable<StoredFields<R>[K]> extends ProjectedList<infer C> ? readonly InsertedEntity<C>[] : NonNullable<StoredFields<R>[K]> extends ProjectedEntity<infer C> ? InsertedEntity<C> | EntityReference | null : StoredFields<R>[K]
} }>
export interface OptimisticDraftOptions { readonly mutationId: string; readonly result?: Promise<unknown> }
interface Changes {
  create: { entity: TemporaryEntity; values: RecordData }[]
  lists: OptimisticListChange[]
  slots: OptimisticSlotChange[]
  computed: OptimisticComputedChange[]
}
function changes(): Changes { return { create: [], lists: [], slots: [], computed: [] } }
function reference(value: unknown): value is EntityReference {
  return !!value && typeof value === 'object' && (entityBrand in value || temporaryBrand in value)
}

/** A private draft until commit: no queries, inferred joins, or network writes. */
export class OptimisticDraft {
  private changes = changes()
  private patches: OptimisticEntityPatch[] = []
  private deletions: string[] = []
  private bindings = new Set<ListBinding>()
  private shapeChecks: (() => void)[] = []
  private filling = false
  private changedEntities = new Set<string>()
  private closed = false
  private context: string | undefined
  private options: OptimisticDraftOptions
  constructor(private host: OptimisticHost & { readonly connectionContext?: string; readonly store: ModelStore }, options: OptimisticDraftOptions) {
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
  insert<R>(list: ProjectedList<R>, input: OptimisticInput<NoInfer<R>>, position?: OptimisticPosition): InsertedEntity<R>
  insert<R>(list: ProjectedList<R>, input: EntityReference, position?: OptimisticPosition): EntityReference
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
    this.changes.create.push(...staged.create); this.changes.lists.push(...staged.lists); this.changes.slots.push(...staged.slots); this.changes.computed.push(...staged.computed)
    this.bindings.add(binding)
    return entity
  }
  remove<R>(list: ProjectedList<R>, entity: EntityReference): void {
    this.assertOpen()
    const binding = listBinding(this.host, list)
    this.changes.lists.push({ type: 'remove', target: binding.target(binding.shape.nodeId, binding.ancestors), entityId: entity.entityId })
    this.bindings.add(binding)
  }
  patch<R>(entity: ProjectedEntity<R>, set: OptimisticPatch<NoInfer<R>>, unset: readonly (keyof OptimisticPatch<NoInfer<R>> & string)[] = []): void {
    this.assertOpen()
    const binding = entityBinding(this.host, entity)
    const fields = binding.fields.filter(field => !field.computed)
    const patch = { entityId: entity.entityId, paths: prepareProjectedPatch(set, fields, unset, binding.location) }
    if (!patch.paths.length) return // Explicit empty merges neither publish nor reserve the entity.
    this.reserveEntities([patch.entityId]); this.patches.push(patch)
  }
  /** Explicit canonical escape hatch for callers without compiled row metadata. */
  patchCanonical(entity: EntityReference, set: RecordData, unset: readonly string[] = []): void {
    this.assertOpen()
    const patch = { entityId: entity.entityId, ...prepareOptimisticPatch({ entityId: entity.entityId, set, unset }) }
    this.reserveEntities([patch.entityId])
    this.patches.push(patch)
  }
  /** Fill selected occurrence-specific computations, using native generated types. */
  fill<P>(shape: OptimisticShape<P>, predict: OptimisticFill<NoInfer<P>>): void {
    this.assertOpen()
    this.filling = true
    let filled: ReturnType<typeof fillOptimisticShape>
    try {
      const reader = this.hasChanges ? this.host.store[previewOptimistic](this.mutation()) : this.host.store
      filled = fillOptimisticShape(this.host, shape, predict, reader)
      // Reuse the normal layer validator to reject conflicts before staging.
      if (filled.computed.length) this.host.store[validateComputed]([...this.changes.computed, ...filled.computed])
    }
    finally { this.filling = false }
    this.changes.computed.push(...filled.computed)
    this.shapeChecks.push(filled.validate)
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
    for (const check of this.shapeChecks) check()
    this.closed = true
    try {
      return this.host.beginOptimistic(this.mutation())
    } catch (error) { this.closed = false; throw error }
  }
  /** Useful for callers whose optimistic scope contains only application-owned state. */
  get hasChanges(): boolean { return this.changes.create.length + this.changes.lists.length + this.changes.slots.length + this.changes.computed.length + this.patches.length + this.deletions.length > 0 }
  private mutation(): OptimisticMutation { return { ...this.options, ...this.changes, patches: this.patches, delete: this.deletions } }
  private assertOpen(): void {
    if (this.closed) throw new Error('Optimistic draft is already committed')
    if (this.filling) throw new Error('Cannot mutate an optimistic draft from its fill callback')
  }
  private reserveEntities(ids: readonly string[]): void {
    if (new Set(ids).size !== ids.length || ids.some(id => typeof id !== 'string' || !id || this.changedEntities.has(id))) throw new Error('Optimistic entity IDs must be nonempty and unique within a mutation')
    for (const id of ids) this.changedEntities.add(id)
  }
  private build(binding: ListBinding, shape: ProjectionShape, ancestors: readonly string[], input: unknown, staged: Changes): EntityReference {
    if (reference(input)) return input
    if (isMergeValue(input)) throw new Error('mergeValue is only supported in stored-field patches')
    if (!input || typeof input !== 'object' || Array.isArray(input)) throw new Error('Optimistic entity input must be an object or entity reference')
    const fields: Record<string, unknown> = Object.create(null)
    for (const [key, value] of Object.entries(input)) {
      if (!Object.hasOwn(shape.children, key)) fields[key] = value
    }
    const stored: Record<string, unknown> = Object.create(null)
    for (const [name, value] of Object.entries(fields)) {
      const field = shape.fields.find(field => field.name === name)
      if (!field) throw new Error(`${shape.nodeId}: unknown field ${name}`)
      if (field.computed) throw new Error(`${shape.nodeId}: computed field ${name} must be filled through a projection`)
      stored[name] = value
    }
    const values = prepareRecordData(encodeFields(stored, shape.fields.filter(field => !field.computed), shape.nodeId))
    const entity = this.host.temporaryEntity()
    staged.create.push({ entity, values })
    const inserted: Record<string, unknown> = decodeFields(values, shape.fields.filter(field => !field.computed), shape.nodeId, undefined, true)
    const path = [...ancestors, entity.entityId]
    for (const [key, child] of Object.entries(shape.children)) {
      if (!Object.hasOwn(input, key)) continue
      const value = (input as Record<string, unknown>)[key]
      if (value === undefined) continue
      const target = binding.target(child.nodeId, path)
      if (child.kind === 'entity') {
        const nested = value === null ? null : this.build(binding, child, path, value, staged)
        inserted[key] = nested
        staged.slots.push({ target, entityId: nested?.entityId ?? null })
      } else {
        if (!Array.isArray(value)) throw new Error(`Optimistic list ${key} must be an array`)
        staged.lists.push({ type: 'clear', target })
        const nested = value.map(item => this.build(binding, child, path, item, staged))
        inserted[key] = Object.freeze(nested)
        for (const item of nested) staged.lists.push({ type: 'insert', target, entityId: item.entityId, position: 'end' })
      }
    }
    const result = Object.freeze({ entityId: entity.entityId, [entityBrand]: true as const, status: 'optimistic' as const, value: Object.freeze(inserted) })
    bindEntity(result, { owner: binding.owner, fields: shape.fields, location: shape.nodeId })
    return result
  }
}
