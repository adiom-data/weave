import { prepareRecordData } from './protocol.js'
import type { RecordData } from './protocol.js'
import type { EntitySnapshot } from './store.js'
import { OptimisticSelections } from './optimistic_selection.js'
import type { PreparedSelectionChanges } from './optimistic_selection.js'

/** Top-level stored fields; nested values are replaced whole, never deep-merged. */
export interface OptimisticEntityPatch {
  readonly entityId: string
  readonly set?: RecordData
  readonly unset?: readonly string[]
}
/** Shared staging validation: drafts must not retain a known-invalid patch. */
export function prepareOptimisticPatch(patch: OptimisticEntityPatch): Readonly<{ set: RecordData; unset: readonly string[] }> {
  const set = prepareRecordData(patch.set ?? {})
  const unset = [...(patch.unset ?? [])]
  if (unset.some(key => typeof key !== 'string') || new Set(unset).size !== unset.length || unset.some(key => Object.hasOwn(set, key))) throw new Error('Optimistic set/unset fields must be disjoint; unset fields must be unique strings')
  if (!Object.keys(set).length && !unset.length) throw new Error('Optimistic entity patch must change at least one field')
  return { set, unset: Object.freeze(unset) }
}
export interface OptimisticMutation {
  readonly mutationId: string
  readonly patches?: readonly OptimisticEntityPatch[]
  readonly create?: readonly { readonly entity: TemporaryEntity; readonly values: RecordData }[]
  readonly delete?: readonly string[]
  readonly lists?: readonly OptimisticListChange[]
  readonly slots?: readonly OptimisticSlotChange[]
  /** Rejection rolls the overlay back. Success alone does not confirm CDC. */
  readonly result?: Promise<unknown>
}
export const temporaryBrand: unique symbol = Symbol('TemporaryEntity')
declare const targetBrand: unique symbol
/** Client-local identity, shared by references without requiring a database key. */
export interface TemporaryEntity { readonly entityId: string; readonly [temporaryBrand]: true }
/** A logical projection slot, minted by its owning projection handle. */
export interface OptimisticTarget { readonly kind: 'entity' | 'list'; readonly [targetBrand]: true }
export type OptimisticPosition = 'start' | 'end' | Readonly<{ before: string }> | Readonly<{ after: string }>
export type OptimisticListChange = Readonly<{ target: OptimisticTarget }> & (
  | Readonly<{ type: 'insert' | 'move'; entityId: string; position: OptimisticPosition }>
  | Readonly<{ type: 'remove'; entityId: string }>
  | Readonly<{ type: 'clear' }>
)
export interface OptimisticSlotChange { readonly target: OptimisticTarget; readonly entityId: string | null }
export interface OptimisticHandle {
  readonly mutationId: string
  /** Idempotent, generation-safe, local-only rollback; never undoes a write. */
  rollback(): void
}
type EntityChange = { readonly type: 'patch'; readonly set: RecordData; readonly unset: readonly string[] } |
  { readonly type: 'new'; readonly values: RecordData } | { readonly type: 'delete' }
interface Layer { readonly entities: ReadonlyMap<string, EntityChange>; readonly targets: PreparedSelectionChanges }
interface Cached { base: EntitySnapshot; value: EntitySnapshot; deleted: boolean; dirty: boolean }
export interface OptimisticChanges { readonly entities: readonly string[]; readonly targets: readonly string[] }

function equal(a: unknown, b: unknown): boolean {
  if (Object.is(a, b)) return true
  if (!a || !b || typeof a !== 'object' || typeof b !== 'object' || Array.isArray(a) !== Array.isArray(b)) return false
  const before = a as Record<string, unknown>, after = b as Record<string, unknown>
  const keys = Object.keys(before)
  return keys.length === Object.keys(after).length && keys.every(key => Object.hasOwn(after, key) && equal(before[key], after[key]))
}

/** Internal ordered layers and entity routing. Authoritative records live only in ModelStore. */
export class OptimisticLayers {
  private layers = new Map<string, Layer>()
  private entities = new Map<string, Map<string, EntityChange>>()
  private cache = new Map<string, Cached>()
  private temporaryEntities = new WeakSet<TemporaryEntity>()
  private creators = new Map<string, string>()
  private selections = new OptimisticSelections()

  getDiagnostics(): Readonly<{ layers: number; entities: number; targets: number; materializedEntities: number }> {
    return Object.freeze({ layers: this.layers.size, entities: this.entities.size, targets: this.selections.size, materializedEntities: this.cache.size })
  }

  temporary(): TemporaryEntity {
    const entity: TemporaryEntity = Object.freeze({ entityId: `local_${crypto.randomUUID()}`, [temporaryBrand]: true as const })
    this.temporaryEntities.add(entity)
    return entity
  }
  target(scope: string, node: string, ancestors: readonly string[], kind: 'entity' | 'list'): OptimisticTarget {
    return this.selections.target(scope, node, ancestors, kind)
  }
  selection(key: string, base: readonly string[], backed: (id: string) => boolean): readonly string[] { return this.selections.read(key, base, backed) }
  hasSelection(key: string): boolean { return this.selections.has(key) }

  add(input: OptimisticMutation): Layer {
    const id = input.mutationId
    if (typeof id !== 'string' || !id || this.layers.has(id)) throw new Error('Optimistic mutation ID must be nonempty and unique')
    const entities = new Map<string, EntityChange>()
    const add = (entityId: string, value: EntityChange) => {
      if (typeof entityId !== 'string' || !entityId || entities.has(entityId)) throw new Error('Optimistic entity IDs must be nonempty and unique within a mutation')
      entities.set(entityId, value)
    }
    // Validate/own everything before making any layer visible.
    for (const patch of input.patches ?? []) {
      add(patch.entityId, { type: 'patch', ...prepareOptimisticPatch(patch) })
    }
    for (const creation of input.create ?? []) {
      if (!this.temporaryEntities.has(creation.entity) || this.creators.has(creation.entity.entityId)) throw new Error('Optimistic creation requires an unused temporary reference from this client')
      add(creation.entity.entityId, { type: 'new', values: prepareRecordData(creation.values) })
    }
    for (const entityId of input.delete ?? []) add(entityId, { type: 'delete' })
    const targets = this.selections.prepare(input.lists ?? [], input.slots ?? [])
    if (!entities.size && !targets.size) throw new Error('Optimistic mutation requires at least one change')
    const layer = { entities, targets }
    this.layers.set(id, layer)
    this.selections.add(id, targets)
    for (const [entityId, change] of entities) {
      let layers = this.entities.get(entityId)
      if (!layers) this.entities.set(entityId, layers = new Map())
      layers.set(id, change)
      if (change.type === 'new') this.creators.set(entityId, id)
      this.invalidate(entityId)
    }
    return layer
  }
  changes(layer: Layer): OptimisticChanges { return { entities: [...layer.entities.keys()], targets: [...layer.targets.keys()] } }
  remove(id: string, expected?: Layer): OptimisticChanges {
    const layer = this.layers.get(id)
    if (!layer || (expected && layer !== expected)) return { entities: [], targets: [] }
    this.layers.delete(id)
    this.selections.remove(id, layer.targets.keys())
    for (const entityId of layer.entities.keys()) {
      const layers = this.entities.get(entityId)
      layers?.delete(id)
      if (!layers?.size) this.entities.delete(entityId)
      if (this.creators.get(entityId) === id) {
        this.creators.delete(entityId)
        // Once temporary backing retires, its materialized payload has no
        // authoritative lifetime. Dangling patch layers can only be dormant.
        this.cache.delete(entityId)
      } else if (!layers?.size && this.cache.get(entityId)?.base.status !== 'available') this.cache.delete(entityId)
      else this.invalidate(entityId)
    }
    return this.changes(layer)
  }
  // Keep unbacked predictions registered, but release old materialized values.
  dropEntity(id: string): void { this.cache.delete(id) }
  clear(): void { this.layers.clear(); this.entities.clear(); this.cache.clear(); this.creators.clear(); this.selections.clear() }
  // A local deletion changes visibility, not whether a reference is backed.
  // Patches and deletes alone must never invent backing for unloaded entities.
  hasBacking(id: string, base: EntitySnapshot): boolean { return base.status === 'available' || this.creators.has(id) }
  read(id: string, base: EntitySnapshot): EntitySnapshot {
    if (!this.hasBacking(id, base)) { this.cache.delete(id); return base }
    const layers = this.entities.get(id)
    const cached = this.cache.get(id)
    if (!layers && !cached) return base
    if (cached && cached.base === base && !cached.dirty) return cached.value
    let value = base
    let deleted = false
    if (layers) {
      for (const change of layers.values()) {
        if (change.type === 'new') { value = Object.freeze({ status: 'available', values: change.values, provisional: true }); deleted = false; continue }
        if (value.status !== 'available') continue
        if (change.type === 'delete') { value = Object.freeze({ status: 'unavailable' }); deleted = true; continue }
        const values: Record<string, RecordData[string]> = Object.assign(Object.create(null), value.values)
        for (const key of change.unset) delete values[key]
        Object.assign(values, change.set)
        value = Object.freeze({ ...value, values: Object.freeze(values) })
      }
    }
    const previous = cached?.value ?? base
    const shared = equal(previous, value) ? previous : value
    if (!layers && shared === base) this.cache.delete(id)
    else this.cache.set(id, { base, value: shared, deleted, dirty: false })
    return shared
  }
  deleted(id: string, base: EntitySnapshot): boolean { this.read(id, base); return this.cache.get(id)?.deleted ?? false }
  private invalidate(id: string): void { const cached = this.cache.get(id); if (cached) cached.dirty = true }
}
