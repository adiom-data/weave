import { prepareRecordData } from './protocol.js'
import type { RecordData } from './protocol.js'
import type { EntitySnapshot } from './store.js'
import { OptimisticSelections } from './optimistic_selection.js'
import type { PreparedSelectionChanges } from './optimistic_selection.js'
import { OptimisticComputations, equalPrediction } from './optimistic_computed.js'
import type { PreparedComputedChanges, ComputedPrediction } from './optimistic_computed.js'
import { preparePaths, applyPaths } from './optimistic_paths.js'
import type { OptimisticPathChange } from './optimistic_paths.js'

/** Stored fields. set replaces whole values; paths address individual object keys. */
export interface OptimisticEntityPatch {
  readonly entityId: string
  readonly set?: RecordData
  readonly unset?: readonly string[]
  readonly paths?: readonly OptimisticPathChange[]
}
/** Shared staging validation: drafts must not retain a known-invalid patch. */
export function prepareOptimisticPatch(patch: OptimisticEntityPatch): Readonly<{ paths: readonly OptimisticPathChange[] }> {
  const set = prepareRecordData(patch.set ?? {})
  const unset = [...(patch.unset ?? [])]
  if (unset.some(key => typeof key !== 'string') || new Set(unset).size !== unset.length || unset.some(key => Object.hasOwn(set, key))) throw new Error('Optimistic set/unset fields must be disjoint; unset fields must be unique strings')
  const paths = preparePaths([...Object.entries(set).map(([key, value]) => ({ path: [key], value })), ...unset.map(key => ({ path: [key], unset: true as const })), ...(patch.paths ?? [])])
  if (!paths.length) throw new Error('Optimistic entity patch must change at least one field')
  return { paths }
}
export interface OptimisticComputedChange {
  readonly target: OptimisticTarget
  readonly entityId: string
  readonly set?: RecordData
  readonly invalidate?: readonly string[]
}
export interface OptimisticMutation {
  readonly mutationId: string
  readonly patches?: readonly OptimisticEntityPatch[]
  readonly create?: readonly { readonly entity: TemporaryEntity; readonly values: RecordData }[]
  readonly delete?: readonly string[]
  readonly lists?: readonly OptimisticListChange[]
  readonly slots?: readonly OptimisticSlotChange[]
  readonly computed?: readonly OptimisticComputedChange[]
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
type EntityChange = { readonly type: 'patch'; readonly paths: readonly OptimisticPathChange[] } |
  { readonly type: 'new'; readonly values: RecordData } | { readonly type: 'delete' }
interface Layer { readonly entities: ReadonlyMap<string, EntityChange>; readonly targets: PreparedSelectionChanges; readonly computed: PreparedComputedChanges }
interface EntityImage { readonly record: EntitySnapshot; readonly backed: boolean; readonly deleted: boolean }
interface Cached { base: EntityImage; value: EntityImage; dirty: boolean }
/** A complete effective read view. Backing and presentation metadata must travel
 * with values when another layer is composed; neither is inferred from visibility. */
export interface OptimisticReadView {
  entity(id: string): EntityImage
  selection(key: string, base: readonly string[], backed?: (id: string) => boolean): Readonly<{ ids: readonly string[]; overridden: boolean }>
  computed(key: string, base: RecordData): ComputedPrediction
}
export interface OptimisticChanges { readonly entities: readonly string[]; readonly targets: readonly string[] }

/** Internal ordered layers and entity routing. Authoritative records live only in ModelStore. */
export class OptimisticLayers {
  private layers = new Map<string, Layer>()
  private entities = new Map<string, Map<string, EntityChange>>()
  private cache = new Map<string, Cached>()
  private temporaryEntities = new WeakSet<TemporaryEntity>()
  private creators = new Map<string, string>()
  private selections = new OptimisticSelections()
  private computations = new OptimisticComputations()
  // A private staged overlay shares identity validation, never live layers or caches.
  fork(): OptimisticLayers {
    const fork = new OptimisticLayers()
    fork.temporaryEntities = this.temporaryEntities
    fork.selections = this.selections.fork()
    return fork
  }

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
  // Used both over authoritative records and over an existing effective view.
  // The store supplies routing/tracking, not a second set of composition rules.
  over(base: OptimisticReadView): OptimisticReadView {
    const view: OptimisticReadView = {
      entity: id => this.read(id, base.entity(id)),
      selection: (key, ids, backed = id => view.entity(id).backed) => {
        // All membership layers resolve backing against the final view. A newer
        // creation can make an earlier, previously dormant selection effective.
        const current = base.selection(key, ids, backed)
        return { ids: this.selections.read(key, current.ids, backed), overridden: current.overridden || this.selections.has(key) }
      },
      computed: (key, values) => this.computations.read(key, base.computed(key, values)),
    }
    return view
  }
  prepareComputed(changes: readonly OptimisticComputedChange[]): PreparedComputedChanges {
    return this.computations.prepare(changes, target => this.selections.describe(target))
  }

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
    const computed = this.prepareComputed(input.computed ?? [])
    if (!entities.size && !targets.size && !computed.size) throw new Error('Optimistic mutation requires at least one change')
    const layer = { entities, targets, computed }
    this.layers.set(id, layer)
    this.selections.add(id, targets)
    this.computations.add(id, computed)
    for (const [entityId, change] of entities) {
      let layers = this.entities.get(entityId)
      if (!layers) this.entities.set(entityId, layers = new Map())
      layers.set(id, change)
      if (change.type === 'new') this.creators.set(entityId, id)
      this.invalidate(entityId)
    }
    return layer
  }
  changes(layer: Layer): OptimisticChanges { return { entities: [...layer.entities.keys()], targets: [...layer.targets.keys(), ...layer.computed.keys()] } }
  remove(id: string, expected?: Layer): OptimisticChanges {
    const layer = this.layers.get(id)
    if (!layer || (expected && layer !== expected)) return { entities: [], targets: [] }
    this.layers.delete(id)
    this.selections.remove(id, layer.targets.keys())
    this.computations.remove(id, layer.computed.keys())
    for (const entityId of layer.entities.keys()) {
      const layers = this.entities.get(entityId)
      layers?.delete(id)
      if (!layers?.size) this.entities.delete(entityId)
      if (this.creators.get(entityId) === id) {
        this.creators.delete(entityId)
        // Once temporary backing retires, its materialized payload has no
        // authoritative lifetime. Dangling patch layers can only be dormant.
        this.cache.delete(entityId)
      } else if (!layers?.size && this.cache.get(entityId)?.base.record.status !== 'available') this.cache.delete(entityId)
      else this.invalidate(entityId)
    }
    return this.changes(layer)
  }
  // Keep unbacked predictions registered, but release old materialized values.
  dropEntity(id: string): void { this.cache.delete(id) }
  clear(): void { this.layers.clear(); this.entities.clear(); this.cache.clear(); this.creators.clear(); this.selections.clear(); this.computations.clear() }
  private read(id: string, base: EntityImage): EntityImage {
    // Deletion hides a backed entity; patches cannot invent backing. An explicit
    // creation can provide backing even when the inherited record is unavailable.
    const backed = base.backed || this.creators.has(id)
    if (!backed) { this.cache.delete(id); return base }
    const layers = this.entities.get(id)
    const cached = this.cache.get(id)
    if (!layers && !cached) return base
    if (cached && cached.base.record === base.record && cached.base.backed === base.backed && cached.base.deleted === base.deleted && !cached.dirty) return cached.value
    let value = base.record
    let deleted = base.deleted
    if (layers) {
      for (const change of layers.values()) {
        if (change.type === 'new') { value = Object.freeze({ status: 'available', values: change.values, provisional: true }); deleted = false; continue }
        if (value.status !== 'available') continue
        if (change.type === 'delete') { value = Object.freeze({ status: 'unavailable' }); deleted = true; continue }
        value = Object.freeze({ ...value, values: applyPaths(value.values, change.paths) })
      }
    }
    const previous = cached?.value.record ?? base.record
    const shared = equalPrediction(previous, value) ? previous : value
    const image = shared === base.record && backed === base.backed && deleted === base.deleted ? base : Object.freeze({ record: shared, backed, deleted })
    if (!layers && shared === base.record) this.cache.delete(id)
    else this.cache.set(id, { base, value: image, dirty: false })
    return image
  }
  private invalidate(id: string): void { const cached = this.cache.get(id); if (cached) cached.dirty = true }
}
