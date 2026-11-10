import type { ComputedFields, FieldShape, ProjectedEntity, ProjectedList, ProjectionShape, ProvisionalValue, TypedProjectionHandle } from './bindings.js'
import type { OptimisticComputedChange } from './optimistic.js'
import type { EntityReference } from './optimistic_draft.js'
import { entityBrand, entityBinding, projectionBinding } from './projection_metadata.js'
import type { ProjectionBinding, OptimisticHost } from './projection_metadata.js'
import type { ProjectionReader } from './projection.js'
import { encodeValue } from './value_input.js'
import { decodeValue } from './bindings.js'
import { prepareRecordData } from './protocol.js'

export const invalidateComputed: unique symbol = Symbol('invalidateComputed')
export interface OptimisticOccurrence<R> {
  readonly entityId: string
  readonly current: ProvisionalValue<R>
  readonly ancestors: readonly string[]
}
type Row<T> = NonNullable<T> extends ProjectedList<infer R> ? R : Extract<NonNullable<T>, { status: 'available' }> extends { value: infer R } ? R : never
type ChildKeys<R> = { [K in keyof R]-?: NonNullable<R[K]> extends ProjectedList<unknown> | ProjectedEntity<unknown> ? K : never }[keyof R]
export type ComputedFill<T> = {
  readonly [K in keyof ComputedFields<Row<T>>]?: (occurrence: OptimisticOccurrence<Row<T>>) => ComputedFields<Row<T>>[K] | typeof invalidateComputed | undefined
} & { readonly [K in ChildKeys<Row<T>>]?: ComputedFill<Row<T>[K]> }
type ProjectionData<H> = H extends TypedProjectionHandle<infer T> ? T : never
export type OptimisticFill<P> = { readonly [K in keyof P]?: ComputedFill<ProjectionData<P[K]>> }
declare const shapeBrand: unique symbol
export interface OptimisticShape<P> { readonly [shapeBrand]: P }
interface ShapeBinding {
  readonly owner: OptimisticHost & { readonly connectionContext?: string }
  readonly generation: string | undefined
  readonly entities: ReadonlySet<string>
  readonly projections: ReadonlyMap<string, ProjectionBinding>
}
const shapes = new WeakMap<object, ShapeBinding>()

/** Select affected identities within named concrete projection handles. */
export function unionOptimisticShapes<const P extends Record<string, TypedProjectionHandle<unknown>>>(entities: readonly EntityReference[], projections: P): OptimisticShape<P> {
  const bindings = new Map<string, ProjectionBinding>()
  const handles = new Set<object>()
  let first: ProjectionBinding | undefined
  for (const [name, handle] of Object.entries(projections)) {
    if (handles.has(handle)) throw new Error('Duplicate concrete projection handle in optimistic shape')
    handles.add(handle)
    const binding = projectionBinding(handle)
    binding.validate()
    if (first && first.owner !== binding.owner) throw new Error('Optimistic projections must belong to the same client')
    first ??= binding
    bindings.set(name, binding)
  }
  if (!first) throw new Error('Optimistic shape requires at least one projection')
  const ids = new Set<string>()
  const seen = new Set<object>()
  const collect = (value: unknown): void => {
    if (!value || typeof value !== 'object' || seen.has(value)) return
    seen.add(value)
    if (Array.isArray(value)) { for (const item of value) collect(item); return }
    if (!(entityBrand in value)) return
    entityBinding(first!.owner, value)
    const row = value as ProjectedEntity<Record<string, unknown>>
    ids.add(row.entityId)
    // A staged insertion reference includes its explicitly inserted children.
    if (row.status === 'optimistic') for (const item of Object.values(row.value)) collect(item)
  }
  for (const entity of entities) {
    if (!entity || typeof entity.entityId !== 'string' || !entity.entityId) throw new Error('Optimistic shapes require entity references')
    ids.add(entity.entityId)
    collect(entity)
  }
  const shape = Object.freeze({}) as OptimisticShape<P>
  shapes.set(shape, { owner: first.owner, generation: first.owner.connectionContext, entities: ids, projections: bindings })
  return shape
}

export interface FilledOptimisticShape { readonly computed: readonly OptimisticComputedChange[]; validate(): void }
interface FillPlan {
  readonly fields: readonly (readonly [FieldShape, (value: unknown) => unknown])[]
  readonly children: readonly (readonly [string, ProjectionShape, FillPlan])[]
}
// Own the structure/function references before evaluating any row. Callbacks
// cannot change subsequent targets by mutating the caller's fill object.
function compileFill(shape: ProjectionShape, input: unknown): FillPlan {
  if (!input || typeof input !== 'object' || Array.isArray(input)) throw new Error('Optimistic fill requires a nested field object')
  const fields: [FieldShape, (value: unknown) => unknown][] = []
  const children: [string, ProjectionShape, FillPlan][] = []
  const canonical = new Set<string>()
  for (const [name, value] of Object.entries(input)) {
    const field = shape.fields.find(field => field.name === name && field.computed)
    if (Object.hasOwn(shape.children, name)) {
      if (value !== undefined) children.push([name, shape.children[name], compileFill(shape.children[name], value)])
    } else {
      if (!field) throw new Error(`Unknown computed fill field ${name}`)
      if (value === undefined) continue
      if (typeof value !== 'function') throw new Error(`Computed fill ${name} must be a function`)
      if (canonical.has(field.path[0])) throw new Error('Multiple fillers for the same computed slot; select one alias')
      canonical.add(field.path[0])
      fields.push([field, value as (value: unknown) => unknown])
    }
  }
  return { fields, children }
}
export function fillOptimisticShape<P>(owner: ShapeBinding['owner'], input: OptimisticShape<P>, fill: OptimisticFill<P>, reader: ProjectionReader): FilledOptimisticShape {
  const binding = shapes.get(input)
  if (!binding || binding.owner !== owner) throw new Error('Optimistic shape does not belong to this client')
  const validate = (): void => {
    if (owner.connectionContext !== binding.generation) throw new Error('Optimistic shape belongs to an expired connection')
    for (const projection of binding.projections.values()) projection.validate()
  }
  validate()
  if (!fill || typeof fill !== 'object' || Array.isArray(fill)) throw new Error('Optimistic fill requires a projection-shaped object')
  const computed = new Map<string, OptimisticComputedChange>()
  const slots = new Set<string>()
  for (const name of Object.keys(fill)) if (!binding.projections.has(name)) throw new Error(`Unknown optimistic projection ${name}`)
  const plans: [string, ProjectionBinding, FillPlan][] = []
  for (const [name, projection] of binding.projections) {
    const plan = Object.hasOwn(fill, name) ? (fill as Record<string, unknown>)[name] : undefined
    if (plan !== undefined) plans.push([name, projection, compileFill(projection.shape, plan)])
  }
  for (const [name, projection, plan] of plans) {
    const nodes = new Set<string>()
    const fields = new Map<string, FillPlan['fields']>()
    const index = (shape: ProjectionShape, plan: FillPlan): boolean => {
      let needed = !!plan.fields.length
      for (const [, child, childPlan] of plan.children) needed = index(child, childPlan) || needed
      if (needed) nodes.add(shape.nodeId)
      if (plan.fields.length) fields.set(shape.nodeId, plan.fields)
      return needed
    }
    if (!index(projection.shape, plan)) continue
    for (const row of projection.select(reader, binding.entities, nodes)) {
      const selected = fields.get(row.nodeId)
      if (!selected) continue
      const current = row.current()
      if (current === undefined) continue
      for (const [field, callback] of selected) {
        const slot = JSON.stringify([name, row.nodeId, row.ancestors, row.entityId, field.path[0]])
        if (slots.has(slot)) continue
        slots.add(slot)
        const result = callback(Object.freeze({ entityId: row.entityId, current, ancestors: Object.freeze([...row.ancestors]) }))
        if (result === undefined) continue
        if (result && (typeof result === 'object' || typeof result === 'function') && 'then' in result && typeof result.then === 'function') {
          void Promise.resolve(result).catch(() => {})
          throw new Error('Computed fillers must be synchronous')
        }
        const write = result === invalidateComputed ? result : prepareRecordData({ value: encodeValue(result, field.type, `${row.nodeId}.${field.name}`) }).value
        // A computation predicts a complete field value, unlike a partial
        // inserted record. Use the reader codec to validate nested required fields.
        if (write !== invalidateComputed) decodeValue(write, field.type, `${row.nodeId}.${field.name}`)
        const targetKey = JSON.stringify([name, row.nodeId, row.ancestors, row.entityId])
        const previous = computed.get(targetKey)
        const target = previous?.target ?? projection.target(row.nodeId, row.ancestors)
        if (write === invalidateComputed) computed.set(targetKey, { ...previous, target, entityId: row.entityId, invalidate: [...previous?.invalidate ?? [], field.path[0]] })
        else computed.set(targetKey, { ...previous, target, entityId: row.entityId, set: { ...previous?.set, [field.path[0]]: write } })
      }
    }
  }
  validate()
  return { computed: [...computed.values()], validate }
}
