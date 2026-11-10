import type { FieldShape, ProjectionShape } from './bindings.js'
import type { OptimisticHandle, OptimisticMutation, OptimisticTarget, TemporaryEntity } from './optimistic.js'
import type { ProjectionReader } from './projection.js'

export const entityBrand: unique symbol = Symbol('ProjectedEntity')
export const previewOptimistic: unique symbol = Symbol('OptimisticPreview')
export const validateComputed: unique symbol = Symbol('ValidateComputedPrediction')
export interface OptimisticHost {
  temporaryEntity(): TemporaryEntity
  beginOptimistic(mutation: OptimisticMutation): OptimisticHandle
}
export interface ListBinding {
  readonly owner: OptimisticHost
  readonly shape: ProjectionShape
  readonly ancestors: readonly string[]
  target(nodeId: string, ancestors: readonly string[]): OptimisticTarget
}
// Entity identity/codecs are independent of the occurrence that produced a row.
// Only lists and computed occurrences have projection-scoped lifetimes.
export interface EntityBinding {
  readonly owner: OptimisticHost
  readonly fields: readonly FieldShape[]
  readonly location: string
}
// Weak metadata travels with returned lists without exposing server occurrence
// IDs, retaining list data, or requiring a globally "current" projection.
const lists = new WeakMap<object, ListBinding>()
const entities = new WeakMap<object, EntityBinding>()
export function bindEntity(entity: object, binding: EntityBinding): void { entities.set(entity, binding) }
export function entityBinding(owner: OptimisticHost, entity: object): EntityBinding {
  const binding = entities.get(entity)
  if (!binding || binding.owner !== owner) throw new Error('Entity does not belong to this client')
  return binding
}
export interface ProjectionBinding {
  readonly owner: OptimisticHost & { readonly connectionContext?: string }
  readonly shape: ProjectionShape
  validate(): void
  select(reader: ProjectionReader, entities: ReadonlySet<string>, nodes: ReadonlySet<string>): Iterable<ProjectionCandidate>
  target(node: string, ancestors: readonly string[]): OptimisticTarget
}
export interface ProjectionCandidate {
  readonly nodeId: string
  readonly entityId: string
  readonly ancestors: readonly string[]
  current(): unknown
}
const projections = new WeakMap<object, ProjectionBinding>()
export function bindProjection(handle: object, binding: ProjectionBinding): void { projections.set(handle, binding) }
export function projectionBinding(handle: object): ProjectionBinding {
  const binding = projections.get(handle)
  if (!binding) throw new Error('Expected a concrete typed projection handle')
  return binding
}
export function bindList(list: object, binding: ListBinding): void { lists.set(list, binding) }
export function listBinding(owner: OptimisticHost, list: object): ListBinding {
  const binding = lists.get(list)
  if (!binding || binding.owner !== owner) throw new Error('List does not belong to this client')
  return binding
}
