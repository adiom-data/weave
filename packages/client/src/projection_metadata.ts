import type { ProjectionShape } from './bindings.js'
import type { OptimisticHandle, OptimisticMutation, OptimisticTarget, TemporaryEntity } from './optimistic.js'

export const entityBrand: unique symbol = Symbol('ProjectedEntity')
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
// Weak metadata travels with returned lists without exposing server occurrence
// IDs, retaining list data, or requiring a globally "current" projection.
const lists = new WeakMap<object, ListBinding>()
export function bindList(list: object, binding: ListBinding): void { lists.set(list, binding) }
export function listBinding(owner: OptimisticHost, list: object): ListBinding {
  const binding = lists.get(list)
  if (!binding || binding.owner !== owner) throw new Error('List does not belong to this client')
  return binding
}
