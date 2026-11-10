import type { OptimisticListChange, OptimisticSlotChange, OptimisticTarget, OptimisticPosition } from './optimistic.js'

/** Validate and own positions at both draft staging and low-level installation. */
export function prepareOptimisticPosition(position: OptimisticPosition): OptimisticPosition {
  if (position === 'start' || position === 'end') return position
  if (position && typeof position === 'object') {
    const keys = Object.keys(position)
    const key = keys[0]
    if (keys.length === 1 && (key === 'before' || key === 'after')) {
      const anchor = (position as unknown as Record<string, unknown>)[key]
      if (typeof anchor === 'string' && anchor) return Object.freeze(key === 'before' ? { before: anchor } : { after: anchor })
    }
  }
  throw new Error('Optimistic list position requires start, end, before, or after')
}

interface Descriptor { readonly key: string; readonly kind: 'entity' | 'list' }
type Change = OptimisticListChange | ({ readonly type: 'slot' } & OptimisticSlotChange)
export type PreparedSelectionChanges = ReadonlyMap<string, readonly Change[]>

export const selectionKey = (scope: string, node: string, ancestors: readonly string[]) => JSON.stringify(['optimisticSelection', scope, node, ancestors])

/** Client-owned logical targets and ordered membership overlays, never server IDs. */
export class OptimisticSelections {
  private targets = new WeakMap<OptimisticTarget, Descriptor>()
  private layers = new Map<string, Map<string, readonly Change[]>>()
  fork(): OptimisticSelections {
    const fork = new OptimisticSelections()
    fork.targets = this.targets
    return fork
  }
  target(scope: string, node: string, ancestors: readonly string[], kind: 'entity' | 'list'): OptimisticTarget {
    const target = Object.freeze({ kind }) as OptimisticTarget
    this.targets.set(target, { key: selectionKey(scope, node, ancestors), kind })
    return target
  }
  describe(target: OptimisticTarget): string {
    const descriptor = this.targets.get(target)
    if (!descriptor) throw new Error('Optimistic change requires a target from this client generation')
    return descriptor.key
  }
  prepare(lists: readonly OptimisticListChange[], slots: readonly OptimisticSlotChange[]): PreparedSelectionChanges {
    const result = new Map<string, Change[]>()
    const add = (target: OptimisticTarget, expected: 'entity' | 'list', change: Change) => {
      const descriptor = this.targets.get(target)
      if (!descriptor || descriptor.kind !== expected) throw new Error(`Optimistic ${expected} change requires a target from this client`)
      const previous = result.get(descriptor.key) ?? []
      previous.push(Object.freeze(change)); result.set(descriptor.key, previous)
    }
    for (const change of lists) {
      if (change.type === 'clear') { add(change.target, 'list', { type: 'clear', target: change.target }); continue }
      if (!change.entityId || typeof change.entityId !== 'string' || !['insert', 'remove', 'move'].includes(change.type)) throw new Error('Invalid optimistic list change')
      const position = change.type === 'remove' ? undefined : prepareOptimisticPosition(change.position)
      add(change.target, 'list', change.type === 'remove'
        ? { type: 'remove', target: change.target, entityId: change.entityId }
        : { type: change.type, target: change.target, entityId: change.entityId, position: position! })
    }
    for (const change of slots) {
      if (change.entityId !== null && (typeof change.entityId !== 'string' || !change.entityId)) throw new Error('Optimistic entity slot requires an entity ID or null')
      add(change.target, 'entity', { type: 'slot', target: change.target, entityId: change.entityId })
    }
    for (const values of result.values()) Object.freeze(values)
    return result
  }
  add(id: string, changes: ReadonlyMap<string, readonly Change[]>): void {
    for (const [key, values] of changes) {
      let layers = this.layers.get(key)
      if (!layers) this.layers.set(key, layers = new Map())
      layers.set(id, values)
    }
  }
  remove(id: string, keys: Iterable<string>): void {
    for (const key of keys) {
      const layers = this.layers.get(key)
      layers?.delete(id)
      if (!layers?.size) this.layers.delete(key)
    }
  }
  clear(): void { this.layers.clear(); this.targets = new WeakMap() }
  has(key: string): boolean { return this.layers.has(key) }
  get size(): number { return this.layers.size }
  read(key: string, base: readonly string[], backed: (id: string) => boolean): readonly string[] {
    const layers = this.layers.get(key)
    if (!layers) return base
    let result = [...base]
    for (const changes of layers.values()) for (const change of changes) {
      if (change.type === 'clear') { result = []; continue }
      if (change.type === 'slot') {
        if (change.entityId === null) result = []
        else if (backed(change.entityId)) result = [change.entityId]
        continue
      }
      const current = result.indexOf(change.entityId)
      if (change.type === 'remove') { if (current >= 0) result.splice(current, 1); continue }
      if (change.type === 'move' ? current < 0 : current < 0 && !backed(change.entityId)) continue
      const position = change.position
      let index: number
      if (position === 'start') index = 0
      else if (position === 'end') index = result.length
      else {
        const before = Object.hasOwn(position, 'before')
        const anchor = before ? (position as { before: string }).before : (position as { after: string }).after
        index = result.indexOf(anchor)
        if (index < 0 || anchor === change.entityId) continue
        if (!before) index++
      }
      if (current >= 0) { result.splice(current, 1); if (current < index) index-- }
      result.splice(index, 0, change.entityId)
    }
    return result.length === base.length && result.every((id, i) => id === base[i]) ? base : Object.freeze(result)
  }
}
