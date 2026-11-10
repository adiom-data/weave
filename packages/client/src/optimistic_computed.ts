import { prepareRecordData } from './protocol.js'
import type { RecordData, Value } from './protocol.js'
import type { OptimisticComputedChange, OptimisticTarget } from './optimistic.js'

export type ComputedPredictionState = 'predicted' | 'stale'
export type ComputedPredictionStates = Readonly<Record<string, ComputedPredictionState>>
export interface ComputedPrediction { readonly values: RecordData; readonly states: ComputedPredictionStates }
type Write = Readonly<{ value: Value }> | Readonly<{ stale: true }>
export type PreparedComputedChanges = ReadonlyMap<string, ReadonlyMap<string, Write>>
export const optimisticComputedKey = (selection: string, entity: string): string => JSON.stringify(['optimisticComputed', selection, entity])
const noStates: ComputedPredictionStates = Object.freeze(Object.create(null))
export const authoritativeComputed = (values: RecordData): ComputedPrediction => ({ values, states: noStates })

// Compose occurrence values and metadata together, whether the base is the
// server model or an earlier overlay. No expression or dependency inference.
export class OptimisticComputations {
  private layers = new Map<string, Map<string, ReadonlyMap<string, Write>>>()

  prepare(changes: readonly OptimisticComputedChange[], describe: (target: OptimisticTarget) => string): PreparedComputedChanges {
    const result = new Map<string, Map<string, Write>>()
    for (const change of changes) {
      if (typeof change.entityId !== 'string' || !change.entityId) throw new Error('Computed predictions require an entity ID')
      const key = optimisticComputedKey(describe(change.target), change.entityId)
      const values = prepareRecordData(change.set ?? {})
      const invalid = [...(change.invalidate ?? [])]
      if (invalid.some(name => typeof name !== 'string' || !name || Object.hasOwn(values, name)) || new Set(invalid).size !== invalid.length) throw new Error('Computed predictions and invalidations must be disjoint named fields')
      if (!Object.keys(values).length && !invalid.length) throw new Error('Computed prediction must change at least one field')
      let fields = result.get(key)
      if (!fields) result.set(key, fields = new Map())
      const add = (name: string, write: Write): void => {
        const previous = fields!.get(name)
        if (previous && !sameWrite(previous, write)) throw new Error('Conflicting computed predictions for the same target')
        fields!.set(name, write)
      }
      for (const [name, value] of Object.entries(values)) add(name, { value })
      for (const name of invalid) add(name, { stale: true })
    }
    return result
  }
  add(id: string, changes: PreparedComputedChanges): void {
    for (const [key, writes] of changes) {
      let layers = this.layers.get(key)
      if (!layers) this.layers.set(key, layers = new Map())
      layers.set(id, writes)
    }
  }
  remove(id: string, keys: Iterable<string>): void {
    for (const key of keys) {
      const layers = this.layers.get(key)
      layers?.delete(id)
      if (!layers?.size) this.layers.delete(key)
    }
  }
  read(key: string, base: ComputedPrediction): ComputedPrediction {
    const layers = this.layers.get(key)
    if (!layers) return base
    const values = Object.assign(Object.create(null), base.values) as Record<string, Value>
    const states = Object.assign(Object.create(null), base.states) as Record<string, ComputedPredictionState>
    for (const fields of layers.values()) for (const [name, write] of fields) {
      if ('value' in write) { values[name] = write.value; states[name] = 'predicted' }
      else states[name] = 'stale'
    }
    return { values: Object.freeze(values), states: Object.freeze(states) }
  }
  clear(): void { this.layers.clear() }
}

// Canonical values contain only immutable data (including exact numeric text).
export function equalPrediction(a: unknown, b: unknown): boolean {
  if (Object.is(a, b)) return true
  if (!a || !b || typeof a !== 'object' || typeof b !== 'object' || Array.isArray(a) !== Array.isArray(b)) return false
  const left = a as Record<string, unknown>, right = b as Record<string, unknown>
  const keys = Object.keys(left)
  return keys.length === Object.keys(right).length && keys.every(key => Object.hasOwn(right, key) && equalPrediction(left[key], right[key]))
}
function sameWrite(a: Write, b: Write): boolean { return equalPrediction(a, b) }
