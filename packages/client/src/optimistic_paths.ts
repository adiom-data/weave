import { prepareRecordData } from './protocol.js'
import type { RecordData, Value } from './protocol.js'
import { equalPrediction } from './optimistic_computed.js'

/** A path is object-only. unset removes that key, unlike a predicted null. */
export interface OptimisticPathChange { readonly path: readonly string[]; readonly value?: Value; readonly unset?: true }

export function preparePaths(changes: readonly OptimisticPathChange[]): readonly OptimisticPathChange[] {
  const paths: OptimisticPathChange[] = []
  for (const change of changes) {
    if (!Array.isArray(change.path) || !change.path.length || change.path.some(part => typeof part !== 'string')) throw new Error('Optimistic field paths require object-key segments')
    if ((change.value === undefined) === (change.unset !== true)) throw new Error('Optimistic path requires exactly one value or unset')
    const value = change.value === undefined ? undefined : prepareRecordData({ field: change.value }).field
    const owned: OptimisticPathChange = Object.freeze({ path: Object.freeze([...change.path]), ...(value === undefined ? { unset: true as const } : { value }) })
    const duplicate = paths.find(previous => equalPrediction(previous.path, owned.path))
    if (duplicate) {
      if (!equalPrediction(duplicate, owned)) throw new Error('Conflicting optimistic predictions for the same field')
      continue
    }
    if (paths.some(previous => prefix(previous.path, owned.path) || prefix(owned.path, previous.path))) throw new Error('Overlapping optimistic field paths; supply only one alias')
    paths.push(owned)
  }
  return Object.freeze(paths)
}

const prefix = (left: readonly string[], right: readonly string[]): boolean => left.length <= right.length && left.every((part, index) => part === right[index])

export function applyPaths(base: RecordData, changes: readonly OptimisticPathChange[]): RecordData {
  const result = Object.assign(Object.create(null), base) as Record<string, Value>
  const apply = (fields: Record<string, Value>, path: readonly string[], index: number, change: OptimisticPathChange): void => {
    const name = path[index]
    if (index === path.length - 1) {
      if (change.unset) delete fields[name]
      else fields[name] = change.value!
      return
    }
    const value = fields[name]
    // Do not invent an object just to unset an absent nested key.
    if (change.unset && (!value || !('objectValue' in value))) return
    const children = Object.assign(Object.create(null), value && 'objectValue' in value ? value.objectValue?.fields : undefined) as Record<string, Value>
    apply(children, path, index + 1, change)
    fields[name] = Object.freeze({ objectValue: Object.freeze({ fields: Object.freeze(children) }) })
  }
  for (const change of changes) apply(result, change.path, 0, change)
  return Object.freeze(result)
}
