import type { FieldShape, ValueShape } from './bindings.js'
import { encodeValue } from './value_input.js'
import { isMergeValue, mergeMembers, unsetValue } from './optimistic_values.js'
import { preparePaths } from './optimistic_paths.js'
import type { OptimisticPathChange } from './optimistic_paths.js'

const structured: ValueShape = Object.freeze({ kind: 'structured' })

/** Compile selected-field edits to the engine's existing object-key paths.
 * Only explicit merge markers recurse. Native typed values and canonical
 * opaque values otherwise retain their ordinary replacement semantics. */
export function prepareProjectedPatch(set: unknown, fields: readonly FieldShape[], unset: readonly string[], location: string): readonly OptimisticPathChange[] {
  if (!set || typeof set !== 'object' || Array.isArray(set) || isMergeValue(set)) throw new Error('Optimistic patch requires stored fields')
  const entries = Object.entries(set)
  const paths: OptimisticPathChange[] = []
  const active = new Set<object>()
  // A shape can be reused at several branches. Index it once for this patch,
  // without retaining projection metadata beyond the compilation call.
  const indexes = new Map<readonly FieldShape[], ReadonlyMap<string, FieldShape>>()
  const assigned = new Set<string>()
  let merged = false
  const find = (fields: readonly FieldShape[], name: string, at: string): FieldShape => {
    let index = indexes.get(fields)
    if (!index) {
      const stored = new Map<string, FieldShape>()
      for (const field of fields) {
        if (field.computed) continue
        const key = field.name
        if (!stored.has(key)) stored.set(key, field)
      }
      indexes.set(fields, index = stored)
    }
    const field = index.get(name)
    if (!field) throw new Error(`${at}: Unknown stored field ${name}`)
    return field
  }
  const visit = (value: unknown, type: ValueShape, path: readonly string[], at: string): void => {
    if (value === undefined) return
    if (value === unsetValue) { paths.push({ path, unset: true }); return }
    const members = mergeMembers(value)
    if (members !== undefined) {
      if (type.kind !== 'object' && type.kind !== 'structured') throw new Error(`${at}: mergeValue requires an embedded or opaque object field`)
      if (active.has(value as object)) throw new Error(`${at}: cyclic mergeValue input`)
      active.add(value as object)
      merged = true
      try {
        for (const [name, child] of Object.entries(members)) {
          if (type.kind === 'object') {
            const field = find(type.fields, name, at)
            visit(child, field.type, [...path, ...field.path], `${at}.${name}`)
          } else visit(child, structured, [...path, name], `${at}.${name}`)
        }
      } finally { active.delete(value as object) }
      return
    }
    paths.push({ path, value: encodeValue(value, type, at) })
  }
  for (const [name, value] of entries) {
    const field = find(fields, name, location)
    visit(value, field.type, field.path, `${location}.${name}`)
    if (value !== undefined) assigned.add(name)
  }
  for (const name of unset) {
    const field = find(fields, name, location)
    if (assigned.has(name)) throw new Error('Optimistic set/unset must be disjoint')
    paths.push({ path: field.path, unset: true })
  }
  const prepared = preparePaths(paths)
  if (!prepared.length && !merged) throw new Error('Optimistic entity patch must change at least one field')
  return prepared
}
