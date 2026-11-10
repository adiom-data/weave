import { prepareRecordData } from './protocol.js'
import type { RecordData, Value } from './protocol.js'
import { equalPrediction } from './optimistic_computed.js'

/** A path is object-only. unset removes that key, unlike a predicted null. */
export interface OptimisticPathChange { readonly path: readonly string[]; readonly value?: Value; readonly unset?: true }

interface PathNode {
  readonly children: Map<string, PathNode>
  change?: OptimisticPathChange
}
const node = (): PathNode => ({ children: new Map() })

/** Reject ancestor/descendant conflicts while walking only the inserted path.
 * Equal leaf writes coalesce, including different aliases of the same field. */
function insert(root: PathNode, change: OptimisticPathChange): boolean {
  let current = root
  for (const segment of change.path) {
    if (current.change) throw new Error('Overlapping optimistic field paths; supply only one alias')
    let child = current.children.get(segment)
    if (!child) current.children.set(segment, child = node())
    current = child
  }
  if (current.change) {
    if (!equalPrediction(current.change, change)) throw new Error('Conflicting optimistic predictions for the same field')
    return false
  }
  if (current.children.size) throw new Error('Overlapping optimistic field paths; supply only one alias')
  current.change = change
  return true
}

export function preparePaths(changes: readonly OptimisticPathChange[]): readonly OptimisticPathChange[] {
  const paths: OptimisticPathChange[] = []
  const root = node()
  for (const change of changes) {
    if (!Array.isArray(change.path) || !change.path.length || change.path.some(part => typeof part !== 'string')) throw new Error('Optimistic field paths require object-key segments')
    if ((change.value === undefined) === (change.unset !== true)) throw new Error('Optimistic path requires exactly one value or unset')
    const value = change.value === undefined ? undefined : prepareRecordData({ field: change.value }).field
    const owned: OptimisticPathChange = Object.freeze({ path: Object.freeze([...change.path]), ...(value === undefined ? { unset: true as const } : { value }) })
    if (insert(root, owned)) paths.push(owned)
  }
  return Object.freeze(paths)
}

export function applyPaths(base: RecordData, changes: readonly OptimisticPathChange[]): RecordData {
  const root = node()
  for (const change of changes) insert(root, change)
  // Group sibling edits before applying them: each affected object is copied
  // once, and untouched subtrees retain their immutable references. Rebuilding
  // the index is linear in path length and needs no long-lived side cache.
  const apply = (fields: RecordData | undefined, current: PathNode): RecordData | undefined => {
    let result: Record<string, Value> | undefined
    const writable = (): Record<string, Value> => result ??= Object.assign(Object.create(null), fields)
    for (const [name, child] of current.children) {
      const value = fields && Object.hasOwn(fields, name) ? fields[name] : undefined
      if (child.change) {
        if (child.change.unset) {
          if (value !== undefined) delete writable()[name]
        } else writable()[name] = child.change.value!
      } else {
        // Nested values have already passed canonical record validation.
        const previous = (value && 'objectValue' in value ? value.objectValue?.fields : undefined) as RecordData | undefined
        const children = apply(previous, child)
        // Deletion alone must not invent a missing object or replace a scalar.
        if (children !== previous) writable()[name] = Object.freeze({ objectValue: Object.freeze({ fields: children! }) })
      }
    }
    return result ? Object.freeze(result) : fields
  }
  return apply(base, root)!
}
