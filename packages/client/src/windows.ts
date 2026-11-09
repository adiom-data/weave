import type { NodeView } from './projection.js'

type WindowSelector = Readonly<{ occurrenceId: string }> | Readonly<{ nodeId: string; ancestors: readonly string[] }>
type ListView = Extract<NodeView, { type: 'list' }>

// Both raw handles and typed controls resolve only live coverage. Retained
// error displays may contain old query/occurrence IDs, including under a failed
// ancestor, but must never advertise those as current pagination controls.
export function findLiveWindow(root: NodeView | undefined, selector: WindowSelector): ListView | undefined {
  const path = 'ancestors' in selector ? selector.ancestors : undefined
  const visit = (node: NodeView | undefined, depth: number): ListView | undefined => {
    if (!node || node.error || node.stale) return undefined
    const matches = 'occurrenceId' in selector ? node.occurrenceId === selector.occurrenceId : node.nodeId === selector.nodeId && depth === selector.ancestors.length
    if (node.type === 'list' && node.query && node.occurrenceId && matches) return node
    for (const row of node.type === 'entity' ? [node.row] : node.rows) {
      if (row.error || row.stale || (path && row.entityId !== path[depth])) continue
      for (const child of Object.values(row.children)) {
        const found = visit(child, depth + 1)
        if (found) return found
      }
    }
    return undefined
  }
  return visit(root, 0)
}
