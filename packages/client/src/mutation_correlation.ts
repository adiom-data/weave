import type { TransactionNotification } from './client.js'
import type { MutationContext } from './mutations.js'

export const connectionContextHeader = 'X-Weave-Connection-Context'
export const mutationIdHeader = 'X-Weave-Mutation-Id'
export const mutationReceiptKind = 'weave.mutation.committed'

export interface MutationHeaderNames {
  readonly connectionContext?: string
  readonly mutationId?: string
}

/** HTTP-compatible names also work with gRPC metadata (lowercase if required). */
export function mutationHeaders(context: MutationContext, names: MutationHeaderNames = {}): Record<string, string> {
  const connection = names.connectionContext ?? connectionContextHeader
  const mutation = names.mutationId ?? mutationIdHeader
  if (!connection || !mutation || connection.toLowerCase() === mutation.toLowerCase()) throw new Error('Correlation header names must be nonempty and distinct')
  return { [connection]: context.connectionContext, [mutation]: context.mutationId }
}

/** Decodes only committed receipts. Invalid metadata never confirms a mutation. */
export function decodeMutationReceipts(transaction: TransactionNotification): readonly string[] {
  if (!transaction.atomic) return []
  const ids: string[] = []
  for (const message of transaction.messages) {
    if (message.kind !== mutationReceiptKind) continue
    try {
      // Canonical UUIDs are ASCII, a subset of UTF-8. Validate the entire byte
      // string rather than replacing malformed text or accepting a wrapper.
      const mutationId = atob(message.payload)
      if (mutationId.length === 36 && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(mutationId)) ids.push(mutationId)
    } catch { /* Malformed metadata is not a receipt. */ }
  }
  return ids
}
