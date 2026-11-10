import type { ReactiveClient, TransactionNotification } from './client.js'
import type { OptimisticHandle } from './optimistic.js'
import type { OptimisticDraft } from './optimistic_draft.js'
import type { Selection } from './store.js'
import { invokeSafely } from './callbacks.js'
import { decodeMutationReceipts } from './mutation_correlation.js'

export class MutationOutcomeUnknownError extends Error {}
/** Synchronization ended without an observed receipt; this does not establish rollback. */
export class MutationUnconfirmedError extends MutationOutcomeUnknownError {}
export interface MutationContext { readonly mutationId: string; readonly connectionContext: string }
export interface MutationReceipt extends MutationContext { readonly checkpoint: string }
export type RpcOutcome<T> =
  | Readonly<{ kind: 'succeeded'; response: T }>
  | Readonly<{ kind: 'rejected' | 'unknown' | 'not-sent'; error: unknown }>
export type OptimisticRetirement =
  | Readonly<{ reason: 'applied'; receipt: MutationReceipt }>
  | Readonly<{ reason: 'rejected' | 'unconfirmed' | 'rolled-back' | 'disconnected' | 'disposed' | 'not-sent'; error?: unknown }>
export interface MutationState<T> {
  readonly rpc: Readonly<{ kind: 'pending' }> | RpcOutcome<T>
  readonly reactive: Readonly<{ kind: 'waiting' }> | Readonly<{ kind: 'applied'; receipt: MutationReceipt }> | Readonly<{ kind: 'unconfirmed'; error: unknown }>
  readonly optimistic: Readonly<{ kind: 'active' }> | Readonly<{ kind: 'retired'; event: OptimisticRetirement }>
}
export type OptimisticWriter = Pick<OptimisticDraft, 'entity' | 'insert' | 'remove' | 'patch' | 'patchCanonical' | 'delete' | 'fill'>
export interface MutationRequest<T> {
  send(context: MutationContext): Promise<T>
  optimistic?(draft: OptimisticWriter): void
  onResponseSettled?(outcome: RpcOutcome<T>): void
  onApplied?(receipt: MutationReceipt): void
  onOptimisticRetired?(event: OptimisticRetirement): void
}
export interface MutationOperation<T> {
  readonly mutationId: string
  readonly response: Promise<T>
  readonly applied: Promise<MutationReceipt>
  readonly state: Selection<MutationState<T>>
  rollbackOptimistic(): void
}
export interface MutationHarnessOptions {
  /** Optional pure decoder; defaults to the standard committed mutation receipt. */
  decodeReceipts?(transaction: TransactionNotification): readonly string[]
  /** Only return rejected when the endpoint contract establishes no write committed. */
  classifyError?(error: unknown): 'rejected' | 'unknown'
  readonly synchronizationTimeoutMs?: number
  readonly maxPending?: number
  readonly onListenerError?: (error: unknown) => void
}

function deferred<T>() {
  let resolve!: (value: T) => void, reject!: (error: unknown) => void
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no })
  // Both signals are independently optional to observe.
  void promise.catch(() => {})
  return { promise, resolve, reject }
}

/** A stable external-store cell; lifecycle notifications never run inside publication. */
class StateCell<T> implements Selection<T> {
  private listeners = new Set<{ readonly listener: () => void; notified: T }>()
  private queued = false
  constructor(private value: T, private report: (error: unknown) => void) {}
  getSnapshot = (): T => this.value
  subscribe = (listener: () => void): (() => void) => {
    const subscription = { listener, notified: this.value }
    this.listeners.add(subscription)
    return () => { this.listeners.delete(subscription) }
  }
  set(value: T): void {
    this.value = value
    if (this.queued) return
    this.queued = true
    queueMicrotask(() => {
      this.queued = false
      for (const subscription of [...this.listeners]) {
        if (!this.listeners.has(subscription) || subscription.notified === this.value) continue
        subscription.notified = this.value
        invokeSafely(subscription.listener, this.report)
      }
    })
  }
}

interface PendingMutation {
  readonly context: MutationContext
  applied(receipt: MutationReceipt): void
  stop(event: Exclude<OptimisticRetirement, { reason: 'applied' }>): void
}

/** Optional mutation coordination. No RPC implementation, retries, auth, or authoritative writes. */
export class MutationHarness {
  private pending = new Map<string, PendingMutation>()
  private disposed = false
  private stopReceipts: () => void
  private stopConnection: () => void
  private options: MutationHarnessOptions
  private timeoutMs: number
  private maxPending: number
  constructor(private reactive: ReactiveClient, options: MutationHarnessOptions = {}) {
    this.options = Object.freeze({ ...options })
    this.timeoutMs = options.synchronizationTimeoutMs ?? 15000
    this.maxPending = options.maxPending ?? 256
    if (![this.timeoutMs, this.maxPending].every(value => Number.isSafeInteger(value) && value > 0)) throw new Error('Mutation limits must be positive integers')
    this.stopReceipts = reactive.registerOptimisticReceiptDecoder(transaction => (this.options.decodeReceipts ?? decodeMutationReceipts)(transaction), (transaction, ids) => {
      for (const id of ids) {
        const entry = this.pending.get(id)
        if (entry && transaction.connectionContext === entry.context.connectionContext) entry.applied(Object.freeze({ ...entry.context, checkpoint: transaction.checkpoint }))
      }
    })
    const connection = reactive.store.observe(store => store.getConnection())
    this.stopConnection = connection.subscribe(() => {
      if (connection.getSnapshot().status !== 'connected') this.stopPending({ reason: 'disconnected', error: new MutationUnconfirmedError('Disconnected before mutation synchronization; outcome may be unknown') })
    })
  }
  run<T>(input: MutationRequest<T>): MutationOperation<T> {
    if (this.disposed) throw new Error('Mutation harness is disposed')
    const connectionContext = this.reactive.connectionContext
    if (!connectionContext || this.reactive.store.getConnection().status !== 'connected') throw new Error('Reactive connection handshake is not ready; no mutation was sent')
    if (this.pending.size >= this.maxPending) throw new Error('Too many pending mutations; no mutation was sent')
    const request = Object.freeze({ ...input })
    const context = Object.freeze({ mutationId: crypto.randomUUID(), connectionContext })
    const response = deferred<T>(), confirmation = deferred<MutationReceipt>()
    const cell = new StateCell<MutationState<T>>(Object.freeze({ rpc: Object.freeze({ kind: 'pending' }), reactive: Object.freeze({ kind: 'waiting' }), optimistic: Object.freeze({ kind: 'active' }) }), error => this.report(error))
    let layer: OptimisticHandle | undefined, stopped = false
    const update = (change: Partial<MutationState<T>>) => cell.set(Object.freeze({ ...cell.getSnapshot(), ...change }))
    const release = () => {
      const value = cell.getSnapshot()
      if (value.rpc.kind !== 'pending' && value.reactive.kind !== 'waiting') this.pending.delete(context.mutationId)
    }
    const retire = (event: OptimisticRetirement) => {
      if (cell.getSnapshot().optimistic.kind === 'retired') return
      const owned = Object.freeze(event)
      update({ optimistic: Object.freeze({ kind: 'retired', event: owned }) })
      const previous = layer
      layer = undefined
      previous?.rollback()
      this.callback(request.onOptimisticRetired, owned)
    }
    const timer = setTimeout(() => entry.stop({ reason: 'unconfirmed', error: new MutationUnconfirmedError('Mutation receipt timed out; reactive application is unconfirmed') }), this.timeoutMs)
    const entry: PendingMutation = {
      context,
      applied: receipt => {
        if (cell.getSnapshot().reactive.kind !== 'waiting') return
        clearTimeout(timer)
        update({ reactive: Object.freeze({ kind: 'applied', receipt }) })
        retire({ reason: 'applied', receipt })
        confirmation.resolve(receipt)
        this.callback(request.onApplied, receipt)
        release()
      },
      stop: event => {
        if (cell.getSnapshot().reactive.kind !== 'waiting') return
        stopped = true
        clearTimeout(timer)
        const error = event.error ?? new MutationUnconfirmedError('Mutation synchronization ended without a receipt')
        update({ reactive: Object.freeze({ kind: 'unconfirmed', error }) })
        retire(event)
        confirmation.reject(error)
        release()
      },
    }
    this.pending.set(context.mutationId, entry)
    const settled = (outcome: RpcOutcome<T>) => {
      update({ rpc: Object.freeze(outcome) })
      if (outcome.kind === 'rejected' || outcome.kind === 'not-sent') entry.stop({ reason: outcome.kind, error: outcome.error })
      this.callback(request.onResponseSettled, outcome)
      release()
      if (outcome.kind === 'succeeded') response.resolve(outcome.response)
      else response.reject(outcome.error)
    }
    let setupError: unknown, setupFailed = false
    try {
      if (request.optimistic) {
        const draft = this.reactive.optimistic({ mutationId: context.mutationId })
        let accepting = true
        const check = () => { if (!accepting) throw new Error('Optimistic writer is only valid during its callback') }
        // Transparent forwarding preserves generic/overloaded signatures.
        const scoped = <F extends (...args: never[]) => unknown>(method: F): F => ((...args: never[]) => { check(); return method.apply(draft, args) }) as F
        const writer: OptimisticWriter = Object.freeze({
          entity: scoped(draft.entity), insert: scoped(draft.insert),
          remove: scoped(draft.remove), patch: scoped(draft.patch),
          patchCanonical: scoped(draft.patchCanonical), fill: scoped(draft.fill), delete: scoped(draft.delete),
        })
        try {
          const result: unknown = request.optimistic(writer)
          if (result && (typeof result === 'object' || typeof result === 'function') && 'then' in result) {
            void Promise.resolve(result).catch(() => {})
            throw new Error('Optimistic callback must be synchronous')
          }
        } finally { accepting = false }
        if (this.disposed || this.reactive.connectionContext !== connectionContext) throw new Error('Connection changed or harness disposed during optimistic setup; no mutation was sent')
        if (draft.hasChanges) layer = draft.commit()
      }
    } catch (error) { setupError = error; setupFailed = true }
    // The caller receives the operation before network IO; registration and
    // prediction installation always precede even a synchronous send callback.
    void Promise.resolve().then(async () => {
      if (setupFailed) { settled({ kind: 'not-sent', error: setupError }); return }
      if (stopped || this.disposed || this.reactive.connectionContext !== connectionContext) {
        settled({ kind: 'not-sent', error: new Error('Connection changed or synchronization ended before mutation send; no mutation was sent') }); return
      }
      try { settled({ kind: 'succeeded', response: await request.send(context) }) }
      catch (cause) {
        let kind: 'rejected' | 'unknown' = 'unknown'
        try {
          kind = this.options.classifyError?.(cause) ?? 'unknown'
          if (kind !== 'rejected' && kind !== 'unknown') throw new Error('Invalid mutation error classification')
        } catch (error) { kind = 'unknown'; this.report(error) }
        const error = kind === 'unknown' && !(cause instanceof MutationOutcomeUnknownError)
          ? new MutationOutcomeUnknownError('Mutation RPC outcome unknown; do not automatically retry', { cause }) : cause
        settled({ kind, error })
      }
    })
    return Object.freeze({ mutationId: context.mutationId, response: response.promise, applied: confirmation.promise, state: Object.freeze({ getSnapshot: cell.getSnapshot, subscribe: cell.subscribe }), rollbackOptimistic: () => retire({ reason: 'rolled-back' }) })
  }
  dispose(): void {
    if (this.disposed) return
    this.disposed = true
    this.stopReceipts(); this.stopConnection()
    this.stopPending({ reason: 'disposed', error: new MutationUnconfirmedError('Mutation harness disposed before synchronization') })
  }
  private stopPending(event: Exclude<OptimisticRetirement, { reason: 'applied' }>): void {
    // A disconnect/disposal during publication must not beat the receipt for
    // that already-applied transaction. Never include operations opened later.
    const entries = [...this.pending.values()]
    queueMicrotask(() => { for (const entry of entries) entry.stop(event) })
  }
  private callback<T>(callback: ((value: T) => void) | undefined, value: T): void {
    if (callback) queueMicrotask(() => invokeSafely(() => callback(value), error => this.report(error)))
  }
  private report(error: unknown): void {
    const unhandled = (reported: unknown) => queueMicrotask(() => { throw reported })
    const report = this.options.onListenerError
    if (report) invokeSafely(() => report(error), unhandled)
    else unhandled(error)
  }
}
