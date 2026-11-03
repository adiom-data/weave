import { prepareProjectionRequest, ProtocolError } from './protocol.js'
import { decodeSocketMessage, encodeSocketMessage } from './socket_protocol.js'
import type { AuthStatus, SocketRequest } from './socket_protocol.js'
import type { BoundaryUpdate, DecodeLimits, ProjectionRequest, ServerMessage, TransactionMessage } from './protocol.js'
import { ModelStore } from './store.js'
import { readProjection } from './projection.js'
import type { ProjectionSnapshot } from './projection.js'
import type { ModelStoreOptions, Selection } from './store.js'
import { ProjectionPublication } from './publication.js'
import type { ProjectionDataNode } from './publication.js'
import type { OptimisticMutation, OptimisticHandle, OptimisticTarget, TemporaryEntity } from './optimistic.js'
import type { ConnectionSnapshot } from './store.js'
import type { ClientDiagnostics, ConnectionFailure, DiagnosticEvent, FailureAction, FailureKind, FailureSummary } from './diagnostics.js'
import { OptimisticDraft } from './optimistic_draft.js'
import type { OptimisticDraftOptions } from './optimistic_draft.js'
import { invokeSafely } from './callbacks.js'

export interface Socket {
  readonly readyState: number
  binaryType: 'arraybuffer' | 'blob'
  onopen: ((event: Event) => void) | null
  onmessage: ((event: MessageEvent) => void) | null
  onerror: ((event: Event) => void) | null
  onclose: ((event: CloseEvent) => void) | null
  send(data: Uint8Array<ArrayBuffer>): void
  close(): void
}
export interface ClientOptions extends DecodeLimits, ModelStoreOptions {
  /** Synchronous application policy. Defaults to retry; never interprets auth. */
  readonly failurePolicy?: (failure: ConnectionFailure) => FailureAction
  /** Delivered asynchronously after state transitions; contains no model data. */
  readonly onDiagnostic?: (event: DiagnosticEvent) => void
  readonly onConnectionOpened?: (context: string) => void
  /** Connection-level auth notices, not replies to individual token updates.
   * Successful verification is silent. Token acquisition/scheduling is external. */
  readonly onAuthStatus?: (status: AuthStatus) => void
  /** Runs after the complete transaction's model changes have been published. */
  readonly onTransaction?: (transaction: TransactionNotification) => void
  readonly url: string | (() => string)
  readonly createSocket?: (url: string) => Socket
  readonly reconnectDelayMs?: number
  readonly maxReconnectDelayMs?: number
  readonly requestTimeoutMs?: number
  readonly maxMessageBytes?: number
  readonly connectionTimeoutMs?: number
  /** Set to zero to disable application-level heartbeat checks. */
  readonly heartbeatIntervalMs?: number
  readonly heartbeatTimeoutMs?: number
}
export interface TransactionNotification {
  /** The source connection, even if a publication listener has since disconnected. */
  readonly connectionContext?: string
  readonly checkpoint: string
  readonly atomic: boolean
  readonly connectionSequence: bigint
  readonly messages: readonly TransactionMessage[]
}
/** Pure application decoder. Returned IDs retire layers in the same atomic publication. */
export type OptimisticReceiptDecoder = (transaction: TransactionNotification) => readonly string[]
export type OptimisticReceiptAppliedListener = (transaction: TransactionNotification, ids: readonly string[]) => void
interface ReceiptRegistration { readonly decode: OptimisticReceiptDecoder; readonly onApplied?: OptimisticReceiptAppliedListener }
interface PendingRequest { resolve: () => void; reject: (error: Error) => void; timer: ReturnType<typeof setTimeout>; subscriptionId: string }
interface ConnectionGeneration {
  context?: string
  socket?: Socket
  opened?: boolean
  subscriptions: Map<string, { requestId: string; terminal: boolean }>
  closing: Map<string, string>
  connectionTimer?: ReturnType<typeof setTimeout>
  heartbeatTimer?: ReturnType<typeof setTimeout>
  heartbeatId?: string
}

export interface ProjectionHandle {
  readonly id: string
  readonly request: ProjectionRequest
  readonly view: Selection<ProjectionSnapshot>
  readonly data: Selection<ProjectionDataNode | undefined>
  /** Logical node and ancestor entity path; works before its server occurrence loads. */
  target(nodeId?: string, ancestors?: readonly string[]): OptimisticTarget
  updateWindow(instanceId: string, updates: readonly BoundaryUpdate[]): Promise<void>
  dispose(): void
}
interface ProjectionRegistration {
  readonly handle: ProjectionHandle
  readonly publication: ProjectionPublication
  readonly source: Selection<ProjectionSnapshot>
  readonly stop: () => void
}

// Owns the connection, not React. A socket is one model generation; stale
// callbacks and requests never cross reconnects. Mutation APIs stay external.
export class ReactiveClient {
  readonly store: ModelStore
  private generation?: ConnectionGeneration
  private running = false
  private retry?: ReturnType<typeof setTimeout>
  private attempts = 0
  private nextSubscription = 0n
  private nextRequest = 0n
  private nextHeartbeat = 0n
  private handles = new Map<string, ProjectionRegistration>()
  private requests = new Map<string, PendingRequest>()
  private bySource = new Map<Selection<unknown>, ProjectionRegistration>()
  private clearing = false
  private lifecycle = 0
  private counters = { connectionAttempts: 0, retriesScheduled: 0, failures: 0, messagesReceived: 0, bytesReceived: 0 }
  private lastFailure?: FailureSummary
  private transactionListeners = new Set<(transaction: TransactionNotification) => void>()
  private optimisticReceiptDecoders = new Set<ReceiptRegistration>()
  /** Undefined before the server handshake and immediately after disconnect. */
  get connectionContext(): string | undefined { return this.generation?.context }
  /** Fire-and-forget WebSocket credential update; the server coalesces work.
   * No request ID, acknowledgment, or automatic retry. The application obtains
   * and periodically sends tokens; changed claims require reconnecting.
   * No token is retained or replayed across connection generations. */
  updateAuthToken(token: string): void {
    const generation = this.generation
    if (!generation?.opened || generation.socket?.readyState !== 1) throw new Error('Connection is not open')
    if (!token || typeof token !== 'string') throw new Error('Authentication token is required')
    try { this.send(generation, { authToken: { token } }) }
    catch (error) { this.lost(generation, asError(error), 'send'); throw asError(error) }
  }
  /** Observes routed metadata after atomic model publication; does not create a subscription. */
  subscribeTransactions(listener: (transaction: TransactionNotification) => void): () => void {
    this.transactionListeners.add(listener)
    return () => { this.transactionListeners.delete(listener) }
  }
  /** Applied callbacks follow successful atomic publication, including publication-time disconnects.
   * Cleanup only affects future messages; a decoded in-flight registration still completes. */
  registerOptimisticReceiptDecoder(decode: OptimisticReceiptDecoder, onApplied?: OptimisticReceiptAppliedListener): () => void {
    const registration = Object.freeze({ decode, onApplied })
    this.optimisticReceiptDecoders.add(registration)
    return () => { this.optimisticReceiptDecoders.delete(registration) }
  }
  beginOptimistic(mutation: OptimisticMutation): OptimisticHandle {
    if (this.clearing || !this.generation?.context || this.generation.socket?.readyState !== 1) throw new Error('Optimistic mutations require a ready connection handshake')
    return this.store.beginOptimistic(mutation)
  }
  temporaryEntity(): TemporaryEntity { return this.store.temporaryEntity() }
  optimistic(options: OptimisticDraftOptions): OptimisticDraft {
    if (this.clearing || !this.generation?.context || this.generation.socket?.readyState !== 1) throw new Error('Optimistic drafts require a ready connection handshake')
    return new OptimisticDraft(this, options)
  }
  getDiagnostics(): ClientDiagnostics {
    return Object.freeze({ ...this.counters, connection: this.store.getConnection().status,
      activeProjections: this.handles.size, pendingWindowRequests: this.requests.size,
      lastFailure: this.lastFailure, store: this.store.getDiagnostics() })
  }
  private diagnostic(event: DiagnosticEvent): void {
    if (!this.options.onDiagnostic) return
    const value = Object.freeze(event)
    queueMicrotask(() => invokeSafely(() => this.options.onDiagnostic?.(value), error => this.reportListenerError(error)))
  }
  constructor(private options: ClientOptions) {
    this.store = new ModelStore({ onListenerError: options.onListenerError })
    for (const number of [options.reconnectDelayMs, options.maxReconnectDelayMs, options.requestTimeoutMs, options.maxMessageBytes, options.maxDepth, options.maxNodes, options.connectionTimeoutMs, options.heartbeatTimeoutMs]) {
      if (number !== undefined && (!Number.isSafeInteger(number) || number <= 0)) throw new Error('Client limits must be positive integers')
    }
    if (options.heartbeatIntervalMs !== undefined && (!Number.isSafeInteger(options.heartbeatIntervalMs) || options.heartbeatIntervalMs < 0)) throw new Error('Heartbeat interval must be a nonnegative integer')
    this.store.subscribeCommits(affected => this.publish(affected))
  }
  connect(): void {
    if (this.clearing) return
    this.lifecycle++
    this.running = true
    if (!this.generation && !this.retry) this.open()
  }
  disconnect(): void {
    this.lifecycle++
    this.running = false
    clearTimeout(this.retry); this.retry = undefined
    const generation = this.generation
    this.generation = undefined
    if (generation) this.closeGeneration(generation)
    this.failRequests(new Error('Connection closed'))
    this.resetModel()
  }
  // Identity changes must not carry data or subscriptions into a new subject.
  // Call before changing credentials; create fresh handles afterwards.
  clear(): void {
    if (this.clearing) return
    this.clearing = true
    try {
      this.disconnect()
      for (const { handle } of [...this.handles.values()]) handle.dispose()
    } finally { this.clearing = false }
  }
  private reportListenerError(error: unknown): void {
    const unhandled = (reported: unknown) => queueMicrotask(() => { throw reported })
    const report = this.options.onListenerError
    if (report) invokeSafely(() => report(error), unhandled)
    else unhandled(error)
  }
  private publish(affected: ReadonlySet<Selection<unknown>>): void {
    const handles: ProjectionRegistration[] = []
    for (const source of affected) {
      const registration = this.bySource.get(source)
      if (registration) handles.push(registration)
    }
    for (const { source, publication } of handles) publication.stage(source.getSnapshot())
    for (const { publication } of handles) publication.flush()
  }
  private resetModel(connection?: ConnectionSnapshot): void {
    if (this.store.getConnection().status !== 'disconnected' && (!connection || connection.status === 'disconnected')) this.diagnostic({ type: 'disconnected' })
    for (const { publication, handle } of this.handles.values()) {
      if (this.store.getSubscription(handle.id).status === 'ready') publication.prepareReset(readProjection(this.store, handle.id, handle.request, { authoritative: true }).root)
    }
    this.store.reset(connection)
  }
  subscribeProjection(request: ProjectionRequest): ProjectionHandle {
    if (this.clearing) throw new Error('Cannot subscribe while client is clearing')
    // Own a frozen definition independent of mutable caller configuration.
    const definition = prepareProjectionRequest(request)
    const id = `projection-${++this.nextSubscription}`
    const publication = new ProjectionPublication(error => this.reportListenerError(error))
    const source = this.store.observe(store => readProjection(store, id, definition))
    // Keep dependency tracking active. Publication itself is driven once by
    // the model commit boundary, not once per affected projection selector.
    const stop = source.subscribe(() => {})
    let disposed = false
    const handle: ProjectionHandle = Object.freeze({
      id, request: definition, view: publication.view, data: publication.data,
      target: (nodeId: string = definition.root.nodeId, ancestors: readonly string[] = []): OptimisticTarget => {
        if (disposed) throw new Error('Projection is disposed')
        let found: { kind: 'entity' | 'list'; depth: number } | undefined
        const visit = (node: ProjectionRequest['root'], depth: number) => {
          if (node.nodeId === nodeId) found = { kind: node.entity ? 'entity' : 'list', depth }
          for (const child of node.children ?? []) visit(child, depth + 1)
        }
        visit(definition.root, 0)
        if (!found || found.depth !== ancestors.length || ancestors.some(id => typeof id !== 'string' || !id)) throw new Error('Optimistic target requires a definition node and one entity ID per ancestor')
        return this.store.projectionTarget(id, nodeId, ancestors, found.kind)
      },
      updateWindow: (instanceId: string, updates: readonly BoundaryUpdate[]) => disposed
        ? Promise.reject(new Error('Projection is disposed')) : this.updateWindow(id, instanceId, updates),
      dispose: () => {
        if (disposed) return
        disposed = true
        this.release(id)
      },
    })
    const registration = { handle, publication, source, stop }
    this.handles.set(id, registration)
    this.diagnostic({ type: 'projection-opened', projectionId: id })
    this.bySource.set(source, registration)
    this.store.setSubscription(id, { status: 'loading' })
    if (this.generation) this.sendSubscription(handle, this.generation)
    return handle
  }
  private release(id: string): void {
    const registration = this.handles.get(id)
    if (!registration) return
    this.diagnostic({ type: 'projection-closed', projectionId: id })
    this.handles.delete(id)
    this.bySource.delete(registration.source)
    registration.stop()
    registration.publication.clear()
    const generation = this.generation
    const installation = generation?.subscriptions.get(id)
    generation?.subscriptions.delete(id)
    this.store.setSubscription(id, { status: 'closed' })
    for (const [requestId, pending] of this.requests) if (pending.subscriptionId === id) this.finishRequest(requestId, new Error('Projection disposed'))
    if (installation && !installation.terminal && generation === this.generation && generation?.socket?.readyState === 1) {
      const requestId = this.requestId()
      generation.closing.set(requestId, id)
      try { this.send(generation, { subscriptionId: id, requestId, unsubscribe: {} }) }
      catch (error) { this.lost(generation, asError(error), 'send') }
    } else this.store.forgetSubscription(id)
    registration.publication.flush()
  }
  private updateWindow(subscriptionId: string, instanceId: string, updates: readonly BoundaryUpdate[]): Promise<void> {
    const generation = this.generation
    if (!this.handles.has(subscriptionId) || this.store.getSubscription(subscriptionId).status !== 'ready' || generation?.socket?.readyState !== 1) return Promise.reject(new Error('Projection is not ready'))
    if (!this.handles.get(subscriptionId)?.publication.hasWindow(instanceId)) return Promise.reject(new Error('Window does not belong to the current projection'))
    const requestId = this.requestId()
    let data: Uint8Array<ArrayBuffer>
    try { data = encodeSocketMessage({ subscriptionId, requestId, updateProjectionWindow: { instanceId, updates } }) }
    catch (error) { return Promise.reject(asError(error)) }
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => this.finishRequest(requestId, new Error('Window request timed out')), this.options.requestTimeoutMs ?? 30000)
      this.requests.set(requestId, { resolve, reject, timer, subscriptionId })
      try { this.sendBytes(generation, data) }
      catch (error) { this.finishRequest(requestId, asError(error)); this.lost(generation, asError(error), 'send') }
    })
  }
  private requestId(): string { return `request-${++this.nextRequest}` }
  private send(generation: ConnectionGeneration, message: SocketRequest): void {
    this.sendBytes(generation, encodeSocketMessage(message))
  }
  private sendBytes(generation: ConnectionGeneration, data: Uint8Array<ArrayBuffer>): void {
    if (generation !== this.generation || generation.socket?.readyState !== 1) throw new Error('Connection is not open')
    generation.socket.send(data)
  }
  private sendSubscription(handle: ProjectionHandle, generation: ConnectionGeneration): void {
    if (generation !== this.generation || generation.socket?.readyState !== 1 ||
      this.handles.get(handle.id)?.handle !== handle || generation.subscriptions.has(handle.id)) return
    const requestId = this.requestId()
    // Reserve before sending/calling out: startup and connected listeners use
    // the same once-per-generation installation path.
    generation.subscriptions.set(handle.id, { requestId, terminal: false })
    try { this.send(generation, { subscriptionId: handle.id, requestId, subscribeProjection: handle.request }) }
    catch (error) { this.lost(generation, asError(error), 'send') }
  }
  private open(): void {
    if (!this.running || this.generation) return
    const generation: ConnectionGeneration = { subscriptions: new Map(), closing: new Map() }
    this.generation = generation
    this.counters.connectionAttempts++
    this.diagnostic({ type: 'connecting' })
    this.resetModel({ status: 'connecting' })
    if (this.generation !== generation) return
    let socket: Socket
    try {
      const url = typeof this.options.url === 'function' ? this.options.url() : this.options.url
      if (this.generation !== generation) return
      socket = (this.options.createSocket ?? (url => new WebSocket(url)))(url)
    } catch (error) { this.lost(generation, asError(error), 'connect'); return }
    if (this.generation !== generation) { socket.close(); return }
    generation.socket = socket
    socket.binaryType = 'arraybuffer'
    generation.connectionTimer = setTimeout(() => this.lost(generation, new Error('Connection timed out'), 'connection-timeout'), this.options.connectionTimeoutMs ?? 10000)
    socket.onopen = () => {
      if (this.generation !== generation || generation.opened) return
      generation.opened = true
      clearTimeout(generation.connectionTimer)
      this.attempts = 0
      this.diagnostic({ type: 'connected' })
      this.store.setConnection({ status: 'connected' })
      for (const { handle } of this.handles.values()) {
        if (this.generation !== generation) break
        this.sendSubscription(handle, generation)
      }
      this.scheduleHeartbeat(generation)
    }
    socket.onmessage = event => {
      if (this.generation !== generation) return
      this.counters.messagesReceived++
      if (event.data instanceof ArrayBuffer) this.counters.bytesReceived += event.data.byteLength
      try {
        if (!(event.data instanceof ArrayBuffer) || event.data.byteLength > (this.options.maxMessageBytes ?? 8388608)) throw new ProtocolError('Invalid or oversized binary socket message')
        const message = decodeSocketMessage(new Uint8Array(event.data), this.options)
        if (message.type === 'model') this.receive(message.message, generation)
        else if (message.type === 'authStatus') {
          invokeSafely(() => this.options.onAuthStatus?.(message.status), error => this.reportListenerError(error))
        } else if (generation.heartbeatId === message.id) {
          clearTimeout(generation.heartbeatTimer)
          generation.heartbeatId = undefined
          this.scheduleHeartbeat(generation)
        }
      } catch (error) { this.lost(generation, asError(error), 'protocol') }
    }
    socket.onerror = () => this.lost(generation, new Error('WebSocket error'))
    socket.onclose = event => this.lost(generation, new Error('Connection closed'), 'close', event && typeof event.code === 'number' ? { code: event.code, reason: event.reason, wasClean: event.wasClean } : undefined)
  }
  private receive(message: ServerMessage, generation: ConnectionGeneration): void {
    if (message.event.type === 'connectionOpened') {
      if (generation.context !== undefined) throw new ProtocolError('Duplicate connection context')
      const context = message.event.context
      generation.context = context
      this.diagnostic({ type: 'handshake' })
      invokeSafely(() => this.options.onConnectionOpened?.(context), error => this.reportListenerError(error))
      return
    }
    const retire = new Set<string>()
    const appliedReceipts: (() => void)[] = []
    if (message.event.type === 'batch' && message.event.batch.kind === 'COMMIT' && message.event.batch.atomic && message.event.batch.messages.length) {
      const batch = message.event.batch
      const transaction = Object.freeze({ connectionContext: generation.context, checkpoint: batch.checkpoint, atomic: batch.atomic,
        connectionSequence: message.connectionSequence, messages: batch.messages })
      for (const { decode, onApplied } of [...this.optimisticReceiptDecoders]) {
        try {
          const decoded = decode(transaction)
          if (!Array.isArray(decoded)) throw new Error('Optimistic receipt decoder must return nonempty mutation IDs')
          // Snapshot before validation or effects: sparse arrays, getters, and
          // custom iterators must not partially retire layers or poison CDC.
          const ids = [...decoded]
          if (ids.some(id => typeof id !== 'string' || !id)) throw new Error('Optimistic receipt decoder must return nonempty mutation IDs')
          const owned = Object.freeze([...new Set(ids)])
          for (const id of owned) retire.add(id)
          if (onApplied) appliedReceipts.push(() => onApplied(transaction, owned))
        } catch (error) { this.reportListenerError(error) }
        if (this.generation !== generation) return
      }
    }
    this.store.apply(message, { retireOptimistic: [...retire] })
    for (const notify of appliedReceipts) invokeSafely(notify, error => this.reportListenerError(error))
    // Store observers may disconnect/reconnect synchronously. Never apply old
    // request bookkeeping to the replacement generation after they return.
    if (this.generation !== generation) return
    if (message.event.type === 'ready' && this.handles.has(message.subscriptionId)) this.diagnostic({ type: 'projection-ready', projectionId: message.subscriptionId })
    const closing = generation.closing.get(message.requestId)
    if (closing && message.event.type === 'batch') {
      generation.closing.delete(message.requestId)
      this.store.forgetSubscription(closing)
    }
    if (message.event.type === 'error') {
      const error = new Error(`${message.event.code}: ${message.event.message}`)
      const pending = this.requests.has(message.requestId)
      this.finishRequest(message.requestId, error)
      const installation = generation.subscriptions.get(message.subscriptionId)
      const terminal = message.event.code === 'RESYNC_REQUIRED' || (!pending && message.requestId === installation?.requestId)
      if (terminal && installation) installation.terminal = true
      if (terminal) this.diagnostic({ type: 'projection-error', projectionId: message.subscriptionId, code: message.event.code })
      if (terminal && this.handles.has(message.subscriptionId)) {
        this.store.setSubscription(message.subscriptionId, { status: 'error', error: error.message })
        for (const [id, request] of this.requests) if (request.subscriptionId === message.subscriptionId) this.finishRequest(id, error)
      } else if (terminal) {
        // A server-side close can race our Unsubscribe. Terminal failure ends
        // the scope's sequence; the later unknown-scope acknowledgment may
        // start at one and must not be compared to that retired sequence.
        this.store.forgetSubscription(message.subscriptionId)
      }
    } else if (message.event.type === 'batch') {
      this.finishRequest(message.requestId)
      if (message.event.batch.messages.length && this.generation === generation) {
        const batch = message.event.batch
        const transaction = Object.freeze({ connectionContext: generation.context, checkpoint: batch.checkpoint, atomic: batch.atomic,
          connectionSequence: message.connectionSequence, messages: batch.messages })
        const listeners = [...this.transactionListeners]
        if (this.options.onTransaction) listeners.unshift(this.options.onTransaction)
        for (const listener of listeners) {
          if (this.generation !== generation) break
          invokeSafely(() => listener(transaction), error => this.reportListenerError(error))
        }
      }
    }
  }
  private lost(generation: ConnectionGeneration, error: Error, kind: FailureKind = 'transport', close?: ConnectionFailure['close']): void {
    if (this.generation !== generation) return
    this.generation = undefined
    this.closeGeneration(generation)
    this.failRequests(error)
    const lifecycle = this.lifecycle
    this.counters.failures++
    const summary = Object.freeze({ kind, ...(close && { closeCode: close.code, wasClean: close.wasClean }) })
    this.lastFailure = summary
    let action: FailureAction = 'retry'
    try {
      action = this.options.failurePolicy ? this.options.failurePolicy(Object.freeze({ kind, error, close: close && Object.freeze(close) })) : 'retry'
      if (!['retry', 'pause', 'clear'].includes(action)) throw new Error('Invalid connection failure action')
    } catch (policyError) { action = 'pause'; this.reportListenerError(policyError) }
    this.diagnostic({ type: 'failure', failure: summary, action })
    if (this.lifecycle !== lifecycle || this.generation) return
    if (action === 'clear') this.clear()
    else if (action === 'pause') this.disconnect()
    else this.scheduleRetry(error)
  }
  private closeGeneration(generation: ConnectionGeneration): void {
    clearTimeout(generation.connectionTimer)
    clearTimeout(generation.heartbeatTimer)
    generation.heartbeatId = undefined
    generation.socket?.close()
  }
  private scheduleHeartbeat(generation: ConnectionGeneration): void {
    const interval = this.options.heartbeatIntervalMs ?? 15000
    if (!interval || generation !== this.generation) return
    generation.heartbeatTimer = setTimeout(() => {
      if (generation !== this.generation) return
      const id = `heartbeat-${++this.nextHeartbeat}`
      generation.heartbeatId = id
      generation.heartbeatTimer = setTimeout(() => this.lost(generation, new Error('Heartbeat timed out'), 'heartbeat-timeout'), this.options.heartbeatTimeoutMs ?? 10000)
      try { this.send(generation, { ping: id }) }
      catch (error) { this.lost(generation, asError(error), 'send') }
    }, interval)
  }
  private scheduleRetry(error: Error): void {
    this.resetModel({ status: 'disconnected', error: error.message })
    if (!this.running || this.generation || this.retry) return
    const delay = Math.min((this.options.reconnectDelayMs ?? 1000) * 2 ** Math.min(this.attempts++, 20), this.options.maxReconnectDelayMs ?? 30000)
    this.counters.retriesScheduled++
    this.diagnostic({ type: 'retry-scheduled', attempt: this.attempts, delayMs: delay })
    this.retry = setTimeout(() => { this.retry = undefined; this.open() }, delay)
  }
  private finishRequest(id: string, error?: Error): void {
    const pending = this.requests.get(id)
    if (!pending) return
    clearTimeout(pending.timer)
    this.requests.delete(id)
    this.diagnostic({ type: 'window-settled', projectionId: pending.subscriptionId, failed: !!error })
    if (error) pending.reject(error); else pending.resolve()
  }
  private failRequests(error: Error): void { for (const id of this.requests.keys()) this.finishRequest(id, error) }
}
function asError(value: unknown): Error { return value instanceof Error ? value : new Error(String(value)) }
