import { immutable, ProtocolError, branchKey } from './protocol.js'
import type { Bounds, ModelBatch, Occurrence, RecordData, ServerMessage, BranchFailure } from './protocol.js'
import { OptimisticLayers } from './optimistic.js'
import type { OptimisticMutation, OptimisticHandle, OptimisticTarget, TemporaryEntity, OptimisticChanges, OptimisticComputedChange, OptimisticReadView } from './optimistic.js'
import { selectionKey } from './optimistic_selection.js'
import { optimisticComputedKey, authoritativeComputed } from './optimistic_computed.js'
import type { ComputedPrediction } from './optimistic_computed.js'
import { previewOptimistic, validateComputed } from './projection_metadata.js'
import type { StoreDiagnostics } from './diagnostics.js'
import { invokeSafely } from './callbacks.js'

export type EntitySnapshot = Readonly<{ status: 'unavailable' } | { status: 'pending' } | { status: 'available'; values: RecordData; provisional?: true }>
export interface QuerySnapshot { readonly id: string; readonly nodeId: string; readonly ids: readonly string[]; readonly version: bigint; readonly bounds: Bounds }
export interface SubscriptionSnapshot { readonly status: 'loading' | 'ready' | 'error' | 'closed'; readonly error?: string }
export interface ConnectionSnapshot { readonly status: 'disconnected' | 'connecting' | 'connected'; readonly error?: string }
export type Listener = () => void
export interface ModelStoreOptions { readonly onListenerError?: (error: unknown) => void }
export interface Selection<T> {
  readonly getSnapshot: () => T
  readonly subscribe: (listener: Listener) => () => void
}

interface SelectionSource<T> {
  revision(): number
  read(): { value: T; dependencies: Set<string> }
  subscribe(keys: Set<string>, observer: SelectionObserver): () => void
  notify(listeners: Iterable<Listener>): void
}
interface SelectionObserver {
  readonly selection: Selection<unknown>
  invalidate(): void
  deliver(): void
}
export type CommitListener = (affected: ReadonlySet<Selection<unknown>>) => void

function reportAsync(error: unknown): void { queueMicrotask(() => { throw error }) }
const unavailable: EntitySnapshot = Object.freeze({ status: 'unavailable' })
const pending: EntitySnapshot = Object.freeze({ status: 'pending' })
const loading: SubscriptionSnapshot = Object.freeze({ status: 'loading' })
const noOccurrences: readonly Occurrence[] = Object.freeze([])
const noComputed: RecordData = Object.freeze(Object.create(null))
const computedKey = (occurrence: string, entity: string) => JSON.stringify(['computed', occurrence, entity])
const entityKey = (id: string) => JSON.stringify(['entity', id])
const authoritativeEntityKey = (id: string) => JSON.stringify(['authoritativeEntity', id])
const queryKey = (id: string) => JSON.stringify(['query', id])
const statusKey = (id: string) => JSON.stringify(['status', id])
const occurrenceKey = (scope: string, node: string, parent: string, entity: string) => JSON.stringify(['occurrences', scope, node, parent, entity])
const bucketKey = (v: Occurrence) => occurrenceKey(v.subscriptionId, v.nodeId, v.parentOccurrenceId, v.parentEntityId)
const branchesKey = (scope: string) => JSON.stringify(['branches', scope])
const noFailures: readonly BranchFailure[] = Object.freeze([])

// A dependency-tracked, cached selector. Snapshots stay referentially stable
// between relevant commits; the first subscriber installs dependencies lazily.
class StoreSelection<T> implements Selection<T> {
  private value!: T
  private notifiedValue?: T
  private revision = -1
  private dirty = true
  private dependencies = new Set<string>()
  private listeners = new Set<Listener>()
  private stop?: () => void
  constructor(private source: SelectionSource<T>) {}
  getSnapshot = (): T => {
    if (this.revision !== this.source.revision()) {
      if (!this.stop || this.dirty) this.refresh()
      else this.revision = this.source.revision()
    }
    return this.value
  }
  subscribe = (listener: Listener): (() => void) => {
    const current = this.getSnapshot()
    if (!this.listeners.size) this.notifiedValue = current
    this.listeners.add(listener)
    if (!this.stop) this.listen()
    return () => {
      this.listeners.delete(listener)
      if (!this.listeners.size) {
        this.stop?.(); this.stop = undefined
        this.notifiedValue = undefined
      }
    }
  }
  private listen(): void {
    this.stop = this.source.subscribe(this.dependencies, {
      selection: this,
      invalidate: () => { this.dirty = true },
      deliver: this.changed,
    })
  }
  private changed = (): void => {
    if (!this.listeners.size) return
    const current = this.getSnapshot()
    // Reads may refresh the cache before our notification turn. Only delivery
    // advances this baseline; another selector's read cannot consume it.
    if (Object.is(this.notifiedValue, current)) return
    this.notifiedValue = current
    this.source.notify([...this.listeners])
  }
  private refresh(): void {
    const result = this.source.read()
    this.value = result.value
    this.dirty = false
    this.dependencies = result.dependencies
    this.revision = this.source.revision()
    if (this.stop) { this.stop(); this.listen() }
  }
}

// Owns the connection's read model, not datastore query evaluation. Every batch
// is staged in touched-key overlays, validated, then committed before notifying.
export class ModelStore {
  private branches = new Map<string, readonly BranchFailure[]>()
  private computed = new Map<string, Map<string, RecordData>>()
  // Immutable list images share a lazily built membership index. Scalar-only
  // updates reuse it; retired images/indexes are collected together.
  private queryMembers = new WeakMap<readonly string[], ReadonlySet<string>>()
  private entities = new Map<string, EntitySnapshot>()
  private availableEntities = 0
  private pendingEntities = 0
  private optimistic = new OptimisticLayers()
  private readonly optimisticView: OptimisticReadView
  private queries = new Map<string, QuerySnapshot>()
  private occurrences = new Map<string, Occurrence>()
  private buckets = new Map<string, readonly Occurrence[]>()
  private statuses = new Map<string, SubscriptionSnapshot>()
  private connection: ConnectionSnapshot = Object.freeze({ status: 'disconnected' })
  private connectionSequence = 0n
  private sequences = new Map<string, bigint>()
  private listeners = new Map<string, Set<SelectionObserver>>()
  private commitListeners = new Set<CommitListener>()
  private collecting?: Set<string>
  private _revision = 0
  constructor(private options: ModelStoreOptions = {}) {
    this.optimisticView = this.optimistic.over({
      entity: id => {
        this.read(entityKey(id))
        const record = this.entities.get(id) ?? unavailable
        return { record, backed: record.status === 'available', deleted: false }
      },
      selection: (_key, ids) => ({ ids, overridden: false }),
      computed: (_key, values) => authoritativeComputed(values),
    })
  }
  getDiagnostics(): StoreDiagnostics {
    return Object.freeze({ entities: Object.freeze({ available: this.availableEntities, pending: this.pendingEntities }),
      queries: this.queries.size, occurrences: this.occurrences.size, subscriptions: this.statuses.size,
      trackedKeys: this.listeners.size, optimistic: this.optimistic.getDiagnostics() })
  }
  // Materializers use one notification per atomic model commit, before ordinary
  // selectors notify. Only dependency-indexed, affected selections are included;
  // all have been invalidated before any materializer is called.
  subscribeCommits(listener: CommitListener): () => void {
    this.commitListeners.add(listener)
    return () => { this.commitListeners.delete(listener) }
  }
  observe<T>(selector: (store: ModelStore) => T): Selection<T> {
    return new StoreSelection({
      revision: () => this._revision,
      read: () => this.track(selector),
      subscribe: (keys, listener) => this.subscribeKeys(keys, listener),
      notify: listeners => this.notifyListeners(listeners),
    })
  }
  getEntity(id: string): EntitySnapshot { return this.optimisticView.entity(id).record }
  getAuthoritativeEntity(id: string): EntitySnapshot { this.read(authoritativeEntityKey(id)); return this.entities.get(id) ?? unavailable }
  [validateComputed](changes: readonly OptimisticComputedChange[]): void { this.optimistic.prepareComputed(changes) }
  // A read-only staged draft over the current effective model. No publication,
  // listeners, or changes to live layers; discarded when filling is complete.
  [previewOptimistic](mutation: OptimisticMutation): import('./projection.js').ProjectionReader {
    const overlay = this.optimistic.fork()
    overlay.add(mutation)
    const view = overlay.over(this.optimisticView)
    return {
      getEntity: id => view.entity(id).record,
      getAuthoritativeEntity: id => this.getAuthoritativeEntity(id),
      getSubscription: id => this.getSubscription(id),
      getConnection: () => this.getConnection(),
      getBranchErrors: id => this.getBranchErrors(id),
      getOccurrences: (...args) => this.getOccurrences(...args),
      getQuery: id => this.getQuery(id),
      getComputed: (...args) => this.getComputed(...args),
      isOptimisticallyDeleted: id => view.entity(id).deleted,
      projectionSelection: (scope, node, ancestors, base) => this.selectFrom(view, scope, node, ancestors, base),
      projectionComputed: (scope, node, ancestors, id, occurrence) => this.computedFrom(view, scope, node, ancestors, id, occurrence),
    }
  }
  getBranchErrors(scope: string): readonly BranchFailure[] { this.read(branchesKey(scope)); return this.branches.get(scope) ?? noFailures }
  temporaryEntity(): TemporaryEntity { return this.optimistic.temporary() }
  projectionTarget(scope: string, node: string, ancestors: readonly string[], kind: 'entity' | 'list'): OptimisticTarget { return this.optimistic.target(scope, node, ancestors, kind) }
  projectionSelection(scope: string, node: string, ancestors: readonly string[], base: readonly string[]): Readonly<{ ids: readonly string[]; overridden: boolean }> {
    return this.selectFrom(this.optimisticView, scope, node, ancestors, base)
  }
  private selectFrom(view: OptimisticReadView, scope: string, node: string, ancestors: readonly string[], base: readonly string[]): Readonly<{ ids: readonly string[]; overridden: boolean }> {
    const key = selectionKey(scope, node, ancestors)
    this.read(key)
    return view.selection(key, base)
  }
  isOptimisticallyDeleted(id: string): boolean { return this.optimisticView.entity(id).deleted }
  beginOptimistic(mutation: OptimisticMutation): OptimisticHandle {
    const { mutationId } = mutation
    const layer = this.optimistic.add(mutation)
    this.notify(this.optimisticKeys(this.optimistic.changes(layer)))
    const handle = Object.freeze({ mutationId, rollback: () => {
      const changed = this.optimistic.remove(mutationId, layer)
      if (changed.entities.length || changed.targets.length) this.notify(this.optimisticKeys(changed))
    } })
    if (mutation.result) void mutation.result.then(() => {}, () => handle.rollback())
    return handle
  }
  private optimisticKeys(changes: OptimisticChanges): Set<string> { return new Set([...changes.entities.map(entityKey), ...changes.targets]) }
  getQuery(id: string): QuerySnapshot | undefined { this.read(queryKey(id)); return this.queries.get(id) }
  getComputed(occurrenceId: string, entityId: string): RecordData {
    this.read(computedKey(occurrenceId, entityId))
    return this.computed.get(occurrenceId)?.get(entityId) ?? noComputed
  }
  projectionComputed(scope: string, node: string, ancestors: readonly string[], entity: string, occurrence: string | undefined): ComputedPrediction {
    return this.computedFrom(this.optimisticView, scope, node, ancestors, entity, occurrence)
  }
  private computedFrom(view: OptimisticReadView, scope: string, node: string, ancestors: readonly string[], entity: string, occurrence: string | undefined): ComputedPrediction {
    const base = occurrence ? this.getComputed(occurrence, entity) : noComputed
    const key = optimisticComputedKey(selectionKey(scope, node, ancestors), entity)
    this.read(key)
    return view.computed(key, base)
  }
  getOccurrences(scope: string, node: string, parent = '', entity = ''): readonly Occurrence[] {
    const key = occurrenceKey(scope, node, parent, entity)
    this.read(key)
    return this.buckets.get(key) ?? noOccurrences
  }
  getSubscription(id: string): SubscriptionSnapshot { this.read(statusKey(id)); return this.statuses.get(id) ?? loading }
  getConnection(): ConnectionSnapshot { this.read('connection'); return this.connection }

  setConnection(value: ConnectionSnapshot): void {
    this.connection = immutable({ ...value })
    this.notify(new Set(['connection']))
  }
  setSubscription(id: string, value: SubscriptionSnapshot): void {
    this.statuses.set(id, immutable({ ...value }))
    this.notify(new Set([statusKey(id)]))
  }
  forgetSubscription(id: string): void {
    this.statuses.delete(id)
    this.sequences.delete(id)
    this.branches.delete(id)
    this.notify(new Set([statusKey(id), branchesKey(id)]))
  }
  // A reconnect is a new model generation, never an overlay over stale IDs.
  reset(connection: ConnectionSnapshot = { status: 'disconnected' }): void {
    this.optimistic.clear()
    this.entities.clear(); this.queries.clear(); this.occurrences.clear(); this.buckets.clear()
    this.computed.clear()
    this.branches.clear()
    this.queryMembers = new WeakMap()
    this.availableEntities = 0; this.pendingEntities = 0
    this.connectionSequence = 0n; this.sequences.clear()
    this.statuses.clear()
    this.connection = immutable({ ...connection })
    this.notify(new Set(this.listeners.keys()))
  }

  apply(message: ServerMessage, options: { readonly retireOptimistic?: readonly string[] } = {}): void {
    const { connectionSequence, sequence, subscriptionId, event } = message
    if (connectionSequence !== 0n && connectionSequence !== this.connectionSequence + 1n) throw new ProtocolError('Connection sequence gap')
    if (sequence !== 0n && (!subscriptionId || sequence !== (this.sequences.get(subscriptionId) ?? 0n) + 1n)) throw new ProtocolError('Subscription sequence gap')
    if (connectionSequence !== 0n && event.type !== 'batch') throw new ProtocolError('Connection sequence requires a batch')
    const retire = options.retireOptimistic ?? []
    if (retire.some(id => typeof id !== 'string' || !id) || (retire.length && (event.type !== 'batch' || event.batch.kind !== 'COMMIT' || !event.batch.atomic))) throw new ProtocolError('Optimistic retirement requires an atomic commit and nonempty mutation IDs')
    const changed = event.type === 'batch' ? this.stageBatch(event.batch) : new Set<string>()
    // Retirement joins the authoritative commit before any reader is notified.
    for (const id of retire) for (const key of this.optimisticKeys(this.optimistic.remove(id))) changed.add(key)
    // No potentially failing protocol work after this point.
    if (connectionSequence !== 0n) this.connectionSequence = connectionSequence
    if (sequence !== 0n) this.sequences.set(subscriptionId, sequence)
    if (event.type === 'ready') {
      this.statuses.set(subscriptionId, Object.freeze({ status: 'ready' }))
      changed.add(statusKey(subscriptionId))
    }
    this.notify(changed)
  }

  private stageBatch(batch: ModelBatch): Set<string> {
    const branches = new Map<string, Map<string, BranchFailure>>()
    for (const change of batch.branches) {
      let scope = branches.get(change.subscriptionId)
      if (!scope) { scope = new Map(this.getBranchErrors(change.subscriptionId).map(error => [branchKey(error), error])); branches.set(change.subscriptionId, scope) }
      const key = branchKey(change)
      if (change.type === 'error') {
        const { type: _, ...failure } = change
        scope.set(key, immutable(failure))
      } else scope.delete(key)
    }
    const entities = new Map<string, EntitySnapshot | null>()
    const queries = new Map<string, QuerySnapshot | null>()
    const occurrences = new Map<string, Occurrence | null>()
    const getQuery = (id: string) => queries.has(id) ? queries.get(id) ?? undefined : this.queries.get(id)
    const membersOf = (ids: readonly string[]): ReadonlySet<string> => {
      let members = this.queryMembers.get(ids)
      if (!members) { members = new Set(ids); this.queryMembers.set(ids, members) }
      return members
    }
    for (const change of batch.entities) {
      entities.set(change.id, change.type === 'released' ? null : change.type === 'pending' ? pending : immutable({ status: 'available', values: change.values }))
    }
    for (const change of batch.queries) {
      if (change.type === 'closed') { queries.set(change.id, null); continue }
      const previous = getQuery(change.id)
      if (change.type === 'reset') {
        if (new Set(change.ids).size !== change.ids.length || change.version === 0n || (previous && change.version <= previous.version)) throw new ProtocolError('Invalid query reset')
        queries.set(change.id, immutable({ id: change.id, nodeId: change.nodeId, ids: [...change.ids], version: change.version, bounds: change.bounds }))
        continue
      }
      if (!previous) throw new ProtocolError('Change for unknown query')
      if (change.type === 'window') { queries.set(change.id, immutable({ ...previous, bounds: change.bounds })); continue }
      if (previous.version !== change.base || change.version !== change.base + 1n) throw new ProtocolError('Query version mismatch')
      const removed = new Set(change.removed)
      const members = membersOf(previous.ids)
      if (removed.size !== change.removed.length || [...removed].some(id => !members.has(id))) throw new ProtocolError('Invalid query removal')
      const retained = previous.ids.filter(id => !removed.has(id))
      const present = new Set(retained)
      const ids: string[] = []
      let sourceIndex = 0
      let lastIndex = -1
      for (const insertion of [...change.insertions].sort((a, b) => a.index - b.index)) {
        if (insertion.index <= lastIndex || present.has(insertion.id)) throw new ProtocolError('Invalid query insertion')
        while (ids.length < insertion.index) {
          if (sourceIndex === retained.length) throw new ProtocolError('Invalid query insertion index')
          ids.push(retained[sourceIndex++])
        }
        ids.push(insertion.id)
        present.add(insertion.id)
        lastIndex = insertion.index
      }
      while (sourceIndex < retained.length) ids.push(retained[sourceIndex++])
      queries.set(change.id, immutable({ ...previous, ids, version: change.version }))
    }
    for (const change of batch.occurrences) {
      occurrences.set(change.type === 'detached' ? change.id : change.value.id, change.type === 'detached' ? null : change.value)
    }
    // Index updates are also staged; no mutation of live buckets during validation.
    const buckets = new Map<string, readonly Occurrence[]>()
    const editBucket = (value: Occurrence, add: boolean) => {
      const key = bucketKey(value)
      const previous = buckets.get(key) ?? this.buckets.get(key) ?? noOccurrences
      const next = previous.filter(item => item.id !== value.id)
      if (add) next.push(value)
      buckets.set(key, Object.freeze(next))
    }
    for (const [id, next] of occurrences) {
      const previous = this.occurrences.get(id)
      if (previous) editBucket(previous, false)
      if (next) editBucket(next, true)
    }
    for (const value of buckets.values()) if (value.length > 1) throw new ProtocolError('Ambiguous projection edge')
    const changed = new Set<string>()
    // Stage sparse row patches. Updating one computed value must not copy the
    // whole list occurrence's computed map. Live maps change only after every
    // event in this batch has passed validation.
    const computed = new Map<string, Map<string, RecordData | undefined>>()
    const retiredComputed = new Set<string>()
    const valuesFor = (id: string): Map<string, RecordData | undefined> => {
      let values = computed.get(id)
      if (!values) { values = new Map(); computed.set(id, values) }
      return values
    }
    for (const change of batch.computed) {
      if (change.type === 'upsert') {
        const occurrence = occurrences.has(change.occurrenceId) ? occurrences.get(change.occurrenceId) : this.occurrences.get(change.occurrenceId)
        if (!occurrence) throw new ProtocolError('Computed values for an unknown occurrence')
        if (occurrence.target.type === 'entity') {
          if (occurrence.target.id !== change.entityId) throw new ProtocolError('Computed values for another entity')
        } else {
          const id = occurrence.target.id
          const query = getQuery(id)
          if (!query || !membersOf(query.ids).has(change.entityId)) throw new ProtocolError('Computed values for a nonmember')
        }
        valuesFor(change.occurrenceId).set(change.entityId, immutable(change.values))
      } else valuesFor(change.occurrenceId).set(change.entityId, undefined)
      changed.add(computedKey(change.occurrenceId, change.entityId))
    }
    for (const [id, occurrence] of occurrences) if (!occurrence) {
      for (const entity of this.computed.get(id)?.keys() ?? []) changed.add(computedKey(id, entity))
      for (const entity of valuesFor(id).keys()) changed.add(computedKey(id, entity))
      retiredComputed.add(id)
    }
    for (const [id, value] of entities) {
      const previous = this.entities.get(id)
      if (previous?.status === 'available') this.availableEntities--
      if (previous?.status === 'pending') this.pendingEntities--
      if (value?.status === 'available') this.availableEntities++
      if (value?.status === 'pending') this.pendingEntities++
      if (value) this.entities.set(id, value); else this.entities.delete(id)
      if (!value || value.status !== 'available') this.optimistic.dropEntity(id)
      changed.add(entityKey(id)); changed.add(authoritativeEntityKey(id))
    }
    for (const [id, value] of queries) { if (value) this.queries.set(id, value); else this.queries.delete(id); changed.add(queryKey(id)) }
    for (const [id, value] of occurrences) { if (value) this.occurrences.set(id, immutable(value)); else this.occurrences.delete(id) }
    for (const [key, value] of buckets) { if (value.length) this.buckets.set(key, value); else this.buckets.delete(key); changed.add(key) }
    for (const [id, patches] of computed) {
      if (retiredComputed.has(id)) { this.computed.delete(id); continue }
      const values = this.computed.get(id) ?? new Map<string, RecordData>()
      for (const [entity, value] of patches) { if (value) values.set(entity, value); else values.delete(entity) }
      if (values.size) this.computed.set(id, values); else this.computed.delete(id)
    }
    for (const [id, errors] of branches) {
      if (errors.size) this.branches.set(id, Object.freeze([...errors.values()])); else this.branches.delete(id)
      changed.add(branchesKey(id))
    }
    return changed
  }

  private track<T>(selector: (store: ModelStore) => T): { value: T; dependencies: Set<string> } {
    const previous = this.collecting
    const dependencies = new Set<string>()
    this.collecting = dependencies
    try { return { value: selector(this), dependencies } } finally { this.collecting = previous }
  }
  private subscribeKeys(keys: Set<string>, observer: SelectionObserver): () => void {
    for (const key of keys) {
      let listeners = this.listeners.get(key)
      if (!listeners) {
        this.listeners.set(key, listeners = new Set())
      }
      listeners.add(observer)
    }
    return () => {
      for (const key of keys) {
        const listeners = this.listeners.get(key)
        listeners?.delete(observer)
        if (!listeners?.size) this.listeners.delete(key)
      }
    }
  }
  private read(key: string): void { this.collecting?.add(key) }
  private notify(keys: Set<string>): void {
    this._revision++
    const observers = new Set<SelectionObserver>()
    for (const key of keys) {
      for (const observer of this.listeners.get(key) ?? []) observers.add(observer)
    }
    for (const observer of observers) observer.invalidate()
    const affected = new Set([...observers].map(observer => observer.selection))
    this.notifyListeners([...this.commitListeners].map(listener => () => listener(affected)))
    this.notifyListeners([...observers].map(observer => () => observer.deliver()))
  }
  private notifyListeners(listeners: Iterable<Listener>): void {
    const handler = this.options.onListenerError ?? reportAsync
    const report = (error: unknown) => invokeSafely(() => handler(error), reportAsync)
    for (const listener of listeners) {
      // A consumer exception must not turn an already-committed batch into a
      // protocol failure or prevent healthy subscribers from seeing it.
      invokeSafely(listener, report)
    }
  }
}
