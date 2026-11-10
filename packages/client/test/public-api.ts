import { ModelStore, ReactiveClient, MutationHarness, unionOptimisticShapes, invalidateComputed, type CompiledProjection, type ProjectionFields, type TypedProjectionHandle, type ProjectionHandle, type EntitySnapshot, type Selection, type OptimisticEntityPatch, type ProjectedEntity, type ProjectedList, type ProvisionalValue } from '../dist/index.js'
import { useTypedProjection } from '../dist/react.js'
import { mergeValue, unsetValue, type Value, type OptimisticWriter } from '../dist/index.js'

type MergeTask = ProjectionFields<{
  title: string
  metadata: { color: string; nested: { dense: boolean; locale: string | null }; items: readonly { label: string }[] }
  payload: Value
}, { canEdit: boolean }>
function mergedPredictions(writer: OptimisticWriter, task: ProjectedEntity<MergeTask>): void {
  writer.patch(task, {
    metadata: mergeValue({ color: 'red', nested: mergeValue({ dense: true, locale: unsetValue }) }),
    payload: mergeValue({ color: { textValue: 'red' }, nested: mergeValue({ dense: { boolValue: true } }), old: unsetValue }),
  })
  writer.patch(task, { title: unsetValue })
  // @ts-expect-error native selected fields retain their scalar types
  writer.patch(task, { metadata: mergeValue({ color: 123 }) })
  // @ts-expect-error nested native fields retain their scalar types
  writer.patch(task, { metadata: mergeValue({ nested: mergeValue({ dense: 'true' }) }) })
  // @ts-expect-error a merge cannot target an unknown selected member
  writer.patch(task, { metadata: mergeValue({ unknown: true }) })
  // @ts-expect-error scalar fields cannot be merged
  writer.patch(task, { title: mergeValue({}) })
  // @ts-expect-error arrays replace atomically, not by position
  writer.patch(task, { metadata: mergeValue({ items: mergeValue({}) }) })
  // @ts-expect-error opaque leaves use canonical values, not guessed native scalars
  writer.patch(task, { payload: mergeValue({ color: 'red' }) })
  // @ts-expect-error opaque canonical values still validate their tagged payload type
  writer.patch(task, { payload: mergeValue({ color: { textValue: 123 } }) })
  // @ts-expect-error computed values are never stored-field patches
  writer.patch(task, { canEdit: unsetValue })
  // @ts-expect-error unmarked object replacement still requires the full native value
  writer.patch(task, { metadata: { color: 'red' } })
}
void mergedPredictions

type ComputedTask = ProjectionFields<{ title: string; completed: boolean }, { canEdit: boolean; role: bigint }>
type ComputedProject = ProjectedEntity<ProjectionFields<{ tasks: ProjectedList<ComputedTask> | undefined }, { canEdit: string }>>
function computedPredictions(client: ReactiveClient, handle: TypedProjectionHandle<ComputedProject>, task: ProjectedEntity<ComputedTask>, list: ProjectedList<ComputedTask>): void {
  const shape = unionOptimisticShapes([task], { project: handle })
  const draft = client.optimistic({ mutationId: 'computed' })
  draft.patch(task, { completed: true })
  // @ts-expect-error computed fields are not stored patches
  draft.patch(task, { canEdit: true })
  // @ts-expect-error stored patches use native types
  draft.patch(task, { completed: 'true' })
  const inserted = draft.insert(list, { title: 'New', completed: false })
  const insertedShape = unionOptimisticShapes([inserted], { project: handle })
  draft.fill(insertedShape, { project: { tasks: { canEdit: () => false } } })
  // @ts-expect-error inserted reference must not turn computed fields into stored patches
  draft.patch(inserted, { canEdit: true })
  // @ts-expect-error computed fields are not stored insertion inputs
  draft.insert(list, { title: 'New', canEdit: true })
  draft.fill(shape, { project: { canEdit: () => 'root', tasks: {
    canEdit: row => { const completed: boolean | undefined = row.current.completed; return completed === true ? false : invalidateComputed },
    role: () => 1n,
  } } })
  // @ts-expect-error bool computation cannot return a string
  draft.fill(shape, { project: { tasks: { canEdit: () => 'no' } } })
  // @ts-expect-error same computation name at root has a distinct string type
  draft.fill(shape, { project: { canEdit: () => false } })
  // @ts-expect-error stored fields cannot be computed fillers
  draft.fill(shape, { project: { tasks: { title: () => 'no' } } })
  // @ts-expect-error invalid projection path
  draft.fill(shape, { project: { unknown: {} } })
  // @ts-expect-error invalid concrete projection key
  draft.fill(shape, { unknown: {} })
  draft.commit()
}
void computedPredictions

function useComputedPredictions(client: ReactiveClient, definition: CompiledProjection<{ id: string }, ComputedProject>, task: ProjectedEntity<ComputedTask>): void {
  const projection = useTypedProjection(client, definition, { id: 'project' })
  // @ts-expect-error a hook handle is not available before mounting
  const unguarded: TypedProjectionHandle<ComputedProject> = projection.handle
  if (projection.status === 'ready' && projection.handle) {
    const handle: TypedProjectionHandle<ComputedProject> = projection.handle
    const shape = unionOptimisticShapes([task], { project: handle })
    const draft = client.optimistic({ mutationId: 'react-computed' })
    draft.fill(shape, { project: { tasks: { canEdit: () => true, role: () => 1n } } })
    // @ts-expect-error the handle must retain computed result types
    draft.fill(shape, { project: { tasks: { canEdit: () => 'no' } } })
    // @ts-expect-error the hook's borrowed handle is read-only
    projection.handle = handle
  }
  void unguarded
}
void useComputedPredictions

const revisionClient = new ReactiveClient({ url: 'ws://localhost', clientRevision: 42n, onClientVersionStatus: notice => {
  const revision: bigint = notice.minimumRevision
  void revision
} })
void revisionClient

const store = new ModelStore({ onListenerError: error => console.error(error) })
const selection: Selection<EntitySnapshot> = store.observe(model => model.getEntity('entity'))
const stop = selection.subscribe(() => { selection.getSnapshot() })
stop()

// Consumers use the read/subscribe interface, not store tracking capabilities.
// @ts-expect-error private staged draft readers are not a public store API
store.previewOptimistic({ mutationId: 'private' })
// @ts-expect-error dependency tracking is private
store.track(model => model.getEntity('entity'))
// @ts-expect-error internal dependency keys are not a public subscription API
store.subscribeKeys(new Set<string>(), () => {})
// @ts-expect-error revision tracking is private
store.changedSince(new Set<string>(), 0)
// @ts-expect-error store revisions are private bookkeeping
store.revision
// @ts-expect-error selections have no public refresh operation
selection.refresh()

// Projection consumers cannot drive internal installation/publication phases.
function projectionAPI(client: ReactiveClient, handle: ProjectionHandle): void {
  // @ts-expect-error registration cleanup is private
  client.release(handle.id)
  // @ts-expect-error publication coordination is private
  client.publish()
  // @ts-expect-error error reporting is private
  client.reportListenerError(new Error())
  // @ts-expect-error windows are updated through their owning handle
  client.updateWindow(handle.id, 'window', [])
  // @ts-expect-error handles do not expose publication phases
  handle.stage()
  // @ts-expect-error handles do not expose publication phases
  handle.flush()
  // @ts-expect-error window validation is internal
  handle.hasWindow('window')
  // @ts-expect-error selections are read-only
  handle.data.stage(undefined)
}
void projectionAPI

function optimisticAPI(client: ReactiveClient, entityId: string, result: Promise<unknown>): void {
  const patches: readonly OptimisticEntityPatch[] = [{ entityId, set: { title: { textValue: 'Predicted' } }, unset: ['obsolete'] }]
  const handle = client.beginOptimistic({ mutationId: 'mutation', patches, result })
  handle.rollback()
  const stop = client.registerOptimisticReceiptDecoder(transaction => transaction.messages.length ? ['mutation'] : [])
  stop()
  // @ts-expect-error canonical tagged values, not arbitrary JS primitives
  client.beginOptimistic({ mutationId: 'bad', patches: [{ entityId, set: { title: 'plain string' } }] })
  // @ts-expect-error optimistic values remain immutable
  patches[0].set!.title.textValue = 'mutated'
}
void optimisticAPI

function graphAPI(client: ReactiveClient, projection: ProjectionHandle): void {
  const temporary = client.temporaryEntity()
  client.beginOptimistic({ mutationId: 'create',
    create: [{ entity: temporary, values: {} }],
    slots: [{ target: projection.target(), entityId: temporary.entityId }],
  })
  // @ts-expect-error arbitrary IDs are not client-owned temporary references
  client.beginOptimistic({ mutationId: 'bad', create: [{ entity: { entityId: 'invented' }, values: {} }] })
}
void graphAPI

function provisionalAPI(entity: ProjectedEntity<{ title: string; nested: { required: string } }>): void {
  if (entity.status === 'available') { const title: string = entity.value.title; void title }
  if (entity.status === 'optimistic') {
    const title: string | undefined = entity.value.title
    const nested: string | undefined = entity.value.nested?.required
    // @ts-expect-error incomplete predictions do not promise required fields
    const required: string = entity.value.title
    void title; void nested; void required
  }
}
void provisionalAPI

function provisionalStoredObjects(value: ProvisionalValue<{
  entityId: string
  status: 'available'
  value: string
  nested: { entityId: string; status: string; required: string }
  items: readonly { entityId: string; status: string; required: string }[]
  child: ProjectedEntity<{ title: string }>
  children: ProjectedList<{ title: string }>
}>): void {
  const id: string | undefined = value.entityId
  // @ts-expect-error stored objects are partial even when they resemble wrappers
  const requiredValue: string = value.value
  // @ts-expect-error nested stored objects are partial too
  const requiredNested: string = value.nested!.required
  // @ts-expect-error embedded list elements remain partial
  const requiredItem: string = value.items![0].required
  if (value.child?.status === 'available') {
    const title: string = value.child.value.title
    void title
  }
  if (value.child?.status === 'unavailable') {
    // @ts-expect-error actual wrappers retain their discriminated availability
    value.child.value
  }
  if (value.children) {
    const list: ProjectedList<{ title: string }> = value.children
    void list
  }
  void id; void requiredValue; void requiredNested; void requiredItem
}
void provisionalStoredObjects

function typedDraftAPI(client: ReactiveClient, links: ProjectedList<{
  position: string
  task: ProjectedEntity<{ title: string; completed: boolean; metadata: { label: string } }> | undefined
}>): void {
  const draft = client.optimistic({ mutationId: 'typed' })
  draft.insert(links, { task: { title: 'New task', completed: false, metadata: { label: 'Label' } } })
  draft.insert(links, { position: '1024', task: null })
  // @ts-expect-error positions are decoded decimals, not JS numbers
  draft.insert(links, { position: 1024 })
  // @ts-expect-error generated child field types are retained
  draft.insert(links, { task: { completed: 'false' } })
  // @ts-expect-error unknown child expansion names are rejected
  draft.insert(links, { unrelatedTask: {} })
  // @ts-expect-error embedded object fields are also checked
  draft.insert(links, { task: { metadata: { unknown: 'x' } } })
  const shared = draft.entity({ title: { textValue: 'Shared' } })
  draft.insert(links, { task: shared })
  draft.commit().rollback()
  const snapshot = client.getDiagnostics()
  // @ts-expect-error diagnostic snapshots are immutable
  snapshot.store.entities.pending = 3
}
void typedDraftAPI

function mutationAPI(client: ReactiveClient, list: ProjectedList<{ title: string }>): void {
  const harness = new MutationHarness(client, { decodeReceipts: () => [] })
  const operation = harness.run({
    send: async context => ({ id: context.mutationId }),
    optimistic: draft => {
      draft.insert(list, { title: 'Temporary' })
      // @ts-expect-error installation is exclusively owned by the harness
      draft.commit()
      // @ts-expect-error typed optimistic fields are preserved
      draft.insert(list, { title: 123 })
    },
    onResponseSettled: outcome => {
      if (outcome.kind === 'succeeded') { const id: string = outcome.response.id; void id }
    },
  })
  const response: Promise<{ id: string }> = operation.response
  const status = operation.state.getSnapshot()
  // @ts-expect-error lifecycle state is read-only
  status.optimistic = { kind: 'active' }
  operation.rollbackOptimistic()
  harness.dispose()
  void response
}
void mutationAPI
