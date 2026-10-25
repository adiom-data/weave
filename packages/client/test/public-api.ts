import { ModelStore, ReactiveClient, MutationHarness, type ProjectionHandle, type EntitySnapshot, type Selection, type OptimisticEntityPatch, type ProjectedEntity, type ProjectedList, type ProvisionalValue } from '../dist/index.js'

const store = new ModelStore({ onListenerError: error => console.error(error) })
const selection: Selection<EntitySnapshot> = store.observe(model => model.getEntity('entity'))
const stop = selection.subscribe(() => { selection.getSnapshot() })
stop()

// Consumers use the read/subscribe interface, not store tracking capabilities.
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
