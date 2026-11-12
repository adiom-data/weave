// Shared, framework-independent execution support for generated projection
// bindings. This layer decodes published data; it never evaluates queries or
// maintains a second authoritative cache.
import type { ReactiveClient, ProjectionHandle } from './client.js'
import type { ProjectionDataNode, ProjectionDataRow } from './publication.js'
import type { ProjectionSnapshot } from './projection.js'
import type { ProjectionRequest, BoundaryUpdate, Bounds, RecordData, Value } from './protocol.js'
import type { BranchFailure } from './protocol.js'
import { prepareProjectionRequest, immutable } from './protocol.js'
import type { Selection } from './store.js'
import type { OptimisticTarget } from './optimistic.js'
import { bindList, bindEntity, bindProjection, entityBrand } from './projection_metadata.js'
import { encodeValue, ProjectionDecodeError } from './value_input.js'
import { findLiveWindow } from './windows.js'
import type { ComputedPredictionStates } from './optimistic_computed.js'
import { ProjectionTraversal } from './projection_reader.js'
import type { ProjectionBranch } from './projection_reader.js'
import { projectionRowData } from './publication.js'
import type { ProjectionCandidate } from './projection_metadata.js'
export { ProjectionDecodeError } from './value_input.js'

// Only runtime-owned wrappers retain their availability contract. Stored
// objects may have any field names, including entityId, status, and value.
export type ProvisionalValue<T> = T extends ProjectedList<unknown> | { readonly [entityBrand]: true } ? T :
  T extends readonly (infer E)[] ? readonly ProvisionalValue<E>[] :
  T extends object ? { readonly [K in keyof T]?: ProvisionalValue<T[K]> } : T

declare const projectionFields: unique symbol
/** Generated type-only field classification. No metadata is added to row values. */
export type ProjectionFields<S, C> = S & C & { readonly [projectionFields]?: { readonly stored: S; readonly computed: C } }
export type StoredFields<T> = typeof projectionFields extends keyof T ? NonNullable<T[typeof projectionFields]> extends { stored: infer S } ? S : never : T
export type ComputedFields<T> = typeof projectionFields extends keyof T ? NonNullable<T[typeof projectionFields]> extends { computed: infer C } ? C : never : Record<never, never>

export type ProjectedEntity<T> = Readonly<{ entityId: string; [entityBrand]: true; error?: BranchFailure; stale?: boolean; computedState?: ComputedPredictionStates }> & (
  | Readonly<{ status: 'available'; value: T }>
  | Readonly<{ status: 'pending' | 'unavailable' }>
  | Readonly<{ status: 'optimistic'; value: ProvisionalValue<T> }>
)
declare const listBrand: unique symbol
/** An immutable ordered list with a private, occurrence-relative window locator. */
export interface ProjectedList<T> extends ReadonlyArray<ProjectedEntity<T>> { readonly [listBrand]: true; readonly error?: BranchFailure; readonly stale?: boolean }
export type ScalarKind = 'bool' | 'text' | 'uuid' | 'datetime' | 'bytes' | 'decimal' | 'integer' | 'int32' | 'float' | 'finiteFloat' | 'structured'
export type ValueShape = Readonly<{ nullable?: boolean }> & (
  | Readonly<{ kind: ScalarKind }>
  | Readonly<{ kind: 'list'; element: ValueShape }>
  | Readonly<{ kind: 'object'; fields: readonly FieldShape[] }>
)
export interface FieldShape { readonly name: string; readonly path: readonly string[]; readonly type: ValueShape; readonly computed?: boolean }
export interface ProjectionShape {
  readonly nodeId: string
  readonly kind: 'entity' | 'list'
  readonly fields: readonly FieldShape[]
  readonly children: Readonly<Record<string, ProjectionShape>>
}
export interface ParameterShape { readonly name: string; readonly type: ValueShape }
export interface ProjectionOptions { readonly waitForSync?: boolean; readonly allowPartialErrors?: boolean; readonly atLeastFence?: string }
export interface TypedProjectionSnapshot<T> {
  readonly errors?: readonly BranchFailure[]
  readonly status: ProjectionSnapshot['status']
  readonly error?: string
  readonly stale?: boolean
  readonly data?: T
}
export interface WindowControl {
  /** Current ready-generation bounds; undefined while rebuilding, errored, or detached. */
  readonly bounds: Selection<Bounds | undefined>
  update(updates: readonly BoundaryUpdate[]): Promise<void>
  extend(side: 'SIDE_LEFT' | 'SIDE_RIGHT', count: number): Promise<void>
  contract(side: 'SIDE_LEFT' | 'SIDE_RIGHT', count: number): Promise<void>
  move(side: 'SIDE_LEFT' | 'SIDE_RIGHT', cursor: string): Promise<void>
}
export interface TypedProjectionHandle<T> {
  readonly id: string
  readonly data: Selection<T | undefined>
  readonly view: Selection<TypedProjectionSnapshot<T>>
  window(list: ProjectedList<unknown>): WindowControl
  target(listOrNode?: ProjectedList<unknown> | string, ancestors?: readonly string[]): OptimisticTarget
  dispose(): void
}
export interface CompiledProjection<P, T> {
  request(parameters: P, options?: ProjectionOptions): ProjectionRequest
  subscribe(client: ReactiveClient, parameters: P, options?: ProjectionOptions): TypedProjectionHandle<T>
  prepare(parameters: P, options?: ProjectionOptions): PreparedProjection<T>
}
/** Pure preparation owns parameter values before asynchronous lifecycle work. */
export interface PreparedProjection<T> {
  readonly key: string
  readonly request: ProjectionRequest
  subscribe(client: ReactiveClient): TypedProjectionHandle<T>
}

function lookup(record: RecordData, segments: readonly string[]): Value | undefined {
  let value = record[segments[0]]
  for (const segment of segments.slice(1)) {
    if (!value || !('objectValue' in value)) return undefined
    value = value.objectValue?.fields?.[segment] as Value
  }
  return value
}
export function decodeValue(value: Value | undefined, shape: ValueShape, location: string, previous?: unknown, provisional = false): unknown {
  if (value === undefined && provisional) return undefined
  if (!value || 'nullValue' in value) {
    if (!shape.nullable) throw new ProjectionDecodeError(`${location}: required field is missing or null`)
    return value ? null : undefined
  }
  const invalid = (): never => { throw new ProjectionDecodeError(`${location}: expected ${shape.kind}`) }
  switch (shape.kind) {
    case 'bool': return 'boolValue' in value ? value.boolValue : invalid()
    case 'text': return 'textValue' in value ? value.textValue : invalid()
    case 'uuid': return 'uuidValue' in value ? value.uuidValue : invalid()
    case 'datetime': return 'datetimeValue' in value ? value.datetimeValue : invalid()
    case 'bytes': return 'bytesValue' in value ? value.bytesValue : invalid()
    case 'decimal': return 'decimalValue' in value ? value.decimalValue : invalid()
    case 'integer': {
      if (!('integerValue' in value) || !/^-?\d+$/.test(value.integerValue!)) return invalid()
      const integer = BigInt(value.integerValue!)
      return integer >= -(1n << 63n) && integer < (1n << 63n) ? integer : invalid()
    }
    case 'int32': {
      if (!('integerValue' in value)) return invalid()
      const number = Number(value.integerValue)
      return Number.isInteger(number) && number >= -2147483648 && number <= 2147483647 ? number : invalid()
    }
    case 'float': case 'finiteFloat': {
      if (!('floatValue' in value)) return invalid()
      const number = Number(value.floatValue)
      return shape.kind === 'float' || Number.isFinite(number) ? number : invalid()
    }
    case 'structured': return value
    case 'list': {
      if (!('listValue' in value) || !shape.element) return invalid()
      const before = Array.isArray(previous) ? previous : undefined
      return reuseArray(before, (value.listValue?.values ?? []).map((item, index) => decodeValue(item as Value, shape.element!, `${location}[${index}]`, before?.[index], provisional)))
    }
    case 'object': {
      if (!('objectValue' in value)) return invalid()
      const before = previous && typeof previous === 'object' ? previous as Readonly<Record<string, unknown>> : undefined
      return reuseObject(before, decodeFields(value.objectValue?.fields as RecordData ?? {}, shape.fields ?? [], location, before, provisional))
    }
    default: return invalid()
  }
}
export function decodeFields(values: RecordData, fields: readonly FieldShape[], location: string, previous?: Readonly<Record<string, unknown>>, provisional = false, computed: RecordData = {}): Record<string, unknown> {
  const out: Record<string, unknown> = Object.create(null)
  for (const field of fields) out[field.name] = decodeValue(lookup(field.computed ? computed : values, field.path), field.type, `${location}.${field.name}`, previous?.[field.name], provisional)
  return out
}

// Reconcile only selected values, after successful decoding. These shallow
// checks compose bottom-up and do not retain extra historical entity records.
function reuseObject<T extends object>(previous: T | undefined, next: T): Readonly<T> {
  const keys = Object.keys(next)
  const before = previous as Readonly<Record<string, unknown>> | undefined, after = next as Readonly<Record<string, unknown>>
  return previous && keys.length === Object.keys(previous).length && keys.every(key => Object.hasOwn(previous, key) && Object.is(before![key], after[key]))
    ? previous : Object.freeze(next)
}
function reuseArray<T>(previous: readonly T[] | undefined, next: T[]): readonly T[] {
  return previous && previous.length === next.length && next.every((item, index) => Object.is(item, previous[index])) ? previous : Object.freeze(next)
}

function mapSelection<A, B>(source: Selection<A>, project: (value: A, previous: B | undefined) => B): Selection<B> {
  let initialized = false, input: A, output: B
  const getSnapshot = (): B => {
    const next = source.getSnapshot()
    if (!initialized || next !== input) { const result = project(next, initialized ? output : undefined); input = next; output = result; initialized = true }
    return output
  }
  return Object.freeze({ getSnapshot, subscribe: (listener: () => void) => {
    // Notification baselines belong to subscribers, not cached reads. Reading
    // this selection from an earlier listener cannot consume another's update.
    let notified = getSnapshot(), failed = false
    return source.subscribe(() => {
      let next: B
      try { next = getSnapshot() } catch {
        // Notify readers so React can surface decode failures through its own
        // error boundary rather than silently retaining the last valid value.
        failed = true; return listener()
      }
      if (failed || !Object.is(next, notified)) { notified = next; failed = false; return listener() }
    })
  } })
}

interface Locator { nodeId: string; ancestors: readonly string[] }
// One immutable record supplies either stored fields or computed fields. Keep
// their decoded images independent, and never reuse a permissive optimistic
// decode for a strict authoritative read. Weak keys retain no record history.
class FieldImages {
  private strict = new WeakMap<RecordData, Readonly<Record<string, unknown>>>()
  private provisional = new WeakMap<RecordData, Readonly<Record<string, unknown>>>()
  constructor(private fields: readonly FieldShape[], private location: string, private computed: boolean) {}
  read(values: RecordData, previous: Readonly<Record<string, unknown>> | undefined, provisional: boolean): Readonly<Record<string, unknown>> {
    const images = provisional ? this.provisional : this.strict
    let decoded = images.get(values)
    if (!decoded) {
      decoded = Object.freeze(this.computed
        ? decodeFields(emptyComputed, this.fields, this.location, previous, provisional, values)
        : decodeFields(values, this.fields, this.location, previous, provisional))
      images.set(values, decoded)
    }
    return decoded
  }
}
class Materializer {
  constructor(private bind: (list: object, shape: ProjectionShape, locator: Locator) => void, private bindRow: (row: object, shape: ProjectionShape, locator: Locator) => void) {}
  private nodes = new WeakMap<ProjectionDataNode, unknown>()
  private rows = new WeakMap<ProjectionDataRow, ProjectedEntity<unknown>>()
  private fields = new WeakMap<ProjectionShape, { stored: FieldImages; computed: FieldImages }>()
  readonly lists = new WeakMap<object, Locator>()
  row(value: ProjectionDataRow, shape: ProjectionShape, ancestors: readonly string[], before?: ProjectedEntity<unknown>): ProjectedEntity<unknown> {
    const cached = this.rows.get(value)
    if (cached) return cached
    let output: ProjectedEntity<unknown>
    if (value.entity.status !== 'available') output = before?.entityId === value.entityId && before.status === value.entity.status ? before : Object.freeze({ [entityBrand]: true as const, entityId: value.entityId, status: value.entity.status })
    else {
      const oldFields = (before?.status === 'available' || before?.status === 'optimistic') && before.entityId === value.entityId ? before.value as Readonly<Record<string, unknown>> : undefined
      const status = value.entity.provisional ? 'optimistic' : 'available'
      let images = this.fields.get(shape)
      if (!images) {
        images = {
          stored: new FieldImages(shape.fields.filter(field => !field.computed), shape.nodeId, false),
          computed: new FieldImages(shape.fields.filter(field => field.computed), shape.nodeId, true),
        }
        this.fields.set(shape, images)
      }
      const fields = Object.assign(Object.create(null),
        images.stored.read(value.entity.values, oldFields, !!value.entity.provisional),
        images.computed.read(value.computed ?? emptyComputed, oldFields, !!value.entity.provisional))
      for (const [name, child] of Object.entries(shape.children)) fields[name] = this.node(value.children[child.nodeId], child, [...ancestors, value.entityId], oldFields?.[name])
      const shared = reuseObject(oldFields, fields)
      output = (before?.status === 'available' || before?.status === 'optimistic') && before.status === status && before.value === shared ? before : Object.freeze({ [entityBrand]: true as const, entityId: value.entityId, status, value: shared })
    }
    // Presentation state belongs to this occurrence, not its entity image.
    // Reconcile it even when recovery restores an identical payload.
    if (value.error || output.error || output.stale) {
      const { error: _error, stale: _stale, ...image } = output
      output = reuseObject(before, value.error ? { ...image, error: value.error, stale: value.stale } : image) as ProjectedEntity<unknown>
    }
    const states: Record<string, 'predicted' | 'stale'> = Object.create(null)
    for (const field of shape.fields) if (field.computed && value.computedState?.[field.path[0]]) states[field.name] = value.computedState[field.path[0]]
    if (Object.keys(states).length || output.computedState) {
      const { computedState: _, ...image } = output
      output = reuseObject(before, { ...image, ...(Object.keys(states).length ? { computedState: reuseObject(before?.computedState, states) } : {}) }) as ProjectedEntity<unknown>
    }
    this.bindRow(output, shape, { nodeId: shape.nodeId, ancestors })
    this.rows.set(value, output)
    return output
  }
  node(node: ProjectionDataNode | undefined, shape: ProjectionShape, ancestors: readonly string[] = [], previous?: unknown): unknown {
    if (!node) return undefined
    if (node.nodeId !== shape.nodeId || node.type !== shape.kind) throw new ProjectionDecodeError(`Unexpected projection node ${node.nodeId}`)
    if (this.nodes.has(node)) return this.nodes.get(node)
    let output: ProjectedEntity<unknown> | readonly ProjectedEntity<unknown>[]
    if (node.type === 'entity') output = this.row(node.row, shape, ancestors, previous as ProjectedEntity<unknown> | undefined)
    else {
      const before = Array.isArray(previous) ? previous as readonly ProjectedEntity<unknown>[] : undefined
      const byID = new Map(before?.map(item => [item.entityId, item]))
      const next = node.rows.map(item => this.row(item, shape, ancestors, byID.get(item.entityId)))
      if (node.error) Object.assign(next, { error: node.error, stale: node.stale })
      output = (before as ProjectedList<unknown> | undefined)?.error === node.error && (before as ProjectedList<unknown> | undefined)?.stale === node.stale ? reuseArray(before, next) : Object.freeze(next)
    }
    if (node.type === 'list') {
      const locator = { nodeId: shape.nodeId, ancestors }
      this.lists.set(output, locator)
      this.bind(output, shape, locator)
    }
    this.nodes.set(node, output)
    return output
  }
}
const emptyComputed: RecordData = Object.freeze({})

/** Used by generated modules; no GraphQL parser or schema ships to browsers. */
export function defineProjection<P, T>(root: ProjectionRequest['root'], shape: ProjectionShape, parameters: readonly ParameterShape[]): CompiledProjection<P, T> {
  root = prepareProjectionRequest({ root }).root
  shape = immutable(structuredClone(shape))
  parameters = immutable(structuredClone(parameters))
  const encodeParameters = (input: P): Record<string, Value> => {
    if (!input || typeof input !== 'object') throw new ProjectionDecodeError('Projection parameters must be an object')
    const values: Record<string, Value> = Object.create(null)
    for (const parameter of parameters) values[parameter.name] = encodeValue((input as Record<string, unknown>)[parameter.name], parameter.type, `Parameter ${parameter.name}`)
    return immutable(structuredClone(values))
  }
  const request = (input: P, options?: ProjectionOptions): ProjectionRequest => Object.freeze({ root, parameters: encodeParameters(input), waitForSync: options?.waitForSync ?? false, allowPartialErrors: options?.allowPartialErrors ?? false, ...(options?.atLeastFence === undefined ? {} : { atLeastFence: options.atLeastFence }) })
  const attach = (client: ReactiveClient, request: ProjectionRequest): TypedProjectionHandle<T> => {
    const raw: ProjectionHandle = client.subscribeProjection(request)
    const materializer = new Materializer(
      (list, shape, locator) => bindList(list, { owner: client, shape, ancestors: locator.ancestors, target: raw.target }),
      (row, shape) => bindEntity(row, { owner: client, fields: shape.fields, location: shape.nodeId }),
    )
    const data = mapSelection<ProjectionDataNode | undefined, T | undefined>(raw.data, (node, previous) => materializer.node(node, shape, [], previous) as T | undefined)
    const view = mapSelection<ProjectionSnapshot, TypedProjectionSnapshot<T>>(raw.view, (snapshot, previous) => reuseObject(previous,
      { status: snapshot.status, error: snapshot.error, errors: snapshot.errors, stale: snapshot.stale, data: data.getSnapshot() }))
    const handle: TypedProjectionHandle<T> = Object.freeze({
      id: raw.id, data, view, dispose: raw.dispose,
      target: (listOrNode?: ProjectedList<unknown> | string, ancestors?: readonly string[]): OptimisticTarget => {
        if (listOrNode === undefined || typeof listOrNode === 'string') return raw.target(listOrNode, ancestors)
        const locator = materializer.lists.get(listOrNode)
        if (!locator) throw new Error('List does not belong to this projection')
        return raw.target(locator.nodeId, locator.ancestors)
      },
      window: (list: ProjectedList<unknown>): WindowControl => {
        const locator = materializer.lists.get(list)
        if (!locator) throw new Error('List does not belong to this projection')
        const currentWindow = (snapshot: ProjectionSnapshot) => snapshot.status === 'ready' ? findLiveWindow(snapshot.root, locator) : undefined
        const bounds = mapSelection(raw.view, snapshot => currentWindow(snapshot)?.query?.bounds)
        const update = (updates: readonly BoundaryUpdate[]): Promise<void> => {
          const window = currentWindow(raw.view.getSnapshot())
          return window?.occurrenceId ? raw.updateWindow(window.occurrenceId, updates) : Promise.reject(new Error('List has no active server window'))
        }
        return Object.freeze({ bounds, update,
          extend: (side: 'SIDE_LEFT' | 'SIDE_RIGHT', count: number) => update([{ side, operation: 'OPERATION_EXTEND', count }]),
          contract: (side: 'SIDE_LEFT' | 'SIDE_RIGHT', count: number) => update([{ side, operation: 'OPERATION_CONTRACT', count }]),
          move: (side: 'SIDE_LEFT' | 'SIDE_RIGHT', cursor: string) => update([{ side, operation: 'OPERATION_MOVE', cursor }]),
        })
      },
    })
    bindProjection(handle, { owner: client, shape, target: raw.target,
      validate: () => {
        raw.target()
        if (!client.connectionContext || raw.view.getSnapshot().status !== 'ready') throw new Error('Optimistic shapes require a ready projection')
      },
      select: function* (reader, entities, nodes): Iterable<ProjectionCandidate> {
        const traversal = new ProjectionTraversal(reader, raw.id, request)
        interface Path { branch: ProjectionBranch; id: string; parent?: Path; available?: boolean }
        const available = (path: Path): boolean => {
          if (path.available === undefined) path.available = (!path.parent || available(path.parent)) && traversal.canVisit(path.branch, path.id) && traversal.entity(path.id).status === 'available'
          return path.available
        }
        function* visit(branch: ProjectionBranch | undefined, shape: ProjectionShape, parent?: Path): Iterable<ProjectionCandidate> {
          if (!branch || branch.error || !nodes.has(shape.nodeId)) return
          const children = Object.entries(shape.children).filter(([, child]) => nodes.has(child.nodeId))
          const ids = branch.node.entity ? branch.ids.slice(0, 1) : branch.ids
          for (const id of ids) {
            // Route by identities first. Validate ancestor payload availability
            // only when a matching descendant is found; cache it for diamonds.
            if (!children.length && !entities.has(id)) continue
            if (branch.failed?.has(id)) continue
            const path: Path = { branch, id, parent }
            if (entities.has(id) && available(path)) yield {
              nodeId: shape.nodeId, entityId: id, ancestors: branch.ancestors,
              current: () => {
                const row = materializer.row(projectionRowData(traversal.row(branch, id)), shape, branch.ancestors)
                return row.status === 'available' || row.status === 'optimistic' ? row.value : undefined
              },
            }
            if (children.length && path.available !== false) {
              for (const [, child] of children) {
                const node = branch.node.children?.find(node => node.nodeId === child.nodeId)
                if (node) yield* visit(traversal.child(branch, id, node), child, path)
              }
            }
          }
        }
        yield* visit(traversal.root(), shape)
      },
    })
    return handle
  }
  const prepare = (input: P, options?: ProjectionOptions): PreparedProjection<T> => {
    const owned = request(input, options)
    // The definition owns the fixed root. Every request input/option belongs
    // in its key so React cannot keep a subscription with obsolete behavior.
    const { root: _root, ...configuration } = owned
    return Object.freeze({ request: owned,
      key: JSON.stringify(configuration),
      subscribe: (client: ReactiveClient) => attach(client, owned),
    })
  }
  return Object.freeze({
    request, prepare,
    subscribe: (client: ReactiveClient, input: P, options?: ProjectionOptions) => prepare(input, options).subscribe(client),
  })
}
