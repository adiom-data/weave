// Shared, framework-independent execution support for generated projection
// bindings. This layer decodes published data; it never evaluates queries or
// maintains a second authoritative cache.
import type { ReactiveClient, ProjectionHandle } from './client.js'
import type { ProjectionDataNode, ProjectionDataRow } from './publication.js'
import type { ProjectionSnapshot, NodeView } from './projection.js'
import type { ProjectionRequest, BoundaryUpdate, Bounds, RecordData, Value } from './protocol.js'
import { prepareProjectionRequest, immutable } from './protocol.js'
import type { Selection } from './store.js'
import type { OptimisticTarget } from './optimistic.js'
import { bindList, entityBrand } from './projection_metadata.js'
import { encodeValue, ProjectionDecodeError } from './value_input.js'
export { ProjectionDecodeError } from './value_input.js'

// Only runtime-owned wrappers retain their availability contract. Stored
// objects may have any field names, including entityId, status, and value.
export type ProvisionalValue<T> = T extends ProjectedList<unknown> | { readonly [entityBrand]: true } ? T :
  T extends readonly (infer E)[] ? readonly ProvisionalValue<E>[] :
  T extends object ? { readonly [K in keyof T]?: ProvisionalValue<T[K]> } : T

export type ProjectedEntity<T> = Readonly<{ entityId: string; [entityBrand]: true }> & (
  | Readonly<{ status: 'available'; value: T }>
  | Readonly<{ status: 'pending' | 'unavailable' }>
  | Readonly<{ status: 'optimistic'; value: ProvisionalValue<T> }>
)
declare const listBrand: unique symbol
/** An immutable ordered list with a private, occurrence-relative window locator. */
export interface ProjectedList<T> extends ReadonlyArray<ProjectedEntity<T>> { readonly [listBrand]: true }
export type ScalarKind = 'bool' | 'text' | 'uuid' | 'datetime' | 'bytes' | 'decimal' | 'integer' | 'int32' | 'float' | 'finiteFloat' | 'structured'
export type ValueShape = Readonly<{ nullable?: boolean }> & (
  | Readonly<{ kind: ScalarKind }>
  | Readonly<{ kind: 'list'; element: ValueShape }>
  | Readonly<{ kind: 'object'; fields: readonly FieldShape[] }>
)
export interface FieldShape { readonly name: string; readonly path: readonly string[]; readonly type: ValueShape }
export interface ProjectionShape {
  readonly nodeId: string
  readonly kind: 'entity' | 'list'
  readonly fields: readonly FieldShape[]
  readonly children: Readonly<Record<string, ProjectionShape>>
}
export interface ParameterShape { readonly name: string; readonly type: ValueShape }
export interface ProjectionOptions { readonly waitForSync?: boolean }
export interface TypedProjectionSnapshot<T> {
  readonly status: ProjectionSnapshot['status']
  readonly error?: string
  readonly stale?: boolean
  readonly data?: T
}
export interface WindowControl {
  /** Current ready-generation bounds; undefined while rebuilding or detached. */
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
function decode(value: Value | undefined, shape: ValueShape, location: string, previous?: unknown, provisional = false): unknown {
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
      return reuseArray(before, (value.listValue?.values ?? []).map((item, index) => decode(item as Value, shape.element!, `${location}[${index}]`, before?.[index], provisional)))
    }
    case 'object': {
      if (!('objectValue' in value)) return invalid()
      const before = previous && typeof previous === 'object' ? previous as Readonly<Record<string, unknown>> : undefined
      return reuseObject(before, decodeFields(value.objectValue?.fields as RecordData ?? {}, shape.fields ?? [], location, before, provisional))
    }
    default: return invalid()
  }
}
function decodeFields(values: RecordData, fields: readonly FieldShape[], location: string, previous?: Readonly<Record<string, unknown>>, provisional = false): Record<string, unknown> {
  const out: Record<string, unknown> = Object.create(null)
  for (const field of fields) out[field.name] = decode(lookup(values, field.path), field.type, `${location}.${field.name}`, previous?.[field.name], provisional)
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
class Materializer {
  constructor(private bind: (list: object, shape: ProjectionShape, locator: Locator) => void) {}
  private nodes = new WeakMap<ProjectionDataNode, unknown>()
  private rows = new WeakMap<ProjectionDataRow, ProjectedEntity<unknown>>()
  readonly lists = new WeakMap<object, Locator>()
  node(node: ProjectionDataNode | undefined, shape: ProjectionShape, ancestors: readonly string[] = [], previous?: unknown): unknown {
    if (!node) return undefined
    if (node.nodeId !== shape.nodeId || node.type !== shape.kind) throw new ProjectionDecodeError(`Unexpected projection node ${node.nodeId}`)
    if (this.nodes.has(node)) return this.nodes.get(node)
    const row = (value: ProjectionDataRow, before?: ProjectedEntity<unknown>): ProjectedEntity<unknown> => {
      const cached = this.rows.get(value)
      if (cached) return cached
      let output: ProjectedEntity<unknown>
      if (value.entity.status !== 'available') output = before?.entityId === value.entityId && before.status === value.entity.status ? before : Object.freeze({ [entityBrand]: true as const, entityId: value.entityId, status: value.entity.status })
      else {
        const oldFields = (before?.status === 'available' || before?.status === 'optimistic') && before.entityId === value.entityId ? before.value as Readonly<Record<string, unknown>> : undefined
        const status = value.entity.provisional ? 'optimistic' : 'available'
        const fields = decodeFields(value.entity.values, shape.fields, shape.nodeId, oldFields, value.entity.provisional)
        for (const [name, child] of Object.entries(shape.children)) fields[name] = this.node(value.children[child.nodeId], child, [...ancestors, value.entityId], oldFields?.[name])
        const shared = reuseObject(oldFields, fields)
        output = (before?.status === 'available' || before?.status === 'optimistic') && before.status === status && before.value === shared ? before : Object.freeze({ [entityBrand]: true as const, entityId: value.entityId, status, value: shared })
      }
      this.rows.set(value, output)
      return output
    }
    let output: ProjectedEntity<unknown> | readonly ProjectedEntity<unknown>[]
    if (node.type === 'entity') output = row(node.row, previous as ProjectedEntity<unknown> | undefined)
    else {
      const before = Array.isArray(previous) ? previous as readonly ProjectedEntity<unknown>[] : undefined
      const byID = new Map(before?.map(item => [item.entityId, item]))
      output = reuseArray(before, node.rows.map(item => row(item, byID.get(item.entityId))))
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

function findWindow(node: NodeView | undefined, locator: Locator, ancestors: readonly string[] = []): Extract<NodeView, { type: 'list' }> | undefined {
  if (!node) return undefined
  if (node.nodeId === locator.nodeId && node.type === 'list' && ancestors.length === locator.ancestors.length && ancestors.every((key, i) => key === locator.ancestors[i])) return node
  for (const row of node.type === 'entity' ? [node.row] : node.rows) {
    if (row.entityId !== locator.ancestors[ancestors.length]) continue
    for (const child of Object.values(row.children)) {
      const result = findWindow(child, locator, [...ancestors, row.entityId])
      if (result) return result
    }
  }
  return undefined
}

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
  const request = (input: P, options?: ProjectionOptions): ProjectionRequest => Object.freeze({ root, parameters: encodeParameters(input), waitForSync: options?.waitForSync ?? false })
  const attach = (client: ReactiveClient, request: ProjectionRequest): TypedProjectionHandle<T> => {
    const raw: ProjectionHandle = client.subscribeProjection(request)
    const materializer = new Materializer((list, shape, locator) => bindList(list, { owner: client, shape, ancestors: locator.ancestors, target: raw.target }))
    const data = mapSelection<ProjectionDataNode | undefined, T | undefined>(raw.data, (node, previous) => materializer.node(node, shape, [], previous) as T | undefined)
    const view = mapSelection<ProjectionSnapshot, TypedProjectionSnapshot<T>>(raw.view, (snapshot, previous) => reuseObject(previous,
      { status: snapshot.status, error: snapshot.error, stale: snapshot.stale, data: data.getSnapshot() }))
    return Object.freeze({
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
        const bounds = mapSelection(raw.view, snapshot => snapshot.status === 'ready' ? findWindow(snapshot.root, locator)?.query?.bounds : undefined)
        const update = (updates: readonly BoundaryUpdate[]): Promise<void> => {
          const window = findWindow(raw.view.getSnapshot().root, locator)
          if (!window) return Promise.reject(new Error('List occurrence is no longer present'))
          return window.occurrenceId ? raw.updateWindow(window.occurrenceId, updates) : Promise.reject(new Error('List has no active server window'))
        }
        return Object.freeze({ bounds, update,
          extend: (side: 'SIDE_LEFT' | 'SIDE_RIGHT', count: number) => update([{ side, operation: 'OPERATION_EXTEND', count }]),
          contract: (side: 'SIDE_LEFT' | 'SIDE_RIGHT', count: number) => update([{ side, operation: 'OPERATION_CONTRACT', count }]),
          move: (side: 'SIDE_LEFT' | 'SIDE_RIGHT', cursor: string) => update([{ side, operation: 'OPERATION_MOVE', cursor }]),
        })
      },
    })
  }
  const prepare = (input: P, options?: ProjectionOptions): PreparedProjection<T> => {
    const owned = request(input, options)
    return Object.freeze({ request: owned,
      key: JSON.stringify({ parameters: owned.parameters, waitForSync: owned.waitForSync }),
      subscribe: (client: ReactiveClient) => attach(client, owned),
    })
  }
  return Object.freeze({
    request, prepare,
    subscribe: (client: ReactiveClient, input: P, options?: ProjectionOptions) => prepare(input, options).subscribe(client),
  })
}
