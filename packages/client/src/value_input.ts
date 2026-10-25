import type { ValueShape } from './bindings.js'
import type { RecordData, Value } from './protocol.js'

export class ProjectionDecodeError extends Error {}

/** Same shape-directed scalar encoder for parameters and optimistic values. */
export function encodeValue(value: unknown, shape: ValueShape, location: string): Value {
  const invalid = (): never => { throw new ProjectionDecodeError(`${location}: expected ${shape.kind}`) }
  if (value === null && shape.nullable) return { nullValue: true }
  switch (shape.kind) {
    case 'bool': return typeof value === 'boolean' ? { boolValue: value } : invalid()
    case 'text': return typeof value === 'string' ? { textValue: value } : invalid()
    case 'uuid': return typeof value === 'string' && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value) ? { uuidValue: value.toLowerCase() } : invalid()
    case 'datetime': return typeof value === 'string' ? { datetimeValue: value } : invalid()
    case 'bytes': return typeof value === 'string' ? { bytesValue: value } : invalid()
    case 'decimal': return typeof value === 'string' && /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$/.test(value) ? { decimalValue: value } : invalid()
    case 'integer': return typeof value === 'bigint' && value >= -(1n << 63n) && value < (1n << 63n) ? { integerValue: value.toString() } : invalid()
    case 'int32': return typeof value === 'number' && Number.isInteger(value) && value >= -2147483648 && value <= 2147483647 ? { integerValue: String(value) } : invalid()
    case 'float': case 'finiteFloat': return typeof value === 'number' && (shape.kind === 'float' || Number.isFinite(value)) ? { floatValue: String(value) } : invalid()
    case 'structured': return value && typeof value === 'object' && !Array.isArray(value) ? value as Value : invalid()
    case 'list': return Array.isArray(value) ? { listValue: { values: value.map((item, i) => encodeValue(item, shape.element, `${location}[${i}]`)) } } : invalid()
    case 'object': return { objectValue: { fields: encodeFields(value, shape.fields, location) } }
  }
}

export function encodeFields(value: unknown, fields: readonly { name: string; path: readonly string[]; type: ValueShape }[], location: string): RecordData {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new ProjectionDecodeError(`${location}: expected object`)
  const input = value as Record<string, unknown>
  const result: Record<string, Value> = Object.create(null)
  const paths: (readonly string[])[] = []
  for (const key of Object.keys(input)) if (!fields.some(field => field.name === key)) throw new ProjectionDecodeError(`${location}: unknown field ${key}`)
  for (const field of fields) {
    if (!Object.hasOwn(input, field.name) || input[field.name] === undefined) continue
    if (paths.some(path => path.slice(0, Math.min(path.length, field.path.length)).every((segment, i) => segment === field.path[i]))) throw new ProjectionDecodeError('Overlapping optimistic field paths; supply only one alias')
    paths.push(field.path)
    assignPath(result, field.path, encodeValue(input[field.name], field.type, `${location}.${field.name}`))
  }
  return result
}

function assignPath(record: Record<string, Value>, path: readonly string[], value: Value): void {
  if (!path.length) throw new ProjectionDecodeError('Optimistic field path is empty')
  let fields = record
  for (const segment of path.slice(0, -1)) {
    if (!Object.hasOwn(fields, segment)) fields[segment] = { objectValue: { fields: Object.create(null) } }
    const child = fields[segment]
    if (!('objectValue' in child)) throw new ProjectionDecodeError('Overlapping optimistic field paths')
    fields = child.objectValue!.fields as Record<string, Value>
  }
  const leaf = path[path.length - 1]
  if (Object.hasOwn(fields, leaf)) throw new ProjectionDecodeError('Overlapping optimistic field paths; supply only one alias')
  fields[leaf] = value
}
