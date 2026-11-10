import type { Value } from './protocol.js'

declare const mergeBrand: unique symbol
/** An opt-in patch of one object's members, not a replacement value. */
export interface MergeValue<T extends object> { readonly [mergeBrand]: T }
/** Remove an object key, rather than assigning an explicit null. */
export const unsetValue: unique symbol = Symbol('unsetValue')

export interface OpaqueMergeFields {
  readonly [key: string]: Value | MergeValue<OpaqueMergeFields> | typeof unsetValue | undefined
}
export type PatchField<T> = T | typeof unsetValue | (
  Value extends NonNullable<T> ? MergeValue<OpaqueMergeFields> :
  NonNullable<T> extends readonly unknown[] ? never :
  NonNullable<T> extends object ? MergeValue<{ readonly [K in keyof NonNullable<T>]?: PatchField<NonNullable<T>[K]> }> : never
)

// Markers are client-local instructions. Neither the brand nor their payload
// is part of a stored value or wire representation. Inputs are owned when the
// writer stages the patch, just like ordinary replacement inputs.
const merges = new WeakMap<object, Readonly<Record<string, unknown>>>()
export function mergeValue<const T extends object>(fields: T): MergeValue<T> {
  if (!fields || typeof fields !== 'object' || Array.isArray(fields) || isMergeValue(fields)) throw new Error('mergeValue requires an object-member map')
  const marker = Object.freeze({}) as MergeValue<T>
  merges.set(marker, fields as Readonly<Record<string, unknown>>)
  return marker
}
export function isMergeValue(value: unknown): value is MergeValue<object> {
  return !!value && typeof value === 'object' && merges.has(value)
}
export function mergeMembers(value: unknown): Readonly<Record<string, unknown>> | undefined {
  return value && typeof value === 'object' ? merges.get(value) : undefined
}
