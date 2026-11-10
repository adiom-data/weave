import { useCallback, useEffect, useMemo, useState, useSyncExternalStore } from 'react'
import type { BoundaryUpdate, ProjectionRequest } from './protocol.js'
import type { ProjectionHandle, ReactiveClient } from './client.js'
import type { ProjectionSnapshot } from './projection.js'
import type { ConnectionSnapshot, Selection } from './store.js'
import type { CompiledProjection, ProjectionOptions, TypedProjectionHandle, TypedProjectionSnapshot, ProjectedList, WindowControl } from './bindings.js'
import type { OptimisticTarget } from './optimistic.js'

export function useSelection<T>(selection: Selection<T>): T {
  return useSyncExternalStore(selection.subscribe, selection.getSnapshot, selection.getSnapshot)
}

/** Owns a client's connection for this component's lifetime. Mount once per
 * client; other components can read connection state with useSelection instead.
 * Keep the client stable (for example, create it with useMemo).
 */
export function useConnection(client: ReactiveClient): ConnectionSnapshot {
  const selection = useMemo(() => client.store.observe(store => store.getConnection()), [client])
  const snapshot = useSelection(selection)
  useEffect(() => {
    client.connect()
    return () => client.disconnect()
  }, [client])
  return snapshot
}
const loading: ProjectionSnapshot = Object.freeze({ status: 'loading' })
const emptySubscribe = () => () => {}
const getLoading = () => loading

export interface ProjectionResult extends ProjectionSnapshot {
  readonly updateWindow: (instanceId: string, updates: readonly BoundaryUpdate[]) => Promise<void>
}

// Keep request referentially stable (module constant or useMemo). Network
// ownership lives in an effect, never in render; StrictMode can safely replay it.
export function useProjection(client: ReactiveClient, request: ProjectionRequest): ProjectionResult {
  const [active, setActive] = useState<{ client: ReactiveClient; request: ProjectionRequest; handle: ProjectionHandle }>()
  useEffect(() => {
    const handle = client.subscribeProjection(request)
    setActive({ client, request, handle })
    return () => handle.dispose()
  }, [client, request])
  const handle = active?.client === client && active.request === request ? active.handle : undefined
  const snapshot = useSyncExternalStore(handle?.view.subscribe ?? emptySubscribe, handle?.view.getSnapshot ?? getLoading, getLoading)
  const updateWindow = useCallback((instanceId: string, updates: readonly BoundaryUpdate[]) => {
    return handle ? handle.updateWindow(instanceId, updates) : Promise.reject(new Error('Projection is not mounted'))
  }, [handle])
  return useMemo(() => ({ ...snapshot, updateWindow }), [snapshot, updateWindow])
}

export interface TypedProjectionResult<T> extends TypedProjectionSnapshot<T> {
  /** Borrowed handle for this mounted subscription. The hook owns disposal.
   * Undefined before effects run or while the requested inputs are changing.
   */
  readonly handle: TypedProjectionHandle<T> | undefined
  window(list: ProjectedList<unknown>): WindowControl
  target(listOrNode?: ProjectedList<unknown> | string, ancestors?: readonly string[]): OptimisticTarget
}

export function useTypedProjection<P, T>(client: ReactiveClient, definition: CompiledProjection<P, T>, parameters: P, options?: ProjectionOptions): TypedProjectionResult<T> {
  const prepared = definition.prepare(parameters, options)
  const key = prepared.key
  const [active, setActive] = useState<{ client: ReactiveClient; definition: CompiledProjection<P, T>; key: string; handle: TypedProjectionHandle<T> }>()
  useEffect(() => {
    const handle = prepared.subscribe(client)
    setActive({ client, definition, key, handle })
    return () => handle.dispose()
    // The generated parameter key captures values, not caller object identity.
  }, [client, definition, key])
  const handle = active?.client === client && active.definition === definition && active.key === key ? active.handle : undefined
  const snapshot = useSyncExternalStore(handle?.view.subscribe ?? emptySubscribe, handle?.view.getSnapshot ?? getLoading, getLoading)
  const window = useCallback((list: ProjectedList<unknown>): WindowControl => {
    if (!handle) throw new Error('Projection is not mounted')
    return handle.window(list)
  }, [handle])
  const target = useCallback((listOrNode?: ProjectedList<unknown> | string, ancestors?: readonly string[]): OptimisticTarget => {
    if (!handle) throw new Error('Projection is not mounted')
    return handle.target(listOrNode, ancestors)
  }, [handle])
  return useMemo(() => ({ ...snapshot, handle, window, target }), [snapshot, handle, window, target])
}
