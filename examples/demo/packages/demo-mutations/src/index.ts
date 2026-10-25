import { Code, ConnectError, createClient } from '@connectrpc/connect'
import { createConnectTransport } from '@connectrpc/connect-web'
import { MutationHarness, mutationHeaders } from '@adiom-data/weave'
import type { ReactiveClient, MutationOperation, MutationRequest } from '@adiom-data/weave'
import { TaskMutationService } from './gen/demo/v1/mutations_pb.js'
import type { CreateTaskResponse, UpdateTaskResponse, DeleteTaskResponse } from './gen/demo/v1/mutations_pb.js'

export * from './gen/demo/v1/mutations_pb.js'
export { MutationOutcomeUnknownError } from '@adiom-data/weave'
export type { MutationOperation } from '@adiom-data/weave'

export { mutationReceiptKind as receiptKind, connectionContextHeader, mutationIdHeader } from '@adiom-data/weave'

export interface MutationOptions {
  readonly baseUrl: string
  readonly timeoutMs?: number
  readonly maxPending?: number
  readonly fetch?: typeof globalThis.fetch
  readonly onListenerError?: (error: unknown) => void
}
type Hooks<T> = Omit<MutationRequest<T>, 'send'>

/** Application-only concerns: typed Connect calls, headers, receipt format, and error contract. */
export class TaskMutations {
  private harness: MutationHarness
  private rpc: ReturnType<typeof createClient<typeof TaskMutationService>>
  private timeoutMs: number
  constructor(reactive: ReactiveClient, options: MutationOptions) {
    this.timeoutMs = options.timeoutMs ?? 15000
    this.rpc = createClient(TaskMutationService, createConnectTransport({ baseUrl: options.baseUrl, useBinaryFormat: true, fetch: options.fetch }))
    this.harness = new MutationHarness(reactive, {
      synchronizationTimeoutMs: this.timeoutMs, maxPending: options.maxPending, onListenerError: options.onListenerError,
      classifyError: error => error instanceof ConnectError && ![Code.Unknown, Code.Unavailable, Code.Canceled, Code.DeadlineExceeded, Code.Internal, Code.DataLoss].includes(error.code) ? 'rejected' : 'unknown',
    })
  }
  createTask(input: { readonly projectId: string; readonly title: string }, hooks: Hooks<CreateTaskResponse> = {}): MutationOperation<CreateTaskResponse> {
    const request = { projectId: input.projectId, title: input.title }
    return this.execute(headers => this.rpc.createTask(request, { timeoutMs: this.timeoutMs, headers }), hooks)
  }
  updateTask(input: { readonly projectId: string; readonly taskId: string; readonly completed: boolean }, hooks: Hooks<UpdateTaskResponse> = {}): MutationOperation<UpdateTaskResponse> {
    const request = { projectId: input.projectId, taskId: input.taskId, completed: input.completed }
    return this.execute(headers => this.rpc.updateTask(request, { timeoutMs: this.timeoutMs, headers }), hooks)
  }
  deleteTask(input: { readonly projectId: string; readonly taskId: string }, hooks: Hooks<DeleteTaskResponse> = {}): MutationOperation<DeleteTaskResponse> {
    const request = { projectId: input.projectId, taskId: input.taskId }
    return this.execute(headers => this.rpc.deleteTask(request, { timeoutMs: this.timeoutMs, headers }), hooks)
  }
  dispose(): void { this.harness.dispose() }
  private execute<T>(send: (headers: Record<string, string>) => Promise<T>, hooks: Hooks<T>): MutationOperation<T> {
    return this.harness.run({ ...hooks, send: context => send({
      ...mutationHeaders(context),
    }) })
  }
}
