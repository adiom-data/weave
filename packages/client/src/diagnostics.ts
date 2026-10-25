/** The application, not the runtime, assigns meaning to transport close codes. */
export type FailureAction = 'retry' | 'pause' | 'clear'
export type FailureKind = 'connect' | 'transport' | 'close' | 'protocol' | 'connection-timeout' | 'heartbeat-timeout' | 'send'
export interface ConnectionFailure {
  readonly kind: FailureKind
  readonly error: Error
  readonly close?: Readonly<{ code: number; reason: string; wasClean: boolean }>
}
// Diagnostics intentionally omit URLs, credentials, payloads, close reasons,
// and arbitrary exception text. The failure-policy hook receives full details.
export interface FailureSummary {
  readonly kind: FailureKind
  readonly closeCode?: number
  readonly wasClean?: boolean
}
export type DiagnosticEvent = Readonly<
  | { type: 'connecting' | 'connected' | 'disconnected' | 'handshake' }
  | { type: 'failure'; failure: FailureSummary; action: FailureAction }
  | { type: 'retry-scheduled'; attempt: number; delayMs: number }
  | { type: 'projection-opened' | 'projection-closed' | 'projection-ready'; projectionId: string }
  | { type: 'projection-error'; projectionId: string; code: string }
  | { type: 'window-settled'; projectionId: string; failed: boolean }
>
export interface StoreDiagnostics {
  readonly entities: Readonly<{ available: number; pending: number }>
  readonly queries: number
  readonly occurrences: number
  readonly subscriptions: number
  readonly trackedKeys: number
  readonly optimistic: Readonly<{ layers: number; entities: number; targets: number; materializedEntities: number }>
}
export interface ClientDiagnostics {
  readonly connection: 'disconnected' | 'connecting' | 'connected'
  readonly activeProjections: number
  readonly pendingWindowRequests: number
  readonly connectionAttempts: number
  readonly retriesScheduled: number
  readonly failures: number
  readonly messagesReceived: number
  readonly bytesReceived: number
  readonly lastFailure?: FailureSummary
  readonly store: StoreDiagnostics
}
