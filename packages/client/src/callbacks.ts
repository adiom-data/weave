/** Invoke without awaiting; report both synchronous errors and promise rejections.
 * The reporting sink must not throw synchronously. */
export function invokeSafely(callback: () => unknown, report: (error: unknown) => void): void {
  try {
    const result = callback()
    if (result && (typeof result === 'object' || typeof result === 'function') && 'then' in result) void Promise.resolve(result).catch(report)
  } catch (error) { report(error) }
}
