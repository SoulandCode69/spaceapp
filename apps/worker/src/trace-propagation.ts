/**
 * Temporal header carrying the W3C trace context between workflow, child
 * workflow, continue-as-new run and activity. Pure and ASCII only (a
 * traceparent is ASCII), so it works inside the workflow sandbox without a
 * payload converter or TextEncoder.
 */
export const TRACEPARENT_HEADER = "space-traceparent";
/** traceparent of the span this run or workflow continues (an OTLP span link). */
export const LINK_HEADER = "space-trace-link";

interface HeaderPayload {
  metadata?: Record<string, Uint8Array> | null;
  data?: Uint8Array | null;
}

function ascii(value: string): Uint8Array {
  const bytes = new Uint8Array(value.length);
  for (let i = 0; i < value.length; i += 1) bytes[i] = value.charCodeAt(i) & 0x7f;
  return bytes;
}

export function encodeTraceHeader(value: string): HeaderPayload {
  return { metadata: { encoding: ascii("binary/plain") }, data: ascii(value) };
}

export function decodeTraceHeader(headers: Record<string, unknown> | null | undefined, name: string): string | null {
  const payload = headers?.[name] as HeaderPayload | undefined;
  const data = payload?.data;
  if (!data || data.length === 0 || data.length > 128) return null;
  let text = "";
  for (let i = 0; i < data.length; i += 1) text += String.fromCharCode(data[i]!);
  return text;
}
