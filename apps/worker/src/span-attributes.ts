/**
 * The only attributes a worker or workflow span may carry. Everything else is
 * dropped before export: prompts, model output, file paths, URLs, error
 * messages and any other user text never leave the process. String values must
 * also look like identifiers (no spaces, slashes beyond `:._-`), numbers must be
 * finite. Pure, so the workflow sandbox and the activity side share it.
 */
export const spanAttributeAllowlist = [
  "space.trace_id",
  "space.workflow_id",
  "space.run_id",
  "space.room_id",
  "space.pane_id",
  "space.mission_id",
  "space.workflow_type",
  "space.activity_type",
  "space.attempt",
  "space.status",
  "space.outcome",
  "space.reason_code",
  "space.category",
  "space.duration_ms"
] as const;

const allowed: ReadonlySet<string> = new Set(spanAttributeAllowlist);
const identifierPattern = /^[A-Za-z0-9:._-]{1,200}$/;

export function allowlistedSpanAttributes(raw: Readonly<Record<string, unknown>>): Record<string, string | number> {
  const attributes: Record<string, string | number> = {};
  for (const [key, value] of Object.entries(raw)) {
    if (!allowed.has(key)) continue;
    if (typeof value === "string" && identifierPattern.test(value)) attributes[key] = value;
    else if (typeof value === "number" && Number.isInteger(value)) attributes[key] = value;
  }
  return attributes;
}

/** Maps correlation ids (see trace-correlation.ts) to allowlisted span attributes. */
export function correlationSpanAttributes(correlation: Readonly<Record<string, string | undefined>>): Record<string, string> {
  const attributes: Record<string, string> = {};
  const names: Record<string, string> = {
    traceId: "space.trace_id",
    roomId: "space.room_id",
    paneId: "space.pane_id",
    missionId: "space.mission_id"
  };
  for (const [key, name] of Object.entries(names)) {
    const value = correlation[key];
    if (value) attributes[name] = value;
  }
  return attributes;
}
