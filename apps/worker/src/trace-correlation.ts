import { parseTraceparent, traceIdFromTraceparent } from "@space/contracts";
import { logIdentifier } from "./worker-log.js";

/**
 * Identifier-only correlation read from workflow or activity arguments. Pure
 * (no Node APIs), so the workflow interceptors may bundle it into the
 * deterministic workflow sandbox. Prompts, content and messages are never read.
 */

// Keys whose values are identifiers.
const correlationKeys = [
  "traceId",
  "traceparent",
  "continuesTraceparent",
  "requestId",
  "roomId",
  "paneId",
  "missionId",
  "roomAgentMissionId",
  "agentRunId",
  "agentSessionId",
  "actionId",
  "goalId",
  "dotId",
  "turnId",
  "cliTaskId",
  "revisionId"
] as const;
const nestedKeys = ["turn", "item", "bridge", "input", "turnInput"] as const;

export type CorrelationKey = (typeof correlationKeys)[number];

function readCorrelation(value: unknown, into: Partial<Record<CorrelationKey, string>>, depth: number): void {
  if (!value || typeof value !== "object" || Array.isArray(value)) return;
  const record = value as Record<string, unknown>;
  for (const key of correlationKeys) {
    if (into[key] !== undefined) continue;
    const identifier = key === "traceparent" || key === "continuesTraceparent"
      ? (parseTraceparent(record[key]) ? (record[key] as string) : null)
      : logIdentifier(record[key]);
    if (identifier !== null) into[key] = identifier;
  }
  if (depth <= 0) return;
  for (const key of nestedKeys) readCorrelation(record[key], into, depth - 1);
}

/** All allowlisted identifier fields found in the arguments (two levels deep). */
export function readArgumentCorrelation(args: readonly unknown[]): Partial<Record<CorrelationKey, string>> {
  const found: Partial<Record<CorrelationKey, string>> = {};
  for (const arg of args) readCorrelation(arg, found, 2);
  return found;
}

/** The W3C trace context the API put into the arguments, if any. */
export function argumentTraceparent(args: readonly unknown[]) {
  return parseTraceparent(readArgumentCorrelation(args).traceparent);
}

/** Allowlisted correlation fields from activity arguments (identifiers only). */
export function activityCorrelation(args: readonly unknown[]): Record<string, string> {
  const found = readArgumentCorrelation(args);
  const correlation: Record<string, string> = {};
  if (found.traceId) correlation.traceId = found.traceId;
  const w3cTraceId = traceIdFromTraceparent(found.traceparent);
  if (w3cTraceId) correlation.w3cTraceId = w3cTraceId;
  if (found.requestId && found.requestId !== found.traceId) correlation.parentTraceId = found.requestId;
  const missionId = found.missionId ?? found.roomAgentMissionId;
  if (missionId) correlation.missionId = missionId;
  for (const key of ["roomId", "paneId", "agentRunId", "agentSessionId", "actionId", "goalId", "dotId", "turnId", "cliTaskId", "revisionId"] as const) {
    if (found[key]) correlation[key] = found[key]!;
  }
  return correlation;
}
