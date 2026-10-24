import {
  ContinueAsNew,
  isCancellation,
  proxySinks,
  uuid4,
  workflowInfo,
  type Sinks,
  type WorkflowInterceptorsFactory
} from "@temporalio/workflow";
import { categorizeFailureCode, formatTraceparent, isSafeErrorCode, parseTraceparent, type FailureCategory } from "@space/contracts";
import { readArgumentCorrelation } from "./trace-correlation.js";
import { LINK_HEADER, TRACEPARENT_HEADER, decodeTraceHeader, encodeTraceHeader } from "./trace-propagation.js";

/**
 * Workflow-side tracing. Workflow code does no I/O for it: the interceptors
 * only compute ids (deterministic `uuid4`, workflow time) and hand one plain
 * record per workflow run to a sink, which the worker process turns into an
 * OTLP span. The sink is registered with `callDuringReplay: false`, so replay
 * never emits a span again (workflow-span-sinks.ts). The trace context travels
 * to activities, child workflows and continue-as-new runs in a Temporal header.
 *
 * This file is loaded into the workflow sandbox through
 * `interceptors.workflowModules`; keep its imports free of Node APIs.
 */

export type WorkflowSpanStatus = "COMPLETED" | "FAILED" | "CANCELLED" | "CONTINUED_AS_NEW";

export interface WorkflowSpanData {
  traceId: string;
  spanId: string;
  parentSpanId: string;
  link: { traceId: string; spanId: string } | null;
  startMs: number;
  endMs: number;
  status: WorkflowSpanStatus;
  reasonCode: string | null;
  category: FailureCategory | null;
  /** Identifier-only correlation read from the workflow input (traceId, roomId, paneId, missionId). */
  correlation: Record<string, string>;
}

export interface WorkflowSpanSinks extends Sinks {
  spans: {
    recordSpan(data: WorkflowSpanData): void;
  };
}

function newSpanId(): string {
  const id = uuid4().replace(/-/g, "").slice(0, 16);
  return /^0+$/.test(id) ? "0000000000000001" : id;
}

function failureCode(error: unknown): string | null {
  let current = error as { type?: unknown; code?: unknown; cause?: unknown } | null | undefined;
  for (let depth = 0; current && typeof current === "object" && depth < 5; depth += 1) {
    if (isSafeErrorCode(current.code)) return current.code;
    if (isSafeErrorCode(current.type)) return current.type;
    current = current.cause as typeof current;
  }
  return null;
}

export const interceptors: WorkflowInterceptorsFactory = () => {
  const { spans } = proxySinks<WorkflowSpanSinks>();
  let active: { traceId: string; spanId: string; flags: string; parentTraceparent: string } | null = null;

  const propagate = (headers: Record<string, unknown>): Record<string, unknown> =>
    active ? { ...headers, [TRACEPARENT_HEADER]: encodeTraceHeader(formatTraceparent(active.traceId, active.spanId, active.flags)) } : headers;

  return {
    inbound: [{
      async execute(input, next) {
        const found = readArgumentCorrelation(input.args);
        const fromInput = parseTraceparent(found.traceparent);
        const continues = parseTraceparent(found.continuesTraceparent);
        const fromHeader = parseTraceparent(decodeTraceHeader(input.headers, TRACEPARENT_HEADER));
        const headerLink = parseTraceparent(decodeTraceHeader(input.headers, LINK_HEADER));
        const memoTraceparent = workflowInfo().memo?.traceparent;
        const fromMemo = parseTraceparent(memoTraceparent);
        // A continue-as-new run keeps its parent from the header; a background
        // continuation starts the trace named by its own input and links back;
        // otherwise the parent workflow's span, then the API request's span.
        const parent = headerLink ? fromHeader ?? fromInput ?? fromMemo
          : continues && fromInput ? fromInput
          : fromHeader ?? fromInput ?? fromMemo;
        if (!parent) return next(input);
        const linked = headerLink ?? continues;
        const traceId = parent.traceId;
        const spanId = newSpanId();
        const parentTraceparent = formatTraceparent(parent.traceId, parent.parentId, parent.flags);
        active = { traceId, spanId, flags: parent.flags, parentTraceparent };
        const startMs = Date.now();
        const finish = (status: WorkflowSpanStatus, error: unknown) => {
          const reasonCode = status === "FAILED" ? failureCode(error) : null;
          spans.recordSpan({
            traceId,
            spanId,
            parentSpanId: parent.parentId,
            link: linked ? { traceId: linked.traceId, spanId: linked.parentId } : null,
            startMs,
            endMs: Date.now(),
            status,
            reasonCode,
            category: status === "FAILED" ? categorizeFailureCode(reasonCode) : null,
            correlation
          });
        };
        const correlation: Record<string, string> = {};
        const missionId = found.missionId ?? found.roomAgentMissionId;
        for (const [key, value] of [["traceId", found.traceId], ["roomId", found.roomId], ["paneId", found.paneId], ["missionId", missionId]] as const) {
          if (value) correlation[key] = value;
        }
        try {
          const result = await next(input);
          finish("COMPLETED", null);
          return result;
        } catch (error) {
          finish(error instanceof ContinueAsNew ? "CONTINUED_AS_NEW" : isCancellation(error) ? "CANCELLED" : "FAILED", error);
          throw error;
        }
      }
    }],
    outbound: [{
      scheduleActivity: (input, next) => next({ ...input, headers: propagate(input.headers) as typeof input.headers }),
      scheduleLocalActivity: (input, next) => next({ ...input, headers: propagate(input.headers) as typeof input.headers }),
      startChildWorkflowExecution: (input, next) => next({ ...input, headers: propagate(input.headers) as typeof input.headers }),
      continueAsNew: (input, next) => next({
        ...input,
        headers: active
          ? {
              ...input.headers,
              [TRACEPARENT_HEADER]: encodeTraceHeader(active.parentTraceparent),
              [LINK_HEADER]: encodeTraceHeader(formatTraceparent(active.traceId, active.spanId, active.flags))
            } as typeof input.headers
          : input.headers
      })
    }]
  };
};
