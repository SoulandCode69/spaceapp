import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import type { InjectedSinks } from "@temporalio/worker";
import type { SpanExporter } from "@space/runtime";
import { createActivityObservabilityInterceptors } from "./activity-observability.js";
import { allowlistedSpanAttributes, correlationSpanAttributes } from "./span-attributes.js";
import type { WorkflowSpanSinks } from "./workflow-interceptors.js";

/**
 * Worker options for tracing: activity interceptor (always, for logs and
 * metrics), plus workflow interceptors and sinks only when span export is on.
 * Spread into Worker.create; with no exporter nothing is added to workflows.
 */
export function tracingWorkerOptions(spans: SpanExporter | null | undefined) {
  const sinks = createWorkflowSpanSinks(spans);
  let interceptorModule = fileURLToPath(new URL("./workflow-interceptors.js", import.meta.url));
  // Running from the TypeScript source (tests, worktree runs) has no built .js.
  if (!existsSync(interceptorModule)) interceptorModule = interceptorModule.replace(/\.js$/, ".ts");
  return {
    interceptors: {
      activity: [createActivityObservabilityInterceptors({ spans })],
      ...(sinks ? { workflowModules: [interceptorModule] } : {})
    },
    ...(sinks ? { sinks } : {})
  };
}

/**
 * Worker-side half of workflow tracing: turns the record the workflow
 * interceptor emits into an OTLP INTERNAL span. `callDuringReplay: false`
 * means replaying a history never exports a second span. The sink never
 * throws, so a broken exporter cannot fail a workflow task.
 */
export function createWorkflowSpanSinks(spans: SpanExporter | null | undefined): InjectedSinks<WorkflowSpanSinks> | undefined {
  if (!spans) return undefined;
  return {
    spans: {
      recordSpan: {
        callDuringReplay: false,
        fn(info, data) {
          try {
            spans.record({
              traceId: data.traceId,
              spanId: data.spanId,
              parentSpanId: data.parentSpanId,
              name: `workflow ${info.workflowType}`,
              startTimeUnixNano: BigInt(data.startMs) * 1_000_000n,
              endTimeUnixNano: BigInt(Math.max(data.startMs, data.endMs)) * 1_000_000n,
              attributes: allowlistedSpanAttributes({
                ...correlationSpanAttributes(data.correlation),
                "space.workflow_id": info.workflowId,
                "space.run_id": info.runId,
                "space.workflow_type": info.workflowType,
                "space.status": data.status,
                "space.reason_code": data.reasonCode,
                "space.category": data.category,
                "space.duration_ms": Math.max(0, data.endMs - data.startMs)
              }),
              error: data.status === "FAILED",
              kind: 1,
              ...(data.link ? { links: [data.link] } : {})
            });
          } catch {
            // Tracing must never fail workflow work.
          }
        }
      }
    }
  };
}
