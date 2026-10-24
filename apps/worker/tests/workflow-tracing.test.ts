import { createServer, type Server } from "node:http";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "vitest";
import { MetricsRegistry, createOtlpSpanExporter, type ServerSpan, type SpanExporter } from "@space/runtime";
import { createWorkerMetrics } from "../src/worker-metrics.js";
import { observeActivityExecution } from "../src/activity-observability.js";
import { allowlistedSpanAttributes, spanAttributeAllowlist } from "../src/span-attributes.js";
import { LINK_HEADER, TRACEPARENT_HEADER, decodeTraceHeader, encodeTraceHeader } from "../src/trace-propagation.js";
import { createWorkflowSpanSinks, tracingWorkerOptions } from "../src/workflow-span-sinks.js";

const traceId = "4bf92f3577b34da6a3ce929d0e0e4736";
const apiSpanId = "00f067aa0ba902b7";
const traceparent = `00-${traceId}-${apiSpanId}-01`;

function recordingExporter(): SpanExporter & { spans: ServerSpan[] } {
  const spans: ServerSpan[] = [];
  return { spans, record: (span) => { spans.push(span); }, flush: async () => undefined, close: async () => undefined, stats: () => ({ exported: 0, dropped: 0, failed: 0 }) };
}

describe("span attribute allowlist", () => {
  it("drops prompts, paths, URLs, error messages and unknown keys", () => {
    const attributes = allowlistedSpanAttributes({
      "space.room_id": "room:1",
      "space.attempt": 2,
      "space.reason_code": "TOOL_UNAVAILABLE",
      "space.status": "this is a free text error message",
      "space.workflow_id": "https://example.test/secret?token=1",
      "space.pane_id": "/home/user/project/file.ts",
      "space.prompt": "SECRET prompt",
      "space.duration_ms": 1.5,
      "http.url": "http://x"
    });
    expect(attributes).toEqual({ "space.room_id": "room:1", "space.attempt": 2, "space.reason_code": "TOOL_UNAVAILABLE" });
    expect(Object.keys(attributes).every((key) => (spanAttributeAllowlist as readonly string[]).includes(key))).toBe(true);
  });

  it("exports only allowlisted attributes for activity spans", async () => {
    const exporter = recordingExporter();
    const metrics = createWorkerMetrics(new MetricsRegistry());
    const info = { activityType: "runTurn", activityId: "1", attempt: 3, currentAttemptScheduledTimestampMs: 1, workflowType: "parentWorkflow", workflowExecution: { workflowId: "wf:1", runId: "run-1" } } as never;
    const prompt = "SECRET prompt text /home/user/file";
    await observeActivityExecution(info, [{ traceId: "req:abc", roomId: "room:1", paneId: "pane:1", prompt, turn: { prompt, traceparent } }],
      async () => ({ status: "FAILED", error: prompt }), { metrics, spans: exporter });
    const [span] = exporter.spans;
    expect(span).toBeDefined();
    expect(Object.keys(span!.attributes).every((key) => (spanAttributeAllowlist as readonly string[]).includes(key))).toBe(true);
    expect(JSON.stringify(span!.attributes)).not.toContain("SECRET");
    expect(span!.attributes).toMatchObject({ "space.attempt": 3, "space.outcome": "FAILED_RESULT", "space.reason_code": "ACTIVITY_RESULT_FAILED",
      "space.room_id": "room:1", "space.pane_id": "pane:1", "space.workflow_id": "wf:1", "space.run_id": "run-1" });
  });
});

describe("trace propagation", () => {
  it("round-trips a traceparent through a Temporal header payload", () => {
    const headers = { [TRACEPARENT_HEADER]: encodeTraceHeader(traceparent) };
    expect(decodeTraceHeader(headers, TRACEPARENT_HEADER)).toBe(traceparent);
    expect(decodeTraceHeader(headers, LINK_HEADER)).toBeNull();
  });

  it("parents an activity span under the workflow span named by the header", async () => {
    const exporter = recordingExporter();
    const metrics = createWorkerMetrics(new MetricsRegistry());
    const info = { activityType: "runTurn", activityId: "1", attempt: 1, currentAttemptScheduledTimestampMs: 1 } as never;
    const workflowSpanId = "aaaaaaaaaaaaaaaa";
    await observeActivityExecution(info, [{ traceparent }], async () => "ok",
      { metrics, spans: exporter, traceparentHeader: `00-${traceId}-${workflowSpanId}-01` });
    expect(exporter.spans[0]).toMatchObject({ traceId, parentSpanId: workflowSpanId });
  });
});

describe("workflow span sink", () => {
  it("is registered with callDuringReplay false so replay emits no spans", () => {
    expect(createWorkflowSpanSinks(null)).toBeUndefined();
    const sinks = createWorkflowSpanSinks(recordingExporter())!;
    expect(sinks.spans!.recordSpan!.callDuringReplay).toBe(false);
  });

  it("adds no workflow interceptors or sinks when export is off", () => {
    const options = tracingWorkerOptions(null);
    expect(options.interceptors).not.toHaveProperty("workflowModules");
    expect(options).not.toHaveProperty("sinks");
  });

  it("records a span with a link and only allowlisted attributes", () => {
    const exporter = recordingExporter();
    const sinks = createWorkflowSpanSinks(exporter)!;
    sinks.spans!.recordSpan!.fn({ workflowType: "parentWorkflow", workflowId: "wf:1", runId: "run-2" } as never, {
      traceId, spanId: "bbbbbbbbbbbbbbbb", parentSpanId: apiSpanId, link: { traceId, spanId: "cccccccccccccccc" },
      startMs: 1000, endMs: 1500, status: "FAILED", reasonCode: "TOOL_UNAVAILABLE", category: "TOOL_UNAVAILABLE",
      correlation: { traceId: "req:abc", roomId: "room:1", prompt: "SECRET" }
    });
    expect(exporter.spans[0]).toMatchObject({ name: "workflow parentWorkflow", parentSpanId: apiSpanId, error: true, kind: 1,
      links: [{ traceId, spanId: "cccccccccccccccc" }], startTimeUnixNano: 1_000_000_000n, endTimeUnixNano: 1_500_000_000n });
    expect(JSON.stringify(exporter.spans[0]!.attributes)).not.toContain("SECRET");
    expect(exporter.spans[0]!.attributes).toMatchObject({ "space.workflow_id": "wf:1", "space.run_id": "run-2", "space.room_id": "room:1", "space.duration_ms": 500 });
  });
});

// Real Temporal test server + fake OTLP collector. Needs @temporalio/testing and
// the dev server it downloads; skipped (not failed) when they are unavailable.
interface OtlpSpan { traceId: string; spanId: string; parentSpanId?: string; name: string; links?: Array<{ traceId: string; spanId: string }>; attributes: Array<{ key: string; value: Record<string, string> }> }

describe("Temporal integration: one connected trace", () => {
  const closers: Array<() => Promise<unknown>> = [];
  afterEach(async () => { for (const close of closers.splice(0).reverse()) await close().catch(() => undefined); });

  it("exports workflow, child workflow and activity spans under the API span and replays silently", async (context) => {
    let testing: typeof import("@temporalio/testing");
    let env: Awaited<ReturnType<typeof testing.TestWorkflowEnvironment.createLocal>>;
    try {
      testing = await import("@temporalio/testing");
      env = await testing.TestWorkflowEnvironment.createLocal();
    } catch {
      context.skip();
      return;
    }
    closers.push(() => env.teardown());
    const { Worker } = await import("@temporalio/worker");

    const received: OtlpSpan[] = [];
    const collector: Server = createServer((request, response) => {
      const chunks: Buffer[] = [];
      request.on("data", (chunk: Buffer) => chunks.push(chunk));
      request.on("end", () => {
        if (request.url === "/v1/traces") {
          const body = JSON.parse(Buffer.concat(chunks).toString("utf8"));
          for (const resource of body.resourceSpans) for (const scope of resource.scopeSpans) received.push(...scope.spans);
        }
        response.writeHead(200).end("{}");
      });
    });
    await new Promise<void>((resolve) => collector.listen(0, "127.0.0.1", resolve));
    closers.push(() => new Promise((resolve) => collector.close(resolve)));
    const port = (collector.address() as { port: number }).port;
    const exporter = createOtlpSpanExporter({ endpoint: `http://127.0.0.1:${port}`, serviceName: "space-worker-test", flushIntervalMs: 60_000 });
    closers.push(() => exporter.close());

    const workflowsPath = fileURLToPath(new URL("./support/tracing-workflows.ts", import.meta.url));
    const contractsSource = fileURLToPath(new URL("../../../packages/contracts/src/index.ts", import.meta.url));
    const bundlerOptions = {
      webpackConfigHook: (config: { resolve?: Record<string, unknown> }) => ({
        ...config,
        resolve: { ...config.resolve, extensionAlias: { ".js": [".ts", ".js"] }, alias: { "@space/contracts$": contractsSource } }
      })
    };
    const tracing = tracingWorkerOptions(exporter);
    const taskQueue = `tracing-${Date.now()}`;
    const worker = await Worker.create({
      connection: env.nativeConnection,
      taskQueue,
      workflowsPath,
      bundlerOptions,
      activities: { echoStep: async () => "ok" },
      ...tracing
    } as never);

    const spans = () => received.map((span) => ({ ...span, attrs: Object.fromEntries(span.attributes.map((a) => [a.key, Object.values(a.value)[0]])) }));
    const input = { traceId: "req:abc123", roomId: "room:1", prompt: "SECRET prompt text", traceparent };
    const [parentHandle, continuedHandle] = await worker.runUntil(async () => {
      const parent = await env.client.workflow.start("parentWorkflow", { taskQueue, workflowId: `parent-${taskQueue}`, args: [input] });
      await parent.result();
      const continued = await env.client.workflow.start("continuingWorkflow", { taskQueue, workflowId: `continue-${taskQueue}`, args: [{ traceId: "req:abc123", round: 0, traceparent }] });
      await continued.result();
      return [parent, continued] as const;
    });
    await exporter.flush();

    const all = spans();
    expect(all.every((span) => span.traceId === traceId)).toBe(true);
    const workflowSpans = all.filter((span) => span.name.startsWith("workflow "));
    const parentSpan = workflowSpans.find((span) => span.name === "workflow parentWorkflow")!;
    const childSpan = workflowSpans.find((span) => span.name === "workflow childWorkflow")!;
    expect(parentSpan.parentSpanId).toBe(apiSpanId);
    expect(childSpan.parentSpanId).toBe(parentSpan.spanId);
    const activitySpans = all.filter((span) => span.name === "activity echoStep");
    expect(activitySpans.filter((span) => span.parentSpanId === parentSpan.spanId)).toHaveLength(1);
    expect(activitySpans.filter((span) => span.parentSpanId === childSpan.spanId)).toHaveLength(1);
    expect(JSON.stringify(received)).not.toContain("SECRET");

    // Continue-as-new: the second run links to the first run's span.
    const runs = workflowSpans.filter((span) => span.name === "workflow continuingWorkflow");
    expect(runs).toHaveLength(2);
    const linked = runs.find((span) => span.links?.length)!;
    const first = runs.find((span) => span !== linked)!;
    expect(linked.links![0]!.spanId).toBe(first.spanId);

    // Replaying a recorded history must not export anything.
    const before = received.length;
    const history = await parentHandle.fetchHistory();
    await Worker.runReplayHistory({ workflowsPath, bundlerOptions, ...tracing } as never, history, parentHandle.workflowId);
    await exporter.flush();
    expect(received.length).toBe(before);
    void continuedHandle;
  }, 120_000);
});
