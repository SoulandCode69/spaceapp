// Workflows for workflow-tracing.test.ts. They import only @temporalio/workflow
// so the workflow bundle stays small; the activity is a stub.
import { continueAsNew, executeChild, proxyActivities } from "@temporalio/workflow";

const { echoStep } = proxyActivities<{ echoStep(input: unknown): Promise<string> }>({ startToCloseTimeout: "10 seconds" });

export async function childWorkflow(input: { traceId: string }): Promise<string> {
  return echoStep(input);
}

export async function parentWorkflow(input: { traceId: string; roomId: string; prompt: string }): Promise<string> {
  await echoStep(input);
  await executeChild(childWorkflow, { args: [{ traceId: input.traceId }] });
  return "done";
}

export async function continuingWorkflow(input: { traceId: string; round: number }): Promise<string> {
  if (input.round === 0) await continueAsNew<typeof continuingWorkflow>({ ...input, round: 1 });
  return echoStep(input);
}
