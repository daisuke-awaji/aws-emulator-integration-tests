import { CreateEventSourceMappingCommand, CreateFunctionCommand, GetEventSourceMappingCommand, InvokeCommand, LambdaClient, waitUntilFunctionActiveV2 } from "@aws-sdk/client-lambda";
import { CreateQueueCommand, GetQueueAttributesCommand, SendMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import { CloudWatchLogsClient, FilterLogEventsCommand } from "@aws-sdk/client-cloudwatch-logs";
import { zipFiles } from "../test/helpers/zip.ts";

const lambda = new LambdaClient({});
const sqs = new SQSClient({ useQueueUrlAsEndpoint: false });
const logs = new CloudWatchLogsClient({});
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const role = "arn:aws:iam::000000000000:role/lambda-role";

// 1) モジュール可用性
const modProbe = `
export const handler = async () => {
  const out = {};
  for (const m of ["node:crypto","node:buffer","node:util","node:http","node:https","node:fs","node:path","node:zlib","@aws-sdk/client-dynamodb","@aws-sdk/client-sqs"]) {
    try { const x = await import(m); out[m] = "ok(" + Object.keys(x).length + ")"; } catch (e) { out[m] = "ERR " + String(e.message).slice(0,80); }
  }
  out.randomUUID = typeof globalThis.crypto?.randomUUID;
  out.Buffer = typeof globalThis.Buffer;
  out.TextEncoder = typeof globalThis.TextEncoder;
  out.setTimeout = typeof setTimeout;
  out.structuredClone = typeof structuredClone;
  return out;
};`;
const m = "mods-" + Date.now();
await lambda.send(new CreateFunctionCommand({ FunctionName: m, Runtime: "nodejs22.x", Handler: "index.handler", Role: role, Code: { ZipFile: zipFiles({ "index.mjs": modProbe }) } }));
await waitUntilFunctionActiveV2({ client: lambda, maxWaitTime: 30 }, { FunctionName: m });
const r = { FunctionError: "skipped", Payload: new TextEncoder().encode("") };
console.log("MODULES:", r.FunctionError ?? "", new TextDecoder().decode(r.Payload));

// 2) ESM: SQS -> Lambda
const handler = `
export const handler = async (event) => {
  console.log("BATCH", JSON.stringify(event.Records.map(r => ({ id: r.messageId, body: r.body, src: r.eventSource, arn: r.eventSourceARN, rc: r.attributes?.ApproximateReceiveCount }))));
  return { batchItemFailures: event.Records.filter(r => r.body.includes("fail")).map(r => ({ itemIdentifier: r.messageId })) };
};`;
const fn = "consumer-" + Date.now();
await lambda.send(new CreateFunctionCommand({ FunctionName: fn, Runtime: "nodejs22.x", Handler: "index.handler", Role: role, Timeout: 2, Code: { ZipFile: zipFiles({ "index.mjs": handler }) } }));
await waitUntilFunctionActiveV2({ client: lambda, maxWaitTime: 30 }, { FunctionName: fn });
const q = await sqs.send(new CreateQueueCommand({ QueueName: "esm-" + Date.now(), Attributes: { VisibilityTimeout: "2" } }));
const qa = await sqs.send(new GetQueueAttributesCommand({ QueueUrl: q.QueueUrl, AttributeNames: ["QueueArn"] }));
const esm = await lambda.send(new CreateEventSourceMappingCommand({ FunctionName: fn, EventSourceArn: qa.Attributes!.QueueArn, BatchSize: 10, FunctionResponseTypes: ["ReportBatchItemFailures"], MaximumBatchingWindowInSeconds: 0 }));
console.log("ESM create:", esm.State, esm.UUID, esm.FunctionResponseTypes);
for (let i = 0; i < 10; i++) {
  const g = await lambda.send(new GetEventSourceMappingCommand({ UUID: esm.UUID }));
  if (g.State === "Enabled") { console.log("ESM state Enabled after", i, "polls"); break; }
  await sleep(500);
}
const t0 = Date.now();
await sqs.send(new SendMessageCommand({ QueueUrl: q.QueueUrl, MessageBody: "ok-1" }));
await sqs.send(new SendMessageCommand({ QueueUrl: q.QueueUrl, MessageBody: "fail-1" }));
for (let i = 0; i < 20; i++) {
  await sleep(500);
  const a = await sqs.send(new GetQueueAttributesCommand({ QueueUrl: q.QueueUrl, AttributeNames: ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"] }));
  console.log(`t=${Date.now() - t0}ms visible=${a.Attributes!.ApproximateNumberOfMessages} inflight=${a.Attributes!.ApproximateNumberOfMessagesNotVisible}`);
}
try {
  const ev = await logs.send(new FilterLogEventsCommand({ logGroupName: `/aws/lambda/${fn}` }));
  console.log("LOGS:"); for (const e of ev.events ?? []) console.log("  ", e.message?.trim());
} catch (e) { console.log("LOGS ERR", (e as Error).name, (e as Error).message); }
for (const c of [lambda, sqs, logs]) c.destroy();
