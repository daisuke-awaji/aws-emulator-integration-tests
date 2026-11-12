import { CreateEventSourceMappingCommand, CreateFunctionCommand, GetEventSourceMappingCommand, LambdaClient, waitUntilFunctionActiveV2, type CreateEventSourceMappingCommandInput } from "@aws-sdk/client-lambda";
import { CreateTableCommand, DynamoDBClient, PutItemCommand, waitUntilTableExists } from "@aws-sdk/client-dynamodb";
import { CreateQueueCommand, GetQueueAttributesCommand, ReceiveMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import { CloudWatchLogsClient, FilterLogEventsCommand } from "@aws-sdk/client-cloudwatch-logs";
import { zipFiles } from "../test/helpers/zip.ts";
const lambda = new LambdaClient({}); const ddb = new DynamoDBClient({}); const sqs = new SQSClient({ useQueueUrlAsEndpoint: false }); const logs = new CloudWatchLogsClient({});
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const show = (e: unknown) => `${(e as Error).name}: ${(e as Error).message}`;
let n = 0; const uid = (p: string) => `${p}-${Date.now()}-${n++}`;
async function table() {
  const T = uid("t");
  const r = await ddb.send(new CreateTableCommand({ TableName: T, BillingMode: "PAY_PER_REQUEST", AttributeDefinitions: [{ AttributeName: "pk", AttributeType: "S" }], KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }], StreamSpecification: { StreamEnabled: true, StreamViewType: "NEW_IMAGE" } }));
  await waitUntilTableExists({ client: ddb, maxWaitTime: 30 }, { TableName: T });
  return { T, arn: r.TableDescription!.LatestStreamArn! };
}
async function fn(code: string) {
  const F = uid("f");
  await lambda.send(new CreateFunctionCommand({ FunctionName: F, Runtime: "nodejs22.x", Handler: "index.handler", Role: "arn:aws:iam::000000000000:role/r", Timeout: 3, Code: { ZipFile: zipFiles({ "index.mjs": code }) } }));
  await waitUntilFunctionActiveV2({ client: lambda, maxWaitTime: 30 }, { FunctionName: F });
  return F;
}
async function esm(i: CreateEventSourceMappingCommandInput) {
  const r = await lambda.send(new CreateEventSourceMappingCommand({ StartingPosition: "TRIM_HORIZON", BatchSize: 10, MaximumBatchingWindowInSeconds: 0, ...i }));
  for (let k = 0; k < 30; k++) { const g = await lambda.send(new GetEventSourceMappingCommand({ UUID: r.UUID })); if (g.State === "Enabled") break; await sleep(200); }
  return r;
}
async function gotLines(F: string) {
  try { const ev = await logs.send(new FilterLogEventsCommand({ logGroupName: `/aws/lambda/${F}` })); return (ev.events ?? []).map((e) => (e.message ?? "").trim()).filter((m) => m.startsWith("GOT ")); } catch { return []; }
}
async function put(T: string, ...pks: string[]) { for (const pk of pks) await ddb.send(new PutItemCommand({ TableName: T, Item: { pk: { S: pk } } })); }
// pk に "bad" を含むと失敗
const LOG = `console.log("GOT " + e.Records.map(r => r.dynamodb.Keys.pk.S).join(","));`;

console.log("=== A: throw / MaximumRetryAttempts=2 / OnFailure=SQS ===");
{
  const { T, arn } = await table();
  const F = await fn(`export const handler = async (e) => { ${LOG} if (e.Records.some(r => r.dynamodb.Keys.pk.S.includes("bad"))) throw new Error("boom"); return {}; };`);
  const q = await sqs.send(new CreateQueueCommand({ QueueName: uid("onfail") }));
  const qarn = (await sqs.send(new GetQueueAttributesCommand({ QueueUrl: q.QueueUrl, AttributeNames: ["QueueArn"] }))).Attributes!.QueueArn!;
  const m = await esm({ FunctionName: F, EventSourceArn: arn, MaximumRetryAttempts: 2, DestinationConfig: { OnFailure: { Destination: qarn } } });
  console.log("ESM:", m.MaximumRetryAttempts, JSON.stringify(m.DestinationConfig));
  await put(T, "a1", "bad1", "a2");
  await sleep(6000);
  await put(T, "after");
  await sleep(3000);
  console.log("invocations:", await gotLines(F));
  const r = await sqs.send(new ReceiveMessageCommand({ QueueUrl: q.QueueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 1 }));
  for (const x of r.Messages ?? []) console.log("ONFAILURE MSG:", x.Body);
}

console.log("=== B: BisectBatchOnFunctionError + MaxRetry=1 ===");
{
  const { T, arn } = await table();
  const F = await fn(`export const handler = async (e) => { ${LOG} if (e.Records.some(r => r.dynamodb.Keys.pk.S.includes("bad"))) throw new Error("boom"); return {}; };`);
  await esm({ FunctionName: F, EventSourceArn: arn, MaximumRetryAttempts: 1, BisectBatchOnFunctionError: true });
  // 先に書いて TRIM_HORIZON で1バッチにまとめる
  await sleep(500);
  await put(T, "b1", "b2", "b3", "bad", "b5", "b6");
  await sleep(10000);
  console.log("invocations:", await gotLines(F));
}

console.log("=== C: ReportBatchItemFailures (itemIdentifier = SequenceNumber) ===");
{
  const { T, arn } = await table();
  const F = await fn(`let first = true; export const handler = async (e) => { ${LOG}
    const bad = e.Records.find(r => r.dynamodb.Keys.pk.S.includes("bad"));
    if (bad && first) { first = false; return { batchItemFailures: [{ itemIdentifier: bad.dynamodb.SequenceNumber }] }; }
    return { batchItemFailures: [] }; };`);
  await esm({ FunctionName: F, EventSourceArn: arn, FunctionResponseTypes: ["ReportBatchItemFailures"] });
  await put(T, "c1", "c2", "bad", "c4");
  await sleep(5000);
  console.log("invocations:", await gotLines(F));
}

console.log("=== D: LATEST は作成前のレコードを読まない ===");
{
  const { T, arn } = await table();
  await put(T, "old");
  const F = await fn(`export const handler = async (e) => { ${LOG} return {}; };`);
  await esm({ FunctionName: F, EventSourceArn: arn, StartingPosition: "LATEST" });
  await sleep(1000);
  await put(T, "new");
  await sleep(3000);
  console.log("invocations:", await gotLines(F));
}

console.log("=== E: 検証系 ===");
{
  const { arn } = await table();
  const F = await fn(`export const handler = async () => ({});`);
  for (const [label, extra] of [
    ["AT_TIMESTAMP", { StartingPosition: "AT_TIMESTAMP", StartingPositionTimestamp: new Date() }],
    ["BatchSize 10001", { BatchSize: 10001 }],
    ["MaxRetry 10001", { MaximumRetryAttempts: 10001 }],
    ["ParallelizationFactor 11", { ParallelizationFactor: 11 }],
    ["ParallelizationFactor 2", { ParallelizationFactor: 2 }],
  ] as const) {
    try { const r = await lambda.send(new CreateEventSourceMappingCommand({ FunctionName: F, EventSourceArn: arn, StartingPosition: "TRIM_HORIZON", ...extra } as CreateEventSourceMappingCommandInput)); console.log(label, "ACCEPTED", r.ParallelizationFactor ?? ""); } catch (e) { console.log(label, "->", show(e)); }
  }
  // ストリームのないテーブル ARN
  const T2 = uid("nostream");
  const r2 = await ddb.send(new CreateTableCommand({ TableName: T2, BillingMode: "PAY_PER_REQUEST", AttributeDefinitions: [{ AttributeName: "pk", AttributeType: "S" }], KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }] }));
  try { await lambda.send(new CreateEventSourceMappingCommand({ FunctionName: F, EventSourceArn: r2.TableDescription!.TableArn! + "/stream/2026-01-01T00:00:00.000", StartingPosition: "TRIM_HORIZON" })); console.log("fake stream arn ACCEPTED"); } catch (e) { console.log("fake stream arn ->", show(e)); }
}
for (const c of [lambda, ddb, sqs, logs]) c.destroy();
