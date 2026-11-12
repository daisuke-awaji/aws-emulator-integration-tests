import { CreateEventSourceMappingCommand, CreateFunctionCommand, GetEventSourceMappingCommand, InvokeCommand, LambdaClient, waitUntilFunctionActiveV2, type CreateEventSourceMappingCommandInput } from "@aws-sdk/client-lambda";
import { CreateTableCommand, DynamoDBClient, PutItemCommand, waitUntilTableExists } from "@aws-sdk/client-dynamodb";
import { CreateQueueCommand, GetQueueAttributesCommand, ReceiveMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import { CloudWatchLogsClient, FilterLogEventsCommand } from "@aws-sdk/client-cloudwatch-logs";
import { zipFiles } from "../test/helpers/zip.ts";
const lambda = new LambdaClient({}); const ddb = new DynamoDBClient({}); const sqs = new SQSClient({ useQueueUrlAsEndpoint: false }); const logs = new CloudWatchLogsClient({});
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
let n = 0; const uid = (p: string) => `${p}-${Date.now()}-${n++}`;
async function table() { const T = uid("t"); const r = await ddb.send(new CreateTableCommand({ TableName: T, BillingMode: "PAY_PER_REQUEST", AttributeDefinitions: [{ AttributeName: "pk", AttributeType: "S" }], KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }], StreamSpecification: { StreamEnabled: true, StreamViewType: "NEW_IMAGE" } })); await waitUntilTableExists({ client: ddb, maxWaitTime: 30 }, { TableName: T }); return { T, arn: r.TableDescription!.LatestStreamArn! }; }
async function fn(code: string) { const F = uid("f"); await lambda.send(new CreateFunctionCommand({ FunctionName: F, Runtime: "nodejs22.x", Handler: "index.handler", Role: "arn:aws:iam::000000000000:role/r", Timeout: 3, Code: { ZipFile: zipFiles({ "index.mjs": code }) } })); await waitUntilFunctionActiveV2({ client: lambda, maxWaitTime: 30 }, { FunctionName: F }); return F; }
async function esm(i: CreateEventSourceMappingCommandInput) { const r = await lambda.send(new CreateEventSourceMappingCommand({ StartingPosition: "TRIM_HORIZON", BatchSize: 10, MaximumBatchingWindowInSeconds: 0, ...i })); for (let k = 0; k < 30; k++) { const g = await lambda.send(new GetEventSourceMappingCommand({ UUID: r.UUID })); if (g.State === "Enabled") break; await sleep(200); } return r; }
async function lines(F: string, p: string) { try { const ev = await logs.send(new FilterLogEventsCommand({ logGroupName: `/aws/lambda/${F}` })); return (ev.events ?? []).filter((e) => (e.message ?? "").startsWith(p)).map((e) => ({ t: e.timestamp!, m: e.message!.trim() })); } catch { return []; } }

console.log("=== 1: ウォームスタート（モジュール状態の保持）===");
{
  const F = await fn(`let count = 0; const loadedAt = Date.now(); export const handler = async () => ({ count: ++count, loadedAt, init: process.env.AWS_LAMBDA_INITIALIZATION_TYPE });`);
  for (let i = 0; i < 3; i++) { const r = await lambda.send(new InvokeCommand({ FunctionName: F })); console.log(" invoke", i, new TextDecoder().decode(r.Payload)); }
}

console.log("=== 2: MaximumRetryAttempts=0 + OnFailure, 25 秒観測（呼び出し時刻）===");
{
  const { T, arn } = await table();
  const F = await fn(`export const handler = async (e) => { console.log("GOT " + e.Records.map(r => r.dynamodb.Keys.pk.S).join(",")); throw new Error("boom"); };`);
  const q = await sqs.send(new CreateQueueCommand({ QueueName: uid("onfail") }));
  const qarn = (await sqs.send(new GetQueueAttributesCommand({ QueueUrl: q.QueueUrl, AttributeNames: ["QueueArn"] }))).Attributes!.QueueArn!;
  const m = await esm({ FunctionName: F, EventSourceArn: arn, MaximumRetryAttempts: 0, DestinationConfig: { OnFailure: { Destination: qarn } } });
  const g = await lambda.send(new GetEventSourceMappingCommand({ UUID: m.UUID }));
  console.log(" Get:", "MaxRetry", g.MaximumRetryAttempts, "Bisect", g.BisectBatchOnFunctionError, "Dest", JSON.stringify(g.DestinationConfig), "LastProcessingResult", g.LastProcessingResult);
  const t0 = Date.now();
  await ddb.send(new PutItemCommand({ TableName: T, Item: { pk: { S: "x" } } }));
  await sleep(25000);
  const ls = await lines(F, "GOT ");
  console.log(" invocations:", ls.length, "offsets(ms):", ls.map((l) => l.t - t0).join(","));
  const g2 = await lambda.send(new GetEventSourceMappingCommand({ UUID: m.UUID }));
  console.log(" after: State", g2.State, "LastProcessingResult", g2.LastProcessingResult);
  const r = await sqs.send(new ReceiveMessageCommand({ QueueUrl: q.QueueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 1 }));
  console.log(" onFailure messages:", (r.Messages ?? []).length, (r.Messages ?? []).map((x) => x.Body?.slice(0, 300)));
}
for (const c of [lambda, ddb, sqs, logs]) c.destroy();
