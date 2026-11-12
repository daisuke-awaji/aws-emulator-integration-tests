import { CreateEventSourceMappingCommand, CreateFunctionCommand, GetEventSourceMappingCommand, LambdaClient, waitUntilFunctionActiveV2 } from "@aws-sdk/client-lambda";
import { CreateTableCommand, DynamoDBClient, PutItemCommand, waitUntilTableExists } from "@aws-sdk/client-dynamodb";
import { DynamoDBStreamsClient, DescribeStreamCommand, GetShardIteratorCommand, GetRecordsCommand } from "@aws-sdk/client-dynamodb-streams";
import { CloudWatchLogsClient, FilterLogEventsCommand } from "@aws-sdk/client-cloudwatch-logs";
import { zipFiles } from "../test/helpers/zip.ts";
const lambda = new LambdaClient({}); const ddb = new DynamoDBClient({}); const st = new DynamoDBStreamsClient({}); const logs = new CloudWatchLogsClient({});
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
let n = 0; const uid = (p: string) => `${p}-${Date.now()}-${n++}`;
async function table() { const T = uid("t"); const r = await ddb.send(new CreateTableCommand({ TableName: T, BillingMode: "PAY_PER_REQUEST", AttributeDefinitions: [{ AttributeName: "pk", AttributeType: "S" }], KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }], StreamSpecification: { StreamEnabled: true, StreamViewType: "KEYS_ONLY" } })); await waitUntilTableExists({ client: ddb, maxWaitTime: 30 }, { TableName: T }); return { T, arn: r.TableDescription!.LatestStreamArn! }; }
const put = (T: string, pk: string) => ddb.send(new PutItemCommand({ TableName: T, Item: { pk: { S: pk } } }));

console.log("=== A: Streams API の LATEST イテレータ ===");
{
  const { T, arn } = await table();
  await put(T, "old");
  const sh = (await st.send(new DescribeStreamCommand({ StreamArn: arn }))).StreamDescription!.Shards![0]!;
  const it = (await st.send(new GetShardIteratorCommand({ StreamArn: arn, ShardId: sh.ShardId, ShardIteratorType: "LATEST" }))).ShardIterator;
  await put(T, "new");
  const r = await st.send(new GetRecordsCommand({ ShardIterator: it }));
  console.log(" GetRecords(LATEST):", r.Records?.map((x) => x.dynamodb?.Keys?.pk?.S));
}

console.log("=== B: old を書いてから ESM(LATEST) を作るまでの間隔を変える ===");
const F = uid("f");
await lambda.send(new CreateFunctionCommand({ FunctionName: F, Runtime: "nodejs22.x", Handler: "index.handler", Role: "arn:aws:iam::000000000000:role/r", Timeout: 3, Code: { ZipFile: zipFiles({ "index.mjs": `export const handler = async (e) => { console.log("GOT " + e.Records.map(r => r.eventSourceARN.split("/")[1] + ":" + r.dynamodb.Keys.pk.S).join(",")); return {}; };` }) } }));
await waitUntilFunctionActiveV2({ client: lambda, maxWaitTime: 30 }, { FunctionName: F });
const trials: { T: string; waitMs: number; pollsToEnabled: number }[] = [];
for (const waitMs of [0, 250, 500, 750, 1000, 1500, 2000, 3000, 5000]) {
  const { T, arn } = await table();
  await put(T, "old");
  await sleep(waitMs); // ← old と ESM 作成の間隔
  const m = await lambda.send(new CreateEventSourceMappingCommand({ FunctionName: F, EventSourceArn: arn, StartingPosition: "LATEST", MaximumBatchingWindowInSeconds: 0 }));
  let polls = 0; for (; polls < 50; polls++) { const g = await lambda.send(new GetEventSourceMappingCommand({ UUID: m.UUID })); if (g.State === "Enabled") break; await sleep(100); }
  await sleep(500);
  await put(T, "new");
  trials.push({ T, waitMs, pollsToEnabled: polls });
}
await sleep(4000);
const got = ((await logs.send(new FilterLogEventsCommand({ logGroupName: `/aws/lambda/${F}` }))).events ?? []).map((e) => e.message!.trim()).filter((x) => x.startsWith("GOT ")).flatMap((x) => x.slice(4).split(","));
for (const t of trials) console.log(` gap(old→ESM)=${String(t.waitMs).padStart(4)}ms pollsToEnabled=${t.pollsToEnabled} ->`, got.filter((g) => g.startsWith(t.T + ":")).map((g) => g.split(":")[1]).join(",") || "(none)");
for (const c of [lambda, ddb, st, logs]) c.destroy();
