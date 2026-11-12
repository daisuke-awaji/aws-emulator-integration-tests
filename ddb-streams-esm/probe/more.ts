import { CreateEventSourceMappingCommand, CreateFunctionCommand, GetEventSourceMappingCommand, LambdaClient, UpdateFunctionCodeCommand, waitUntilFunctionActiveV2, waitUntilFunctionUpdatedV2, type CreateEventSourceMappingCommandInput } from "@aws-sdk/client-lambda";
import { CreateTableCommand, DynamoDBClient, PutItemCommand, UpdateItemCommand, DeleteItemCommand, waitUntilTableExists, type StreamViewType } from "@aws-sdk/client-dynamodb";
import { CloudWatchLogsClient, FilterLogEventsCommand } from "@aws-sdk/client-cloudwatch-logs";
import { zipFiles } from "../test/helpers/zip.ts";
const lambda = new LambdaClient({}); const ddb = new DynamoDBClient({}); const logs = new CloudWatchLogsClient({});
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
let n = 0; const uid = (p: string) => `${p}-${Date.now()}-${n++}`;
async function table(view: StreamViewType = "NEW_AND_OLD_IMAGES") { const T = uid("t"); const r = await ddb.send(new CreateTableCommand({ TableName: T, BillingMode: "PAY_PER_REQUEST", AttributeDefinitions: [{ AttributeName: "pk", AttributeType: "S" }], KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }], StreamSpecification: { StreamEnabled: true, StreamViewType: view } })); await waitUntilTableExists({ client: ddb, maxWaitTime: 30 }, { TableName: T }); return { T, arn: r.TableDescription!.LatestStreamArn! }; }
async function fn(code: string) { const F = uid("f"); await lambda.send(new CreateFunctionCommand({ FunctionName: F, Runtime: "nodejs22.x", Handler: "index.handler", Role: "arn:aws:iam::000000000000:role/r", Timeout: 3, Code: { ZipFile: zipFiles({ "index.mjs": code }) } })); await waitUntilFunctionActiveV2({ client: lambda, maxWaitTime: 30 }, { FunctionName: F }); return F; }
async function esm(i: CreateEventSourceMappingCommandInput) { const r = await lambda.send(new CreateEventSourceMappingCommand({ StartingPosition: "TRIM_HORIZON", BatchSize: 10, MaximumBatchingWindowInSeconds: 0, ...i })); for (let k = 0; k < 30; k++) { const g = await lambda.send(new GetEventSourceMappingCommand({ UUID: r.UUID })); if (g.State === "Enabled") break; await sleep(200); } return r; }
async function events(F: string) { try { const ev = await logs.send(new FilterLogEventsCommand({ logGroupName: `/aws/lambda/${F}` })); return (ev.events ?? []).map((e) => (e.message ?? "").trim()).filter((m) => m.startsWith("EVENT ")).map((m) => JSON.parse(m.slice(6))); } catch { return []; } }
const DUMP = `export const handler = async (e) => { console.log("EVENT " + JSON.stringify(e)); return {}; };`;

console.log("=== 1: StreamViewType ごとの dynamodb フィールド ===");
for (const view of ["KEYS_ONLY", "NEW_IMAGE", "OLD_IMAGE", "NEW_AND_OLD_IMAGES"] as const) {
  const { T, arn } = await table(view); const F = await fn(DUMP);
  await ddb.send(new PutItemCommand({ TableName: T, Item: { pk: { S: "k" }, v: { N: "1" } } }));
  await ddb.send(new UpdateItemCommand({ TableName: T, Key: { pk: { S: "k" } }, UpdateExpression: "SET v = :v", ExpressionAttributeValues: { ":v": { N: "2" } } }));
  await ddb.send(new DeleteItemCommand({ TableName: T, Key: { pk: { S: "k" } } }));
  await esm({ FunctionName: F, EventSourceArn: arn });
  await sleep(2500);
  const recs = (await events(F)).flatMap((e) => e.Records);
  console.log(view.padEnd(19), recs.map((r: any) => `${r.eventName}[${Object.keys(r.dynamodb).filter((k) => /Image|Keys/.test(k)).join("+")}]`).join(" "));
}

console.log("=== 2: FilterCriteria (eventName=INSERT かつ NewImage.v.N >= 10) ===");
{
  const { T, arn } = await table(); const F = await fn(DUMP);
  await esm({ FunctionName: F, EventSourceArn: arn, FilterCriteria: { Filters: [{ Pattern: JSON.stringify({ eventName: ["INSERT"], dynamodb: { NewImage: { v: { N: [{ numeric: [">=", 10] }] } } } }) }] } });
  for (const [pk, v] of [["small", 1], ["big", 50]] as const) await ddb.send(new PutItemCommand({ TableName: T, Item: { pk: { S: pk }, v: { N: String(v) } } }));
  await ddb.send(new UpdateItemCommand({ TableName: T, Key: { pk: { S: "big" } }, UpdateExpression: "SET v = :v", ExpressionAttributeValues: { ":v": { N: "99" } } }));
  await sleep(2500);
  console.log(" delivered:", (await events(F)).flatMap((e) => e.Records).map((r: any) => r.eventName + ":" + r.dynamodb.Keys.pk.S));
}

console.log("=== 3: throw でシャードが止まり、UpdateFunctionCode で再開・順序維持 ===");
{
  const { T, arn } = await table();
  const F = await fn(`export const handler = async (e) => { console.log("EVENT " + JSON.stringify({ Records: e.Records.map(r => ({ k: r.dynamodb.Keys.pk.S })), v: "bad" })); throw new Error("boom"); };`);
  await esm({ FunctionName: F, EventSourceArn: arn, BatchSize: 2 });
  for (const pk of ["r1", "r2", "r3", "r4", "r5"]) await ddb.send(new PutItemCommand({ TableName: T, Item: { pk: { S: pk } } }));
  await sleep(3000);
  const before = await events(F);
  console.log(" while failing:", before.length, "calls, keys seen:", [...new Set(before.flatMap((e: any) => e.Records.map((r: any) => r.k)))].join(","));
  await lambda.send(new UpdateFunctionCodeCommand({ FunctionName: F, ZipFile: zipFiles({ "index.mjs": `export const handler = async (e) => { console.log("EVENT " + JSON.stringify({ Records: e.Records.map(r => ({ k: r.dynamodb.Keys.pk.S })), v: "good" })); return {}; };` }) }));
  await waitUntilFunctionUpdatedV2({ client: lambda, maxWaitTime: 30 }, { FunctionName: F });
  await sleep(4000);
  const after = (await events(F)).filter((e: any) => e.v === "good");
  console.log(" after fix:", after.map((e: any) => "[" + e.Records.map((r: any) => r.k).join(",") + "]").join(" "));
}

console.log("=== 4: Bisect（全 6 件を 1 バッチにしてから ESM 作成）===");
{
  const { T, arn } = await table();
  for (const pk of ["b1", "b2", "b3", "bad", "b5", "b6"]) await ddb.send(new PutItemCommand({ TableName: T, Item: { pk: { S: pk } } }));
  const F = await fn(`export const handler = async (e) => { console.log("EVENT " + JSON.stringify({ Records: e.Records.map(r => ({ k: r.dynamodb.Keys.pk.S })) })); if (e.Records.some(r => r.dynamodb.Keys.pk.S === "bad")) throw new Error("boom"); return {}; };`);
  await esm({ FunctionName: F, EventSourceArn: arn, BisectBatchOnFunctionError: true, MaximumRetryAttempts: 2 });
  await sleep(5000);
  console.log(" batch sizes:", (await events(F)).map((e: any) => e.Records.length).join(","));
}
for (const c of [lambda, ddb, logs]) c.destroy();
