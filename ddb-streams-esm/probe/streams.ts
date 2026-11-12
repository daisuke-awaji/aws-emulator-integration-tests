import { CreateEventSourceMappingCommand, CreateFunctionCommand, GetEventSourceMappingCommand, LambdaClient, waitUntilFunctionActiveV2 } from "@aws-sdk/client-lambda";
import { CreateTableCommand, DescribeTableCommand, DynamoDBClient, PutItemCommand, UpdateItemCommand, DeleteItemCommand, waitUntilTableExists } from "@aws-sdk/client-dynamodb";
import { DynamoDBStreamsClient, DescribeStreamCommand, GetShardIteratorCommand, GetRecordsCommand, ListStreamsCommand } from "@aws-sdk/client-dynamodb-streams";
import { CloudWatchLogsClient, FilterLogEventsCommand } from "@aws-sdk/client-cloudwatch-logs";
import { zipFiles } from "../test/helpers/zip.ts";
const lambda = new LambdaClient({}); const ddb = new DynamoDBClient({}); const st = new DynamoDBStreamsClient({}); const logs = new CloudWatchLogsClient({});
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const show = (e: unknown) => `${(e as Error).name}: ${(e as Error).message}`;
const T = "s-" + Date.now();
await ddb.send(new CreateTableCommand({ TableName: T, BillingMode: "PAY_PER_REQUEST", AttributeDefinitions: [{ AttributeName: "pk", AttributeType: "S" }], KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }], StreamSpecification: { StreamEnabled: true, StreamViewType: "NEW_AND_OLD_IMAGES" } }));
await waitUntilTableExists({ client: ddb, maxWaitTime: 30 }, { TableName: T });
const d = await ddb.send(new DescribeTableCommand({ TableName: T }));
console.log("LatestStreamArn:", d.Table?.LatestStreamArn, "| label:", d.Table?.LatestStreamLabel, "| spec:", JSON.stringify(d.Table?.StreamSpecification));
const streamArn = d.Table!.LatestStreamArn!;
try { const ds = await st.send(new DescribeStreamCommand({ StreamArn: streamArn })); console.log("DescribeStream:", ds.StreamDescription?.StreamStatus, "shards:", ds.StreamDescription?.Shards?.length, "viewType:", ds.StreamDescription?.StreamViewType); } catch (e) { console.log("DescribeStream ERR", show(e)); }
try { const ls = await st.send(new ListStreamsCommand({ TableName: T })); console.log("ListStreams:", ls.Streams?.length); } catch (e) { console.log("ListStreams ERR", show(e)); }

const code = `export const handler = async (e) => { console.log("EVENT " + JSON.stringify(e)); return {}; };`;
const fn = "s-fn-" + Date.now();
await lambda.send(new CreateFunctionCommand({ FunctionName: fn, Runtime: "nodejs22.x", Handler: "index.handler", Role: "arn:aws:iam::000000000000:role/r", Timeout: 3, Code: { ZipFile: zipFiles({ "index.mjs": code }) } }));
await waitUntilFunctionActiveV2({ client: lambda, maxWaitTime: 30 }, { FunctionName: fn });
try { await lambda.send(new CreateEventSourceMappingCommand({ FunctionName: fn, EventSourceArn: streamArn })); console.log("ESM without StartingPosition: ACCEPTED"); } catch (e) { console.log("ESM without StartingPosition:", show(e)); }
await ddb.send(new PutItemCommand({ TableName: T, Item: { pk: { S: "before" }, v: { N: "0" } } }));
const esm = await lambda.send(new CreateEventSourceMappingCommand({ FunctionName: fn, EventSourceArn: streamArn, StartingPosition: "TRIM_HORIZON", BatchSize: 100, MaximumBatchingWindowInSeconds: 0 }));
console.log("ESM:", esm.State, esm.StartingPosition, "| MaxRetry", esm.MaximumRetryAttempts, "| MaxAge", esm.MaximumRecordAgeInSeconds, "| Bisect", esm.BisectBatchOnFunctionError, "| PF", esm.ParallelizationFactor, "| Window", esm.MaximumBatchingWindowInSeconds);
for (let i = 0; i < 20; i++) { const g = await lambda.send(new GetEventSourceMappingCommand({ UUID: esm.UUID })); if (g.State === "Enabled") { console.log("Enabled after", i, "polls"); break; } await sleep(300); }
await ddb.send(new PutItemCommand({ TableName: T, Item: { pk: { S: "a" }, v: { N: "1" } } }));
await ddb.send(new UpdateItemCommand({ TableName: T, Key: { pk: { S: "a" } }, UpdateExpression: "SET v = :v", ExpressionAttributeValues: { ":v": { N: "2" } } }));
await ddb.send(new DeleteItemCommand({ TableName: T, Key: { pk: { S: "a" } } }));
await sleep(4000);
try {
  const ev = await logs.send(new FilterLogEventsCommand({ logGroupName: `/aws/lambda/${fn}` }));
  for (const e of ev.events ?? []) { const m = (e.message ?? "").trim(); if (m.startsWith("EVENT ")) { const x = JSON.parse(m.slice(6)); console.log("BATCH of", x.Records.length); for (const r of x.Records) console.log("  ", JSON.stringify(r)); } else console.log("  log:", m.slice(0, 120)); }
} catch (e) { console.log("LOGS ERR", show(e)); }
try {
  const ds = await st.send(new DescribeStreamCommand({ StreamArn: streamArn }));
  const sh = ds.StreamDescription!.Shards![0]!;
  const it = await st.send(new GetShardIteratorCommand({ StreamArn: streamArn, ShardId: sh.ShardId, ShardIteratorType: "TRIM_HORIZON" }));
  const gr = await st.send(new GetRecordsCommand({ ShardIterator: it.ShardIterator }));
  console.log("GetRecords direct:", gr.Records?.map((r) => r.eventName + ":" + r.dynamodb?.Keys?.pk?.S).join(","));
} catch (e) { console.log("GetRecords ERR", show(e)); }
for (const c of [lambda, ddb, st, logs]) c.destroy();
