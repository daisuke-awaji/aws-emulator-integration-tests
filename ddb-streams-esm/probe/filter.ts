import { CreateEventSourceMappingCommand, CreateFunctionCommand, GetEventSourceMappingCommand, LambdaClient, waitUntilFunctionActiveV2 } from "@aws-sdk/client-lambda";
import { CreateTableCommand, DynamoDBClient, PutItemCommand, UpdateItemCommand, waitUntilTableExists } from "@aws-sdk/client-dynamodb";
import { CloudWatchLogsClient, FilterLogEventsCommand } from "@aws-sdk/client-cloudwatch-logs";
import { zipFiles } from "../test/helpers/zip.ts";
const lambda = new LambdaClient({}); const ddb = new DynamoDBClient({}); const logs = new CloudWatchLogsClient({});
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
let n = 0; const uid = (p: string) => `${p}-${Date.now()}-${n++}`;
const patterns: Record<string, object> = {
  "doc:Keys S": { dynamodb: { Keys: { pk: { S: ["c1"] } } } },
  "doc:NewImage S": { dynamodb: { NewImage: { mgr: { S: ["Pat", "Shirley"] } } } },
  "doc:New AND Old": { dynamodb: { NewImage: { mgr: { S: ["Pat"] } }, OldImage: { mgr: { S: ["Terry"] } } } },
  "eventName only": { eventName: ["INSERT"] },
};
for (const [label, p] of Object.entries(patterns)) {
  const T = uid("t");
  const r = await ddb.send(new CreateTableCommand({ TableName: T, BillingMode: "PAY_PER_REQUEST", AttributeDefinitions: [{ AttributeName: "pk", AttributeType: "S" }], KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }], StreamSpecification: { StreamEnabled: true, StreamViewType: "NEW_AND_OLD_IMAGES" } }));
  await waitUntilTableExists({ client: ddb, maxWaitTime: 30 }, { TableName: T });
  const F = uid("f");
  await lambda.send(new CreateFunctionCommand({ FunctionName: F, Runtime: "nodejs22.x", Handler: "index.handler", Role: "arn:aws:iam::000000000000:role/r", Timeout: 3, Code: { ZipFile: zipFiles({ "index.mjs": `export const handler = async (e) => { console.log("GOT " + e.Records.map(r => r.eventName + ":" + r.dynamodb.Keys.pk.S + ":" + (r.dynamodb.NewImage?.mgr?.S ?? "-")).join(",")); return {}; };` }) } }));
  await waitUntilFunctionActiveV2({ client: lambda, maxWaitTime: 30 }, { FunctionName: F });
  // c1: Terry で作成 → Pat に変更 / c2: Shirley で作成 / c3: Bob で作成
  await ddb.send(new PutItemCommand({ TableName: T, Item: { pk: { S: "c1" }, mgr: { S: "Terry" } } }));
  await ddb.send(new UpdateItemCommand({ TableName: T, Key: { pk: { S: "c1" } }, UpdateExpression: "SET mgr = :m", ExpressionAttributeValues: { ":m": { S: "Pat" } } }));
  await ddb.send(new PutItemCommand({ TableName: T, Item: { pk: { S: "c2" }, mgr: { S: "Shirley" } } }));
  await ddb.send(new PutItemCommand({ TableName: T, Item: { pk: { S: "c3" }, mgr: { S: "Bob" } } }));
  const m = await lambda.send(new CreateEventSourceMappingCommand({ FunctionName: F, EventSourceArn: r.TableDescription!.LatestStreamArn!, StartingPosition: "TRIM_HORIZON", BatchSize: 1, MaximumBatchingWindowInSeconds: 0, FilterCriteria: { Filters: [{ Pattern: JSON.stringify(p) }] } }));
  for (let k = 0; k < 30; k++) { const g = await lambda.send(new GetEventSourceMappingCommand({ UUID: m.UUID })); if (g.State === "Enabled") break; await sleep(200); }
  await sleep(3000);
  let got: string[] = [];
  try { got = ((await logs.send(new FilterLogEventsCommand({ logGroupName: `/aws/lambda/${F}` }))).events ?? []).map((e) => e.message!.trim()).filter((x) => x.startsWith("GOT ")).map((x) => x.slice(4)); } catch {}
  console.log(label.padEnd(16), "=>", got.join(" | ") || "(none)");
}
for (const c of [lambda, ddb, logs]) c.destroy();
