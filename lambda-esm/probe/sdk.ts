import { CreateFunctionCommand, InvokeCommand, LambdaClient, waitUntilFunctionActiveV2 } from "@aws-sdk/client-lambda";
import { CreateTableCommand, DynamoDBClient, GetItemCommand, waitUntilTableExists } from "@aws-sdk/client-dynamodb";
import { zipFiles } from "../test/helpers/zip.ts";
const lambda = new LambdaClient({}); const ddb = new DynamoDBClient({});
const T = "inner-" + Date.now();
await ddb.send(new CreateTableCommand({ TableName: T, BillingMode: "PAY_PER_REQUEST", AttributeDefinitions: [{ AttributeName: "pk", AttributeType: "S" }], KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }] }));
await waitUntilTableExists({ client: ddb, maxWaitTime: 30 }, { TableName: T });
const code = `
import { DynamoDBClient, PutItemCommand } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand, GetCommand } from "@aws-sdk/lib-dynamodb";
export const handler = async (e) => {
  const out = {};
  for (const m of ["@aws-sdk/client-sts","@aws-sdk/client-s3","@aws-sdk/client-sqs","@aws-sdk/client-dynamodb","@aws-sdk/lib-dynamodb","@aws-sdk/client-sns","@aws-sdk/client-eventbridge","@aws-sdk/client-secrets-manager","@aws-sdk/client-ssm"]) {
    try { const x = await import(m); const k = Object.keys(x); out[m] = k.length + ": " + k.filter(n => n.endsWith("Command")).slice(0, 8).join(",") + (k.filter(n => n.endsWith("Command")).length > 8 ? ",…" : ""); } catch (x) { out[m] = "ERR " + String(x.message).slice(0, 60); }
  }
  const c = new DynamoDBClient({});
  await c.send(new PutItemCommand({ TableName: process.env.TABLE, Item: { pk: { S: e.id }, n: { N: "1" } } }));
  const doc = DynamoDBDocumentClient.from(c);
  await doc.send(new PutCommand({ TableName: process.env.TABLE, Item: { pk: e.id + "-doc", nested: { a: [1, "x"] } } }));
  out.docGet = (await doc.send(new GetCommand({ TableName: process.env.TABLE, Key: { pk: e.id + "-doc" } }))).Item;
  try { await c.send(new PutItemCommand({ TableName: "no-such-table", Item: { pk: { S: "x" } } })); } catch (x) { out.errName = x.name; out.errIsSdk = typeof x.$metadata; }
  out.regionType = typeof c.config?.region;
  return out;
};`;
await lambda.send(new CreateFunctionCommand({ FunctionName: "sdkprobe", Runtime: "nodejs22.x", Handler: "index.handler", Role: "arn:aws:iam::000000000000:role/r", Timeout: 3, Environment: { Variables: { TABLE: T } }, Code: { ZipFile: zipFiles({ "index.mjs": code }) } }));
await waitUntilFunctionActiveV2({ client: lambda, maxWaitTime: 30 }, { FunctionName: "sdkprobe" });
const r = await lambda.send(new InvokeCommand({ FunctionName: "sdkprobe", Payload: JSON.stringify({ id: "from-lambda" }) }));
console.log("SDKPROBE:", r.FunctionError ?? "ok"); console.log(JSON.stringify(JSON.parse(new TextDecoder().decode(r.Payload)), null, 1));
const g = await ddb.send(new GetItemCommand({ TableName: T, Key: { pk: { S: "from-lambda" } }, ConsistentRead: true }));
console.log("ITEM visible from test:", JSON.stringify(g.Item));
lambda.destroy(); ddb.destroy();
