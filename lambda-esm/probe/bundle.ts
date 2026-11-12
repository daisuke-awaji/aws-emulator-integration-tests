import { readFileSync } from "node:fs";
import { CreateFunctionCommand, InvokeCommand, LambdaClient, waitUntilFunctionActiveV2 } from "@aws-sdk/client-lambda";
import { CreateTableCommand, DynamoDBClient, ScanCommand, waitUntilTableExists } from "@aws-sdk/client-dynamodb";
import { zipFiles } from "../test/helpers/zip.ts";
const lambda = new LambdaClient({}); const ddb = new DynamoDBClient({});
const T = "b-" + Date.now();
await ddb.send(new CreateTableCommand({ TableName: T, BillingMode: "PAY_PER_REQUEST", AttributeDefinitions: [{ AttributeName: "pk", AttributeType: "S" }], KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }] }));
await waitUntilTableExists({ client: ddb, maxWaitTime: 30 }, { TableName: T });
for (const kind of ["external", "inlined"]) {
  const name = "bundle-" + kind;
  await lambda.send(new CreateFunctionCommand({ FunctionName: name, Runtime: "nodejs22.x", Handler: "index.handler", Role: "arn:aws:iam::000000000000:role/r", Timeout: 3, Environment: { Variables: { TABLE: T } }, Code: { ZipFile: zipFiles({ "index.mjs": readFileSync(`/tmp/b-${kind}.mjs`) }) } }));
  await waitUntilFunctionActiveV2({ client: lambda, maxWaitTime: 30 }, { FunctionName: name });
  const ev = { Records: [{ messageId: "m-" + kind, body: "hi", eventSource: "aws:sqs" }] };
  const r = await lambda.send(new InvokeCommand({ FunctionName: name, Payload: JSON.stringify(ev) }));
  console.log(kind.padEnd(8), r.FunctionError ?? "ok", new TextDecoder().decode(r.Payload).slice(0, 300));
}
console.log("items:", (await ddb.send(new ScanCommand({ TableName: T }))).Items?.map((i) => i.pk?.S));
lambda.destroy(); ddb.destroy();
