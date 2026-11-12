import { CreateEventSourceMappingCommand, CreateFunctionCommand, InvokeCommand, LambdaClient, waitUntilFunctionActiveV2 } from "@aws-sdk/client-lambda";
import { CreateTableCommand, DynamoDBClient, GetItemCommand, waitUntilTableExists } from "@aws-sdk/client-dynamodb";
import { CreateQueueCommand, GetQueueAttributesCommand, ReceiveMessageCommand, SendMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import { zipFiles } from "../test/helpers/zip.ts";
const lambda = new LambdaClient({}); const ddb = new DynamoDBClient({}); const sqs = new SQSClient({ useQueueUrlAsEndpoint: false });
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms)); const role = "arn:aws:iam::000000000000:role/r";
const mk = async (name: string, code: string, env: Record<string, string> = {}) => {
  await lambda.send(new CreateFunctionCommand({ FunctionName: name, Runtime: "nodejs22.x", Handler: "index.handler", Role: role, Timeout: 2, Environment: { Variables: env }, Code: { ZipFile: zipFiles({ "index.mjs": code }) } }));
  await waitUntilFunctionActiveV2({ client: lambda, maxWaitTime: 30 }, { FunctionName: name });
};
// (1) 関数内から SDK で DynamoDB に書けるか
const T = "inner-" + Date.now();
await ddb.send(new CreateTableCommand({ TableName: T, BillingMode: "PAY_PER_REQUEST", AttributeDefinitions: [{ AttributeName: "pk", AttributeType: "S" }], KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }] }));
await waitUntilTableExists({ client: ddb, maxWaitTime: 30 }, { TableName: T });
await mk("writer", `
import { DynamoDBClient, PutItemCommand } from "@aws-sdk/client-dynamodb";
import { STSClient, GetCallerIdentityCommand } from "@aws-sdk/client-sts";
const ddb = new DynamoDBClient({});
export const handler = async (e) => {
  let who; try { who = (await new STSClient({}).send(new GetCallerIdentityCommand({}))).Arn; } catch (x) { who = "ERR " + x.name; }
  await ddb.send(new PutItemCommand({ TableName: process.env.TABLE, Item: { pk: { S: e.id } } }));
  return { ok: true, who, keyId: process.env.AWS_ACCESS_KEY_ID ? "set" : "unset" };
};`, { TABLE: T });
const r = await lambda.send(new InvokeCommand({ FunctionName: "writer", Payload: JSON.stringify({ id: "from-lambda" }) }));
console.log("WRITER:", r.FunctionError ?? "ok", new TextDecoder().decode(r.Payload).slice(0, 400));
const g = await ddb.send(new GetItemCommand({ TableName: T, Key: { pk: { S: "from-lambda" } }, ConsistentRead: true }));
console.log("ITEM visible from test:", JSON.stringify(g.Item));

// (2) 関数が throw → バッチ全体が再試行、redrive で DLQ へ
const dlq = await sqs.send(new CreateQueueCommand({ QueueName: "dlq-" + Date.now() }));
const dlqArn = (await sqs.send(new GetQueueAttributesCommand({ QueueUrl: dlq.QueueUrl, AttributeNames: ["QueueArn"] }))).Attributes!.QueueArn!;
const src = await sqs.send(new CreateQueueCommand({ QueueName: "src-" + Date.now(), Attributes: { VisibilityTimeout: "2", RedrivePolicy: JSON.stringify({ deadLetterTargetArn: dlqArn, maxReceiveCount: 2 }) } }));
const srcArn = (await sqs.send(new GetQueueAttributesCommand({ QueueUrl: src.QueueUrl, AttributeNames: ["QueueArn"] }))).Attributes!.QueueArn!;
await mk("thrower", `export const handler = async (e) => { console.log("GOT", e.Records.map(r => r.body + "#" + r.attributes.ApproximateReceiveCount).join(",")); throw new Error("boom"); };`);
await lambda.send(new CreateEventSourceMappingCommand({ FunctionName: "thrower", EventSourceArn: srcArn, BatchSize: 10, MaximumBatchingWindowInSeconds: 0 }));
await sleep(1000);
const t0 = Date.now();
await sqs.send(new SendMessageCommand({ QueueUrl: src.QueueUrl, MessageBody: "a" }));
await sqs.send(new SendMessageCommand({ QueueUrl: src.QueueUrl, MessageBody: "b" }));
for (let i = 0; i < 16; i++) {
  await sleep(500);
  const a = (await sqs.send(new GetQueueAttributesCommand({ QueueUrl: src.QueueUrl, AttributeNames: ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"] }))).Attributes!;
  const d = (await sqs.send(new GetQueueAttributesCommand({ QueueUrl: dlq.QueueUrl, AttributeNames: ["ApproximateNumberOfMessages"] }))).Attributes!;
  console.log(`t=${Date.now() - t0}ms src visible=${a.ApproximateNumberOfMessages} inflight=${a.ApproximateNumberOfMessagesNotVisible} dlq=${d.ApproximateNumberOfMessages}`);
  if (d.ApproximateNumberOfMessages === "2") break;
}
const dm = await sqs.send(new ReceiveMessageCommand({ QueueUrl: dlq.QueueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 1 }));
console.log("DLQ bodies:", (dm.Messages ?? []).map((m) => m.Body).sort());
for (const c of [lambda, ddb, sqs]) c.destroy();
