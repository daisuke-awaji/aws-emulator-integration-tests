import { randomUUID } from "node:crypto";
import { CreateTableCommand, DeleteTableCommand, DynamoDBClient, waitUntilTableExists } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import { CreateBucketCommand, S3Client } from "@aws-sdk/client-s3";
import { SQSClient } from "@aws-sdk/client-sqs";
import { GetCallerIdentityCommand, STSClient } from "@aws-sdk/client-sts";
import type { FinalizeRequestMiddleware, MiddlewareStack } from "@smithy/types";

export const EMULATOR_ACCOUNT = "000000000000";
const ALLOWED_HOST = "127.0.0.1";

/** 実 AWS の認証情報になり得る環境変数が残っていたらテストを始めない */
export function assertEmulatorEnvironment(): URL {
  const leaked = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_CONTAINER_CREDENTIALS_FULL_URI", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_WEB_IDENTITY_TOKEN_FILE"].filter((k) => process.env[k]);
  if (leaked.length > 0) throw new Error(`real-credential variables are set: ${leaked.join(", ")}`);
  const raw = process.env.AWS_ENDPOINT_URL;
  if (!raw) throw new Error("AWS_ENDPOINT_URL is not set; run via scripts/with-emulator.sh");
  const url = new URL(raw);
  if (url.hostname !== ALLOWED_HOST) throw new Error(`AWS_ENDPOINT_URL must point at ${ALLOWED_HOST}, got ${url.hostname}`);
  return url;
}

export class BlockedRequestError extends Error {}

// 各サービスクライアントの middlewareStack の入出力型はサービスごとに異なるため any で受ける
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyClient = { middlewareStack: MiddlewareStack<any, any> };

/** 署名後・送信直前に宛先ホストを検査し、エミュレータ以外への送信を止める */
export function guardEmulatorOnly<T extends AnyClient>(client: T): T {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const guard: FinalizeRequestMiddleware<any, any> = (next) => async (args) => {
    const host = (args.request as { hostname?: unknown }).hostname;
    if (host !== ALLOWED_HOST) throw new BlockedRequestError(`blocked request to non-emulator host: ${String(host)}`);
    return next(args);
  };
  client.middlewareStack.add(guard, { step: "finalizeRequest", name: "emulatorOnlyGuard", priority: "low" });
  return client;
}

export interface Clients {
  ddb: DynamoDBClient;
  doc: DynamoDBDocumentClient;
  s3: S3Client;
  sqs: SQSClient;
  sts: STSClient;
}

/** endpoint・認証情報は環境変数から読む（アプリ本番コードと同じ生成方法） */
export function createClients(): Clients {
  const ddb = guardEmulatorOnly(new DynamoDBClient({}));
  return {
    ddb,
    doc: DynamoDBDocumentClient.from(ddb, { marshallOptions: { removeUndefinedValues: true } }),
    s3: guardEmulatorOnly(new S3Client({})),
    sqs: guardEmulatorOnly(createSqsClient()),
    sts: guardEmulatorOnly(new STSClient({})),
  };
}

/**
 * SDK v3 の SQSClient は既定 (useQueueUrlAsEndpoint: true) で、endpoint を明示していないと
 * QueueUrl のホストを接続先に使う。エミュレータは実 AWS 形式の QueueUrl
 * (https://sqs.<region>.amazonaws.com/000000000000/...) を返すため、AWS_ENDPOINT_URL だけでは
 * SendMessage 等が実 AWS へ向かう。エミュレータ利用時だけ無効化する。
 */
export function createSqsClient(): SQSClient {
  return new SQSClient(process.env.AWS_ENDPOINT_URL ? { useQueueUrlAsEndpoint: false } : {});
}

export async function assertEmulatorAccount(sts: STSClient): Promise<void> {
  const id = await sts.send(new GetCallerIdentityCommand({}));
  if (id.Account !== EMULATOR_ACCOUNT) throw new Error(`not the emulator account: ${id.Account}`);
}

export const uniqueName = (prefix: string) => `${prefix}-${randomUUID().slice(0, 8)}`;

export async function createOrdersTable(ddb: DynamoDBClient): Promise<string> {
  const TableName = uniqueName("orders");
  await ddb.send(
    new CreateTableCommand({
      TableName,
      BillingMode: "PAY_PER_REQUEST",
      AttributeDefinitions: [
        { AttributeName: "pk", AttributeType: "S" },
        { AttributeName: "gsi1pk", AttributeType: "S" },
        { AttributeName: "gsi1sk", AttributeType: "S" },
      ],
      KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }],
      GlobalSecondaryIndexes: [
        {
          IndexName: "gsi1",
          KeySchema: [
            { AttributeName: "gsi1pk", KeyType: "HASH" },
            { AttributeName: "gsi1sk", KeyType: "RANGE" },
          ],
          Projection: { ProjectionType: "ALL" },
        },
      ],
    }),
  );
  // 実 AWS と同様に CREATING → ACTIVE を待つ
  await waitUntilTableExists({ client: ddb, maxWaitTime: 60, minDelay: 1, maxDelay: 2 }, { TableName });
  return TableName;
}

export async function deleteTable(ddb: DynamoDBClient, TableName: string): Promise<void> {
  await ddb.send(new DeleteTableCommand({ TableName }));
}

export async function createBucket(s3: S3Client): Promise<string> {
  const Bucket = uniqueName("receipts");
  await s3.send(new CreateBucketCommand({ Bucket }));
  return Bucket;
}

/** 結果整合なリソース（GSI など）向けの簡易ポーリング */
export async function eventually<T>(fn: () => Promise<T>, ok: (v: T) => boolean, timeoutMs = 5000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  let last: T = await fn();
  while (!ok(last) && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 100));
    last = await fn();
  }
  return last;
}
