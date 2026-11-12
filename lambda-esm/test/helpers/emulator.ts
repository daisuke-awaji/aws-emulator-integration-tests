import { randomUUID } from "node:crypto";
import { build } from "esbuild";
import { CloudWatchLogsClient, FilterLogEventsCommand, ResourceNotFoundException } from "@aws-sdk/client-cloudwatch-logs";
import { CreateTableCommand, DeleteTableCommand, DynamoDBClient, waitUntilTableExists } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import {
  CreateEventSourceMappingCommand,
  type CreateEventSourceMappingCommandInput,
  CreateFunctionCommand,
  DeleteEventSourceMappingCommand,
  DeleteFunctionCommand,
  GetEventSourceMappingCommand,
  LambdaClient,
  waitUntilFunctionActiveV2,
} from "@aws-sdk/client-lambda";
import { CreateQueueCommand, DeleteQueueCommand, GetQueueAttributesCommand, SQSClient } from "@aws-sdk/client-sqs";
import { GetCallerIdentityCommand, STSClient } from "@aws-sdk/client-sts";
import type { FinalizeRequestMiddleware, MiddlewareStack } from "@smithy/types";
import { zipFiles } from "./zip.ts";

export const EMULATOR_ACCOUNT = "000000000000";
const ALLOWED_HOST = "127.0.0.1";
const ROLE = `arn:aws:iam::${EMULATOR_ACCOUNT}:role/order-consumer`; // エミュレータは IAM を評価しない

export function assertEmulatorEnvironment(): void {
  const leaked = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_CONTAINER_CREDENTIALS_FULL_URI", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_WEB_IDENTITY_TOKEN_FILE"].filter((k) => process.env[k]);
  if (leaked.length > 0) throw new Error(`real-credential variables are set: ${leaked.join(", ")}`);
  const raw = process.env.AWS_ENDPOINT_URL;
  if (!raw) throw new Error("AWS_ENDPOINT_URL is not set; run via scripts/with-emulator.sh");
  if (new URL(raw).hostname !== ALLOWED_HOST) throw new Error(`AWS_ENDPOINT_URL must point at ${ALLOWED_HOST}`);
}

export class BlockedRequestError extends Error {}

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
  lambda: LambdaClient;
  sqs: SQSClient;
  ddb: DynamoDBClient;
  doc: DynamoDBDocumentClient;
  logs: CloudWatchLogsClient;
  sts: STSClient;
  destroy(): void;
}

export async function setupClients(): Promise<Clients> {
  assertEmulatorEnvironment();
  const ddb = guardEmulatorOnly(new DynamoDBClient({}));
  const c: Clients = {
    lambda: guardEmulatorOnly(new LambdaClient({})),
    // QueueUrl が実 AWS 形式なので、これがないと SQS 呼び出しが実 AWS へ向かう（前回の検証で判明）
    sqs: guardEmulatorOnly(new SQSClient({ useQueueUrlAsEndpoint: false })),
    ddb,
    doc: DynamoDBDocumentClient.from(ddb),
    logs: guardEmulatorOnly(new CloudWatchLogsClient({})),
    sts: guardEmulatorOnly(new STSClient({})),
    destroy() {
      for (const x of [this.lambda, this.sqs, this.ddb, this.logs, this.sts]) x.destroy();
    },
  };
  const id = await c.sts.send(new GetCallerIdentityCommand({}));
  if (id.Account !== EMULATOR_ACCOUNT) throw new Error(`not the emulator account: ${id.Account}`);
  return c;
}

export const uniqueName = (prefix: string) => `${prefix}-${randomUUID().slice(0, 8)}`;
export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** 条件を満たすまでポーリング。満たさなければ最後の値付きで失敗 */
export async function eventually<T>(fn: () => Promise<T>, ok: (v: T) => boolean, timeoutMs: number, label: string): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  let last = await fn();
  while (!ok(last)) {
    if (Date.now() > deadline) throw new Error(`timed out after ${timeoutMs}ms waiting for ${label}; last=${JSON.stringify(last)}`);
    await sleep(200);
    last = await fn();
  }
  return last;
}

export type BundleMode = "external-sdk" | "inline-sdk";

/**
 * esbuild で TypeScript ハンドラーを 1 ファイルの ESM にする。
 * external-sdk: @aws-sdk/* をランタイム同梱のものに任せる（CDK NodejsFunction の Node 18+ 既定と同じ考え方）
 */
export async function bundleHandler(entry: string, mode: BundleMode = "external-sdk"): Promise<Buffer> {
  const r = await build({
    entryPoints: [entry],
    bundle: true,
    platform: "node",
    format: "esm",
    target: "node22",
    write: false,
    logLevel: "silent",
    external: mode === "external-sdk" ? ["@aws-sdk/*"] : [],
  });
  const out = r.outputFiles[0];
  if (!out) throw new Error("esbuild produced no output");
  return zipFiles({ "index.mjs": Buffer.from(out.contents) });
}

export async function deployFunction(
  c: Clients,
  opts: { zip: Buffer; env?: Record<string, string>; timeout?: number; prefix?: string },
): Promise<string> {
  const FunctionName = uniqueName(opts.prefix ?? "fn");
  await c.lambda.send(
    new CreateFunctionCommand({
      FunctionName,
      Runtime: "nodejs22.x",
      Handler: "index.handler",
      Role: ROLE,
      Timeout: opts.timeout ?? 2,
      Environment: { Variables: opts.env ?? {} },
      Code: { ZipFile: opts.zip },
    }),
  );
  await waitUntilFunctionActiveV2({ client: c.lambda, maxWaitTime: 30, minDelay: 1, maxDelay: 1 }, { FunctionName });
  return FunctionName;
}

export interface Queue {
  url: string;
  arn: string;
}

export async function createQueue(c: Clients, prefix: string, attributes: Record<string, string> = {}): Promise<Queue> {
  const { QueueUrl } = await c.sqs.send(new CreateQueueCommand({ QueueName: uniqueName(prefix), Attributes: attributes }));
  const a = await c.sqs.send(new GetQueueAttributesCommand({ QueueUrl, AttributeNames: ["QueueArn"] }));
  return { url: QueueUrl!, arn: a.Attributes!.QueueArn! };
}

/** visibility timeout 2 秒 + maxReceiveCount 付き DLQ のペア */
export async function createQueueWithDlq(c: Clients, maxReceiveCount = 2): Promise<{ src: Queue; dlq: Queue }> {
  const dlq = await createQueue(c, "orders-dlq");
  const src = await createQueue(c, "orders", {
    VisibilityTimeout: "2",
    RedrivePolicy: JSON.stringify({ deadLetterTargetArn: dlq.arn, maxReceiveCount }),
  });
  return { src, dlq };
}

export async function createMapping(c: Clients, input: CreateEventSourceMappingCommandInput): Promise<string> {
  const { UUID } = await c.lambda.send(new CreateEventSourceMappingCommand({ BatchSize: 10, MaximumBatchingWindowInSeconds: 0, ...input }));
  await eventually(
    () => c.lambda.send(new GetEventSourceMappingCommand({ UUID })),
    (m) => m.State === (input.Enabled === false ? "Disabled" : "Enabled"),
    10_000,
    "event source mapping state",
  );
  return UUID!;
}

export async function queueCounts(c: Clients, q: Queue): Promise<{ visible: number; inflight: number }> {
  const a = await c.sqs.send(
    new GetQueueAttributesCommand({ QueueUrl: q.url, AttributeNames: ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"] }),
  );
  return { visible: Number(a.Attributes?.ApproximateNumberOfMessages), inflight: Number(a.Attributes?.ApproximateNumberOfMessagesNotVisible) };
}

/** ロググループは初回実行時に作られる（実 AWS と同じ）ので、まだ無ければ空として扱う */
export async function functionLogs(c: Clients, functionName: string): Promise<string[]> {
  try {
    const r = await c.logs.send(new FilterLogEventsCommand({ logGroupName: `/aws/lambda/${functionName}` }));
    return (r.events ?? []).map((e) => (e.message ?? "").trim());
  } catch (e) {
    if (e instanceof ResourceNotFoundException) return [];
    throw e;
  }
}

export async function createOrdersTable(c: Clients): Promise<string> {
  const TableName = uniqueName("orders");
  await c.ddb.send(
    new CreateTableCommand({
      TableName,
      BillingMode: "PAY_PER_REQUEST",
      AttributeDefinitions: [{ AttributeName: "pk", AttributeType: "S" }],
      KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }],
    }),
  );
  await waitUntilTableExists({ client: c.ddb, maxWaitTime: 30, minDelay: 1, maxDelay: 1 }, { TableName });
  return TableName;
}

/** 後片付けを登録順の逆に、失敗しても全部実行する */
export class Cleanup {
  private tasks: (() => Promise<unknown>)[] = [];
  add(t: () => Promise<unknown>) {
    this.tasks.push(t);
  }
  mapping(c: Clients, UUID: string) {
    this.add(() => c.lambda.send(new DeleteEventSourceMappingCommand({ UUID })));
  }
  fn(c: Clients, FunctionName: string) {
    this.add(() => c.lambda.send(new DeleteFunctionCommand({ FunctionName })));
  }
  queue(c: Clients, q: Queue) {
    this.add(() => c.sqs.send(new DeleteQueueCommand({ QueueUrl: q.url })));
  }
  table(c: Clients, TableName: string) {
    this.add(() => c.ddb.send(new DeleteTableCommand({ TableName })));
  }
  async run() {
    const errors: unknown[] = [];
    for (const t of this.tasks.reverse()) await t().catch((e) => errors.push(e));
    this.tasks = [];
    if (errors.length) throw new AggregateError(errors, "cleanup failed");
  }
}
