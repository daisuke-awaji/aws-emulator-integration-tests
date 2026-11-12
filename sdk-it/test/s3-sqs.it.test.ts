import assert from "node:assert/strict";
import { after, before, describe, it } from "node:test";
import { DeleteBucketCommand, DeleteObjectCommand, S3Client } from "@aws-sdk/client-s3";
import {
  CreateQueueCommand,
  DeleteQueueCommand,
  GetQueueAttributesCommand,
  ReceiveMessageCommand,
} from "@aws-sdk/client-sqs";
import { SendMessageBatchCommand, SendMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import { STSClient } from "@aws-sdk/client-sts";
import { publishOrderEvents } from "../src/order-events.ts";
import { ReceiptStore } from "../src/receipt-store.ts";
import {
  assertEmulatorAccount,
  assertEmulatorEnvironment,
  BlockedRequestError,
  type Clients,
  createBucket,
  createClients,
  createSqsClient,
  guardEmulatorOnly,
  uniqueName,
} from "./helpers/emulator.ts";

describe("ReceiptStore (S3 on AWS Emulator)", () => {
  let c: Clients;
  let bucket: string;
  let store: ReceiptStore;

  before(async () => {
    assertEmulatorEnvironment();
    c = createClients();
    await assertEmulatorAccount(c.sts);
    bucket = await createBucket(c.s3);
    store = new ReceiptStore(c.s3, bucket);
  });

  after(async () => {
    await c.s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: "receipts/r-1.txt" }));
    await c.s3.send(new DeleteBucketCommand({ Bucket: bucket }));
    for (const client of [c.ddb, c.s3, c.sqs, c.sts]) client.destroy();
  });

  it("日本語を含む本文を保存して読み戻せる", async () => {
    await store.save("r-1", "領収書: 1,200円");
    assert.equal(await store.load("r-1"), "領収書: 1,200円");
  });

  it("存在しないキーは NoSuchKey → undefined", async () => {
    assert.equal(await store.load("nope"), undefined);
  });

  it("署名付き URL は SDK を通さない fetch でも取得でき、改ざんすると 403", async () => {
    const url = await store.downloadUrl("r-1");
    assert.equal(new URL(url).hostname, "127.0.0.1", "path-style でエミュレータを指す");
    const ok = await fetch(url);
    assert.equal(ok.status, 200);
    assert.equal(await ok.text(), "領収書: 1,200円");

    const tampered = new URL(url);
    tampered.searchParams.set("X-Amz-Signature", "0".repeat(64));
    const ng = await fetch(tampered);
    assert.equal(ng.status, 403);
    await ng.body?.cancel();
  });

  it("中身が残っているバケットは削除できない (BucketNotEmpty)", async () => {
    await assert.rejects(c.s3.send(new DeleteBucketCommand({ Bucket: bucket })), { name: "BucketNotEmpty" });
  });
});

describe("publishOrderEvents (SQS on AWS Emulator)", () => {
  let c: Clients;
  let queueUrl: string;

  before(async () => {
    assertEmulatorEnvironment();
    c = createClients();
    const r = await c.sqs.send(new CreateQueueCommand({ QueueName: uniqueName("order-events") }));
    queueUrl = r.QueueUrl!;
  });

  after(async () => {
    await c.sqs.send(new DeleteQueueCommand({ QueueUrl: queueUrl }));
    for (const client of [c.ddb, c.s3, c.sqs, c.sts]) client.destroy();
  });

  it("バッチ送信したイベントを属性付きで受信できる", async () => {
    const res = await publishOrderEvents(c.sqs, queueUrl, [
      { type: "OrderCreated", orderId: "o-1" },
      { type: "OrderPaid", orderId: "o-1" },
    ]);
    assert.deepEqual(res, { sent: 2, failedIds: [] });

    const received: { type: string; orderId: string; attr: string | undefined }[] = [];
    for (let i = 0; i < 5 && received.length < 2; i++) {
      const m = await c.sqs.send(
        new ReceiveMessageCommand({ QueueUrl: queueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 1, MessageAttributeNames: ["All"] }),
      );
      for (const msg of m.Messages ?? []) {
        received.push({ ...JSON.parse(msg.Body!), attr: msg.MessageAttributes?.type?.StringValue });
      }
    }
    assert.deepEqual(
      received.sort((a, b) => a.type.localeCompare(b.type)),
      [
        { type: "OrderCreated", orderId: "o-1", attr: "OrderCreated" },
        { type: "OrderPaid", orderId: "o-1", attr: "OrderPaid" },
      ],
    );
  });

  it("受信後は可視性タイムアウト中なのでキューに見えないメッセージとして数えられる", async () => {
    await publishOrderEvents(c.sqs, queueUrl, [{ type: "OrderCreated", orderId: "o-vis" }]);
    await c.sqs.send(new ReceiveMessageCommand({ QueueUrl: queueUrl, WaitTimeSeconds: 1, VisibilityTimeout: 30 }));
    const a = await c.sqs.send(
      new GetQueueAttributesCommand({ QueueUrl: queueUrl, AttributeNames: ["ApproximateNumberOfMessagesNotVisible"] }),
    );
    assert.ok(Number(a.Attributes?.ApproximateNumberOfMessagesNotVisible) >= 1);
  });

  it("11件以上はアプリ側で拒否（SQS のバッチ上限 10）", async () => {
    const events = Array.from({ length: 11 }, (_, i) => ({ type: "OrderCreated" as const, orderId: `x-${i}` }));
    await assert.rejects(publishOrderEvents(c.sqs, queueUrl, events), RangeError);
  });

  it("重複した Entry Id は SQS 側のエラー (BatchEntryIdsNotDistinct)", async () => {
    await assert.rejects(
      c.sqs.send(
        new SendMessageBatchCommand({
          QueueUrl: queueUrl,
          Entries: [
            { Id: "same", MessageBody: "a" },
            { Id: "same", MessageBody: "b" },
          ],
        }),
      ),
      (e: unknown) => {
        assert.ok(!(e instanceof BlockedRequestError), "ガードではなく SQS 側のエラーであること");
        assert.match(String((e as Error).name), /BatchEntryIdsNotDistinct/);
        return true;
      },
    );
  });
});

describe("安全装置", () => {
  it("エミュレータ以外のホストへ向いたクライアントは送信前にブロックされる", async () => {
    // 実リージョンのエンドポイントを明示しても、ガードが送信前に止める（ネットワークに出ない）
    const s3 = guardEmulatorOnly(new S3Client({ endpoint: "https://s3.us-east-1.amazonaws.com", maxAttempts: 1 }));
    const sts = guardEmulatorOnly(new STSClient({ endpoint: "https://sts.amazonaws.com", maxAttempts: 1 }));
    await assert.rejects(s3.send(new DeleteBucketCommand({ Bucket: "anything" })), BlockedRequestError);
    await assert.rejects(assertEmulatorAccount(sts), BlockedRequestError);
    s3.destroy();
    sts.destroy();
  });

  it("回帰: 既定の SQSClient は AWS_ENDPOINT_URL があっても QueueUrl のホスト (実 AWS) へ向かう", async (t) => {
    const sqs = guardEmulatorOnly(createSqsClient()); // 対策済みクライアントは 127.0.0.1 宛て
    const { QueueUrl } = await sqs.send(new CreateQueueCommand({ QueueName: uniqueName("guard") }));
    assert.match(QueueUrl!, /^https:\/\/sqs\.us-east-1\.amazonaws\.com\/000000000000\//, "エミュレータは実 AWS 形式の URL を返す");

    const naive = guardEmulatorOnly(new SQSClient({ maxAttempts: 1, logger: { debug() {}, info() {}, warn() {}, error() {} } }));
    t.after(async () => {
      naive.destroy();
      await sqs.send(new DeleteQueueCommand({ QueueUrl }));
      sqs.destroy();
    });
    await assert.rejects(naive.send(new SendMessageCommand({ QueueUrl, MessageBody: "x" })), (e: unknown) => {
      assert.ok(e instanceof BlockedRequestError);
      assert.match(e.message, /sqs\.us-east-1\.amazonaws\.com/);
      return true;
    });
    // 対策済みクライアントは同じ QueueUrl でもエミュレータへ届く
    const ok = await sqs.send(new SendMessageCommand({ QueueUrl, MessageBody: "x" }));
    assert.ok(ok.MessageId);
  });
});
