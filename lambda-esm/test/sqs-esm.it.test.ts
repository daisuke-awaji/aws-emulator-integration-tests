import assert from "node:assert/strict";
import { after, before, describe, it } from "node:test";
import { GetCommand, ScanCommand } from "@aws-sdk/lib-dynamodb";
import { InvalidParameterValueException } from "@aws-sdk/client-lambda";
import { ReceiveMessageCommand, SendMessageBatchCommand, SendMessageCommand } from "@aws-sdk/client-sqs";
import {
  bundleHandler,
  Cleanup,
  type Clients,
  createMapping,
  createOrdersTable,
  createQueue,
  createQueueWithDlq,
  deployFunction,
  eventually,
  functionLogs,
  type Queue,
  queueCounts,
  setupClients,
  sleep,
} from "./helpers/emulator.ts";
import { zipFiles } from "./helpers/zip.ts";

const HANDLER = new URL("../src/order-consumer.ts", import.meta.url).pathname;

async function sendAll(c: Clients, q: Queue, bodies: string[]) {
  await c.sqs.send(new SendMessageBatchCommand({ QueueUrl: q.url, Entries: bodies.map((b, i) => ({ Id: `m${i}`, MessageBody: b })) }));
}

async function receiveAllBodies(c: Clients, q: Queue): Promise<string[]> {
  const r = await c.sqs.send(new ReceiveMessageCommand({ QueueUrl: q.url, MaxNumberOfMessages: 10, WaitTimeSeconds: 1 }));
  return (r.Messages ?? []).map((m) => m.Body!).sort();
}

// 各テストは自分専用のキュー・関数・テーブルを作るので並行実行できる
describe("SQS → Lambda event source mapping (AWS Emulator)", { concurrency: true }, () => {
  let c: Clients;
  let zip: Buffer;

  before(async () => {
    c = await setupClients();
    zip = await bundleHandler(HANDLER, "external-sdk");
  });
  after(() => c.destroy());

  it("正常メッセージはすべて処理されて DynamoDB に1回ずつ書かれ、キューから消える", async (t) => {
    const cl = new Cleanup();
    t.after(() => cl.run());
    const table = await createOrdersTable(c);
    cl.table(c, table);
    const { src, dlq } = await createQueueWithDlq(c);
    cl.queue(c, dlq);
    cl.queue(c, src);
    const fn = await deployFunction(c, { zip, env: { TABLE_NAME: table }, prefix: "consumer" });
    cl.fn(c, fn);
    cl.mapping(c, await createMapping(c, { FunctionName: fn, EventSourceArn: src.arn, FunctionResponseTypes: ["ReportBatchItemFailures"] }));

    await sendAll(c, src, [1, 2, 3].map((n) => JSON.stringify({ orderId: `o-${n}`, amount: n * 100 })));

    const items = await eventually(
      async () => (await c.doc.send(new ScanCommand({ TableName: table, ConsistentRead: true }))).Items ?? [],
      (v) => v.length === 3,
      8_000,
      "3 items in DynamoDB",
    );
    assert.deepEqual(items.map((i) => [i.pk, i.amount, i.deliveries]).sort(), [["o-1", 100, 1], ["o-2", 200, 1], ["o-3", 300, 1]]);
    await eventually(() => queueCounts(c, src), (q) => q.visible === 0 && q.inflight === 0, 5_000, "source queue drained");

    // 可視性タイムアウト (2s) を過ぎても再配信されない = 成功したメッセージは ESM が削除している
    await sleep(2_500);
    assert.deepEqual(await queueCounts(c, src), { visible: 0, inflight: 0 });
    assert.equal((await c.doc.send(new GetCommand({ TableName: table, Key: { pk: "o-1" } }))).Item?.deliveries, 1);
    assert.deepEqual(await receiveAllBodies(c, dlq), []);
  });

  it("部分バッチレスポンス: 不正メッセージだけが再試行され DLQ へ。正常分は1回だけ処理", async (t) => {
    const cl = new Cleanup();
    t.after(() => cl.run());
    const table = await createOrdersTable(c);
    cl.table(c, table);
    const { src, dlq } = await createQueueWithDlq(c, 2);
    cl.queue(c, dlq);
    cl.queue(c, src);
    const fn = await deployFunction(c, { zip, env: { TABLE_NAME: table }, prefix: "consumer" });
    cl.fn(c, fn);
    cl.mapping(c, await createMapping(c, { FunctionName: fn, EventSourceArn: src.arn, FunctionResponseTypes: ["ReportBatchItemFailures"] }));

    await sendAll(c, src, [JSON.stringify({ orderId: "good", amount: 500 }), "not-json", JSON.stringify({ orderId: "neg", amount: -1 })]);

    const dead = await eventually(() => receiveAllBodies(c, dlq), (v) => v.length === 2, 12_000, "2 messages in DLQ");
    assert.deepEqual(dead, ['{"orderId":"neg","amount":-1}', "not-json"].sort());
    const good = (await c.doc.send(new GetCommand({ TableName: table, Key: { pk: "good" }, ConsistentRead: true }))).Item;
    assert.equal(good?.deliveries, 1, "正常メッセージは再試行に巻き込まれない");
    assert.equal((await c.doc.send(new GetCommand({ TableName: table, Key: { pk: "neg" } }))).Item, undefined);

    const warns = (await functionLogs(c, fn)).filter((l) => l.includes('"invalid order event"'));
    assert.equal(warns.length, 4, "不正2件 × maxReceiveCount 2 回 = 4 回ログに出る");
  });

  it("ReportBatchItemFailures を設定しないと、関数が返した batchItemFailures は無視されバッチ全体が成功扱い", async (t) => {
    const cl = new Cleanup();
    t.after(() => cl.run());
    const table = await createOrdersTable(c);
    cl.table(c, table);
    const { src, dlq } = await createQueueWithDlq(c, 2);
    cl.queue(c, dlq);
    cl.queue(c, src);
    const fn = await deployFunction(c, { zip, env: { TABLE_NAME: table }, prefix: "consumer" });
    cl.fn(c, fn);
    cl.mapping(c, await createMapping(c, { FunctionName: fn, EventSourceArn: src.arn })); // FunctionResponseTypes なし

    await sendAll(c, src, [JSON.stringify({ orderId: "ok", amount: 1 }), "not-json"]);
    await eventually(() => queueCounts(c, src), (q) => q.visible === 0 && q.inflight === 0, 5_000, "source queue drained");
    await sleep(2_500); // 可視性タイムアウト経過後も戻ってこない
    assert.deepEqual(await queueCounts(c, src), { visible: 0, inflight: 0 });
    assert.deepEqual(await receiveAllBodies(c, dlq), [], "不正メッセージも黙って消える（設定漏れの典型的な事故）");
  });

  it("関数が throw するとバッチ全体が再試行され、全メッセージが DLQ へ", async (t) => {
    const cl = new Cleanup();
    t.after(() => cl.run());
    const { src, dlq } = await createQueueWithDlq(c, 2);
    cl.queue(c, dlq);
    cl.queue(c, src);
    // TABLE_NAME 未設定 → handler 冒頭で throw
    const fn = await deployFunction(c, { zip, prefix: "consumer" });
    cl.fn(c, fn);
    cl.mapping(c, await createMapping(c, { FunctionName: fn, EventSourceArn: src.arn, FunctionResponseTypes: ["ReportBatchItemFailures"] }));

    await sendAll(c, src, [JSON.stringify({ orderId: "a", amount: 1 }), JSON.stringify({ orderId: "b", amount: 2 })]);
    const dead = await eventually(() => receiveAllBodies(c, dlq), (v) => v.length === 2, 12_000, "2 messages in DLQ");
    assert.deepEqual(dead, [JSON.stringify({ orderId: "a", amount: 1 }), JSON.stringify({ orderId: "b", amount: 2 })]);
    const logs = await functionLogs(c, fn);
    assert.ok(logs.some((l) => l.includes("TABLE_NAME is not set")), "エラー内容が CloudWatch Logs に出る");
  });

  it("関数に渡るイベントは SQS イベントの形をしている", async (t) => {
    const cl = new Cleanup();
    t.after(() => cl.run());
    const src = await createQueue(c, "shape", { VisibilityTimeout: "5" });
    cl.queue(c, src);
    const code = `export const handler = async (e) => { console.log("EVENT " + JSON.stringify(e)); return { batchItemFailures: [] }; };`;
    const fn = await deployFunction(c, { zip: zipFiles({ "index.mjs": code }), prefix: "shape" });
    cl.fn(c, fn);
    cl.mapping(c, await createMapping(c, { FunctionName: fn, EventSourceArn: src.arn }));

    const sent = await c.sqs.send(
      new SendMessageCommand({ QueueUrl: src.url, MessageBody: "hello", MessageAttributes: { kind: { DataType: "String", StringValue: "test" } } }),
    );
    const line = (await eventually(() => functionLogs(c, fn), (v) => v.some((l) => l.startsWith("EVENT ")), 8_000, "event log")).find((l) =>
      l.startsWith("EVENT "),
    )!;
    const event = JSON.parse(line.slice("EVENT ".length));
    assert.equal(event.Records.length, 1);
    const r = event.Records[0];
    assert.equal(r.messageId, sent.MessageId);
    assert.equal(r.body, "hello");
    assert.equal(r.eventSource, "aws:sqs");
    assert.equal(r.eventSourceARN, src.arn);
    assert.equal(r.awsRegion, "us-east-1");
    assert.equal(r.md5OfBody, sent.MD5OfMessageBody);
    assert.equal(r.attributes.ApproximateReceiveCount, "1");
    assert.match(r.attributes.SentTimestamp, /^\d{13}$/);
    assert.ok(r.receiptHandle);
    assert.equal(r.messageAttributes.kind.stringValue, "test");
    assert.equal(r.messageAttributes.kind.dataType, "String");
    // [エミュレータ差異] 実 Lambda のイベントには stringListValues: [] / binaryListValues: [] も入る
    // (https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html の例)。1.0.322 では省略される。
    // ハンドラーはこれらの配列が無くても動くように書く（?? [] で受ける）。
    assert.deepEqual(Object.keys(r.messageAttributes.kind).sort(), ["dataType", "stringValue"]);
  });

  it("FilterCriteria に合わないメッセージは関数を呼ばずに削除される", async (t) => {
    const cl = new Cleanup();
    t.after(() => cl.run());
    const table = await createOrdersTable(c);
    cl.table(c, table);
    const src = await createQueue(c, "filtered", { VisibilityTimeout: "2" });
    cl.queue(c, src);
    const fn = await deployFunction(c, { zip, env: { TABLE_NAME: table }, prefix: "consumer" });
    cl.fn(c, fn);
    cl.mapping(
      c,
      await createMapping(c, {
        FunctionName: fn,
        EventSourceArn: src.arn,
        FunctionResponseTypes: ["ReportBatchItemFailures"],
        FilterCriteria: { Filters: [{ Pattern: JSON.stringify({ body: { amount: [{ numeric: [">=", 1000] }] } }) }] },
      }),
    );

    await sendAll(c, src, [JSON.stringify({ orderId: "big", amount: 5000 }), JSON.stringify({ orderId: "small", amount: 10 }), "not-json"]);
    await eventually(() => queueCounts(c, src), (q) => q.visible === 0 && q.inflight === 0, 8_000, "source queue drained");
    const items = (await c.doc.send(new ScanCommand({ TableName: table, ConsistentRead: true }))).Items ?? [];
    assert.deepEqual(items.map((i) => i.pk), ["big"]);
    // 不正メッセージ "not-json" もフィルタで落ちるので、関数のバリデーションログは出ない
    assert.equal((await functionLogs(c, fn)).filter((l) => l.includes("invalid order event")).length, 0);
  });

  it("無効 (Enabled: false) のマッピングは消費せず、有効化すると処理が始まる", async (t) => {
    const { UpdateEventSourceMappingCommand } = await import("@aws-sdk/client-lambda");
    const cl = new Cleanup();
    t.after(() => cl.run());
    const table = await createOrdersTable(c);
    cl.table(c, table);
    const src = await createQueue(c, "toggle", { VisibilityTimeout: "2" });
    cl.queue(c, src);
    const fn = await deployFunction(c, { zip, env: { TABLE_NAME: table }, prefix: "consumer" });
    cl.fn(c, fn);
    const uuid = await createMapping(c, { FunctionName: fn, EventSourceArn: src.arn, Enabled: false });
    cl.mapping(c, uuid);

    await sendAll(c, src, [JSON.stringify({ orderId: "later", amount: 1 })]);
    await sleep(1_500);
    assert.deepEqual(await queueCounts(c, src), { visible: 1, inflight: 0 }, "無効中は受信されない");

    await c.lambda.send(new UpdateEventSourceMappingCommand({ UUID: uuid, Enabled: true }));
    await eventually(
      async () => (await c.doc.send(new GetCommand({ TableName: table, Key: { pk: "later" }, ConsistentRead: true }))).Item,
      (v) => v !== undefined,
      8_000,
      "item written after enabling",
    );
  });

  it("実 AWS と同じ検証: キューの可視性タイムアウト < 関数タイムアウト なら作成できない", async (t) => {
    const cl = new Cleanup();
    t.after(() => cl.run());
    const src = await createQueue(c, "short-vt", { VisibilityTimeout: "2" });
    cl.queue(c, src);
    const fn = await deployFunction(c, { zip, timeout: 10, prefix: "consumer" });
    cl.fn(c, fn);
    await assert.rejects(createMapping(c, { FunctionName: fn, EventSourceArn: src.arn }), (e: unknown) => {
      assert.ok(e instanceof InvalidParameterValueException);
      assert.match(e.message, /visibility timeout: 2 seconds is less than Function timeout: 10 seconds/);
      return true;
    });
  });
});
