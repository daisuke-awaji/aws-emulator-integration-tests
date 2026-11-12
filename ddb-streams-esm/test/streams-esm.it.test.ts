import assert from "node:assert/strict";
import { after, before, describe, it } from "node:test";
import { InvalidParameterValueException } from "@aws-sdk/client-lambda";
import { bundleHandler, Cleanup, type Clients, createMapping, deployFunction, eventually, functionLogs, setupClients, sleep } from "./helpers/emulator.ts";
import { createTable, del, projection, put, type StreamTable, waitForLogged } from "./helpers/streams.ts";
import { zipFiles } from "./helpers/zip.ts";

const HANDLER = new URL("../src/stream-projector.ts", import.meta.url).pathname;

interface Env {
  source: StreamTable;
  proj: string;
  control: string;
  fn: string;
}

describe("DynamoDB Streams → Lambda event source mapping (AWS Emulator)", { concurrency: true }, () => {
  let c: Clients;
  let zip: Buffer;
  before(async () => {
    c = await setupClients();
    zip = await bundleHandler(HANDLER, "external-sdk");
  });
  after(() => c.destroy());

  /** ストリーム付きソーステーブル・射影テーブル・制御テーブル・関数を用意（マッピングは各テストで作る） */
  async function setup(cl: Cleanup): Promise<Env> {
    const [source, proj, control] = await Promise.all([
      createTable(c, cl, "src", "NEW_AND_OLD_IMAGES"),
      createTable(c, cl, "proj"),
      createTable(c, cl, "ctl"),
    ]);
    const fn = await deployFunction(c, { zip, prefix: "projector", env: { PROJECTION_TABLE: proj.name, CONTROL_TABLE: control.name } });
    cl.fn(c, fn);
    return { source, proj: proj.name, control: control.name, fn };
  }

  it("INSERT / MODIFY / REMOVE が書き込み順に 1 回ずつ処理される", async (t) => {
    const cl = new Cleanup();
    t.after(() => cl.run());
    const e = await setup(cl);
    cl.mapping(c, await createMapping(c, { FunctionName: e.fn, EventSourceArn: e.source.streamArn, StartingPosition: "TRIM_HORIZON", FunctionResponseTypes: ["ReportBatchItemFailures"] }));

    await put(c, e.source.name, { pk: "order-1", status: "NEW" });
    await put(c, e.source.name, { pk: "order-1", status: "PAID" });
    await put(c, e.source.name, { pk: "order-2", status: "NEW" });
    await del(c, e.source.name, "order-1");

    const rows = await eventually(() => projection(c, e.proj), (v) => v.length === 4, 8_000, "4 projected rows");
    assert.deepEqual(
      rows.map((r) => [r.eventName, r.itemKey, r.itemStatus]),
      [["INSERT", "order-1", "NEW"], ["MODIFY", "order-1", "PAID"], ["INSERT", "order-2", "NEW"], ["REMOVE", "order-1", null]],
    );
    assert.deepEqual(rows.map((r) => r.processedOrder), [1, 2, 3, 4], "SequenceNumber 順 = 処理順");
    await sleep(1_500); // 成功後に同じレコードが再配信されない（チェックポイントが進んでいる）
    assert.deepEqual((await projection(c, e.proj)).map((r) => r.deliveries), [1, 1, 1, 1]);
  });

  it("StartingPosition: TRIM_HORIZON は作成前のレコードも読み、LATEST は作成後だけ読む", async (t) => {
    const cl = new Cleanup();
    t.after(() => cl.run());
    const [a, b] = await Promise.all([setup(cl), setup(cl)]);
    await put(c, a.source.name, { pk: "old", status: "X" });
    await put(c, b.source.name, { pk: "old", status: "X" });
    // エミュレータは LATEST の開始位置を秒単位で決めているらしく、同じ秒に書いたレコードを拾うことがある
    // (streams-differences.it.test.ts 参照)。ここでは実 AWS で期待される挙動だけを見るため 1 秒以上あける
    await sleep(1_500);
    cl.mapping(c, await createMapping(c, { FunctionName: a.fn, EventSourceArn: a.source.streamArn, StartingPosition: "TRIM_HORIZON" }));
    cl.mapping(c, await createMapping(c, { FunctionName: b.fn, EventSourceArn: b.source.streamArn, StartingPosition: "LATEST" }));
    await sleep(500); // LATEST の読み取り開始位置が確定するのを待つ
    await put(c, a.source.name, { pk: "new", status: "Y" });
    await put(c, b.source.name, { pk: "new", status: "Y" });

    const ra = await eventually(() => projection(c, a.proj), (v) => v.length === 2, 8_000, "TRIM_HORIZON rows");
    const rb = await eventually(() => projection(c, b.proj), (v) => v.length === 1, 8_000, "LATEST rows");
    assert.deepEqual(ra.map((r) => r.itemKey), ["old", "new"]);
    assert.deepEqual(rb.map((r) => r.itemKey), ["new"]);
  });

  it("部分バッチレスポンス: 失敗したシーケンス番号から再開し、成功済みの手前は再処理しない", async (t) => {
    const cl = new Cleanup();
    t.after(() => cl.run());
    const e = await setup(cl);
    await put(c, e.control, { pk: "block#b2" }); // b2 の下流だけ一時障害
    cl.mapping(c, await createMapping(c, { FunctionName: e.fn, EventSourceArn: e.source.streamArn, StartingPosition: "TRIM_HORIZON", FunctionResponseTypes: ["ReportBatchItemFailures"] }));

    for (const k of ["b1", "b2", "b3"]) await put(c, e.source.name, { pk: k, status: "NEW" });

    // b2 は再試行され続け、その後ろの b3 も進まない（シャード内の順序が守られる）
    await eventually(() => functionLogs(c, e.fn), (v) => v.filter((l) => l.includes('"transient failure"')).length >= 2, 8_000, "2 transient failures");
    assert.deepEqual((await projection(c, e.proj)).map((r) => r.itemKey), ["b1"]);

    await del(c, e.control, "block#b2"); // 障害復旧
    const rows = await eventually(() => projection(c, e.proj), (v) => v.length === 3, 10_000, "3 projected rows after recovery");
    assert.deepEqual(rows.map((r) => [r.itemKey, r.deliveries]), [["b1", 1], ["b2", 1], ["b3", 1]], "b1 は再処理されない");
    assert.deepEqual(rows.map((r) => r.processedOrder), [1, 2, 3]);
  });

  it("関数が throw するとシャードが止まり、原因が解消すると同じ位置から順番どおり再開する", async (t) => {
    const cl = new Cleanup();
    t.after(() => cl.run());
    const e = await setup(cl);
    await put(c, e.control, { pk: "crash" });
    // MaximumRetryAttempts 既定 (-1 = 無制限) のまま。実 AWS でも期限切れまで再試行し続ける
    cl.mapping(c, await createMapping(c, { FunctionName: e.fn, EventSourceArn: e.source.streamArn, StartingPosition: "TRIM_HORIZON", BatchSize: 2 }));

    for (const k of ["r1", "r2", "r3", "r4", "r5"]) await put(c, e.source.name, { pk: k, status: "NEW" });
    await eventually(
      () => functionLogs(c, e.fn),
      (v) => v.filter((l) => l.includes("downstream is down")).length >= 2,
      8_000,
      "repeated failures",
    );
    assert.deepEqual(await projection(c, e.proj), [], "失敗中は何も先に進まない");

    await del(c, e.control, "crash");
    const rows = await eventually(() => projection(c, e.proj), (v) => v.length === 5, 12_000, "5 rows after recovery");
    assert.deepEqual(rows.map((r) => r.itemKey), ["r1", "r2", "r3", "r4", "r5"]);
    assert.deepEqual(rows.map((r) => r.processedOrder), [1, 2, 3, 4, 5]);
    assert.ok(rows.every((r) => r.deliveries === 1));
  });

  it("StreamViewType ごとにイベントに含まれるイメージが変わる", async (t) => {
    const cl = new Cleanup();
    t.after(() => cl.run());
    const dump = zipFiles({
      "index.mjs": `export const handler = async (e) => { for (const r of e.Records) console.log("REC " + JSON.stringify({ n: r.eventName, f: Object.keys(r.dynamodb).filter((k) => /Image|Keys/.test(k)).sort(), src: r.eventSource, v: r.eventVersion, view: r.dynamodb.StreamViewType })); return {}; };`,
    });
    const views = ["KEYS_ONLY", "NEW_IMAGE", "OLD_IMAGE", "NEW_AND_OLD_IMAGES"] as const;
    const results = await Promise.all(
      views.map(async (view) => {
        const tbl = await createTable(c, cl, "view", view);
        const fn = await deployFunction(c, { zip: dump, prefix: "dump" });
        cl.fn(c, fn);
        await put(c, tbl.name, { pk: "k", v: 1 });
        await put(c, tbl.name, { pk: "k", v: 2 });
        await del(c, tbl.name, "k");
        cl.mapping(c, await createMapping(c, { FunctionName: fn, EventSourceArn: tbl.streamArn, StartingPosition: "TRIM_HORIZON" }));
        const recs = await waitForLogged<{ n: string; f: string[]; src: string; v: string; view: string }>(c, fn, "REC ", 3, 8_000);
        assert.ok(recs.every((r) => r.src === "aws:dynamodb" && r.v === "1.1" && r.view === view));
        return [view, recs.map((r) => `${r.n}:${r.f.join("+")}`)] as const;
      }),
    );
    assert.deepEqual(Object.fromEntries(results), {
      KEYS_ONLY: ["INSERT:Keys", "MODIFY:Keys", "REMOVE:Keys"],
      NEW_IMAGE: ["INSERT:Keys+NewImage", "MODIFY:Keys+NewImage", "REMOVE:Keys"],
      OLD_IMAGE: ["INSERT:Keys", "MODIFY:Keys+OldImage", "REMOVE:Keys+OldImage"],
      NEW_AND_OLD_IMAGES: ["INSERT:Keys+NewImage", "MODIFY:Keys+NewImage+OldImage", "REMOVE:Keys+OldImage"],
    });
  });

  it("実 AWS と同じ検証: StartingPosition は必須、DynamoDB では AT_TIMESTAMP 不可", async (t) => {
    const cl = new Cleanup();
    t.after(() => cl.run());
    const e = await setup(cl);
    const isInvalid = (re: RegExp) => (err: unknown) => {
      assert.ok(err instanceof InvalidParameterValueException, String(err));
      assert.match(err.message, re);
      return true;
    };
    await assert.rejects(createMapping(c, { FunctionName: e.fn, EventSourceArn: e.source.streamArn }), isInvalid(/startingPosition/));
    await assert.rejects(
      createMapping(c, { FunctionName: e.fn, EventSourceArn: e.source.streamArn, StartingPosition: "AT_TIMESTAMP", StartingPositionTimestamp: new Date() }),
      isInvalid(/AT_TIMESTAMP is not supported/),
    );
  });
});
