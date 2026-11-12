/**
 * DynamoDB Streams の ESM でエミュレータが実 AWS と「違う」ところを固定する特性テスト (characterization test)。
 * aws-emulator 1.0.322 時点の観測結果。エミュレータが改善されるとここが落ちるので、そのとき前提を見直す。
 *
 * 実 AWS の仕様:
 * - https://docs.aws.amazon.com/lambda/latest/dg/services-dynamodb-errors.html
 *   関数がエラーを返すと MaximumRetryAttempts まで再試行し、超えたら破棄して OnFailure 宛先へ送り、次へ進む。
 *   BisectBatchOnFunctionError はバッチを二分して再試行する。
 * - https://docs.aws.amazon.com/lambda/latest/dg/with-ddb-filtering.html
 *   FilterCriteria に一致しないレコードは関数に渡らない。
 */
import assert from "node:assert/strict";
import { after, before, describe, it } from "node:test";
import { GetEventSourceMappingCommand, InvokeCommand } from "@aws-sdk/client-lambda";
import { ReceiveMessageCommand } from "@aws-sdk/client-sqs";
import { Cleanup, type Clients, createMapping, createQueue, deployFunction, eventually, setupClients, sleep } from "./helpers/emulator.ts";
import { createTable, put, waitForLogged } from "./helpers/streams.ts";
import { zipFiles } from "./helpers/zip.ts";

/** 受け取ったキーを "GOT [..]" で出し、pk に "bad" を含むレコードがあれば throw */
const FAILING = zipFiles({
  "index.mjs": `export const handler = async (e) => {
    const keys = e.Records.map((r) => r.dynamodb.Keys.pk.S);
    console.log("GOT " + JSON.stringify(keys));
    if (keys.some((k) => k.includes("bad"))) throw new Error("boom");
    return {};
  };`,
});

describe("エミュレータ固有の DynamoDB Streams ESM 挙動 (実 AWS とは異なる)", { concurrency: true }, () => {
  let c: Clients;
  before(async () => {
    c = await setupClients();
  });
  after(() => c.destroy());

  it("MaximumRetryAttempts: 0 でも再試行が止まらず、OnFailure 宛先にも送られない", async (t) => {
    const cl = new Cleanup();
    t.after(() => cl.run());
    const tbl = await createTable(c, cl, "retry", "KEYS_ONLY");
    const onFailure = await createQueue(c, "onfailure");
    cl.queue(c, onFailure);
    const fn = await deployFunction(c, { zip: FAILING, prefix: "failing" });
    cl.fn(c, fn);
    const uuid = await createMapping(c, {
      FunctionName: fn,
      EventSourceArn: tbl.streamArn,
      StartingPosition: "TRIM_HORIZON",
      MaximumRetryAttempts: 0,
      DestinationConfig: { OnFailure: { Destination: onFailure.arn } },
    });
    cl.mapping(c, uuid);
    // 設定値自体は保存される
    const m = await c.lambda.send(new GetEventSourceMappingCommand({ UUID: uuid }));
    assert.equal(m.MaximumRetryAttempts, 0);
    assert.equal(m.DestinationConfig?.OnFailure?.Destination, onFailure.arn);

    await put(c, tbl.name, { pk: "bad-1" });
    // 実 AWS: 1 回の呼び出しで破棄 → OnFailure へ。エミュレータ: 再試行が続く
    const calls = await waitForLogged<string[]>(c, fn, "GOT ", 4, 10_000);
    assert.ok(calls.every((k) => k.join() === "bad-1"));
    const r = await c.sqs.send(new ReceiveMessageCommand({ QueueUrl: onFailure.url, MaxNumberOfMessages: 10, WaitTimeSeconds: 1 }));
    assert.equal(r.Messages?.length ?? 0, 0, "OnFailure 宛先に何も届かない");
    const after = await c.lambda.send(new GetEventSourceMappingCommand({ UUID: uuid }));
    assert.equal(after.LastProcessingResult, "PROBLEM: Function call failed");
  });

  it("BisectBatchOnFunctionError: true でもバッチが分割されない", async (t) => {
    const cl = new Cleanup();
    t.after(() => cl.run());
    const tbl = await createTable(c, cl, "bisect", "KEYS_ONLY");
    const fn = await deployFunction(c, { zip: FAILING, prefix: "failing" });
    cl.fn(c, fn);
    // 6 件を先に書いておき、TRIM_HORIZON で 1 バッチにまとめて渡させる
    for (const k of ["b1", "b2", "b3", "bad", "b5", "b6"]) await put(c, tbl.name, { pk: k });
    cl.mapping(c, await createMapping(c, { FunctionName: fn, EventSourceArn: tbl.streamArn, StartingPosition: "TRIM_HORIZON", BisectBatchOnFunctionError: true, MaximumRetryAttempts: 2 }));

    const calls = await waitForLogged<string[]>(c, fn, "GOT ", 4, 10_000);
    // 実 AWS: 6 → 3+3 → … と半分ずつになり "bad" が孤立する。エミュレータ: 毎回 6 件のまま
    assert.deepEqual(calls.map((k) => k.length).slice(0, 4), [6, 6, 6, 6]);
  });

  it("FilterCriteria が無視され、パターンに合わないレコードも関数に渡る", async (t) => {
    const cl = new Cleanup();
    t.after(() => cl.run());
    const tbl = await createTable(c, cl, "filter", "NEW_AND_OLD_IMAGES");
    const fn = await deployFunction(c, {
      zip: zipFiles({ "index.mjs": `export const handler = async (e) => { console.log("GOT " + JSON.stringify(e.Records.map((r) => r.dynamodb.Keys.pk.S))); return {}; };` }),
      prefix: "filtered",
    });
    cl.fn(c, fn);
    for (const k of ["c1", "c2", "c3"]) await put(c, tbl.name, { pk: k });
    // ドキュメントの「Filtering with table keys」と同じ形のパターン
    cl.mapping(
      c,
      await createMapping(c, {
        FunctionName: fn,
        EventSourceArn: tbl.streamArn,
        StartingPosition: "TRIM_HORIZON",
        FilterCriteria: { Filters: [{ Pattern: JSON.stringify({ dynamodb: { Keys: { pk: { S: ["c1"] } } } }) }] },
      }),
    );
    const got = await eventually(
      async () => (await waitForLogged<string[]>(c, fn, "GOT ", 1, 8_000)).flat(),
      (v) => v.length >= 3,
      8_000,
      "3 records delivered",
    );
    // 実 AWS: ["c1"] だけ。エミュレータ: 全件
    assert.deepEqual(got, ["c1", "c2", "c3"]);
  });

  it("StartingPosition: LATEST でも、マッピング作成と同じ秒に書いたレコードが届く", async (t) => {
    const cl = new Cleanup();
    t.after(() => cl.run());
    const tbl = await createTable(c, cl, "latest", "KEYS_ONLY");
    const fn = await deployFunction(c, {
      zip: zipFiles({ "index.mjs": `export const handler = async (e) => { console.log("GOT " + JSON.stringify(e.Records.map((r) => r.dynamodb.Keys.pk.S))); return {}; };` }),
      prefix: "latest",
    });
    cl.fn(c, fn);
    // 秒の切り替わり直後に書き込み → すぐマッピング作成（同じ秒に収める）
    await sleep(1_000 - (Date.now() % 1_000) + 20);
    await put(c, tbl.name, { pk: "just-before" });
    const uuid = await createMapping(c, { FunctionName: fn, EventSourceArn: tbl.streamArn, StartingPosition: "LATEST" });
    cl.mapping(c, uuid);
    await sleep(500);
    await put(c, tbl.name, { pk: "after" });
    const got = await eventually(
      async () => (await waitForLogged<string[]>(c, fn, "GOT ", 1, 8_000)).flat(),
      (v) => v.includes("after"),
      8_000,
      "record written after mapping",
    );
    // 実 AWS: LATEST は作成時点より前のレコードを読まない（期待値 ["after"]）
    assert.deepEqual(got, ["just-before", "after"]);
  });

  it("実行環境が再利用されない（毎回コールドスタート、モジュールの状態が残らない）", async (t) => {
    const cl = new Cleanup();
    t.after(() => cl.run());
    const fn = await deployFunction(c, {
      zip: zipFiles({ "index.mjs": `let count = 0; export const handler = async () => ({ count: ++count });` }),
      prefix: "warm",
    });
    cl.fn(c, fn);
    const counts: number[] = [];
    for (let i = 0; i < 3; i++) {
      const r = await c.lambda.send(new InvokeCommand({ FunctionName: fn }));
      counts.push(JSON.parse(new TextDecoder().decode(r.Payload)).count);
      await sleep(50);
    }
    // 実 Lambda: 再利用されれば 1,2,3 になり得る（保証はない）。エミュレータ: 常に 1
    assert.deepEqual(counts, [1, 1, 1]);
  });
});
