import assert from "node:assert/strict";
import { after, before, describe, it } from "node:test";
import { DynamoDBServiceException } from "@aws-sdk/client-dynamodb";
import { PutCommand } from "@aws-sdk/lib-dynamodb";
import { InvalidTransitionError, type Order, OrderAlreadyExistsError, OrderRepository } from "../src/order-repository.ts";
import {
  assertEmulatorAccount,
  assertEmulatorEnvironment,
  type Clients,
  createClients,
  createOrdersTable,
  deleteTable,
  eventually,
} from "./helpers/emulator.ts";

const order = (orderId: string, over: Partial<Order> = {}): Order => ({
  orderId,
  customerId: "c-1",
  amount: 1200,
  status: "PENDING",
  createdAt: "2026-11-11T00:00:00.000Z",
  ...over,
});

describe("OrderRepository (DynamoDB on AWS Emulator)", () => {
  let c: Clients;
  let table: string;
  let repo: OrderRepository;

  before(async () => {
    assertEmulatorEnvironment();
    c = createClients();
    await assertEmulatorAccount(c.sts);
    table = await createOrdersTable(c.ddb); // テストファイルごとに専用テーブル
    repo = new OrderRepository(c.doc, table);
  });

  after(async () => {
    if (table) await deleteTable(c.ddb, table);
    for (const client of [c?.ddb, c?.s3, c?.sqs, c?.sts]) client?.destroy();
  });

  it("作成した注文を強い整合性読み込みで取得できる", async () => {
    await repo.create(order("o-1"));
    assert.deepEqual(await repo.get("o-1"), order("o-1"));
  });

  it("存在しない注文は undefined", async () => {
    assert.equal(await repo.get("missing"), undefined);
  });

  it("同じ orderId の二重作成は ConditionalCheckFailed → OrderAlreadyExistsError", async () => {
    await repo.create(order("o-dup"));
    await assert.rejects(repo.create(order("o-dup", { amount: 1 })), OrderAlreadyExistsError);
    assert.equal((await repo.get("o-dup"))?.amount, 1200, "既存データは上書きされない");
  });

  it("状態遷移は PENDING→PAID→SHIPPED の順だけ許可される", async () => {
    await repo.create(order("o-flow"));
    assert.equal(await repo.advance("o-flow", "PENDING"), "PAID");
    await assert.rejects(repo.advance("o-flow", "PENDING"), InvalidTransitionError, "同じ遷移の再実行は拒否");
    assert.equal(await repo.advance("o-flow", "PAID"), "SHIPPED");
    await assert.rejects(repo.advance("o-flow", "SHIPPED"), InvalidTransitionError);
  });

  it("存在しない注文の遷移も条件失敗として扱う（upsert しない）", async () => {
    await assert.rejects(repo.advance("ghost", "PENDING"), InvalidTransitionError);
    assert.equal(await repo.get("ghost"), undefined);
  });

  it("同時に同じ遷移を2回走らせると片方だけ成功する", async () => {
    await repo.create(order("o-race"));
    const results = await Promise.allSettled([repo.advance("o-race", "PENDING"), repo.advance("o-race", "PENDING")]);
    assert.equal(results.filter((r) => r.status === "fulfilled").length, 1);
    const rejected = results.find((r) => r.status === "rejected");
    assert.ok(rejected?.status === "rejected" && rejected.reason instanceof InvalidTransitionError);
  });

  it("GSI で顧客ごとの注文を新しい順に取得できる", async () => {
    await repo.create(order("g-1", { customerId: "c-gsi", createdAt: "2026-11-01T00:00:00Z" }));
    await repo.create(order("g-3", { customerId: "c-gsi", createdAt: "2026-11-03T00:00:00Z" }));
    await repo.create(order("g-2", { customerId: "c-gsi", createdAt: "2026-11-02T00:00:00Z" }));
    const ids = await eventually(() => repo.listByCustomer("c-gsi"), (v) => v.length === 3);
    assert.deepEqual(ids, ["g-3", "g-2", "g-1"]);
  });

  it("トランザクション: 片方が既存なら両方とも書き込まれない", async () => {
    await repo.create(order("t-existing"));
    await assert.rejects(repo.createPair(order("t-new"), order("t-existing")), (e: unknown) => {
      assert.ok(e instanceof OrderAlreadyExistsError);
      assert.match(e.message, /None,ConditionalCheckFailed/);
      return true;
    });
    assert.equal(await repo.get("t-new"), undefined, "ロールバックされている");
  });

  it("実 DynamoDB と同じ制約エラー: キー属性の型違いは ValidationException", async () => {
    await assert.rejects(c.doc.send(new PutCommand({ TableName: table, Item: { pk: 123 } })), (e: unknown) => {
      assert.ok(e instanceof DynamoDBServiceException);
      assert.equal(e.name, "ValidationException");
      assert.equal(e.$metadata.httpStatusCode, 400);
      return true;
    });
  });

  it("実 DynamoDB と同じ制約エラー: 400KB 超のアイテムは ValidationException", async () => {
    const big = "x".repeat(410 * 1024);
    await assert.rejects(c.doc.send(new PutCommand({ TableName: table, Item: { pk: "big", blob: big } })), {
      name: "ValidationException",
    });
  });
});
