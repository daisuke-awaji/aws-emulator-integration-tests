import { CreateTableCommand, DeleteTableCommand, type StreamViewType, waitUntilTableExists } from "@aws-sdk/client-dynamodb";
import { DeleteCommand, PutCommand, ScanCommand } from "@aws-sdk/lib-dynamodb";
import { type Cleanup, type Clients, eventually, functionLogs, uniqueName } from "./emulator.ts";

export interface StreamTable {
  name: string;
  streamArn: string;
}

/** pk(S) だけのテーブル。view を渡すとストリームを有効化する */
export async function createTable(c: Clients, cl: Cleanup, prefix: string, view?: StreamViewType): Promise<StreamTable> {
  const name = uniqueName(prefix);
  const r = await c.ddb.send(
    new CreateTableCommand({
      TableName: name,
      BillingMode: "PAY_PER_REQUEST",
      AttributeDefinitions: [{ AttributeName: "pk", AttributeType: "S" }],
      KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }],
      ...(view ? { StreamSpecification: { StreamEnabled: true, StreamViewType: view } } : {}),
    }),
  );
  cl.add(() => c.ddb.send(new DeleteTableCommand({ TableName: name })));
  await waitUntilTableExists({ client: c.ddb, maxWaitTime: 30, minDelay: 1, maxDelay: 1 }, { TableName: name });
  return { name, streamArn: r.TableDescription?.LatestStreamArn ?? "" };
}

export const put = (c: Clients, table: string, item: Record<string, unknown>) => c.doc.send(new PutCommand({ TableName: table, Item: item }));
export const del = (c: Clients, table: string, pk: string) => c.doc.send(new DeleteCommand({ TableName: table, Key: { pk } }));

export async function scanAll(c: Clients, table: string): Promise<Record<string, unknown>[]> {
  return (await c.doc.send(new ScanCommand({ TableName: table, ConsistentRead: true }))).Items ?? [];
}

export interface ProjectionRow {
  pk: string; // SequenceNumber
  eventName: string;
  itemKey: string;
  itemStatus: string | null;
  processedOrder: number;
  deliveries: number;
}

/** 射影行を SequenceNumber 順に返す（SequenceNumber は桁数が揃った数字列なので BigInt で比較） */
export async function projection(c: Clients, table: string): Promise<ProjectionRow[]> {
  const rows = (await scanAll(c, table)) as unknown as ProjectionRow[];
  return rows.sort((a, b) => (BigInt(a.pk) < BigInt(b.pk) ? -1 : 1));
}

/** console.log(prefix + JSON) した行を拾う（関数ごとのロググループ） */
export async function loggedJson<T = unknown>(c: Clients, fn: string, prefix: string): Promise<T[]> {
  return (await functionLogs(c, fn)).filter((l) => l.startsWith(prefix)).map((l) => JSON.parse(l.slice(prefix.length)) as T);
}

export async function waitForLogged<T = unknown>(c: Clients, fn: string, prefix: string, count: number, timeoutMs: number): Promise<T[]> {
  return eventually(() => loggedJson<T>(c, fn, prefix), (v) => v.length >= count, timeoutMs, `${count} "${prefix}" log lines from ${fn}`);
}
