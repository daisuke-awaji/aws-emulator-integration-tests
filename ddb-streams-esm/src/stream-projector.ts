import type { DynamoDBBatchResponse, DynamoDBRecord, DynamoDBStreamEvent } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, GetCommand, UpdateCommand } from "@aws-sdk/lib-dynamodb";

const doc = DynamoDBDocumentClient.from(new DynamoDBClient({}));

/**
 * 「下流サービス」の状態を制御テーブルで模擬する（テストから切り替える）
 *   pk = "crash"         → 下流が全面停止。throw してシャードごと止める
 *   pk = "block#<key>"   → そのキューの下流だけ一時的に失敗
 */
async function controlFlag(table: string, pk: string): Promise<boolean> {
  const r = await doc.send(new GetCommand({ TableName: table, Key: { pk }, ConsistentRead: true }));
  return r.Item !== undefined;
}

async function nextProcessingOrder(control: string): Promise<number> {
  const r = await doc.send(
    new UpdateCommand({
      TableName: control,
      Key: { pk: "counter" },
      UpdateExpression: "ADD n :one",
      ExpressionAttributeValues: { ":one": 1 },
      ReturnValues: "UPDATED_NEW",
    }),
  );
  return Number(r.Attributes?.n);
}

export function describeRecord(r: DynamoDBRecord): { seq: string; key: string; status: string | null } {
  const seq = r.dynamodb?.SequenceNumber;
  const key = r.dynamodb?.Keys?.pk?.S;
  if (!seq || !key) throw new Error("record without SequenceNumber or Keys.pk.S");
  const status = r.dynamodb?.NewImage?.status?.S ?? null;
  return { seq, key, status };
}

/**
 * DynamoDB Streams → 射影テーブル。SequenceNumber ごとに 1 行、deliveries で処理回数を数える。
 * ストリームは順序に意味があるので、一時失敗した最初のレコードで止めてそれ以降を次回に回す
 * （ReportBatchItemFailures では「最初に失敗したシーケンス番号」から再開される）。
 */
export const handler = async (event: DynamoDBStreamEvent): Promise<DynamoDBBatchResponse> => {
  const projection = process.env.PROJECTION_TABLE;
  const control = process.env.CONTROL_TABLE;
  if (!projection || !control) throw new Error("PROJECTION_TABLE / CONTROL_TABLE is not set");
  if (await controlFlag(control, "crash")) throw new Error("downstream is down");

  for (const r of event.Records) {
    const { seq, key, status } = describeRecord(r);
    if (await controlFlag(control, `block#${key}`)) {
      console.log(JSON.stringify({ level: "WARN", msg: "transient failure", key, seq }));
      return { batchItemFailures: [{ itemIdentifier: seq }] };
    }
    const order = await nextProcessingOrder(control);
    await doc.send(
      new UpdateCommand({
        TableName: projection,
        Key: { pk: seq },
        UpdateExpression: "SET eventName = :e, itemKey = :k, itemStatus = :s, processedOrder = :o ADD deliveries :one",
        ExpressionAttributeValues: { ":e": r.eventName, ":k": key, ":s": status, ":o": order, ":one": 1 },
      }),
    );
  }
  return { batchItemFailures: [] };
};
