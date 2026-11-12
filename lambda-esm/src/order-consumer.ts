import type { SQSBatchItemFailure, SQSBatchResponse, SQSEvent, SQSRecord } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, UpdateCommand } from "@aws-sdk/lib-dynamodb";

const doc = DynamoDBDocumentClient.from(new DynamoDBClient({}));

export interface OrderEvent {
  orderId: string;
  amount: number;
}

/** 形式が不正なメッセージは再試行しても直らない → そのメッセージだけ失敗として返す */
export function parseOrderEvent(r: Pick<SQSRecord, "body">): OrderEvent | undefined {
  try {
    const v: unknown = JSON.parse(r.body);
    if (typeof v !== "object" || v === null) return undefined;
    const { orderId, amount } = v as Record<string, unknown>;
    if (typeof orderId === "string" && orderId !== "" && Number.isInteger(amount) && (amount as number) > 0) {
      return { orderId, amount: amount as number };
    }
  } catch {
    // fallthrough
  }
  return undefined;
}

/**
 * - 不正メッセージ: batchItemFailures に入れる（他のメッセージは削除される）
 * - DynamoDB 障害など: throw してバッチ全体を再試行させる
 * - deliveries を ADD して、同じメッセージが何回処理されたかを観測できるようにする
 */
export const handler = async (event: SQSEvent): Promise<SQSBatchResponse> => {
  const tableName = process.env.TABLE_NAME;
  if (!tableName) throw new Error("TABLE_NAME is not set");
  const batchItemFailures: SQSBatchItemFailure[] = [];
  for (const r of event.Records) {
    const e = parseOrderEvent(r);
    if (!e) {
      console.log(JSON.stringify({ level: "WARN", msg: "invalid order event", messageId: r.messageId }));
      batchItemFailures.push({ itemIdentifier: r.messageId });
      continue;
    }
    await doc.send(
      new UpdateCommand({
        TableName: tableName,
        Key: { pk: e.orderId },
        UpdateExpression: "SET amount = :a, lastMessageId = :m ADD deliveries :one",
        ExpressionAttributeValues: { ":a": e.amount, ":m": r.messageId, ":one": 1 },
      }),
    );
  }
  return { batchItemFailures };
};
