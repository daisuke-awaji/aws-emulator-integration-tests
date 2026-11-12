import { SendMessageBatchCommand, SQSClient } from "@aws-sdk/client-sqs";

export interface OrderEvent {
  type: "OrderCreated" | "OrderPaid";
  orderId: string;
}

/** SendMessageBatch は一部が失敗しても例外にならないため、失敗した Id を呼び出し側へ返す */
export async function publishOrderEvents(
  sqs: SQSClient,
  queueUrl: string,
  events: OrderEvent[],
): Promise<{ sent: number; failedIds: string[] }> {
  if (events.length > 10) throw new RangeError("SQS batch accepts at most 10 entries");
  const r = await sqs.send(
    new SendMessageBatchCommand({
      QueueUrl: queueUrl,
      Entries: events.map((e, i) => ({
        Id: `m${i}`,
        MessageBody: JSON.stringify(e),
        MessageAttributes: { type: { DataType: "String", StringValue: e.type } },
      })),
    }),
  );
  return { sent: r.Successful?.length ?? 0, failedIds: (r.Failed ?? []).map((f) => f.Id ?? "?") };
}
