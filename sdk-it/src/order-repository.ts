import { ConditionalCheckFailedException, TransactionCanceledException } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
  QueryCommand,
  TransactWriteCommand,
  UpdateCommand,
} from "@aws-sdk/lib-dynamodb";

export type OrderStatus = "PENDING" | "PAID" | "SHIPPED";
export interface Order {
  orderId: string;
  customerId: string;
  amount: number;
  status: OrderStatus;
  createdAt: string;
}

export class OrderAlreadyExistsError extends Error {}
export class InvalidTransitionError extends Error {}

const NEXT: Record<OrderStatus, OrderStatus | undefined> = { PENDING: "PAID", PAID: "SHIPPED", SHIPPED: undefined };

const toItem = (o: Order) => ({ pk: `ORDER#${o.orderId}`, gsi1pk: `CUSTOMER#${o.customerId}`, gsi1sk: o.createdAt, ...o });

export class OrderRepository {
  constructor(
    private readonly doc: DynamoDBDocumentClient,
    private readonly tableName: string,
  ) {}

  async create(order: Order): Promise<void> {
    try {
      await this.doc.send(
        new PutCommand({ TableName: this.tableName, Item: toItem(order), ConditionExpression: "attribute_not_exists(pk)" }),
      );
    } catch (e) {
      if (e instanceof ConditionalCheckFailedException) throw new OrderAlreadyExistsError(order.orderId);
      throw e;
    }
  }

  async get(orderId: string): Promise<Order | undefined> {
    const r = await this.doc.send(
      new GetCommand({ TableName: this.tableName, Key: { pk: `ORDER#${orderId}` }, ConsistentRead: true }),
    );
    if (!r.Item) return undefined;
    const { pk: _pk, gsi1pk: _g1, gsi1sk: _g2, ...order } = r.Item;
    return order as Order;
  }

  /** 顧客の注文IDを新しい順に返す（GSI は結果整合） */
  async listByCustomer(customerId: string): Promise<string[]> {
    const r = await this.doc.send(
      new QueryCommand({
        TableName: this.tableName,
        IndexName: "gsi1",
        KeyConditionExpression: "gsi1pk = :c",
        ExpressionAttributeValues: { ":c": `CUSTOMER#${customerId}` },
        ScanIndexForward: false,
      }),
    );
    return (r.Items ?? []).map((i) => i.orderId as string);
  }

  /** 現在の状態が from のときだけ次の状態へ進める（条件付き更新） */
  async advance(orderId: string, from: OrderStatus): Promise<OrderStatus> {
    const to = NEXT[from];
    if (!to) throw new InvalidTransitionError(`${from} is terminal`);
    try {
      const r = await this.doc.send(
        new UpdateCommand({
          TableName: this.tableName,
          Key: { pk: `ORDER#${orderId}` },
          UpdateExpression: "SET #s = :to",
          ConditionExpression: "attribute_exists(pk) AND #s = :from",
          ExpressionAttributeNames: { "#s": "status" },
          ExpressionAttributeValues: { ":to": to, ":from": from },
          ReturnValues: "UPDATED_NEW",
        }),
      );
      return r.Attributes?.status as OrderStatus;
    } catch (e) {
      if (e instanceof ConditionalCheckFailedException) throw new InvalidTransitionError(`${orderId} is not ${from}`);
      throw e;
    }
  }

  /** 2件をトランザクションで作成。どちらかが既存なら両方作らない */
  async createPair(a: Order, b: Order): Promise<void> {
    const put = (o: Order) => ({
      Put: { TableName: this.tableName, Item: toItem(o), ConditionExpression: "attribute_not_exists(pk)" },
    });
    try {
      await this.doc.send(new TransactWriteCommand({ TransactItems: [put(a), put(b)] }));
    } catch (e) {
      if (e instanceof TransactionCanceledException) {
        const codes = (e.CancellationReasons ?? []).map((r) => r.Code ?? "None");
        throw new OrderAlreadyExistsError(`transaction canceled: ${codes.join(",")}`);
      }
      throw e;
    }
  }
}
