import { GetObjectCommand, NoSuchKey, PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

export class ReceiptStore {
  constructor(
    private readonly s3: S3Client,
    private readonly bucket: string,
  ) {}

  private key(orderId: string): string {
    return `receipts/${orderId}.txt`;
  }

  async save(orderId: string, body: string): Promise<void> {
    await this.s3.send(
      new PutObjectCommand({ Bucket: this.bucket, Key: this.key(orderId), Body: body, ContentType: "text/plain; charset=utf-8" }),
    );
  }

  async load(orderId: string): Promise<string | undefined> {
    try {
      const r = await this.s3.send(new GetObjectCommand({ Bucket: this.bucket, Key: this.key(orderId) }));
      return await r.Body?.transformToString();
    } catch (e) {
      if (e instanceof NoSuchKey) return undefined;
      throw e;
    }
  }

  downloadUrl(orderId: string, expiresIn = 60): Promise<string> {
    return getSignedUrl(this.s3, new GetObjectCommand({ Bucket: this.bucket, Key: this.key(orderId) }), { expiresIn });
  }
}
