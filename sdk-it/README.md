# AWS Emulator × AWS SDK for JavaScript v3 × node:test 結合テスト

```bash
npm ci
npm run typecheck
npm run test:it      # 空きポートでエミュレータを起動 → テスト → 停止
```

エミュレータは `EMULATOR_BIN` → PATH 上の `aws-emulator` → `/tmp/ws/aws-emulator-try/pkg/opt/aws-emulator/bin/aws-emulator` の順に探す。

## 検証結果 (2026-11-11)
- aws-emulator 1.0.322 / Node.js 22.23.4 / @aws-sdk/* 3.1167.0 / TypeScript 7.0.2 / tsx 4.23.15 / @types/node 22.20.5 / @smithy/types 4.20.0
- tsc (strict) 0 エラー、node:test 20/20 pass、起動〜停止込み 約5.5秒、連続3回とも成功
- ミューテーション確認: 状態条件を常に真にすると 2 件が意図どおり失敗

## 注意: SQS は AWS_ENDPOINT_URL だけでは実 AWS へ向かう
エミュレータの QueueUrl は `https://sqs.us-east-1.amazonaws.com/000000000000/...` 形式。
SDK v3 の SQSClient は既定 `useQueueUrlAsEndpoint: true` で、`endpoint` を明示しないと QueueUrl のホストへ送る
(@aws-sdk/middleware-sdk-sqs 3.972.50 の queueUrlMiddleware)。エミュレータ利用時は `useQueueUrlAsEndpoint: false` を指定する。
回帰テスト「既定の SQSClient は…実 AWS へ向かう」で検知している。

## 安全装置
1. 実認証情報の環境変数が残っていたら開始しない
2. STS で Account=000000000000 を確認
3. finalizeRequest ミドルウェアで 127.0.0.1 以外への送信をブロック
   (NODE_EXTRA_CA_CERTS は CA を「追加」するだけなので、TLS では外向き通信が止まらないため)
