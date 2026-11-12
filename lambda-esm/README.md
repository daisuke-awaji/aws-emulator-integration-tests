# AWS Emulator で SQS → Lambda イベントソースマッピングを node:test で検証

```bash
npm ci
npm run typecheck
npm run test:it   # 空きポートでエミュレータ起動 → esbuild でハンドラーをビルド → デプロイ → テスト → 停止
```

## 結果 (2026-11-11, aws-emulator 1.0.322 = 最新リリース)
- node:test 11/11 pass、約 12 秒、連続 3 回成功、エミュレータ残存プロセスなし
- ミューテーション: 不正メッセージを握りつぶす → 1 件失敗 / 1 件の不正でバッチ全体を throw → 2 件失敗

### 実 AWS と同じように動いたこと
| 項目 | 観測 |
|---|---|
| ESM の作成 | `Creating` → `Enabled`、無効化/有効化 (`Enabled: false` → `UpdateEventSourceMapping`) |
| 成功時 | メッセージは削除され、可視性タイムアウト後も再配信されない |
| `ReportBatchItemFailures` | 失敗分だけ再配信 (`ApproximateReceiveCount` 1→2→…)、正常分は 1 回だけ処理 |
| 未設定時 | 関数が返した `batchItemFailures` は無視され、バッチ全体が成功扱い |
| 関数が throw | バッチ全体が再試行 |
| RedrivePolicy | `maxReceiveCount` 到達で DLQ へ移動 |
| FilterCriteria | 合わないメッセージは関数を呼ばずに削除 |
| 作成時の検証 | 可視性タイムアウト < 関数タイムアウトで `InvalidParameterValueException` |
| ログ | CloudWatch Logs `/aws/lambda/<name>` に START/END/REPORT と console.log |

### 実 Lambda と違うこと（`test/runtime-differences.it.test.ts` で固定）
1. **関数は本物の Node.js ではなく、組み込み JS エンジン (QuickJS) で動く**。
   `process.version` は `v22.0.0` 固定、`process.versions` は `node` だけ。`structuredClone` / `fetch` /
   `node:http` / `node:https` / `node:zlib` が無い。`node:crypto` / `node:fs` / `node:path` / `node:buffer` / `node:util` はある。
2. **関数内の `@aws-sdk/*` はエミュレータの組み込み実装** (`/var/runtime/node_modules`)。endpoint 設定なしでそのエミュレータにつながる。
   DynamoDB / lib-dynamodb / S3 / SQS / EventBridge / Secrets Manager はコマンドがあるが、**STS / SNS / SSM はコマンドが無い**（import すると `Runtime.UserCodeSyntaxError`）。
3. **本物の SDK をバンドルに含めると `node:zlib` が無く `Runtime.ImportModuleError`**。esbuild で `--external:@aws-sdk/*` にする必要がある。
4. **SQS イベントの `messageAttributes` に `stringListValues` / `binaryListValues` が無い**（実 Lambda の例には空配列で入っている）。
5. 関数のコードはローカルマシン上で隔離されずに動く（公式の制限事項）。IAM（実行ロールの権限）は評価されない。

### 前回からの引き継ぎ
- テスト側の `SQSClient` は `useQueueUrlAsEndpoint: false` が必須（QueueUrl が実 AWS 形式のため）
- 安全装置: 実認証情報の環境変数チェック / STS で 000000000000 確認 / 127.0.0.1 以外への送信を送信前に遮断

## 構成
- `src/order-consumer.ts` — テスト対象ハンドラー（不正メッセージは部分失敗、インフラ障害は throw）
- `test/helpers/emulator.ts` — クライアント生成・安全装置・esbuild ビルド・デプロイ・ESM 作成・後片付け
- `test/helpers/zip.ts` — 依存なしの最小 ZIP 作成（`zip` コマンド不要）
- `test/sqs-esm.it.test.ts` — ESM の振る舞い（8 件、各テストが専用のキュー/関数/テーブルを持ち並行実行）
- `test/runtime-differences.it.test.ts` — エミュレータ固有の差異（3 件）
- `probe/` — 調査に使った使い捨てスクリプト（テストには含まれない）
