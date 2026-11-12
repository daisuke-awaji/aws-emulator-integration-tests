# AWS Emulator で DynamoDB Streams → Lambda イベントソースマッピングを node:test で検証

```bash
npm ci
npm run typecheck
npm run test:it   # 空きポートでエミュレータ起動 → esbuild でハンドラーをビルド → デプロイ → テスト → 停止
```
エミュレータは `EMULATOR_BIN` → PATH 上の `aws-emulator` → `/tmp/ws/aws-emulator-try/pkg/opt/aws-emulator/bin/aws-emulator` の順に探す。
sudo なし環境では公式 `.deb` を `dpkg-deb -x` で展開して使用。

## 結果 (2026-11-12, aws-emulator 1.0.322 = 最新リリース)
- node:test 11/11 pass、約 18 秒、連続 5 回成功、エミュレータ残存プロセスなし
- ミューテーション 4 種（一時障害を握りつぶす / 失敗後も続行 / 誤ったシーケンス番号を返す / 全面障害を無視）すべて、狙ったテストだけが失敗

### 実 AWS と同じように動いたこと（`test/streams-esm.it.test.ts`, 6 件）
| 項目 | 観測 |
|---|---|
| イベント | INSERT / MODIFY / REMOVE が SequenceNumber 順に届く。`eventSource: aws:dynamodb`, `eventVersion: 1.1` |
| StreamViewType | KEYS_ONLY / NEW_IMAGE / OLD_IMAGE / NEW_AND_OLD_IMAGES で含まれるイメージが仕様どおり |
| StartingPosition | TRIM_HORIZON は作成前のレコードも読む。LATEST は作成後だけ（※下の差異 5 を参照） |
| ReportBatchItemFailures | 返したシーケンス番号から再開。手前の成功分は再処理されず、後ろのレコードは追い越さない |
| 関数が throw | シャードが止まり、原因を直すと同じ位置から順番どおり再開（`MaximumRetryAttempts` 既定 -1 の挙動） |
| 作成時の検証 | `StartingPosition` 省略・`AT_TIMESTAMP`・各上限値は実 AWS と同じエラー |

### 実 AWS と違うこと（`test/streams-differences.it.test.ts`, 5 件で固定）
1. **`MaximumRetryAttempts` が効かない**: 0 にしても再試行が止まらない（25 秒で 14 回、間隔は約 250ms ずつ伸びる）。設定値は保存され、`LastProcessingResult` は `PROBLEM: Function call failed`。
2. **`DestinationConfig.OnFailure` に何も送られない**（1 の結果として、破棄が起きない）。
3. **`BisectBatchOnFunctionError` が効かない**: 6 件のバッチが毎回 6 件のまま再試行される。
4. **`FilterCriteria` が無視される**: ドキュメント例と同じ形のパターン（Keys / NewImage / New+Old の AND）でも全レコードが届く。SQS の ESM ではフィルタが効いていた。
5. **`LATEST` でも、マッピング作成と同じ秒に書いたレコードが届く**ことがある（間隔 0〜750ms で発生、1 秒以上なら発生せず）。
   Streams API の `GetShardIterator(LATEST)` は正しい。秒単位で開始位置を決めていると推測（実装は未確認）。
6. **実行環境が再利用されない**: 呼び出しごとにモジュールが再評価される（モジュールスコープの状態・キャッシュが残らない）。

実 AWS の仕様: [services-dynamodb-errors](https://docs.aws.amazon.com/lambda/latest/dg/services-dynamodb-errors.html), [with-ddb-filtering](https://docs.aws.amazon.com/lambda/latest/dg/with-ddb-filtering.html), [re:Post: troubleshoot DynamoDB Streams](https://repost.aws/knowledge-center/lambda-functions-fix-dynamodb-streams)

### 前回 (lambda-esm) から引き継ぐ前提
- 関数は組み込み JS エンジンで動く。`@aws-sdk/*` は external にしてバンドル（同梱 SDK を使う）
- テスト側 `SQSClient` は `useQueueUrlAsEndpoint: false`
- 安全装置: 実認証情報の環境変数チェック / STS で 000000000000 確認 / 127.0.0.1 以外への送信を遮断

## 構成
- `src/stream-projector.ts` — テスト対象。ストリーム → 射影テーブル。一時失敗は最初の失敗レコードで止めて部分失敗、全面障害は throw
- `test/helpers/emulator.ts` — 前回と共通（クライアント・安全装置・ビルド・デプロイ・ESM・後片付け）
- `test/helpers/streams.ts` — ストリーム付きテーブル作成、射影の読み出し、ログ待ち
- `test/streams-esm.it.test.ts` / `test/streams-differences.it.test.ts` — 上記テスト
- `probe/` — 調査に使った使い捨てスクリプト（テストには含まれない）
