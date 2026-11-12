# AWS Emulator integration tests (AWS SDK for JavaScript v3 × node:test)

[AWS Emulator](https://docs.aws.amazon.com/emulator/latest/userguide/what-is-service.html) を使って、
実 AWS に触れずに結合テストを回せる範囲と、実 AWS と挙動が違う点を確かめた検証コードです。

| ディレクトリ | 内容 | テスト |
|---|---|---|
| [`sdk-it/`](sdk-it/) | SDK から DynamoDB（条件付き書き込み・GSI・トランザクション）/ S3（署名付き URL）/ SQS を呼ぶ結合テスト | 20 件 |
| [`lambda-esm/`](lambda-esm/) | SQS → Lambda イベントソースマッピング（部分バッチレスポンス・DLQ・フィルタ）と Lambda ランタイムの差異 | 11 件 |
| [`ddb-streams-esm/`](ddb-streams-esm/) | DynamoDB Streams → Lambda イベントソースマッピングと失敗制御の差異 | 11 件 |
| [`cli-demo/`](cli-demo/) | AWS CLI での最初の動作確認メモとデモスクリプト（作業環境のパスを含む記録） | — |

## 実行方法

前提: Linux（x86_64 / arm64）、Node.js 22、AWS Emulator（[Getting started on Linux](https://docs.aws.amazon.com/emulator/latest/userguide/getting-started-linux.html)）。

```bash
cd sdk-it            # または lambda-esm / ddb-streams-esm
npm ci
npm run typecheck
npm run test:it      # 空きポートでエミュレータを起動 → テスト → 停止
```

エミュレータは `EMULATOR_BIN` → PATH 上の `aws-emulator` の順に探します。
sudo がない環境では公式 `.deb` を `dpkg-deb -x` で展開し、`EMULATOR_BIN` で指定できます。

## 安全装置

テストは実 AWS に届かないよう、次の 3 つを必ず通します。

1. 実 AWS の認証情報になり得る環境変数が残っていたら開始しない（起動スクリプトでも unset）
2. STS で `Account=000000000000`（エミュレータ）であることを確認
3. SDK の送信直前（finalizeRequest ミドルウェア）で `127.0.0.1` 以外への送信を遮断

## 検証結果（aws-emulator 1.0.322 / 2026-11-12 時点）

- 3 プロジェクト計 42 件すべて成功（エミュレータの起動〜停止込みで約 5〜18 秒）
- 実 AWS と挙動が違う点を 11 件確認し、`*-differences.it.test.ts` で「現在の挙動」として固定
  （エミュレータが変わるとこれらのテストが落ちて気づける）
- 詳細は各ディレクトリの README を参照

挙動はエミュレータのバージョンで変わる可能性があります。IAM ポリシーが評価されない、関数コードが隔離されずに動く等は
[公式の制限事項](https://docs.aws.amazon.com/emulator/latest/userguide/what-is-service.html) です。
