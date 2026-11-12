# AWS Emulator 試用メモ (2026-11-11)

- バージョン: aws-emulator 1.0.322 (Release 2026.11.10, commit 7753983c), Debian 12 arm64, sudoなし (dpkg-deb -x で展開)
- 起動: HOME=<作業dir>/home aws-emulator --telemetry off --idle-timeout 900
- CLI: AWS CLI v2.40.1 (作業フォルダにインストール)。※CLI v1.46.3 は SQS で AWS_ENDPOINT_URL を無視して queue.amazonaws.com を向いた (TLSで遮断、到達せず)
- 動作確認 OK: STS get-caller-identity(000000000000) / S3 mb,cp / DynamoDB ValidationException再現・create→wait→put→get / SQS send,receive / SNS→SQS (キューポリシー必須) / Lambda nodejs22.x create+invoke / 複数インスタンスの状態・キー分離
- 注意: IAMポリシーは評価しない(公式) / 1アカウント1リージョン / メモリ上、停止で消える / 未実装は501
