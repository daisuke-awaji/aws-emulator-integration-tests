#!/usr/bin/env bash
# 使い方: bash scripts/with-emulator.sh <テストコマンド...>
# AWS Emulator を空きポートで起動し、SDK をそこへ向ける環境変数を設定してコマンドを実行、終了時に必ず停止する。
set -euo pipefail

EMULATOR_BIN="${EMULATOR_BIN:-$(command -v aws-emulator || echo /tmp/ws/aws-emulator-try/pkg/opt/aws-emulator/bin/aws-emulator)}"
EMU_HOME="$(mktemp -d)"          # 認証情報・CA を実 HOME に書かせない
LOG="$EMU_HOME/emulator.log"

# 実 AWS の認証情報になり得る変数をすべて外す（環境変数は credentials ファイルより優先されるため）
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_PROFILE \
      AWS_CONTAINER_CREDENTIALS_FULL_URI AWS_CONTAINER_CREDENTIALS_RELATIVE_URI \
      AWS_CONTAINER_AUTHORIZATION_TOKEN AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE \
      AWS_WEB_IDENTITY_TOKEN_FILE AWS_ROLE_ARN

HOME="$EMU_HOME" "$EMULATOR_BIN" --telemetry off --port 0 --idle-timeout 300 >"$LOG" 2>&1 &
EMU_PID=$!
cleanup() { kill "$EMU_PID" 2>/dev/null || true; wait "$EMU_PID" 2>/dev/null || true; rm -rf "$EMU_HOME"; }
trap cleanup EXIT

# 起動ログから実際の endpoint を取得（--port 0 で空きポートが選ばれる）
ENDPOINT=""
for _ in $(seq 60); do
  ENDPOINT="$(grep -o '"endpoint_url":"[^"]*"' "$LOG" | head -1 | cut -d'"' -f4 || true)"
  [ -n "$ENDPOINT" ] && break
  sleep 0.5
done
[ -n "$ENDPOINT" ] || { echo "emulator did not start"; cat "$LOG"; exit 1; }
PORT="${ENDPOINT##*:}"
D="$EMU_HOME/.aws/emulator/$PORT"

for _ in $(seq 30); do
  curl -sf --cacert "$D/ca.pem" "$ENDPOINT/__admin/health" >/dev/null && break
  sleep 0.5
done

export AWS_ENDPOINT_URL="$ENDPOINT"
export AWS_SHARED_CREDENTIALS_FILE="$D/emulator_credentials"
export AWS_CONFIG_FILE="$D/config"
export AWS_CA_BUNDLE="$D/ca.pem"
export NODE_EXTRA_CA_CERTS="$D/ca.pem"   # SDK for JavaScript v3 (Node.js) はこちらを読む
export AWS_EC2_METADATA_DISABLED=true
export AWS_REGION=us-east-1 AWS_DEFAULT_REGION=us-east-1
export EMULATOR_LOG="$LOG"

echo "AWS Emulator: $ENDPOINT ($("$EMULATOR_BIN" -v | head -1))"
set +e
"$@"
STATUS=$?
set -e
echo "emulator WARN/ERROR lines: $(grep -cE '"level":"(WARN|ERROR)"' "$LOG" || true)"
exit $STATUS
