#!/bin/bash

# FastAPI サーバー起動スクリプト

# スクリプトのディレクトリに移動
cd "$(dirname "$0")"

# 環境変数の設定（必要に応じて変更）
export REDIS_URL="${REDIS_URL:-redis://localhost:6379/0}"
export ROOT_PATH="${ROOT_PATH:-}"

# ホストとポートの設定
HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8099}"

# uvicorn がインストールされているか確認
if ! command -v uvicorn &> /dev/null; then
    echo "Error: uvicorn is not installed."
    echo "Please install it with: pip install uvicorn[standard]"
    exit 1
fi

if lsof -nP -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "Error: port $PORT is already in use."
  lsof -nP -iTCP:"$PORT" -sTCP:LISTEN
  exit 1
fi


# サーバー起動
echo "Starting FastAPI server..."
echo "  Host: $HOST"
echo "  Port: $PORT"
echo "  Redis URL: $REDIS_URL"
echo "  Root Path: $ROOT_PATH"
echo ""

uvicorn server:app \
    --host "$HOST" \
    --port "$PORT" 
