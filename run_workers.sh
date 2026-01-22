#!/bin/bash

# Worker プロセス管理スクリプト
# 使用方法:
#   ./run_workers.sh start [NUM_WORKERS]  - ワーカーを起動（デフォルト: 4）
#   ./run_workers.sh stop                 - すべてのワーカーを停止
#   ./run_workers.sh status               - ワーカーの状態を表示
#   ./run_workers.sh restart [NUM_WORKERS] - ワーカーを再起動

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PID_FILE="${SCRIPT_DIR}/.worker_pids"
LOG_DIR="${SCRIPT_DIR}/logs"
NUM_WORKERS="${2:-4}"

# ログディレクトリの作成
mkdir -p "$LOG_DIR"

# PIDファイルからプロセスIDを読み込む
read_pids() {
    if [ -f "$PID_FILE" ]; then
        cat "$PID_FILE"
    fi
}

# PIDファイルにプロセスIDを書き込む
write_pids() {
    echo "$1" >> "$PID_FILE"
}

# PIDファイルをクリア
clear_pids() {
    rm -f "$PID_FILE"
}

# プロセスが実行中かチェック
is_running() {
    local pid=$1
    if ps -p "$pid" > /dev/null 2>&1; then
        return 0
    else
        return 1
    fi
}

# ワーカーを起動
start_workers() {
    # 既存のワーカーをチェック
    if [ -f "$PID_FILE" ]; then
        local existing_pids=$(read_pids)
        local running_count=0
        for pid in $existing_pids; do
            if is_running "$pid"; then
                running_count=$((running_count + 1))
            fi
        done
        
        if [ $running_count -gt 0 ]; then
            echo "Warning: $running_count worker(s) are already running."
            echo "Use './run_workers.sh stop' to stop them first, or './run_workers.sh restart' to restart."
            return 1
        else
            # 古いPIDファイルを削除
            clear_pids
        fi
    fi
    
    echo "Starting $NUM_WORKERS worker process(es)..."
    
    # スクリプトディレクトリに移動
    cd "$SCRIPT_DIR" || exit 1
    
    # 各ワーカーを起動
    for i in $(seq 1 "$NUM_WORKERS"); do
        python3 worker.py > "${LOG_DIR}/worker_${i}.log" 2>&1 &
        local pid=$!
        write_pids "$pid"
        echo "  Worker $i started (PID: $pid)"
    done
    
    echo ""
    echo "All workers started successfully!"
    echo "Log files are in: $LOG_DIR"
    echo "Use './run_workers.sh status' to check their status."
    echo "Use './run_workers.sh stop' to stop all workers."
}

# ワーカーを停止
stop_workers() {
    if [ ! -f "$PID_FILE" ]; then
        echo "No PID file found. Workers may not be running."
        return 1
    fi
    
    local pids=$(read_pids)
    local stopped_count=0
    local not_found_count=0
    
    echo "Stopping workers..."
    
    for pid in $pids; do
        if is_running "$pid"; then
            kill "$pid" 2>/dev/null
            if [ $? -eq 0 ]; then
                echo "  Stopped worker (PID: $pid)"
                stopped_count=$((stopped_count + 1))
            fi
        else
            not_found_count=$((not_found_count + 1))
        fi
    done
    
    # 少し待ってから強制終了
    sleep 1
    
    # まだ実行中のプロセスを強制終了
    for pid in $pids; do
        if is_running "$pid"; then
            echo "  Force killing worker (PID: $pid)"
            kill -9 "$pid" 2>/dev/null
            stopped_count=$((stopped_count + 1))
        fi
    done
    
    clear_pids
    
    echo ""
    if [ $stopped_count -gt 0 ]; then
        echo "Stopped $stopped_count worker(s)."
    else
        echo "No running workers found."
    fi
    if [ $not_found_count -gt 0 ]; then
        echo "Note: $not_found_count PID(s) were not found (may have already stopped)."
    fi
}

# ワーカーの状態を表示
status_workers() {
    if [ ! -f "$PID_FILE" ]; then
        echo "No workers are running (no PID file found)."
        return 0
    fi
    
    local pids=$(read_pids)
    local running_count=0
    local stopped_count=0
    
    echo "Worker Status:"
    echo "=============="
    
    for pid in $pids; do
        if is_running "$pid"; then
            echo "  PID $pid: RUNNING"
            running_count=$((running_count + 1))
        else
            echo "  PID $pid: STOPPED"
            stopped_count=$((stopped_count + 1))
        fi
    done
    
    echo ""
    echo "Total: $running_count running, $stopped_count stopped"
    
    if [ $stopped_count -gt 0 ]; then
        echo ""
        echo "Note: Some workers have stopped. You may want to restart them."
    fi
}

# ワーカーを再起動
restart_workers() {
    echo "Restarting workers..."
    stop_workers
    sleep 1
    start_workers
}

# メイン処理
case "$1" in
    start)
        start_workers
        ;;
    stop)
        stop_workers
        ;;
    status)
        status_workers
        ;;
    restart)
        restart_workers
        ;;
    *)
        echo "Usage: $0 {start|stop|status|restart} [NUM_WORKERS]"
        echo ""
        echo "Commands:"
        echo "  start [NUM_WORKERS]  - Start worker processes (default: 4)"
        echo "  stop                 - Stop all worker processes"
        echo "  status               - Show status of worker processes"
        echo "  restart [NUM_WORKERS] - Restart worker processes"
        echo ""
        echo "Examples:"
        echo "  $0 start          # Start 4 workers (default)"
        echo "  $0 start 8        # Start 8 workers"
        echo "  $0 stop           # Stop all workers"
        echo "  $0 status         # Check worker status"
        exit 1
        ;;
esac

exit 0
