#!/usr/bin/env python3
"""
複数のbbp_hexジョブをエンキューするスクリプト
"""
import argparse
import random
import sys
import requests

def main():
    parser = argparse.ArgumentParser(
        description="複数のbbp_hexジョブをエンキューします"
    )
    parser.add_argument(
        "--start",
        type=int,
        required=True,
        help="開始位置（必須）"
    )
    parser.add_argument(
        "--count",
        type=int,
        required=True,
        help="エンキューするジョブ数（必須）"
    )
    parser.add_argument(
        "--digits",
        type=int,
        default=1,
        help="各ジョブで計算する桁数（デフォルト: 1）"
    )
    parser.add_argument(
        "--base",
        type=str,
        default="http://localhost:8099",
        help="サーバーのベースURL（デフォルト: http://localhost:8099）"
    )
    parser.add_argument(
        "--randomize",
        action="store_true",
        help="エンキューの順序をランダマイズします"
    )
    
    args = parser.parse_args()
    
    if args.start < 0:
        print(f"Error: start must be non-negative, got {args.start}", file=sys.stderr)
        sys.exit(1)
    
    if args.count <= 0:
        print(f"Error: count must be positive, got {args.count}", file=sys.stderr)
        sys.exit(1)
    
    if args.digits <= 0:
        print(f"Error: digits must be positive, got {args.digits}", file=sys.stderr)
        sys.exit(1)
    
    base_url = args.base.rstrip("/")
    enqueue_url = f"{base_url}/enqueue"
    
    session = requests.Session()
    success_count = 0
    fail_count = 0
    
    # エンキューする順序を決定
    job_indices = list(range(args.count))
    if args.randomize:
        random.shuffle(job_indices)
        print(f"Enqueueing {args.count} jobs (start={args.start}, digits={args.digits}, randomized)...")
    else:
        print(f"Enqueueing {args.count} jobs (start={args.start}, digits={args.digits})...")
    
    for job_num, i in enumerate(job_indices, 1):
        start_pos = args.start + i*args.digits
        payload = {
            "type": "bbp_hex",
            "start": start_pos,
            "count": args.digits
        }
        
        try:
            response = session.post(enqueue_url, json=payload, timeout=10)
            response.raise_for_status()
            success_count += 1
            print(f"  ✓ Job {job_num}/{args.count}: start={start_pos}, count={args.digits}")
        except requests.exceptions.RequestException as e:
            fail_count += 1
            print(f"  ✗ Job {job_num}/{args.count}: start={start_pos}, count={args.digits} - Error: {e}", file=sys.stderr)
    
    print(f"\nCompleted: {success_count} succeeded, {fail_count} failed")
    
    if fail_count > 0:
        sys.exit(1)

if __name__ == "__main__":
    main()
