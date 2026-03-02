#!/usr/bin/env python
import sys
import csv
from datetime import datetime

"""
Usage:
    python tools/analyze_trades_csv.py logs/trades.csv

If no path is given, defaults to logs/trades.csv
"""

# 1) Resolve path
if len(sys.argv) == 1:
    csv_path = "logs/trades.csv"
elif len(sys.argv) == 2:
    csv_path = sys.argv[1]
else:
    print("Usage: python tools/analyze_trades_csv.py [trades_csv_path]")
    sys.exit(1)

# 2) Load CSV and collect only CLOSE events
try:
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        trades = [row for row in reader if row.get("event") == "close"]
except FileNotFoundError:
    print(f"[ERROR] File not found: {csv_path}")
    sys.exit(1)

if not trades:
    print(f"[INFO] No CLOSE trades in file: {csv_path}")
    sys.exit(0)

# 3) Parse fields and timestamps
parsed_trades = []
for row in trades:
    ts_str = row.get("timestamp")
    profit_str = row.get("profit") or "0"
    stake_str = row.get("stake") or "0"
    side = row.get("side") or "?"

    try:
        # trades.jsonl / CSV uses ISO timestamps, e.g. 2025-11-28T12:56:31.742252+00:00
        # datetime.fromisoformat handles this in Python 3.10+
        ts = datetime.fromisoformat(ts_str)
    except Exception:
        # fallback: ignore invalid timestamps
        continue

    try:
        profit = float(profit_str)
    except ValueError:
        profit = 0.0

    try:
        stake = float(stake_str)
    except ValueError:
        stake = 0.0

    parsed_trades.append(
        {
            "ts": ts,
            "side": side,
            "stake": stake,
            "profit": profit,
        }
    )

if not parsed_trades:
    print("[INFO] No valid CLOSE trades after parsing.")
    sys.exit(0)

# 4) Sort by time
parsed_trades.sort(key=lambda x: x["ts"])

total_pnl = sum(t["profit"] for t in parsed_trades)
wins = [t for t in parsed_trades if t["profit"] > 0]
losses = [t for t in parsed_trades if t["profit"] <= 0]

first_ts = parsed_trades[0]["ts"]
last_ts = parsed_trades[-1]["ts"]
duration_sec = (last_ts - first_ts).total_seconds()
duration_min = duration_sec / 60.0 if duration_sec > 0 else 0.0

print("\n=== AstraQuant LIVE STATS (from trades.csv) ===")
print(f"File: {csv_path}")
print(f"Trades parsed  : {len(parsed_trades)}")
print(f"Wins           : {len(wins)}")
print(f"Losses         : {len(losses)}")
print(f"Net PnL (USD)  : {total_pnl:.2f}")
if len(parsed_trades) > 0:
    winrate = len(wins) / len(parsed_trades) * 100.0
    print(f"Winrate        : {winrate:.1f}%")

print(f"\nTime window    : {first_ts}  →  {last_ts}")
print(f"Duration       : {duration_min:.1f} minutes")

if duration_min > 0:
    pnl_per_min = total_pnl / duration_min
    pnl_per_hour = pnl_per_min * 60.0
    print(f"PnL / minute   : {pnl_per_min:.4f} USD/min")
    print(f"PnL / hour     : {pnl_per_hour:.4f} USD/hour")

print("\nSample last 5 trades:")
for t in parsed_trades[-5:]:
    print(
        f"{t['ts']} | {t['side']:4s} | stake={t['stake']:.2f} "
        f"| profit={t['profit']:.2f}"
    )
