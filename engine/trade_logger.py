# File: engine/trade_logger.py
# ASTRAQUANT V1 — Trade event logger (compat layer)
# Writes trade OPEN/CLOSE records to JSONL + CSV.
# NOTE: In V1 observe-first, execution is disabled; this logger remains safe to import.

import os
import json
import csv
import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Any

logger = logging.getLogger("trade-logger")
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

# Prefer the same directory used by V1Logger / compose
# compose sets: AQ_LOG_DIR=/opt/astraquant/logs/v1
LOG_DIR = os.getenv("AQ_LOG_DIR") or os.getenv("ASTRAQUANT_LOG_DIR") or "/opt/astraquant/logs/v1"
os.makedirs(LOG_DIR, exist_ok=True)


class TradeLogger:
    """
    TradeLogger (compat)
    - Appends every trade event (open/close) to:
        - JSONL: <LOG_DIR>/trades_events.jsonl
        - CSV:   <LOG_DIR>/trades_events.csv
    """

    def __init__(
        self,
        jsonl_name: str = "trades_events.jsonl",
        csv_name: str = "trades_events.csv",
    ) -> None:
        self.jsonl_path = os.path.join(LOG_DIR, jsonl_name)
        self.csv_path = os.path.join(LOG_DIR, csv_name)

        self.fieldnames = [
            "timestamp",
            "event",          # "open" | "close"
            "trade_id",
            "contract_id",
            "market",
            "side",
            "stake",
            "duration_sec",
            "entry_price",
            "exit_price",
            "profit",
            "payout",
            "source",
        ]

        self._lock = asyncio.Lock()

        self._csv_header_written = (
            os.path.exists(self.csv_path) and os.path.getsize(self.csv_path) > 0
        )

    async def log_trade(self, record: Dict[str, Any]) -> None:
        # Ensure timestamp
        if "timestamp" not in record:
            record["timestamp"] = datetime.now(timezone.utc).isoformat()

        # Normalize row for CSV
        row = {k: record.get(k, "") for k in self.fieldnames}

        async with self._lock:
            try:
                # JSONL append
                with open(self.jsonl_path, "a", encoding="utf-8") as jf:
                    jf.write(json.dumps(record, ensure_ascii=False) + "\n")

                # CSV append
                write_header = False
                if not self._csv_header_written:
                    write_header = True
                    self._csv_header_written = True

                with open(self.csv_path, "a", newline="", encoding="utf-8") as cf:
                    writer = csv.DictWriter(cf, fieldnames=self.fieldnames)
                    if write_header:
                        writer.writeheader()
                    writer.writerow(row)

            except Exception as e:
                logger.exception("[TRADE LOGGER] Failed to write trade record: %s", e)
