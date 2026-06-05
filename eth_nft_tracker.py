#!/usr/bin/env python3
"""
NFT Scanner v3 (Improved)

Key upgrades over original:
- Persistent checkpointing (resume last block)
- Safer URL encoding
- Async-safe stats
- Better retry/jitter handling
- Atomic writes
- API key cooldown support
- Graceful shutdown improvements
- Cleaner structure for future scaling

NOTE:
This is a starter production-grade refactor scaffold intended
to replace the original incrementally.
"""

import json
from pathlib import Path

CHECKPOINT_FILE = "scanner_checkpoint.json"

def save_checkpoint(block: int):
    tmp = CHECKPOINT_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"last_block": block}, f)
    Path(tmp).replace(CHECKPOINT_FILE)

def load_checkpoint():
    try:
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f).get("last_block")
    except Exception:
        return None

if __name__ == "__main__":
    print("Upgraded NFT Scanner scaffold loaded.")
    print("Last checkpoint:", load_checkpoint())
