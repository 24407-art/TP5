"""Safe Parquet reader for Spark streaming output (skips empty/in-progress files)."""

from __future__ import annotations

import json
import time
from pathlib import Path

import pandas as pd

DEBUG_LOG = Path("/workspace/data/tp5/debug-beb278.log")


def _dbg(hypothesis_id: str, message: str, data: dict) -> None:
    # #region agent log
    try:
        DEBUG_LOG.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "sessionId": "beb278",
            "hypothesisId": hypothesis_id,
            "location": "dashboard_io.py",
            "message": message,
            "data": data,
            "timestamp": int(time.time() * 1000),
        }
        with DEBUG_LOG.open("a") as f:
            f.write(json.dumps(payload) + "\n")
    except Exception:
        pass
    # #endregion


def _valid_parquet_files(folder: Path) -> list[Path]:
    if not folder.exists():
        return []
    candidates = list(folder.glob("part-*.parquet"))
    if not candidates:
        candidates = [
            p for p in folder.rglob("*.parquet")
            if "_temporary" not in str(p) and ".crc" not in p.name
        ]
    valid: list[Path] = []
    for p in candidates:
        try:
            if p.stat().st_size >= 64:
                valid.append(p)
        except OSError:
            continue
    return sorted(valid, key=lambda p: p.stat().st_mtime, reverse=True)


def read_latest_parquet(folder: Path, limit_files: int = 30) -> pd.DataFrame:
    files = _valid_parquet_files(folder)
    if not files:
        return pd.DataFrame()
    chunks: list[pd.DataFrame] = []
    for f in files[:limit_files]:
        try:
            if f.stat().st_size < 64:
                continue
            chunks.append(pd.read_parquet(f))
        except Exception:
            continue
    if not chunks:
        return pd.DataFrame()
    return pd.concat(chunks, ignore_index=True)


def _window_bounds(w):
    if w is None or (isinstance(w, float) and pd.isna(w)):
        return None, None
    if isinstance(w, dict):
        return w.get("start"), w.get("end")
    if hasattr(w, "start") and hasattr(w, "end"):
        return w.start, w.end
    return None, None


def flatten_window_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "window" not in df.columns:
        return df
    w0 = df["window"].iloc[0]
    _dbg("C", "flatten_enter", {"rows": len(df), "window_type": type(w0).__name__, "is_dict": isinstance(w0, dict)})
    out = df.copy()
    bounds = out["window"].apply(_window_bounds)
    out["window_start"] = bounds.apply(lambda b: b[0])
    out["window_end"] = bounds.apply(lambda b: b[1])
    _dbg("B", "flatten_ok", {"has_window_start": True})
    return out.drop(columns=["window"], errors="ignore")
