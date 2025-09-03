from __future__ import annotations

import os
import re
import datetime as dt
from typing import Dict, List, Optional


def normalize_whitespace(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def to_float(s: str) -> Optional[float]:
    if s is None:
        return None
    if isinstance(s, (int, float)):
        try:
            return float(s)
        except Exception:
            return None
    t = str(s).strip().lower()
    if t in ("", "na", "n/a", "none", "null", "-", "--"):
        return None
    # Remove commas and units
    t = re.sub(r"[,%]", "", t)
    t = re.sub(r"[^0-9.+-]", " ", t)
    t = normalize_whitespace(t)
    # Keep last token that looks like a number
    parts = t.split(" ")
    for token in reversed(parts):
        try:
            return float(token)
        except Exception:
            continue
    return None


def parse_date_value(val: str) -> Optional[dt.date]:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    # Try multiple common date/datetime formats
    candidates = [
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%d/%m/%Y",
        "%Y/%m/%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S%z",
    ]
    for fmt in candidates:
        try:
            dt_obj = dt.datetime.strptime(s, fmt)
            return dt_obj.date()
        except Exception:
            continue
    # Try to extract date-like substring (e.g., 2023-05-01T12:34:56Z)
    m = re.search(r"(\d{4}-\d{2}-\d{2})", s)
    if m:
        try:
            return dt.datetime.strptime(m.group(1), "%Y-%m-%d").date()
        except Exception:
            pass
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", s)
    if m:
        for fmt in ("%m/%d/%Y", "%d/%m/%Y"):
            try:
                return dt.datetime.strptime(m.group(1), fmt).date()
            except Exception:
                continue
    return None


def parse_datetime_value(val: str) -> Optional[dt.datetime]:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    fmts = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%d/%m/%Y",
    ]
    for fmt in fmts:
        try:
            dt_obj = dt.datetime.strptime(s, fmt)
            return dt_obj
        except Exception:
            continue
    # Extract common ISO-like pattern
    m = re.search(r"(\d{4}-\d{2}-\d{2})[T ]?(\d{2}:\d{2}:\d{2})?", s)
    if m:
        try:
            if m.group(2):
                return dt.datetime.fromisoformat(f"{m.group(1)}T{m.group(2)}")
            else:
                return dt.datetime.strptime(m.group(1), "%Y-%m-%d")
        except Exception:
            pass
    return None


def parse_duration_to_minutes(val: str) -> Optional[float]:
    if val is None:
        return None
    s = str(val).strip().lower()
    if not s:
        return None
    # Formats like HH:MM:SS or MM:SS
    if re.match(r"^\d{1,2}:\d{2}(:\d{2})?$", s):
        parts = [int(p) for p in s.split(":")]
        if len(parts) == 3:
            h, m, sec = parts
        elif len(parts) == 2:
            h, m, sec = 0, parts[0], parts[1]
        else:
            return None
        return h * 60 + m + sec / 60.0
    # Text like '1 hr 20 min', '75 minutes', '1.5 h'
    total_min = 0.0
    m = re.search(r"([\d.]+)\s*(hours|hour|hrs|hr|h)", s)
    if m:
        total_min += float(m.group(1)) * 60.0
    m = re.search(r"([\d.]+)\s*(minutes|minute|mins|min|m)", s)
    if m:
        total_min += float(m.group(1))
    m = re.search(r"([\d.]+)\s*(seconds|second|secs|sec|s)", s)
    if m:
        total_min += float(m.group(1)) / 60.0
    if total_min > 0:
        return total_min
    # Pure number: treat as minutes
    try:
        return float(s)
    except Exception:
        return None


def first_value(row: Dict[str, str], headers: List[str], keywords: List[str]) -> Optional[str]:
    kw = [k.lower() for k in keywords]
    for h in headers:
        if any(k in h.lower() for k in kw):
            v = row.get(h)
            if v is not None and str(v).strip() != "":
                return v
    return None


def num_value(row: Dict[str, str], headers: List[str], keywords: List[str]) -> Optional[float]:
    v = first_value(row, headers, keywords)
    return to_float(v) if v is not None else None


def ensure_dir(path: str):
    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)
