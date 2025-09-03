from __future__ import annotations

from typing import List, Optional

from .constants import DATE_COL_CANDIDATES, METRIC_MAP


def infer_date_column(headers: List[str]) -> Optional[str]:
    lower = [h.lower().strip() for h in headers]
    for cand in DATE_COL_CANDIDATES:
        if cand in lower:
            return headers[lower.index(cand)]
    # Fuzzy contains
    for i, h in enumerate(lower):
        for cand in ("date", "start", "time", "logged"):
            if cand in h:
                return headers[i]
    return None


def categorize_path(path: str) -> str:
    import os
    # Use the immediate subdirectory under Fitbit as a category
    parts = os.path.normpath(path).split(os.sep)
    try:
        idx = parts.index("Fitbit")
        if idx >= 0 and idx + 1 < len(parts):
            return parts[idx + 1]
    except ValueError:
        pass
    # Fallback to containing directory
    return os.path.basename(os.path.dirname(path)) or "root"


def match_metric_key(header: str, category: str) -> Optional[str]:
    h = header.lower().strip()
    cat = category.lower()
    for norm_key, fragments in METRIC_MAP.items():
        for frag in fragments:
            if frag in h:
                # Context refinement for sleep metrics
                if norm_key.startswith("sleep") and "sleep" not in (h + " " + cat):
                    continue
                return norm_key
    # Special-case: heart rate columns
    if h in ("resting heart rate", "restingheartrate", "resting_hr", "rhr"):
        return "resting_heart_rate"
    return None


def is_session_headers(headers: List[str], category: str) -> bool:
    lh = [h.lower() for h in headers]
    cat = category.lower()
    # Exclude known non-session categories (per READMEs)
    if "sleep" in cat:
        return False
    if "goal" in cat:
        return False
    # Heuristics for session-like files
    has_start = any("start" in h for h in lh)
    has_end = any(h.startswith("end") or " end" in h or "finish" in h for h in lh)
    has_duration = any("duration" in h or ("minutes" in h and "sleep" not in h) for h in lh)
    has_type = any(any(k in h for k in ["activity", "exercise", "workout", "sport"]) for h in lh)
    # Avoid broad category fallbacks that include 'Activity Goals'
    return (has_start and (has_end or has_duration)) and (has_type or "mindfulness" in cat)
