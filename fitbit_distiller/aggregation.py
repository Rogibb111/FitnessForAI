from __future__ import annotations

from typing import Dict, List

from .constants import AVERAGE_PREFERENCE, SUM_PREFERENCE


def aggregate_value(agg: Dict[str, Dict[str, float]], date: str, key: str, value: float):
    if date not in agg:
        agg[date] = {}
    if key not in agg[date]:
        # Store both sum and count for averaging later if needed
        agg[date][key] = 0.0
        agg[date][f"{key}__count"] = 0.0
    # Always accumulate sum and increment count for averages
    agg[date][key] += value
    agg[date][f"{key}__count"] += 1.0


def finalize_daily(agg: Dict[str, Dict[str, float]]) -> List[Dict[str, float]]:
    out: List[Dict[str, float]] = []
    for date, metrics in sorted(agg.items()):
        record: Dict[str, float] = {"date": date}
        for key, val in list(metrics.items()):
            if key.endswith("__count"):
                continue
            count = metrics.get(f"{key}__count", 1.0)
            if key in AVERAGE_PREFERENCE:
                record[key] = round(val / max(count, 1.0), 3)
            elif key in SUM_PREFERENCE:
                record[key] = round(val, 3)
            else:
                # Default to sum
                record[key] = round(val, 3)
        out.append(record)
    return out
