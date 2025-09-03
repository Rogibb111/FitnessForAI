from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

from fitbit_distiller import (
    to_float, parse_date_value, parse_datetime_value,
    parse_duration_to_minutes, first_value, num_value, ensure_dir,
    read_csv_stream,
    infer_date_column, categorize_path, match_metric_key, is_session_headers,
    aggregate_value, finalize_daily,
)


def process_csv_worker(args):
    import datetime as dt
    from collections import defaultdict
    (csv_path, input_root) = args
    category = categorize_path(str(csv_path))
    headers, rows_iter, encoding_used, errors = read_csv_stream(str(csv_path))
    date_col = infer_date_column(headers) if headers else None

    # Index info
    row_count = 0
    min_date: Optional[str] = None
    max_date: Optional[str] = None
    metric_hits: Dict[str, int] = defaultdict(int)

    # Prepare metric header mapping for efficiency
    header_metric_keys: List[Optional[str]] = []
    for h in headers:
        header_metric_keys.append(match_metric_key(h, category))

    session_mode = is_session_headers(headers, category)

    rel_path = os.path.relpath(csv_path, start=input_root)

    # Guard: disable session extraction for sedentary period files
    if "sedentary_period" in rel_path.lower():
        session_mode = False

    local_daily: Dict[str, Dict[str, float]] = {}
    local_sessions: List[Dict[str, object]] = []

    for row in rows_iter:
        row_count += 1
        date_str: Optional[str] = None
        if date_col and date_col in row:
            d = parse_date_value(row.get(date_col))
            if d:
                date_str = d.isoformat()
        # If we couldn't parse date, try any date-like field
        if not date_str:
            for h in headers:
                d = parse_date_value(row.get(h))
                if d:
                    date_str = d.isoformat()
                    break

        # Session extraction
        if session_mode:
            # Start/end datetime parsing with flexible sources
            start_dt: Optional[dt.datetime] = None
            end_dt: Optional[dt.datetime] = None
            start_candidates = [
                first_value(row, headers, ["start datetime", "start date time", "start date", "start time", "start"]),
            ]
            end_candidates = [
                first_value(row, headers, ["end datetime", "end date time", "end date", "end time", "finish", "end"]),
            ]
            sd = first_value(row, headers, ["start date", "date"]) or first_value(row, headers, ["date start"])
            st = first_value(row, headers, ["start time", "time start", "time"])
            ed = first_value(row, headers, ["end date"]) or first_value(row, headers, ["date end"])
            et = first_value(row, headers, ["end time", "time end"])

            for cand in start_candidates:
                if cand:
                    start_dt = parse_datetime_value(cand)
                    if start_dt:
                        break
            if not start_dt and sd and st:
                start_dt = parse_datetime_value(f"{sd} {st}") or parse_datetime_value(sd) or parse_datetime_value(st)
            if not start_dt and sd:
                start_dt = parse_datetime_value(sd)

            for cand in end_candidates:
                if cand:
                    end_dt = parse_datetime_value(cand)
                    if end_dt:
                        break
            if not end_dt and ed and et:
                end_dt = parse_datetime_value(f"{ed} {et}") or parse_datetime_value(ed) or parse_datetime_value(et)
            if not end_dt and ed:
                end_dt = parse_datetime_value(ed)

            # Duration
            dur_field = first_value(row, headers, ["duration", "length", "elapsed time"]) or first_value(row, headers,
                                                                                                         ["minutes"])
            duration_min = parse_duration_to_minutes(dur_field) if dur_field else None
            if duration_min is None and start_dt and end_dt:
                try:
                    duration_min = max((end_dt - start_dt).total_seconds() / 60.0, 0.0)
                except (TypeError, OverflowError):
                    duration_min = None

            # Activity type / name
            activity_type = first_value(row, headers, [
                "activity type", "activity name", "activity", "exercise", "exercise name", "workout", "sport", "type"
            ])

            # Key metrics
            calories = num_value(row, headers, ["calories", "calorie", "kcal", "energy"])
            distance = num_value(row, headers,
                                 ["distance", "km", "kilometer", "kilometre", "miles", "mi", "meters", "metres"])
            steps_v = num_value(row, headers, ["steps", "step count", "stepcount", "step"])
            avg_hr = num_value(row, headers,
                               ["average heart", "avg heart", "avg hr", "average hr", "avg bpm", "average bpm",
                                "mean hr"])
            max_hr = num_value(row, headers, ["max heart", "max hr", "peak heart", "max bpm"])
            elev_gain = num_value(row, headers,
                                  ["elevation gain", "elevation (m)", "elevation gain (m)", "elevation gain (ft)",
                                   "elev gain", "ascent", "climb"])
            azm_total = num_value(row, headers, ["active zone minutes", "azm", "zone minutes"])
            azm_fat = num_value(row, headers, ["fat burn minutes", "azm - fat burn", "active zone minutes - fat burn",
                                               "fat burn zone minutes", "fat burn"])
            azm_cardio = num_value(row, headers, ["cardio minutes", "azm - cardio", "active zone minutes - cardio",
                                                  "cardio zone minutes", "cardio"])
            azm_peak = num_value(row, headers,
                                 ["peak minutes", "azm - peak", "active zone minutes - peak", "peak zone minutes",
                                  "peak"])

            # Determine session date for aggregation
            if not date_str and start_dt:
                date_str = start_dt.date().isoformat()

            # Emit session record (buffer for later enrichment/write)
            if start_dt or end_dt or duration_min is not None or activity_type:
                rec = {
                    "date": date_str,
                    "start": start_dt.isoformat() if start_dt else None,
                    "end": end_dt.isoformat() if end_dt else None,
                    "duration_min": round(duration_min, 3) if isinstance(duration_min, (int, float)) else None,
                    "type": activity_type,
                    "calories": calories,
                    "distance": distance,
                    "steps": steps_v,
                    "avg_hr": avg_hr,
                    "max_hr": max_hr,
                    "elevation_gain_m": elev_gain,
                    "azm_minutes": azm_total,
                    "azm_fat_burn_minutes": azm_fat,
                    "azm_cardio_minutes": azm_cardio,
                    "azm_peak_minutes": azm_peak,
                    "category": category,
                    "source_path": rel_path,
                    # Internal fields for the enrichment phase:
                    "_start_dt": start_dt,
                    "_end_dt": end_dt,
                }
                public = {k: v for k, v in rec.items() if k not in ("_start_dt", "_end_dt") and v is not None}
                public["_start_dt"] = start_dt
                public["_end_dt"] = end_dt
                local_sessions.append(public)

            # Aggregate to daily workout metrics
            if date_str and duration_min is not None:
                aggregate_value(local_daily, date_str, "workout_minutes", float(duration_min))
                aggregate_value(local_daily, date_str, "workout_count", 1.0)

        if date_str:
            if not min_date or date_str < min_date:
                min_date = date_str
            if not max_date or date_str > max_date:
                max_date = date_str

        # Aggregate metrics if we have a date
        if date_str:
            for h, mk in zip(headers, header_metric_keys):
                if mk is None:
                    continue
                val = to_float(row.get(h))
                if val is None:
                    continue
                metric_hits[mk] += 1
                aggregate_value(local_daily, date_str, mk, val)

    index_record = {
        "path": os.path.relpath(csv_path, start=input_root),
        "category": category,
        "encoding": encoding_used,
        "columns": headers,
        "row_count": row_count,
        "date_column": date_col,
        "date_range": {"min": min_date, "max": max_date},
        "metric_hits": dict(metric_hits),
        "errors": errors,
    }
    return (local_daily, local_sessions, index_record, rel_path)


def main():
    parser = argparse.ArgumentParser(description="Distill Fitbit CSV export into AI-consumable JSONL")
    parser.add_argument("--input", default="Fitbit", help="Path to Fitbit export root directory")
    parser.add_argument("--output", default="distilled", help="Path to output directory")
    parser.add_argument("--no-progress", action="store_true", help="Disable console progress bar output")
    parser.add_argument("--workers", type=int, default=(os.cpu_count() or 1),
                        help="Number of parallel worker processes (default: CPU count)")
    args = parser.parse_args()

    # Ensure argparse values are typed as str for path operations
    input_root: str = str(args.input)
    output_root: str = str(args.output)
    ensure_dir(output_root)

    # Progress display control
    try:
        stderr_isatty = sys.stderr.isatty()
    except Exception:
        stderr_isatty = False
    show_progress = (not args.no_progress) and stderr_isatty

    files_index_path = os.path.join(output_root, "fitbit_files_index.jsonl")
    daily_out_path = os.path.join(output_root, "fitbit_daily_distilled.jsonl")
    sessions_out_path = os.path.join(output_root, "fitbit_activity_sessions.jsonl")
    readme_path = os.path.join(output_root, "README.txt")

    # Aggregators
    daily_agg: Dict[str, Dict[str, float]] = {}

    # Writers
    index_f = open(files_index_path, "w", encoding="utf-8")
    # We'll write sessions after enrichment

    # Buffers for post-run enrichment
    sessions_buffer: List[Dict[str, object]] = []

    # Time series indexes for enrichment
    # Store per-date sorted lists of (datetime, value...)
    hr_series: Dict[str, List[Tuple[dt.datetime, float]]] = {}
    pace_series: Dict[str, List[Tuple[dt.datetime, Optional[float], Optional[float], Optional[float]]]] = {}
    # pace tuple: (timestamp, steps, distance_mm, altitude_gain_mm)

    # Prepare list of CSV files and progress bar
    csv_paths: List[str] = []
    for root, _, files in os.walk(input_root):
        for name in files:
            if name.lower().endswith(".csv"):
                csv_paths.append(os.path.join(root, name))
    total_csv = len(csv_paths)

    def _print_progress(done: int, total: int, current_rel: Optional[str] = None) -> None:
        try:
            cols = shutil.get_terminal_size(fallback=(80, 20)).columns
        except Exception:
            cols = 80
        prefix = f"Processing CSVs: {done}/{total} "
        suffix = ""
        if total > 0:
            pct = int(done * 100 / total)
            suffix = f"({pct}%)"
        # Reserve space for prefix, suffix, and a minimal bar
        bar_space = max(10, cols - len(prefix) - len(suffix) - 5)
        # Build bar
        filled = 0
        if total > 0:
            filled = int(bar_space * done / total)
        bar = "#" * filled + "-" * (bar_space - filled)
        line = f"\r{prefix}[{bar}] {suffix}"
        # Show current file (trim if needed)
        if current_rel:
            max_name = max(0, cols - len(line) - 3)
            name_disp = current_rel if len(current_rel) <= max_name else "…" + current_rel[-(max_name - 1):]
            line += f" {name_disp}"
        # Ensure line doesn't overflow
        line = line[:cols - 1]
        sys.stderr.write(line)
        sys.stderr.flush()
        if done >= total:
            sys.stderr.write("\n")
            sys.stderr.flush()

    csv_count = total_csv
    index_records: List[Dict[str, object]] = []

    if show_progress:
        _print_progress(0, total_csv)

    workers = max(1, int(args.workers))
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {}
        for csv_path in csv_paths:
            future = executor.submit(process_csv_worker, (csv_path, input_root))
            futures[future] = os.path.relpath(csv_path, start=input_root)
        done = 0
        for future in as_completed(futures):
            rel = futures[future]
            try:
                local_daily, local_sessions, index_record, _rel_path = future.result()
                # Merge daily aggregates
                for d, metrics in local_daily.items():
                    if d not in daily_agg:
                        daily_agg[d] = {}
                    for k, v in metrics.items():
                        daily_agg[d][k] = daily_agg[d].get(k, 0.0) + v
                # Extend sessions buffer
                sessions_buffer.extend(local_sessions)
                # Collect index record
                index_records.append(index_record)
            except Exception as e:
                # Record an error index entry and continue
                index_records.append({
                    "path": rel,
                    "category": categorize_path(os.path.join(input_root, rel)),
                    "encoding": None,
                    "columns": [],
                    "row_count": 0,
                    "date_column": None,
                    "date_range": {"min": None, "max": None},
                    "metric_hits": {},
                    "errors": [str(e)],
                })
            finally:
                done += 1
                if show_progress:
                    _print_progress(done, total_csv, rel)

    # Write index records (sorted by path for deterministic output)
    for index_record in sorted(index_records, key=lambda r: r.get("path", "")):
        index_f.write(json.dumps(index_record, ensure_ascii=False) + "\n")
    index_f.close()

    # Sort time series per date for efficient window scans
    for dkey, lst in hr_series.items():
        lst.sort(key=lambda x: x[0])
    for dkey, lst in pace_series.items():
        lst.sort(key=lambda x: x[0])

    # Enrich buffered sessions using time-series data
    with open(sessions_out_path, "w", encoding="utf-8") as sessions_f:
        for rec in sessions_buffer:
            start_dt = rec.get("_start_dt")
            end_dt = rec.get("_end_dt")
            # Only enrich when we have a valid window
            if isinstance(start_dt, dt.datetime) and isinstance(end_dt, dt.datetime) and end_dt >= start_dt:
                # Collect candidate dates (handle potential cross-midnight)
                date_keys = {start_dt.date().isoformat(), end_dt.date().isoformat()}

                # Heart rate enrichment
                hr_values: List[float] = []
                for dkey in date_keys:
                    for ts, bpm in hr_series.get(dkey, []):
                        if start_dt <= ts <= end_dt:
                            hr_values.append(bpm)
                if hr_values:
                    if rec.get("avg_hr") is None and len(hr_values) > 0:
                        rec["avg_hr"] = round(sum(hr_values) / float(len(hr_values)), 3)
                    if rec.get("max_hr") is None:
                        rec["max_hr"] = max(hr_values)

                # Live pace enrichment (steps, distance, altitude gain)
                steps_sum = 0.0
                dist_mm_sum = 0.0
                alt_mm_sum = 0.0
                any_pace_points = False
                for dkey in date_keys:
                    for ts, steps_v, dist_mm, alt_mm in pace_series.get(dkey, []):
                        if start_dt <= ts <= end_dt:
                            any_pace_points = True
                            if steps_v is not None:
                                steps_sum += steps_v
                            if dist_mm is not None:
                                dist_mm_sum += dist_mm
                            if alt_mm is not None:
                                alt_mm_sum += alt_mm
                if any_pace_points:
                    # Backfill steps if missing
                    if rec.get("steps") is None and steps_sum > 0:
                        rec["steps"] = round(steps_sum, 3)
                    # Backfill distance if missing (km from millimeters)
                    if rec.get("distance") is None and dist_mm_sum > 0:
                        rec["distance"] = round(dist_mm_sum / 1_000_000.0, 6)  # km
                    # Backfill elevation gain if missing (meters from millimeters)
                    if rec.get("elevation_gain_m") is None and alt_mm_sum > 0:
                        rec["elevation_gain_m"] = round(alt_mm_sum / 1000.0, 3)

            # Clean internal fields and drop Nones
            rec.pop("_start_dt", None)
            rec.pop("_end_dt", None)
            rec = {k: v for k, v in rec.items() if v is not None}
            sessions_f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    # Finalize daily aggregated metrics
    daily_records = finalize_daily(daily_agg)
    with open(daily_out_path, "w", encoding="utf-8") as df:
        for rec in daily_records:
            df.write(json.dumps(rec, ensure_ascii=False) + "\n")

    # README
    readme = f"""
Fitbit distilled outputs
========================

Generated by distill_fitbit.py.

Files:
- fitbit_daily_distilled.jsonl: one JSON object per line with aggregated daily metrics.
- fitbit_activity_sessions.jsonl: one JSON object per line with per-workout session details (type, start/end, duration, calories, distance, steps, HR stats, AZM splits) and source metadata.
- fitbit_files_index.jsonl: one JSON object per CSV file with basic metadata and detected metrics.

Usage:
    python3 distill_fitbit.py --input Fitbit --output distilled --workers $(python3 -c 'import os;print(os.cpu_count() or 1)')

Progress:
- A live console progress bar is shown while processing CSV files (on TTY only).
- Use --no-progress to disable the progress bar.

Daily schema (fields present if detected in your data):
- date (YYYY-MM-DD)
- steps
- distance (unit as in source, usually kilometers)
- calories
- floors
- resting_heart_rate
- hrv_ms (RMSSD-based if present)
- spo2_percent
- sleep_duration_min
- sleep_score
- readiness_score
- stress_score
- skin_temp_variation
- azm_minutes
- azm_fat_burn_minutes
- azm_cardio_minutes
- azm_peak_minutes
- mindfulness_minutes
- lightly_active_minutes
- fairly_active_minutes
- very_active_minutes
- sedentary_minutes
- workout_minutes (sum of durations from detected sessions)
- workout_count (count of detected sessions)

Sessions schema (fields present if detected):
- date (YYYY-MM-DD)
- start (ISO8601), end (ISO8601)
- duration_min
- type (activity/exercise name)
- calories, distance, steps
- avg_hr, max_hr, elevation_gain_m
- azm_minutes, azm_fat_burn_minutes, azm_cardio_minutes, azm_peak_minutes
- category (top-level Fitbit folder), source_path (relative path in export)

Notes:
- Values that occur multiple times per day are summed by default. Certain metrics (resting_heart_rate, hrv_ms, spo2_percent, sleep_score, readiness_score, stress_score, skin_temp_variation) are averaged across entries.
- workout_* fields are derived from per-session extraction when recognizable activity session files are present.
- The files index helps audit which files contributed to which metrics.
- If some metrics are missing, it may be due to header names not matching built-in heuristics. You can extend METRIC_MAP in the script to add more header fragments.
""".strip()
    with open(readme_path, "w", encoding="utf-8") as rf:
        rf.write(readme + "\n")

    print(f"Processed {csv_count} CSV files.\n" 
          f"Wrote: {os.path.abspath(daily_out_path)}\n"
          f"       {os.path.abspath(sessions_out_path)}\n"
          f"       {os.path.abspath(files_index_path)}\n"
          f"       {os.path.abspath(readme_path)}")


if __name__ == "__main__":
    main()
