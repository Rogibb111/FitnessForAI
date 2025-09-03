from __future__ import annotations

import csv
from typing import Dict, Iterable, List, Optional, Tuple


def detect_delimiter(sample: str) -> str:
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        return dialect.delimiter
    except Exception:
        return ","


def read_csv_stream(path: str) -> Tuple[List[str], Iterable[Dict[str, str]], Optional[str], List[str]]:
    """Return headers, row iterator (dicts), encoding_used, errors (list of strings)."""
    errors: List[str] = []
    encodings = ["utf-8-sig", "utf-8", "latin-1"]
    for enc in encodings:
        try:
            with open(path, "rb") as f:
                raw = f.read(4096)
            sample_text = raw.decode(enc, errors="ignore")
            delimiter = detect_delimiter(sample_text)
            # Re-open as text stream
            def row_iter() -> Iterable[Dict[str, str]]:
                with open(path, "r", encoding=enc, errors="ignore", newline="") as tf:
                    # Use csv.DictReader
                    first_chunk = tf.read(4096)
                    tf.seek(0)
                    sniffer = csv.Sniffer()
                    has_header = True
                    try:
                        has_header = sniffer.has_header(first_chunk)
                    except Exception:
                        pass
                    reader = csv.reader(tf, delimiter=delimiter)
                    headers: List[str] = []
                    for row in reader:
                        if not row or all(not str(cell).strip() for cell in row):
                            continue
                        headers = [str(c).strip() for c in row]
                        break
                    if not headers:
                        return
                    # Build DictReader with found headers
                    tf.seek(0)
                    dict_reader = csv.DictReader(tf, fieldnames=headers, delimiter=delimiter)
                    if has_header:
                        next(dict_reader, None)  # skip header row
                    for r in dict_reader:
                        # Normalize keys to original headers
                        yield {k: (v if v is not None else "").strip() for k, v in r.items()}
            # We need headers separately
            with open(path, "r", encoding=enc, errors="ignore", newline="") as tf2:
                first_line = None
                for line in tf2:
                    if line.strip():
                        first_line = line
                        break
                if first_line is None:
                    return [], iter(()), enc, []
                headers = [c.strip() for c in next(csv.reader([first_line], delimiter=delimiter))]
            return headers, row_iter(), enc, errors
        except Exception as e:
            errors.append(f"{enc}: {e}")
            continue
    return [], iter(()), None, errors
