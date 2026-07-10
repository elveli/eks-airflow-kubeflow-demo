#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# `make dags`: Airflow DAG list + each DAG's recent runs with computed duration.
# The Airflow CLI reports start/end timestamps but no duration, so we append
# one locally (running runs get elapsed-so-far). The CLI's microsecond
# timestamps are trimmed to minute precision for reading; durations are
# computed from the full-precision values first.
# -----------------------------------------------------------------------------
set -euo pipefail

EXEC="kubectl -n airflow exec deploy/airflow-scheduler -c scheduler --"

PYCODE=$(cat <<'EOF'
import datetime as dt
import re
import sys

# 2026-07-10T16:49:07.568789+00:00 -> 2026-07-10T16:49 (also inside run_id)
short = lambda s: re.sub(r"(T\d{2}:\d{2})\S*", r"\1", s)

now = dt.datetime.now(dt.timezone.utc)
rows = []
for i, line in enumerate(sys.stdin.read().splitlines()):
    parts = line.split()
    if i == 0:
        rows.append(parts + ["duration"])
        continue
    try:
        start = dt.datetime.fromisoformat(parts[4])
        running = len(parts) < 6
        end = now if running else dt.datetime.fromisoformat(parts[5])
        if running:
            parts.append("-")  # blank end_date: keep the column grid intact
        dur = str(end - start).split(".")[0]  # trim microseconds
        rows.append([short(p) for p in parts] + [f"{dur}{' (so far)' if running else ''}"])
    except (IndexError, ValueError):
        rows.append(parts)

# Re-align: shortening invalidates the CLI's own column padding.
ncols = max((len(r) for r in rows), default=0)
widths = [max((len(r[c]) for r in rows if c < len(r)), default=0) for c in range(ncols)]
for r in rows:
    print("  ".join(f"{cell:<{widths[c]}}" for c, cell in enumerate(r)).rstrip())
EOF
)

$EXEC airflow dags list -o plain
echo
for d in $($EXEC airflow dags list -o plain | tail -n +2 | awk '{print $1}'); do
  echo "=== recent runs: $d"
  $EXEC airflow dags list-runs -d "$d" -o plain 2>/dev/null | head -4 | python3 -c "$PYCODE"
  echo
done
