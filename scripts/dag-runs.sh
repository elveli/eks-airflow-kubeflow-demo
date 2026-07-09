#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# `make dags`: Airflow DAG list + each DAG's recent runs with computed duration.
# The Airflow CLI reports start/end timestamps but no duration, so we append
# one locally (running runs get elapsed-so-far).
# -----------------------------------------------------------------------------
set -euo pipefail

EXEC="kubectl -n airflow exec deploy/airflow-scheduler -c scheduler --"

PYCODE=$(cat <<'EOF'
import datetime as dt
import sys

now = dt.datetime.now(dt.timezone.utc)
rows = []
for i, line in enumerate(sys.stdin.read().splitlines()):
    parts = line.split()
    line = line.rstrip()
    if i == 0:
        rows.append((line, "duration"))
        continue
    try:
        start = dt.datetime.fromisoformat(parts[4])
        running = len(parts) < 6
        end = now if running else dt.datetime.fromisoformat(parts[5])
        dur = str(end - start).split(".")[0]  # trim microseconds
        rows.append((line, f"{dur}{' (so far)' if running else ''}"))
    except (IndexError, ValueError):
        rows.append((line, ""))

# Align the appended column: pad every line to the widest row first
# (rstrip above removes the CLI's own header padding, so we re-derive it).
width = max((len(l) for l, _ in rows), default=0)
for line, dur in rows:
    print(f"{line:<{width}}  {dur}".rstrip())
EOF
)

$EXEC airflow dags list -o plain
echo
for d in $($EXEC airflow dags list -o plain | tail -n +2 | awk '{print $1}'); do
  echo "=== recent runs: $d"
  $EXEC airflow dags list-runs -d "$d" -o plain 2>/dev/null | head -4 | python3 -c "$PYCODE"
  echo
done
