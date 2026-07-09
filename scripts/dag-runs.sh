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
for i, line in enumerate(sys.stdin):
    parts = line.split()
    line = line.rstrip()
    if i == 0:
        print(f"{line}  duration")
        continue
    try:
        start = dt.datetime.fromisoformat(parts[4])
        running = len(parts) < 6
        end = now if running else dt.datetime.fromisoformat(parts[5])
        dur = str(end - start).split(".")[0]  # trim microseconds
        print(f"{line}  {dur}{' (so far)' if running else ''}")
    except (IndexError, ValueError):
        print(line)
EOF
)

$EXEC airflow dags list -o plain
echo
for d in $($EXEC airflow dags list -o plain | tail -n +2 | awk '{print $1}'); do
  echo "=== recent runs: $d"
  $EXEC airflow dags list-runs -d "$d" -o plain 2>/dev/null | head -4 | python3 -c "$PYCODE"
  echo
done
