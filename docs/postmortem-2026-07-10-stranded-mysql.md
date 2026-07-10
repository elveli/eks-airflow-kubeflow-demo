# Post-mortem: KFP outage from an AZ-stranded MySQL volume

**Date:** 2026-07-10 · **Duration:** ~20 minutes · **Severity:** demo-day annoyance
(KFP API down, one pipeline run failed; no durable data lost)

## Summary

A mid-session spot reclaim replaced the only node in `us-west-2a`. MySQL's
EBS volume is AZ-bound to `us-west-2a`, so its pod could never reschedule —
and everything downstream of MySQL (the KFP API server, then the running
`sklearn-iris-demo` pipeline) failed with it. Recovery was the README's
delete-the-stranded-PVC recipe, applied to the one stranded claim only.

This was the failure mode the README already documented for kill-switch
resumes (`make start`), observed for the first time from plain mid-session
spot churn. Same mechanics, different trigger.

## Impact

- `mysql` Pending ~15 min; `ml-pipeline` (KFP API) crash-looping the whole time.
- One `train_on_kubeflow` run failed (its KFP driver pods couldn't reach the API).
- KFP **run history reset** (the recovery recreates MySQL's volume).
  Models in S3, pipeline artifacts in SeaweedFS, and Airflow history were untouched.

## Timeline (UTC, approximate)

| Time | Event |
|---|---|
| 16:24 | Cluster up: Airflow + KFP deployed, MySQL volume provisioned in `us-west-2a` |
| 17:06 | `etl_simple` run → Dataset event → `train_on_kubeflow` submits KFP run |
| ~17:07 | Spot reclaim: the `us-west-2a` node is cordoned, drained, terminated; replacement boots in `us-west-2b` — both `general` nodes now share one AZ |
| ~17:08 | `mysql` unschedulable (`volume node affinity conflict` + `Insufficient cpu` + pipelines taint); autoscaler: "max node group size reached" |
| ~17:08–17:20 | `ml-pipeline` restart-loops (hangs at "Initializing DB client…"); sklearn driver pods error and exhaust retries |
| ~17:20 | Diagnosis: PV AZs vs node AZs — `mysql-pv-claim` in 2a, all nodes in 2b |
| ~17:22 | Fix: delete `mysql-pv-claim` (only), delete pod, rerun `deploy-kfp.sh` |
| ~17:26 | New volume provisions in 2b, `mysql` Running, `ml-pipeline` stable; re-triggered run completes |

## Root cause

EBS volumes are AZ-bound and this VPC spans two AZs, while spot allocation is
capacity-optimized and ignores AZ balance. Once the last `us-west-2a` node
died, nothing could host MySQL's volume:

1. Node in 2b with capacity → fails the PV's node-affinity
2. Other 2b node → insufficient CPU
3. Pipelines node → `workload: pipelines` taint
4. Autoscaler → `general` group already at max size

The causal chain: **spot reclaim → mysql Pending → KFP API crash-loop →
pipeline driver pods error**. Three alarming pod states, one root cause.

## Recovery

The README recipe ([README → "If mysql/seaweedfs stay Pending…"](../README.md)),
narrowed to the stranded claim — `seaweedfs-pvc` was healthy in 2b, so its
artifacts were spared:

```bash
kubectl -n kubeflow delete pvc mysql-pv-claim --wait=false  # pvc-protection holds it until the pod goes
kubectl -n kubeflow delete pod -l app=mysql                 # release → PVC reaps
./scripts/deploy-kfp.sh                                     # idempotent — recreates the PVC
```

`WaitForFirstConsumer` then provisions the replacement volume in whatever AZ
the new pod lands — the property that makes this recipe reliable while
"get me a spot node in the right AZ" is not.

## What changed because of this

- **`make pvc`** ([scripts/pvc-status.sh](../scripts/pvc-status.sh)): joins each
  PVC → bound volume → AZ and flags `STRANDED` when no live node is in that AZ.
  The diagnosis that took a hand-written jsonpath during the incident is now
  one glance, no AWS credentials needed.
- README: the incident section no longer implies this only happens after
  `make start` — mid-session spot churn triggers it too — and now says to
  delete *only* the stranded PVC.

## Lessons

- **Chase the Pending pod, not the crash-looping ones.** The loud pods
  (`ml-pipeline`, the Error'd pipeline pods) were symptoms; the quiet
  `Pending` one held the root cause.
- Spot churn is routine, not exceptional: two reclaim waves in one ~2 h
  session. The second wave also produced a transient `ImagePullBackOff`
  (1 GB image, CDN connection reset, kubelet retried, self-healed) and killed
  an Airflow task pod mid-watch while the KFP run it was watching succeeded.
- Single-replica stateful services on spot + AZ-bound storage will do this
  again. Acceptable for a demo (documented + detectable); a real deployment
  would pin the stateful pods to an on-demand node group or use storage that
  isn't AZ-bound.
