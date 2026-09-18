# Admin observability

M21 exposes one read-only operational metrics surface. It is intentionally
aggregate-only and scoped to the process that serves the request, except for
values read from the shared Postgres database. The dashboard, JSON endpoint,
and server-sent-events stream use the same authenticated ten-second sampler.
There is no public metrics listener and no fleet collector in telegram-server.

## Access and operator setup

Set `TG_ADMIN_LISTEN_ADDR` and `TG_ADMIN_TOKEN_HASH` together. Leaving both
unset disables the admin server. `TG_ADMIN_TOKEN_HASH` is the lowercase,
64-character SHA-256 hex digest of the raw operator token, not the raw token.
Keep the listener on an operator-only network, normally loopback behind an
authenticated TLS reverse proxy when remote access is required. Do not bind
the admin listener to a public interface without an equivalent network access
control.

For a local shell, a token hash can be prepared without putting the raw token
in the command line:

```bash
export TG_ADMIN_LISTEN_ADDR=127.0.0.1:2444
read -r -s ADMIN_TOKEN
printf '\n'
export TG_ADMIN_TOKEN_HASH="$(printf '%s' "$ADMIN_TOKEN" | sha256sum | cut -d' ' -f1)"
unset ADMIN_TOKEN
```

Prefer a deployment secret manager for the hash. The browser submits the raw
token only to `/admin/login`; successful login creates the admin session used
by the protected routes below.

| Route | Purpose | Access |
|---|---|---|
| `GET /admin/dashboard` | Server-rendered operator dashboard | Authenticated admin session |
| `GET /admin/metrics` | JSON snapshot of the same metric contract | Authenticated admin session |
| `GET /admin/events` | Server-sent-events updates for the dashboard | Authenticated admin session, rechecked while open |

The login response and all session-protected admin responses carry
`Cache-Control: no-store`. The public `/admin/assets/*` responses intentionally
carry `Cache-Control: no-cache` so browsers revalidate the static dashboard
assets. The metrics routes are behind the admin session middleware and the SSE
stream does not remain open after the session is revoked or expires. The admin
listener is separate from the public MTProto listener. The existing login,
logout, CSRF, secure-cookie, and security-header behavior remains the access
boundary.

## Snapshot, freshness, and reset metadata

Each successful JSON snapshot has these fields in addition to the metrics:

| Field | Type | Meaning |
|---|---|---|
| `timestamp` | RFC3339 UTC string | Time at which the complete required sample was collected. It is not request time, fragment-arrival time, heartbeat time, or replay time. |
| `sample_age_seconds` | Nonnegative number | Server-computed age of `timestamp` when the response or fragment is rendered. It is not part of the retained sample. |
| `sample_state` | `available` or `stale` | `available` means the required sample succeeded. `stale` means the last complete sample is being retained after a later required collection failure. |
| `process_started_at` | RFC3339 UTC string | Fixed start time for the process that owns process-local counters. |
| `process_generation` | 32-character lowercase hex string | Random generation fixed for one process lifetime. It is not derived from a request, connection, user, or resource. |
| `replica_id` | String or `null` | Optional `TG_REPLICA_ID`, fixed at startup and limited to 1-64 characters from `A-Z`, `a-z`, `0-9`, `.`, `_`, and `-`. |
| `uninstrumented` | String array | Allowlisted field names that are not measured by the current metrics wiring. Unknown names must be ignored by consumers and never become labels or markup. |

The shared sampler refreshes at most once every ten seconds. A failed refresh
before the first complete sample returns HTTP 503 with the fixed JSON error
`{"error":"metrics_unavailable"}`. After a complete sample exists, a later
failure retains every prior value and timestamp, marks the sample `stale`, and
increases its age. It never advances the timestamp or substitutes a fresh
zero. A successful recovery replaces the stale sample.

Process-local notification, push, and denial windows start empty when the
process starts, run for the elapsed time since that start, and cap at 3,600
seconds. Values age out after one hour. A stable non-null `replica_id` plus a
changed `process_generation` confirms a restart. With no replica ID, or after
the replica ID changes, a generation change means only that the metrics source
changed. A falling rolling count or shorter window alone is never restart
evidence.

## Metric families

Every value belongs to one of four operational categories: a live or
point-in-time gauge, a rolling counter or rate, a fixed distribution or
percentile, or a lag sample. A field that cannot be measured is unavailable or
unfinished; it is never presented as a fabricated zero.

### Live gauges and shared database values

These values are not rolling process counters:

| JSON field | Classification and scope |
|---|---|
| `connections` | Live gauge: live authenticated MTProto connections on this replica. |
| `sessions` | Live gauge: distinct authenticated accounts with at least one live connection on this replica. It is not a distinct fleet-account count. |
| `max_pts_gap` | Live account-head spread: the maximum difference between `pts` heads for accounts with a live connection here. It is not connection delivery lag and is not a fallback for missing lag. With no live accounts it is zero. |
| `total_users` | Shared database count of registered accounts. |
| `active_users_1h`, `active_users_24h` | Shared database counts of accounts whose `last_seen_at` is within the named window. |
| `total_channels`, `total_chats` | Shared database counts. |
| `messages_1h`, `messages_24h` | Shared database counts of non-deleted stored message rows whose message date is in the named window. This includes per-account message copies and channel messages. It is not a count of unique sends, edits, deletes, or message events. |
| `rate_limit_active` | Shared database gauge of unexpired rate-limit rows. It approximates recent throttling activity; it is not a count of denied requests. |

`storage_rows` is a fixed object with `users`, `messages`, `events`,
`channels`, `channel_messages`, `chats`, `files`, and `auth_keys`. `users`,
`channels`, and `chats` are exact counts. The other rows use PostgreSQL's
`pg_class.reltuples` estimate to avoid a full-table scan. A negative estimate
means unavailable and must not be shown as zero.

### Delivery lag

`delivery_lag` is one fixed object:

```json
{
  "worst_pts": 0,
  "state": "available",
  "coverage": "full",
  "eligible_connections": 2,
  "sampled_connections": 2,
  "sampled_at": "2026-09-18T12:00:00Z"
}
```

For each live authenticated connection in one registry snapshot, lag is
`max(0, authoritative account head - that connection's push watermark)`. The
account-head query is deduplicated per account. `eligible_connections` counts
connections, not accounts; `sampled_connections` counts connections for which
an authoritative head was obtained and compared. The sample is bounded and
does not run in the connection write path.

| Field | Values and interpretation |
|---|---|
| `state` | `available` only for a current full sample; `stale` only when a previous full sample is retained after a failed or partial attempt; `unavailable` when no full sample exists. |
| `coverage` | `full` when every eligible connection was sampled, `partial` when some were sampled, and `none` when none were sampled. Zero eligible connections is a successful `full` sample with `worst_pts: 0`. |
| `worst_pts` | Nullable maximum lag. It is non-null only for an available or stale full-sample value. |
| `sampled_at` | Nullable RFC3339 UTC time of the full sample represented by `worst_pts`; it stays unchanged when a later attempt is stale or partial. |

An unavailable first attempt has no trustworthy lag number and must show
`Unavailable`. A partial attempt cannot publish a new zero or advance
`sampled_at`. A valid full sample with no eligible connections is `0 PTS` with
`No live authenticated connections.` A valid full sample with sampled
connections and zero lag is `0 PTS` with `All sampled connections are at the
current head.`

### Rolling notification work

The notification family uses the process-local rolling window described above:

| JSON field | Meaning |
|---|---|
| `notify_count` | Count of valid, successfully parsed notifications consumed by this replica. It excludes malformed and unknown notifications. |
| `notify_rate_per_second` | `notify_count / notify_window_seconds`, or zero when the elapsed window is zero. |
| `notify_window_seconds` | Elapsed process-local window, from zero through 3,600 seconds. |
| `notify_channels` | Fixed counts for the nine compiled channels below, including zero-valued rows. |
| `notify_invalid` | Rolling count of malformed or unknown notifications. It has no channel or payload breakdown and is excluded from `notify_count`. |

`notify_channels` always has exactly these keys:

`tg_updates`, `tg_typing`, `tg_evict`, `tg_channel_post`, `tg_encryption`,
`tg_status`, `tg_encrypted_msg`, `tg_reactions`, and `tg_pinned`.

These are notification delivery-work counts, not unique committed events. One
valid notification can fan out to multiple connection writes. Summing
`notify_count` or a channel count across replicas therefore does not count
unique commits.

### Push distribution and outcomes

Push telemetry covers persisted account updates accepted on `tg_updates` only.
Transient channels remain in notification-throughput metrics and are not
silently included in push latency. Timing starts when a valid `tg_updates`
notification is accepted and ends when the associated connection write
completes. Every attempted connection contributes exactly one fixed outcome:

| `push_outcomes` key | Meaning |
|---|---|
| `success` | The owner-checked connection write succeeded. |
| `owner_mismatch` | The connection no longer belonged to the intended account. |
| `encode_failure` | The update could not be encoded. |
| `write_failure` | The connection write failed for another reason. |

Only successful writes contribute latency samples. The following fields share
the same process-local rolling window and reset at process start:

| JSON field | Type and rule |
|---|---|
| `push_latency_sample_count` | Integer successful-write sample count. It equals `push_outcomes.success` and the sum of all histogram buckets. |
| `push_window_seconds` | Elapsed window in `[0, 3600]`. |
| `push_latency_p50_ms`, `push_latency_p95_ms` | Numeric nearest-rank percentile represented by a fixed bucket upper bound. |
| `push_latency_p50_overflow`, `push_latency_p95_overflow` | Boolean indicating that the matching percentile rank landed in the final overflow bucket. |
| `push_latency_bucket_upper_bounds_ms` | Fixed array of 15 finite upper bounds: `[1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 30000, 60000]`. |
| `push_latency_bucket_counts` | Fixed array of 16 disjoint counts: one for each finite bound and a final bucket for observations above 60,000 ms. |

The finite buckets are `<=1 ms`, then `(1,2]`, `(2,5]`, `(5,10]`,
`(10,20]`, `(20,50]`, `(50,100]`, `(100,200]`, `(200,500]`, `(500,1000]`,
`(1000,2000]`, `(2000,5000]`, `(5000,10000]`, `(10000,30000]`, and
`(30000,60000]` milliseconds, followed by `>60000 ms`. No raw observation is
retained.

Percentiles use nearest rank: `ceil(sample_count * percentile / 100)`, with a
minimum rank of one, over the disjoint bucket counts. A finite percentile is
the selected bucket's upper bound. An overflow percentile has numeric value
`60000` and its matching overflow flag is `true`; the dashboard renders it as
`> 60,000 ms`. With zero successful samples, both numeric percentile fields
are zero and both overflow flags are false. Consumers must use the sample
count and render `No samples`, never `0 ms`.

### Rolling rate-limit denials

Denial telemetry counts only requests that actually returned `FLOOD_WAIT`:

| JSON field | Meaning |
|---|---|
| `rate_limit_denials_count` | Rolling count of denials from the fixed surfaces below. It equals the sum of the fixed surface counts. |
| `rate_limit_denials_rate_per_second` | Denial count divided by `rate_limit_denials_window_seconds`, or zero at zero duration. |
| `rate_limit_denials_window_seconds` | The same process-local elapsed window used by notification and push telemetry. |
| `rate_limit_denials_by_surface` | Fixed integer counts for exactly the 19 approved surfaces below. |
| `rate_limit_denials_dropped` | Rolling count of otherwise countable denials whose internal surface was outside the fixed vocabulary. It is excluded from the denial total and has no rate or breakdown. |

The fixed surface keys, in stable order, are:

`message_send`, `create_chat`, `add_chat_user`, `create_channel`,
`messages_search`, `contacts_search`, `messages_search_global`,
`save_file_part`, `upload_get_file`, `send_code_ip_calls`,
`send_code_ip_distinct_numbers`, `sign_in_fail_ip`, `check_password`,
`check_password_ip`, `get_password_ip`, `sign_up_ip`, `password_proof`,
`get_password`, and `update_profile`.

Allowed requests, refunded reservations, and storage failures do not count.
The `_ip` suffix is only a fixed surface name. It does not expose an address.
No caller-controlled surface or dynamic key is retained.

## Unavailable and unfinished values

Availability is decided before numeric formatting:

- A missing required sample is `Unavailable`, not zero. A stale complete
  sample keeps its old value and timestamp and is visibly marked stale.
- A valid empty rolling window has zero counts and zero rates, with all fixed
  rows still present. A zero-duration window is labeled `Window just started`.
- An empty successful push sample set is `No samples`; failed outcomes remain
  visible and do not become successful latency samples.
- A field listed in `uninstrumented` is `Not yet instrumented` with the helper
  `This metric is not measured yet.` The production wiring supplies the
  notification recorder, so push percentiles are normally instrumented. A
  compatibility or test caller without that recorder may list
  `push_latency_p50_ms` and `push_latency_p95_ms` as uninstrumented.
- RPC timing is intentionally still unavailable in the dashboard. The exact
  capability note is: `RPC timing is not available in this dashboard. Production trace export is unavailable.`
- Negative storage estimates are `Unavailable` and never real zeroes.

Unknown JSON fields and unknown `uninstrumented` names are ignored. They must
not become dashboard labels, DOM content, accessibility names, or metric
series.

## Fleet aggregation

The endpoint is per replica. A future collector may aggregate only compatible
samples with the same observation interval, units, histogram boundaries, and
complete replica coverage. Unequal process-start windows are partial coverage;
do not divide summed counts by summed replica seconds and call the result a
complete fleet rate.

| Family | Fleet rule |
|---|---|
| Notifications | Sum `notify_count`, `notify_channels`, and `notify_invalid` across replicas over compatible windows. Sum `notify_rate_per_second` only for the same common window. These totals remain delivery work, never unique commits. |
| Push outcomes | Sum each of the four `push_outcomes` counts and `push_latency_sample_count` over compatible windows. |
| Push distribution | Sum matching disjoint histogram buckets, including overflow, then recompute p50 and p95 from the merged buckets. Never average replica p50 or p95 values. |
| Rate-limit denials | Sum `rate_limit_denials_count` and every fixed surface count over compatible windows; derive the fleet rate from the common window or sum compatible rates. Keep `rate_limit_denials_dropped` separate from denials. |
| Delivery lag | Take the maximum current `worst_pts` across replicas with full current coverage. Never sum lag. Any stale, unavailable, or partial replica makes fleet coverage partial. |
| Live connections | Sum `connections` only with complete replica coverage. |
| Authenticated sessions | A sum is a count of replica-local connected-account observations, not distinct fleet accounts. Do not label it as unique accounts. |
| `max_pts_gap` | Keep per replica. Its live-account subset means neither a sum nor a maximum establishes fleet-wide account-head spread. |
| Shared database values | Read `total_*`, activity, message, `rate_limit_active`, and `storage_rows` once from the shared database. Never add them once per replica. |

There is no fleet selector in the dashboard. It labels readings as `This
replica · process-local` and keeps the aggregation rules visible beside the
shared database values.

## Tracing posture

Tracing is disabled by default. A nil exporter creates no worker, queue, span
allocation, exporter callback, DNS, or socket work on the RPC path. The shipped
`telegramd` wiring does not install an exporter, and there is no production
trace endpoint, exporter setting, credential, sampling setting, or dashboard
RPC summary.

Tests and local diagnostics may supply a bounded in-process exporter. Such a
span contains only the fixed name `telegram.rpc.server`, a canonical compiled
RPC method or `unknown`, a duration, and one result class:
`success`, `invalid_request`, `unauthenticated`, `unauthorized`,
`rate_limited`, `deadline`, `internal`, or `transport_failure`. The bounded
queue drops on saturation; exporter errors, panics, and bounded shutdown do
not change an RPC result or server shutdown. No payload, identifier, address,
error text, SQL, trace context, or dynamic method value is exported.

## Operator runbook

After logging in through `/admin/login`, an authenticated session can inspect
the JSON contract with a cookie file:

```bash
curl --fail --silent --show-error \
  --cookie admin.cookies \
  http://127.0.0.1:2444/admin/metrics | jq .
```

Use `curl --no-buffer --cookie admin.cookies` against
`/admin/events` only when watching the dashboard stream directly. Do not put
the admin token in a URL, log it, or commit it. Check that metric responses
include `Cache-Control: no-store`; do not cache or persist snapshots in a
browser or shared proxy.

For routine triage, inspect the following in order:

1. `sample_state`, `sample_age_seconds`, `timestamp`, `replica_id`, and
   `process_generation` to distinguish fresh, stale, and changed-source data.
2. `delivery_lag.state` and `delivery_lag.coverage` before interpreting
   `worst_pts`. Partial or unavailable coverage is not a fresh zero.
3. The push sample count, outcome rows, and histogram buckets. Failed outcomes
   explain why a percentile can have no samples.
4. Notification and denial windows, their fixed rows, and their elapsed
   seconds. Do not infer denials from `rate_limit_active`.
5. Shared database counts and estimates, remembering that estimates can be
   unavailable and database values are not per-replica totals.

This milestone supplies operational readings, not alert thresholds or SLO
policy. Do not invent health thresholds from a zero, a percentile bucket, or a
falling rolling count.

### Concrete restart and empty-window example

Suppose replica `edge-2` restarts at `12:00:00` with a new
`process_generation`, while `replica_id` remains `edge-2`:

- When the first sample's elapsed window is zero, it has
  `notify_window_seconds: 0`,
  `notify_count: 0`, `notify_rate_per_second: 0`,
  `push_window_seconds: 0`, and `push_latency_sample_count: 0`. All fixed
  notification and denial rows are zero. The dashboard says `Window just
  started` and `No samples`; those are valid empty values, not unavailable
  data.
- If the first delivery-lag attempt cannot read any authoritative account
  head, it reports `state: "unavailable"`, `coverage: "none"`,
  `worst_pts: null`, and `sampled_at: null`, so the dashboard says
  `Unavailable`. If a prior full sample existed and the next attempt sampled
  only some connections, it reports `state: "stale"` with the prior
  `worst_pts` and `sampled_at`, and never replaces them with a fresh zero.
- The dashboard still shows the unfinished capability note
  `RPC timing is not available in this dashboard. Production trace export is
  unavailable.` If a caller without the process recorder receives
  `uninstrumented: ["push_latency_p95_ms"]`, it shows `Not yet instrumented`
  for that field rather than treating its placeholder zero as `0 ms`.

Once a new complete sample arrives, the stale notice clears. Database gauges
retain their persisted meaning across the restart; only process-local windows
and process-local tracing state reset.
