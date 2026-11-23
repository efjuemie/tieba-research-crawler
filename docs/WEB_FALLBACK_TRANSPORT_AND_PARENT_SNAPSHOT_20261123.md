# Web fallback transport and parent snapshot update

## Changes

- The browser classifies trusted Baidu verification pages separately, rejects gate titles from untrusted hosts, preserves a visible gate only when the response provides manual-gate evidence, and keeps HTTP 429 sticky over later page or transport errors.
- Connection resets retain their exception type, numeric code, phase, and target context. A reset marks the current WEB task retryable and stops further WEB navigation for that drain; API work remains independent.
- Optional comment snapshot reuse is limited to one run and one parent post. The original page request context remains unchanged; every CID and author UID is checked before reuse. A snapshot serves at most eight targets and expires after 30 seconds. Failed identity checks, blocked responses, timeouts, resets, and close failures clear or withhold the cache. Missing growth evidence falls back to the exact CID page path.
- Queue-wait diagnostics report aggregate statuses, the next due time, and whether the browser context started, at most once per 30 seconds while idle. They do not add a per-poll database scan.
- No database migration was added. `web_parent_batch_enabled` defaults to `false` in code and both configuration examples.

## Verification

| Check | Result |
| --- | --- |
| Focused browser, network, CLI, task, live, config, and Playwright suites | 218 passed, 1 skipped |
| Isolated PostgreSQL runtime, scope, and error-code tests | 6 passed |
| Three-CID PostgreSQL P1 flow | CID 1 and 3 completed; CID 2 was rejected by its changed scope generation before writing; API fields and checkpoint stayed unchanged |
| Windows Chromium synthetic Playwright | 1 passed; every request was intercepted and served locally |
| Offline snapshot comparison | Six group sizes (2, 3, 4, 5, 6, 8 CID), nine repeats each; navigation, page_pc, and nestedFloor loads fell from N to 1 per parent |
| Ruff, compileall, and `git diff --check` | Passed |
| Isolated PostgreSQL cleanup | Owned test databases were dropped after connection and ownership checks; the dedicated test cluster was stopped |

The synthetic timing results measure local FakePage adapter overhead only. For eight CIDs, P50 was 3.516 ms without snapshot reuse and 1.584 ms with reuse; P90 was 4.371 ms and 1.909 ms. The same offline check injected a connection reset during page close: the result was retryable, the cache was empty, and the next exact-target navigation succeeded.

The final Windows full suite reported 829 passed, 13 skipped, and 24 failed. All 24 failing node IDs reproduced against the isolated `d94148f` baseline; these include Windows epoch conversion and filesystem/event-loop compatibility cases. The monitor cancellation test passed in the final un-paralleled run. The non-Baidu cookie test now checks that every poll uses only the official Baidu cookie URLs while allowing the bounded polling loop to make more than one check; it still requires timeout and verifies all browser resources close. The cookie-domain validation code was not changed. Linux CI is the cross-platform full-suite gate.

## Limits

- Live Tieba behavior, performance on real long comment threads, and production resume were not tested. The Chromium run was synthetic and network-isolated.
- The historical WinError 10054 cause remains unknown. The user confirmed there was no manual Ctrl+C or IDE Stop and described the ending as automatic PyCharm behavior; no complete original stack trace was available to identify the component.
- No production crawler was started, production recovery was not run, and the production database was not read or written. The isolated test database cluster was stopped after all test connections closed.

## Manual commands

Keep snapshot reuse disabled unless deliberately enabling the reviewed feature in the local `config.toml`:

```toml
[web_fallback]
web_parent_batch_enabled = true
```

Inspect one exact saved comment before a manual retry:

```sh
uv run python -m src.research field-backfill --run-id YOUR_RUN_ID --target-type comment --tid YOUR_TID --cid YOUR_CID --field ip_location_text --limit 1 --dry-run
```

For a visible, explicitly confirmed login or verification, run one exact target in an interactive terminal:

```sh
uv run python -m src.research field-backfill --run-id YOUR_RUN_ID --target-type comment --tid YOUR_TID --cid YOUR_CID --field ip_location_text --limit 1 --max-seconds 300 --browser-profile data/web-fallback-profile --headed --wait-for-login
```

Resume the existing backfill run with its original sort contract. CREATE uses:

```sh
uv run python main.py --mode backfill --resume --run-id YOUR_RUN_ID
```

Only a run originally created with REPLY sorting adds `--backfill-sort reply`.
