# wasmq — TODOs

Actionable checklist derived from [`ANALYSIS.md`](./ANALYSIS.md). Each item names the issue, why it matters, where it lives, and a concrete fix. Ordered by severity, matching the analysis; a suggested execution order is in [§6](#6-suggested-order).

- [ ] Checkbox per item — check off as fixed.
- File:line references point at the code at commit `40314ba`; re-check they still apply before editing.

---

## 1. Critical

### [ ] 1.1 Jobs with no attempts left run forever, every 30 seconds
**Where:** `src/scheduler/src/lib.rs:151-183,187-226`, `src/storage/src/backend/sqlite.rs:189-254`, migration `CHECK (attempts <= max_attempts)`

**Issue:** `load_failed_jobs` doesn't filter `attempts < max_attempts`. `dispatch_job` sends `ClaimJob` with fire-and-forget `ipc.send` and never checks whether the claim succeeded, so it always sends `ExecuteJob` next. `claim_job` correctly refuses jobs at `max_attempts`, but nobody reads that refusal. When the job then finishes, `update_job_completed` does `attempts = attempts + 1`, which violates `CHECK (attempts <= max_attempts)` and fails silently — status stays `failed` — so the next 30 s `periodic_reload` picks it up again, forever. Every retry has real side effects (e.g. the `http` example task re-sends its POST).

**Fix:**
- Change `dispatch_job` to use `ipc.request(ClaimJob)` and only send `ExecuteJob` when Storage confirms the claim.
- Add `AND attempts < max_attempts` to the failed-jobs query, or introduce a terminal `dead` status once attempts are exhausted.
- Make `update_job_completed` conditional on `status IN ('claimed','running') AND claimed_by = ?` so a stale/duplicate completion can't corrupt state.

### [ ] 1.2 Claims don't guard dispatch, so jobs can run twice
**Where:** `src/scheduler/src/lib.rs:195-211`

**Issue:** Same root cause as 1.1 — the claim result is never checked. `load_scheduled_jobs` dedupes only against the in-memory heap, not the DB. A job already dispatched but not yet reflected in storage as claimed can be re-queued and dispatched a second time; since the claim is ignored, both executions run. The SQL claim is already atomic (see the `concurrent_claim_does_not_duplicate` test in `sqlite.rs`) — the scheduler just never uses that guarantee.

**Fix:** Same as 1.1's first bullet — `request()` the claim, skip dispatch when it's refused.

### [ ] 1.3 HTTP API accepts unauthenticated uploads of arbitrary code, on every interface
**Where:** `src/cli/src/server.rs:25`, `src/cli/src/server/api/v0/tasks/create.rs`, `src/executor/src/runtime/wasmtime.rs:62`

**Issue:**
- Server binds hardcoded `0.0.0.0:6283`, ignoring `config.hub.api_addr` (which defaults to `127.0.0.1`). The startup log even prints the *configured* address, misleading whoever's watching.
- No auth on any endpoint — anyone reaching the port can upload a `.wasm` and schedule it for execution.
- Guest code gets unrestricted outbound HTTP via `wasi-http`'s `default_hooks()` — SSRF into the internal network and cloud metadata endpoints (`169.254.169.254`).

**Fix:**
- Bind to `config.hub.api_addr` instead of the hardcoded address.
- Add token authentication on the API.
- Add an allow/deny list for outbound HTTP via a custom `WasiHttpHooks::send_request` implementation.

### [ ] 1.4 Guest code has no CPU, memory or time limits
**Where:** `src/executor/src/runtime/wasmtime.rs:51-86`, `src/cli/src/process/executor.rs:113`

**Issue:** No fuel, no `epoch_interruption`, no `StoreLimits`, no wall-clock timeout. A guest with `loop {}` (or `task/fibonacci` with a large `n`) runs forever, occupying a Tokio worker thread — CPU-bound guest code doesn't yield in `call_async` without epoch/fuel yielding configured. A guest can also grow linear memory up to the full 4 GiB wasm32 limit. The job then stays `claimed` forever (see 2.1).

**Fix:**
- Turn on `Config::epoch_interruption(true)` with a background ticker calling `store.epoch_deadline_async_yield_and_update`.
- Add a per-job wall-clock timeout.
- Add `StoreLimitsBuilder` caps for memory, tables, instances.

### [ ] 1.5 Docker image cannot start
**Where:** `docker/Dockerfile:5`

**Issue:** `ENTRYPOINT ["/opt/wasmq", "start"]`, but no `start` subcommand exists — the CLI has `run` (`src/cli/src/cli/cmd.rs:22`); `hub start` is dead, uncompiled code (see 5.1). The published image and the README/quick-start `docker run -p 6283:6283 ...` instructions fail immediately.

**Fix:** Either change the entrypoint to `["/opt/wasmq", "run"]`, or finish wiring up `hub start` (5.1) and point the entrypoint at that instead if a distinct "long-running server" subcommand is preferred.

---

## 2. High

### [ ] 2.1 No recovery for stuck `claimed` jobs; `running`/`started_at` never set
**Where:** `src/storage/src/lib.rs:68-104`, `src/ipc/src/protocol.rs:31-36`

**Issue:** Storage only handles `JobCompleted`, `StoreJob`, `QueryJobs`, `ClaimJob`, `Ping`, `Shutdown`. `UpdateJobStatus`, `JobStarted`, `JobFailed` fall through to `_ => None` and are silently dropped — so the scheduler's "revert to Scheduled on dispatch failure" path does nothing, `started_at` is never written, and `running` status is never used. If an executor crashes, is killed, or hangs (1.4) after claiming, the job stays `claimed` forever — no lease expiry, no reaper. If `JobCompleted` (sent via `request()`) doesn't get a reply within 30 s, the result is lost and the job stays `claimed` too.

**Fix:**
- Implement handlers for `UpdateJobStatus`, `JobStarted`, `JobFailed` in Storage.
- Add a lease/reaper: periodically re-queue jobs where `claimed_at < now - lease_duration`.
- Add a terminal `dead` status for exhausted retries (ties into 1.1).

### [ ] 2.2 Only executor 0 is ever used
**Where:** `src/cli/src/cli/cmd/component/scheduler.rs:23`, `src/cli/src/process/hub.rs:115`

**Issue:** `SchedulerProcess::new(transport, 1)` hardcodes `executor_count = 1`, so round-robin dispatch always targets executor 0. `Hub::wait_for_components` only pings `0..1`. Extra executors spawn per `config.executors.count` but never get work, and startup failures in executor N>0 go unnoticed.

**Fix:** Thread `config.executors.count` through to `SchedulerProcess::new` instead of the literal `1`, and loop `wait_for_components` over `0..config.executors.count`.

### [ ] 2.3 Configuration is mostly ignored
**Where:** `src/cli/src/cli/cmd/run.rs:20`, `src/config/src/*`

**Issue:**
- `wasmq run` always uses `Config::default()` — no `--config` flag.
- `hub.api_addr` ignored (ties into 1.3).
- `scheduler.check_interval_secs` unused; scheduler uses hardcoded constants (`scheduler/src/lib.rs:16-20`).
- `executors.max_concurrent_jobs` unused; executor spawns unbounded Tokio tasks per `ExecuteJob`.
- `storage.backend = Memory` is the only variant and is never actually read — storage is always SQLite at a fixed path.

**Fix:** Add a `--config <path>` flag to `wasmq run` (mirroring the `component *` subcommands). Wire `check_interval_secs` into the scheduler's constants. Add a semaphore or bounded queue in the executor sized by `max_concurrent_jobs`. Either implement the `Memory` backend or remove the config field and document SQLite as the only backend.

### [ ] 2.4 Startup race between hub and child processes
**Where:** `src/cli/src/process/hub.rs:55-92`, `src/ipc/src/transport/unix_socket.rs:24,184-213`

**Issue:** `spawn_processes` starts children then immediately calls `wait_for_components`, which retries connecting only 3 times with 10 ms/20 ms backoff (~30 ms total). Children need to exec, init tracing, open SQLite, run migrations before binding their socket — routinely longer than 30 ms. `wasmq run` then fails with "Failed to connect ... after 3 retries".

**Fix:** Poll with a deadline (e.g. up to 10 s) instead of a fixed retry count, or add an explicit readiness signal from each child.

### [ ] 2.5 Child process lifecycle is unmanaged
**Where:** `src/cli/src/cli/cmd/run.rs:26-39`

**Issue:** If `run_server` errors, the branch logs and returns `Ok(())` without killing children — they're left orphaned, holding sockets and the SQLite file open. On shutdown, children get `SIGKILL` then `std::process::exit(0)` runs immediately: no draining, in-flight jobs lost in `claimed`, `Shutdown`/`ShutdownAck` protocol messages never used, `Drop` for `TempDir`/socket files doesn't run. Nothing restarts a crashed child; `kill_on_drop(true)` isn't set on the `Command`s.

**Fix:** Kill children on the error branch too. Send `Shutdown` and wait briefly for `ShutdownAck` before `kill()`. Set `kill_on_drop(true)` on every spawned `Command`.

### [ ] 2.6 IPC sockets live in a predictable, world-writable place with no auth
**Where:** `src/config/src/transport.rs:14`, `src/ipc/src/transport/unix_socket.rs:41-48`

**Issue:** Default socket dir is `/tmp/wasmq_sys` (fixed, shared across users). Any local user could pre-create it, or connect and send `StoreJob`/`ExecuteJob`/`ClaimJob` directly — messages aren't authenticated. `UnixSocketTransport::new` unconditionally deletes an existing socket file, so a second instance silently hijacks the first's sockets.

**Fix:** Default to `$XDG_RUNTIME_DIR` or a per-session `TempDir`; create the directory `0700`; refuse to start if a live socket already answers a `Ping`.

### [ ] 2.7 IPC listener can busy-loop
**Where:** `src/ipc/src/channel.rs:34-53`

**Issue:** `IpcServer::listen` loops on `transport.recv()`, and on error just `eprintln!`s and continues. `UnixSocketTransport::recv` only errors when its channel is permanently closed, so the loop spins at 100% CPU flooding stderr.

**Fix:** Break out of the loop (or propagate the error) when `recv()` fails instead of continuing.

### [ ] 2.8 Oversized IPC messages hang the sender
**Where:** `src/ipc/src/transport/unix_socket.rs:167-180,129-136`

**Issue:** Sender casts `serialized.len() as u32` with no check against `MAX_MESSAGE_SIZE` (10 MB) before sending. A too-large message goes out, the receiver bails with "Message too large", and the caller of `request()` waits the full 30 s timeout for a generic error. Lengths above 4 GiB silently truncate. Compounded by `GET /api/v0/jobs` returning *every* job unpaginated (see 3.3) — once that serializes past 10 MB, listing jobs breaks outright.

**Fix:** Check length against `MAX_MESSAGE_SIZE` before sending and fail fast with a clear error. Paginate job queries (3.3) to keep payloads bounded.

### [ ] 2.9 Failed jobs retry immediately, no backoff
**Where:** `src/scheduler/src/lib.rs:151-183`

**Issue:** Failed jobs keep their original (past) `scheduled_at`, so every 30 s reload dispatches them right away — no exponential backoff, no `next_attempt_at`. A task failing due to a downstream outage burns all its attempts within ~90 seconds.

**Fix:** Add a `next_attempt_at` column, computed with exponential backoff on each failure; have `load_failed_jobs` filter on it instead of picking up all failed jobs immediately.

### [ ] 2.10 Storage handles messages one at a time
**Where:** `src/storage/src/lib.rs:50-63`

**Issue:** Every message is `await`ed inline in the receive loop, so one slow query blocks everything else — including health pings (30 s timeout) and the (currently unchecked, see 1.1/1.2) `ClaimJob`. Also: `handle_message` takes `&mut self` without needing it; each message is `clone()`d only to read `from`/`id` afterward.

**Fix:** Spawn a task per message (or use a bounded worker pool) instead of awaiting inline. Drop the unnecessary `&mut self` and `clone()`.

---

## 3. Medium

### [ ] 3.1 Wasmtime engine and component rebuilt for every job
**Where:** `src/executor/src/runtime/wasmtime.rs:51-75`, `src/cli/src/process/executor.rs:50-78`

**Issue:** Every execution does `Engine::new`, `Linker::new` + WASI/HTTP linking, and `Component::from_binary` — a full Cranelift compile. The executor's cache stores raw `Bytes`, so compilation cost repeats every run; for small tasks, compile time dwarfs execution time.

**Fix:** Keep one `Engine` per process; build the `Linker` once. Cache `InstancePre<ComponentRunStates>` (or at least `Component`) per `TaskIdentifier` instead of raw bytes. Optionally precompile with `Engine::precompile_component` at upload time and load via `Component::deserialize`. Consider the pooling allocator for faster instantiation.

Smaller items, same file:
- [ ] `wasm_component_model_async(true)` set twice (lines 54 and 56) — remove the duplicate.
- [ ] Input JSON parsed then re-serialized for nothing (lines 64-66) — just validate it.
- [ ] `rustls::crypto::aws_lc_rs::default_provider().install_default().expect(...)` (line 44) panics if a second `WasmtimeRuntime` is created in-process (`install_default` errors if a provider is already installed). Ignore the `Err`, or gate with `std::sync::Once`.
- [ ] `Executor::run`'s `tokio::spawn(...).await` doesn't isolate CPU-bound guest code (still runs on a runtime worker — see 1.4).
- [ ] Per-process module cache is never evicted — add an eviction policy (LRU / TTL) if it's going to stay bytes-based.

### [ ] 3.2 Error details get lost
**Where:** various

- [ ] `JobResult::Failure(err.to_string())` (`process/executor.rs:121`) drops the `anyhow` context chain — use `format!("{err:#}")`.
- [ ] Storage turns `QueryJobs` errors into an empty `Vec` (`storage/src/lib.rs:87`) — callers can't distinguish "no jobs" from "DB broken".
- [ ] `ClaimJob` failures (including the normal "lost the race" case) are logged at `error` level — demote to `debug`/`warn`.
- [ ] `eprintln!` used alongside `tracing` in IPC and component loops — standardize on `tracing`.

### [ ] 3.3 Job queries don't scale
**Where:** `src/cli/src/server/api/v0/jobs/retrieve.rs:23-53`, `src/storage/src/backend/sqlite.rs:148-187`

- [ ] `GET /jobs?id=X` fetches *every* job and filters in the hub — add an `id` field to `JobQuery` for a direct primary-key lookup.
- [ ] No pagination/limit on the API.
- [ ] `ORDER BY` only applied when `limit` is set — result order otherwise undefined.
- [ ] CLI `job list --status` filters client-side despite the API supporting `?status=`; `--all` silently overrides `--status`.
- [ ] `GET /api/v0/jobs/{id}` (used by `test/smoke.sh:42`) doesn't exist — add it, or fix the script to use the query param.

### [ ] 3.4 Timestamps stored at second precision, with conversion hazards
**Where:** `src/storage/src/backend/sqlite.rs:257-268`

- [ ] `as_secs()` drops sub-second precision; combined with the scheduler's 1 s/10 s sleep bounds, jobs can start up to ~10 s late, and nothing wakes the clock loop early when a new job is inserted.
- [ ] `into_system_time(timestamp as u64)` turns negative timestamps into huge durations — validate before casting.
- [ ] `claimed_at` uses SQLite's clock (`strftime('%s','now')`) while everything else uses the process clock — pick one.
- [ ] Consider storing milliseconds, and waking the scheduler via a `tokio::sync::Notify` on `StoreJob` instead of relying purely on polling.

### [ ] 3.5 Scheduler internals
**Where:** `src/scheduler/src/lib.rs`

- [ ] Duplicate check `queue.iter().any(|j| j.id == job.id)` is O(n²) per load — keep a `HashSet<Ulid>` alongside the heap.
- [ ] `current_executor: tokio::Mutex<usize>` → `AtomicUsize`.
- [ ] `message_consumer` replies with `from: ProcessType::Storage` (line 240) — copy-paste bug, should be `Scheduler`.
- [ ] `run()`'s `tokio::select!` uses `Err(err) = ...` patterns exclusively; if any branch returns `Ok`, that branch disables, and in `Storage::run`/`ExecutorProcess::run` (single-branch) this means the whole `select!` panics once the branch resolves `Ok`.
- [ ] Lookahead query (`LIMIT 10`, 5 min window) + heap-only dedup caps throughput at ~10 jobs/10s when more than 10 jobs are due — increase the limit or reload immediately after draining.
- [ ] `Job` derives `PartialEq`/`Eq` on all fields but implements `Ord`/`PartialOrd` on `scheduled_at` only — violates the `Ord`/`Eq` consistency contract. Wrap the ordering key in `Reverse((scheduled_at, id))` instead.

### [ ] 3.6 Task repository issues
**Where:** `src/repository/src/backend/local.rs`

- [ ] Remove debug `println!("{:?}", file_path)` in `create` (line 51).
- [ ] `list()` aborts the *entire* listing on one badly named file, despite the error text saying "Skipping" — make it actually `continue` and log a warning.
- [ ] `split_once(".wasm")` also matches `foo.wasm.bak`/`x.wasmx` — use `strip_suffix(".wasm")`.
- [ ] Namespace isn't sanitized: `..` passes `TaskIdentifier::from_str`, letting `POST /api/v0/tasks/../x/1.0.0` write outside the intended namespace dir. Restrict namespace/name to `[a-z0-9_-]+`.
- [ ] Writes aren't atomic — a failed `write_all` leaves a truncated file, and `create_new` then blocks any retry. Write to a temp file and `rename` into place.
- [ ] Upload doesn't validate the bytes are a valid component with a matching `handler` export — errors only surface at job run time. Validate via `Component::from_binary` + type check on upload.
- [ ] Axum's default 2 MB body limit applies to multipart uploads — raise it or document the ceiling, and return a clearer error when hit.
- [ ] Hub and every executor each open their own `TaskRepository::local()` — fine single-host, but document/flag this as a constraint if multi-host is ever a goal.

### [ ] 3.7 API correctness
- [ ] Invalid `TaskIdentifier` in `tasks/create.rs:21` returns 502 — should be 400.
- [ ] `POST /jobs` doesn't check the referenced task exists — a typo only fails at run time (and burns retries). Validate against the repository at creation.
- [ ] `Job::new` only checks for spaces/emptiness in `name` — add a length limit; also bound `args` size beyond axum's default JSON limit.
- [ ] `ApiError::into_response` round-trips `StatusCode` through `u16` for no reason — simplify.
- [ ] `JobStatus` serializes PascalCase but DB/CLI use lowercase, and query-string deserialization only accepts PascalCase — add `#[serde(rename_all = "lowercase")]` for consistency.

### [ ] 3.8 CLI UX
- [ ] `http://localhost:6283` hardcoded in five commands — add `--url` / `WASMQ_URL` env var support.
- [ ] Every subcommand (`job new`, `job list`, `job view`, `task list`, `task load`) prints errors but exits 0 — return the error instead so scripts/CI can detect failure.
- [ ] `job view`'s error message says "Failed to list jobs" — should say "Failed to view job" or similar.
- [ ] `task new` ignores namespace/version from `self.name`, only uses `.name`; generated `Cargo.toml` isn't templated with the actual task name (ties into 3.9's placeholder issue).

### [ ] 3.9 `wasmq_handler` proc-macro hygiene
**Where:** `src/task/src/lib.rs:84-123`

- [ ] Injects `use anyhow::Result; use wit_bindgen;` at the call site — clashes with user imports and forces the user crate to depend on `anyhow`, `serde_json`, `wit_bindgen` directly. Re-export from a runtime crate (`wasmq_task::__private::*`) and use absolute paths instead.
- [ ] Generated `struct Wasmq` and `mod bindings` can collide with user items — namespace them more defensively (e.g. a generated-only module).
- [ ] Macro assumes `anyhow::Result<T>` — `Result<T, MyError>` won't compile since `Result<#output_type>` is rebuilt with one type parameter.
- [ ] Macro doesn't validate the handler is `async` or takes exactly one parameter — add compile-time checks with clear error messages.
- [ ] WIT world is inlined in the macro and implicitly duplicated in the host (`HANDLER_FUNC_FQN = "handler"`) — move to a shared `.wit` file, generate host bindings with `wasmtime::component::bindgen!` for type-checked calls.

### [ ] 3.10 Example tasks
- [ ] `task/fibonacci`'s handler is named `sum` — rename for clarity.
- [ ] `task/http` calls `wstd::runtime::block_on` inside an already-async component-async handler, and uses `.unwrap()`/`.expect()` — guest errors become traps instead of clean `Err` returns. Replace with `?`/proper error propagation.

---

## 4. Low / hygiene

### [ ] 4.1 Dead and uncompiled code
- [ ] `src/cli/src/cli/cmd/hub.rs` and `hub/start.rs` aren't declared in `cmd.rs` and never compile. They wouldn't compile even if wired in: `Hub::new(self.config.clone())` passes a `PathBuf` where a `Config` is expected, and `run_server(hub.config(), ...)` passes `&Config` instead of `Arc<Config>`. Either delete them, or fix and wire them in (the Dockerfile expects a `start` subcommand — see 1.5).
- [ ] `RunCmd` is an empty `enum` used only as a namespace for `run()` — consider a plain function instead.
- [ ] Unused protocol variants: `JobAccepted`, `JobFailed` (scheduler ignores replies), `Shutdown`/`ShutdownAck` (nothing sends them — ties into 2.5), `UpdateJobStatus`/`JobStarted` (not handled — ties into 2.1).
- [ ] Unused statuses: `pending`, `running`, `cancelled` — no cancel endpoint exists.
- [ ] `IpcServer::shutdown(&mut self)` can't be called through the `Arc<IpcServer>` every component holds — needs interior mutability or removal. `IpcServer::receiver` is `async` for no reason — make it sync.
- [ ] `ExecutorId` defined twice (`ipc/protocol.rs:6`, `wasmq/proto/job.rs:12`) — consolidate to one definition. `RetrieveJobsQuery` in `client/api/v0/tasks.rs` is defined but never used — remove it.
- [ ] `clap` dependency in `wasmq-executor` is unused — remove from `Cargo.toml`.

### [x] 4.2 Rename to `wasmq` incomplete — **done**
Library names, socket dir, port const, doc comments, macro struct name, and task-template placeholders have all been renamed from `mate` to `wasmq`. No action needed.

### [ ] 4.3 IPC design
- [ ] Transport opens a new Unix-socket connection per message (connect, write, shutdown) — switch to persistent connections or a framed stream (e.g. `tokio_util::codec::LengthDelimitedCodec`).
- [ ] Messages pass through two unbounded channels (transport's internal one, then `IpcServer`'s) — doubles buffering, no backpressure anywhere. Collapse to one, and consider bounding it.
- [ ] No read timeout in `handle_connection` — a client that connects and never writes leaks a task forever. Add a timeout.
- [ ] JSON on the wire works for debugging but a binary format (`postcard`/`bincode`) would be smaller/faster for `Job` payloads — consider switching once the protocol stabilizes.
- [ ] Resolve the `transport.rs:7`/`run.rs:19` TODO/FIXME notes about components touching `Transport` directly instead of going through `IpcService`.

### [ ] 4.4 Storage schema
- [ ] No index covers `status IN ('scheduled','failed') AND attempts < max_attempts` — current `(status, scheduled_at)` index is adequate for now but revisit once 1.1/2.9 land.
- [ ] No retention/cleanup of completed jobs — add a TTL or archival job.
- [ ] `Storage::new` uses `home.to_str().unwrap()`, which panics on non-UTF-8 home paths — use `SqliteConnectOptions::filename(&Path)` directly instead of round-tripping through `&str`.
- [ ] `.env.example`'s `DATABASE_URL` is only used by `sqlx prepare`, never at runtime — note that in the file to avoid confusion.

### [ ] 4.5 Tests and CI
- [ ] Test coverage is limited to the SQLite claim, Unix socket transport, config parsing, `TaskIdentifier` — nothing covers the scheduler, executor, API, retry logic, or an end-to-end run. Add an integration test that schedules a failing job through to exhaustion (would have caught 1.1/1.2/2.1).
- [ ] `test/smoke.sh` is stale: posts a `payload` field where the API expects `task`/`args`/`scheduled_at`, queries `status=Pending`, and calls `/jobs/{id}` which doesn't exist. Fix it and wire it into CI.
- [ ] CI's "Tasks Integrity" job depends on `httpbin.org` — flaky external dependency; consider a local mock.
- [ ] CI doesn't run `cargo sqlx prepare --check` — add it so `.sqlx` metadata can't silently drift.
- [ ] Config test asserts `backend = "Memory"`, which doesn't reflect the actual (SQLite-only) backend — fix the test or the config (ties into 2.3).

### [ ] 4.6 Docs and README
- [ ] README links to `LICENSE.md` (actual files are `LICENSE-MIT.md`/`LICENSE-APACHE.md`); fix the `githeub.com` typo; fix "usinc" typo.
- [ ] Quick start tells users to `docker run` the image, which currently fails (1.5) — fix once the Dockerfile entrypoint is fixed.
- [ ] No docs on the Wasm task interface (WIT world, input/output JSON contract), the security model, or the configuration file format — write these once 1.3/1.4/2.3 land so the docs describe the real, safer behavior.

---

## 5. Suggested order

1. **Job-execution correctness** — 1.1, 1.2, 2.1, 2.9. Make `ClaimJob` a request, gate `ExecuteJob` on success, handle `UpdateJobStatus`/`JobStarted`, add a lease/reaper and a `dead` terminal state, add retry backoff.
2. **Safety** — 1.3, 1.4, 2.6. Bind to the configured address, add API auth, add Wasmtime epoch/fuel + memory limits + per-job timeout, restrict HTTP egress, move sockets to a private runtime dir.
3. **Make it run** — 1.5, 2.4, 2.2, 2.3. Fix the Dockerfile entrypoint, the startup readiness race, the hardcoded executor count, and load config from a file.
4. **Performance** — 3.1, 2.3 (max_concurrent_jobs), 2.10, 3.3. Share `Engine`/`Linker`, cache `InstancePre`, precompile on upload; enforce concurrency limits; handle Storage messages concurrently; paginate queries.
5. **Hygiene** — 4.1, 4.5, 3.8 (exit codes), 4.6. Delete dead code, add the end-to-end integration test, fix CLI exit codes, update docs.
