# wasmq — Codebase Analysis

Scope: every Rust crate under `src/`, the example tasks under `task/`, and the CI, Docker, docs and scripts around them (as of commit `40314ba`).
Method: static review of the source. Nothing was compiled or run for this report, so each finding comes from reading the code. The line numbers are there so each finding can be checked.

Severity legend:

- **Critical**: wrong results, data corruption, remote code execution, or a feature that cannot work.
- **High**: reliability or security problems that will show up in normal use.
- **Medium**: correctness gaps, performance problems, missing safeguards.
- **Low**: code quality, naming, docs, small inconsistencies.

---

## 1. Architecture summary (for context)

```
 CLI (wasmq job/task ...) ──HTTP──▶ Hub (axum, :6283)
                                      │  Unix-socket IPC (JSON, one connection per message)
              ┌───────────────────────┼─────────────────────────┐
              ▼                       ▼                         ▼
          Storage (SQLite)       Scheduler (BinaryHeap)    Executor(0..N) (Wasmtime)
```

- `wasmq run` starts the Hub. The Hub re-executes its own binary to spawn the `component storage|scheduler|executor` child processes.
- The Scheduler polls Storage for due jobs, sends `ClaimJob` to Storage and `ExecuteJob` to an Executor.
- The Executor loads the `.wasm` from `~/.wasmq/repository`, runs it with Wasmtime (component model plus WASI p2 plus wasi-http) and reports `JobCompleted` to Storage.

---

## 2. Critical

### 2.1 Jobs with no attempts left run forever, every 30 seconds
`src/scheduler/src/lib.rs:151-183`, `:187-226`; `src/storage/src/backend/sqlite.rs:189-254`; migration `CHECK (attempts <= max_attempts)`

1. `load_failed_jobs` queries `status = Failed` with **no filter on `attempts < max_attempts`**.
2. `dispatch_job` sends `ClaimJob` with `ipc.send` (fire-and-forget) and **ignores whether the claim succeeded**. It then always sends `ExecuteJob` to the executor.
3. `claim_job` correctly refuses jobs where `attempts >= max_attempts`. Nobody reads that refusal, so the executor runs the job anyway.
4. When the job finishes, `update_job_completed` runs `attempts = attempts + 1`. This breaks `CHECK (attempts <= max_attempts)`, so the UPDATE fails. The status stays `failed`.
5. The next `periodic_reload` (30 s) picks the job up again. Go back to step 2.

Result: a job that failed on its last attempt runs forever, and every run has real side effects (the `http` example task sends a POST each time). A job that *succeeds* on one of these extra runs also cannot record its result.

**Fix:** use `request` for `ClaimJob` and only send `ExecuteJob` when Storage returns the claimed job. Add `AND attempts < max_attempts` to the failed-jobs query, or better, add a terminal `dead` status. Make `update_job_completed` require `status = 'claimed'/'running' AND claimed_by = ?`.

### 2.2 Claims do not guard dispatch, so jobs can run twice
`src/scheduler/src/lib.rs:195-211`

This has the same root cause as 2.1: the claim result is never checked. `load_scheduled_jobs` (called from both the clock loop and `periodic_reload`) removes duplicates only against the in-memory heap. A job that was already popped and dispatched, but whose `ClaimJob` Storage has not processed yet, is still `scheduled` in the database. The job is then re-queued and dispatched a second time. With the claim ignored, both dispatches execute. The concurrency test in `sqlite.rs:298` proves the SQL claim is atomic. The scheduler never uses that guarantee.

### 2.3 The HTTP API accepts unauthenticated uploads of arbitrary code on every interface
`src/cli/src/server.rs:25`, `src/cli/src/server/api/v0/tasks/create.rs`, `src/executor/src/runtime/wasmtime.rs:62`

- The server binds a **hardcoded `0.0.0.0:6283`** and ignores `config.hub.api_addr`, which defaults to `127.0.0.1`. The log line then prints the configured address, which is misleading.
- There is no authentication on any endpoint. Anyone who can reach the port can upload a `.wasm` (`POST /api/v0/tasks/...`) and schedule it (`POST /api/v0/jobs`).
- Guests get **unrestricted outbound HTTP** through `wasi-http` with `default_hooks()`. That is SSRF into the internal network and into cloud metadata endpoints (`169.254.169.254`).

The Wasm sandbox protects the host filesystem, but an exposed instance is still a free, anonymous compute and network relay. **Fix:** bind to `config.hub.api_addr`, add token authentication, and add an allow/deny list for outgoing HTTP (a custom `WasiHttpHooks::send_request`).

### 2.4 Guest code has no CPU, memory or time limits
`src/executor/src/runtime/wasmtime.rs:51-86`, `src/cli/src/process/executor.rs:113`

- No fuel, no `epoch_interruption`, no `StoreLimits`, no wall-clock timeout.
- A guest with `loop {}`, or the bundled `task/fibonacci` with a large `n`, runs forever. It also occupies a Tokio worker thread, because CPU-bound guest code never yields in `call_async` unless epoch/fuel yielding is set up.
- A guest can grow linear memory up to the 4 GiB wasm32 limit.
- The job stays `claimed` forever (see 3.1).

**Fix:** turn on `Config::epoch_interruption(true)` with a background ticker and `store.epoch_deadline_async_yield_and_update`, set a per-job timeout, and add `StoreLimitsBuilder` for memory, tables and instances.

### 2.5 Docker image cannot start
`docker/Dockerfile:5`

`ENTRYPOINT ["/opt/wasmq", "start"]`, but no `start` subcommand exists. The CLI has `run` (`src/cli/src/cli/cmd.rs:22`), and `hub start` is dead code (see 5.1). The published image and the README/quick-start instructions (`docker run -p 6283:6283 ...`) therefore fail right away.

---

## 3. High

### 3.1 No recovery for stuck `claimed` jobs, and `running`/`started_at` are never set
`src/storage/src/lib.rs:68-104`, `src/ipc/src/protocol.rs:31-36`

- Storage handles only `JobCompleted`, `StoreJob`, `QueryJobs`, `ClaimJob`, `Ping` and `Shutdown`. **`UpdateJobStatus`, `JobStarted` and `JobFailed` fall through to `_ => None` and are silently dropped.**
  - So the "revert to Scheduled" path in `dispatch_job` (`scheduler/src/lib.rs:213-223`) does nothing.
  - `started_at` is never written, and the `running` status is never used.
- If an executor crashes, is killed, or hangs (2.4) after a claim, the job stays `claimed` forever. Nothing re-queues it: there is no lease expiry and no reaper for `claimed_at < now - lease`.
- The executor sends `JobCompleted` with `request()`. If Storage does not answer within 30 s, the result is lost and the job stays `claimed`.

### 3.2 Only executor 0 is ever used
`src/cli/src/cli/cmd/component/scheduler.rs:23`, `src/cli/src/process/hub.rs:115`

- `SchedulerProcess::new(transport, 1)` hardcodes `executor_count = 1`, so round-robin always picks executor 0.
- `Hub::wait_for_components` pings only `0..1`.
- The hub spawns `config.executors.count` executors. Extra executors run but never receive work, and a failure in executor N>0 goes unnoticed at startup.

### 3.3 Configuration is mostly ignored
`src/cli/src/cli/cmd/run.rs:20`, `src/config/src/*`

- `wasmq run` always uses `Config::default()`. There is no `--config` flag.
- `hub.api_addr` is ignored (2.3).
- `scheduler.check_interval_secs` is unused. The scheduler uses hardcoded constants in `scheduler/src/lib.rs:16-20`.
- `executors.max_concurrent_jobs` is unused. The executor spawns an unbounded number of Tokio tasks per `ExecuteJob` (`process/executor.rs:113`).
- `storage.backend = Memory` is the only variant and is never read. Storage is always SQLite at `~/.wasmq/storage.sqlite`, and the path is not configurable.
- Transport `base_path` defaults to `/tmp/mate_sys` (see 3.6).

### 3.4 Startup race between the hub and its child processes
`src/cli/src/process/hub.rs:55-92`, `src/ipc/src/transport/unix_socket.rs:24, 184-213`

`spawn_processes` starts the children and immediately calls `wait_for_components`. That call does one `request` per component, and `connect_with_retry` tries only **3 times with 10 ms and 20 ms backoff (~30 ms total)**. Each child must exec, init tracing, open SQLite and run migrations before it binds its socket, and that often takes longer than 30 ms. `wasmq run` then fails with "Failed to connect ... after 3 retries". **Fix:** poll with a deadline (for example up to 10 s) instead of a fixed retry count, or have children signal readiness.

### 3.5 Child process lifecycle
`src/cli/src/cli/cmd/run.rs:26-39`

- If `run_server` returns an error, the `select!` branch logs it and returns `Ok(())` **without killing the children**. They are left orphaned and keep the sockets and SQLite file busy.
- On shutdown the children get `SIGKILL` (`Child::kill`), and then `std::process::exit(0)` runs. Nothing is drained: in-flight jobs are lost in `claimed`, the `Shutdown`/`ShutdownAck` protocol messages are never used, and `Drop` of `TempDir` and the socket files does not run.
- Nothing supervises or restarts a crashed child. `kill_on_drop(true)` is not set on the `Command`s.

### 3.6 IPC sockets live in a predictable, world-writable place with no authentication
`src/config/src/transport.rs:14`, `src/ipc/src/transport/unix_socket.rs:41-48`

- The default directory is `/tmp/mate_sys`. Any local user can pre-create it (with a symlink or loose permissions), or connect to the sockets and send `StoreJob`/`ExecuteJob`/`ClaimJob` directly. Messages are not authenticated.
- `UnixSocketTransport::new` **unconditionally deletes an existing socket file**. Starting a second instance silently takes over the first instance's sockets.
- Two users on one host collide. **Fix:** use `$XDG_RUNTIME_DIR` or a per-session `TempDir`, create the directory `0700`, and refuse to start when a live socket already answers.

### 3.7 The IPC listener can busy-loop
`src/ipc/src/channel.rs:34-53`

`IpcServer::listen` loops on `transport.recv()` and, on error, logs with `eprintln!` and continues. `UnixSocketTransport::recv` returns an error only when its channel is closed, which is permanent. The loop then spins at 100 % CPU and floods stderr. **Fix:** break, or return the error.

### 3.8 Oversized IPC messages hang the sender
`src/ipc/src/transport/unix_socket.rs:167-180, 129-136`

- The sender casts `serialized.len() as u32` without checking `MAX_MESSAGE_SIZE` (10 MB). A larger message goes out, the receiver bails ("Message too large"), and the caller of `request` waits the whole 30 s timeout before it gets a generic error.
- Lengths above 4 GiB are silently truncated.
- `GET /api/v0/jobs` sends **all jobs** in one `JobsResult` (no limit, see 4.3). Once the table serializes to more than 10 MB, listing jobs stops working.

### 3.9 Failed jobs retry immediately with no backoff
`src/scheduler/src/lib.rs:151-183`

Failed jobs keep their original `scheduled_at` (in the past). Every 30 s reload therefore dispatches them straight away. There is no exponential backoff and no `next_attempt_at`. A task that fails because a downstream service is down uses all its attempts within about 90 s.

### 3.10 Storage handles messages one at a time
`src/storage/src/lib.rs:50-63`

Each message is `await`ed inside the receive loop, so one slow query blocks every other component. That includes health pings, which time out after 30 s, and `ClaimJob`, which the scheduler never waits for. Other issues in the same file:

- `handle_message` takes `&mut self` without needing it.
- Each message is cloned (`msg.clone()`) only to read `from` and `id` afterwards.

---

## 4. Medium

### 4.1 Wasmtime engine and component are rebuilt for every job
`src/executor/src/runtime/wasmtime.rs:51-75`, `src/cli/src/process/executor.rs:50-78`

Each execution runs `Engine::new`, `Linker::new` plus WASI/HTTP linking, and **`Component::from_binary`, which is a full Cranelift compile**. The executor cache stores raw `Bytes`, so the compile cost repeats on every run. For small tasks, compilation takes far longer than execution.

**Fix:** keep one `Engine` per process, build the `Linker` once, and cache `InstancePre<ComponentRunStates>` (or at least `Component`) per `TaskIdentifier`. Optionally precompile with `Engine::precompile_component` at upload time and use `Component::deserialize`. Turn on the pooling allocator for fast instantiation.

Smaller points in the same code:

- `wasm_component_model_async(true)` is set twice (lines 54 and 56).
- The input JSON is parsed and then re-serialized for nothing (lines 64-66). Validating it is enough.
- `rustls::crypto::aws_lc_rs::default_provider().install_default().expect(...)` (line 44) **panics when a second `WasmtimeRuntime` is created in the same process**, because `install_default` returns `Err` when a provider is already installed. This affects tests and library users. Ignore the `Err`, or use `std::sync::Once`.
- `Executor::run` does `tokio::spawn(...).await`. That adds no isolation for CPU-bound guest code, because the code still runs on a runtime worker (see 2.4).
- The per-process module cache is never evicted.

### 4.2 Error details are lost
- `JobResult::Failure(err.to_string())` (`process/executor.rs:121`) drops the `anyhow` context chain. Use `format!("{err:#}")`.
- `Storage` turns `QueryJobs` errors into an empty `Vec` (`storage/src/lib.rs:87`). Callers cannot tell "no jobs" from "database broken".
- `ClaimJob` failure (including the normal "row not found" when the claim loses) is logged at `error` level. That is noise.
- `eprintln!` is used alongside `tracing` in IPC and component loops. Use `tracing` everywhere.

### 4.3 Job queries do not scale
`src/cli/src/server/api/v0/jobs/retrieve.rs:23-53`, `src/storage/src/backend/sqlite.rs:148-187`

- `GET /jobs?id=X` fetches **every job** from Storage and filters in the hub. Add an `id` field to `JobQuery` (primary-key lookup).
- There is no pagination or limit on the API.
- `ORDER BY` is added only when `limit` is set, so result order is otherwise undefined.
- The CLI `job list --status` filters client-side even though the API supports `?status=`. `--all` silently overrides `--status`.
- `GET /api/v0/jobs/{id}` (used in `test/smoke.sh:42`) does not exist.

### 4.4 Timestamps are stored at second precision, with conversion hazards
`src/storage/src/backend/sqlite.rs:257-268`

- `as_secs()` drops sub-second precision. Combined with the scheduler's `SLEEP_INTERVAL` minimum of 1 s and `CHECK_INTERVAL` of 10 s, jobs can start up to about 10 s late. For example, a job created just after a reload whose time falls between reloads is only seen after the next 10 s idle sleep, because nothing wakes the clock loop when a job is inserted.
- `into_system_time(timestamp as u64)` turns negative values into huge durations.
- `claimed_at` uses the SQLite clock (`strftime('%s','now')`) while everything else uses the process clock.
- Consider storing milliseconds and waking the scheduler (with a `Notify`) on `StoreJob`.

### 4.5 Scheduler internals
`src/scheduler/src/lib.rs`

- Duplicate check is `queue.iter().any(|j| j.id == job.id)`, which is O(n²) per load. Keep a `HashSet<Ulid>` next to the heap.
- `current_executor: tokio::Mutex<usize>` should be an `AtomicUsize`.
- `message_consumer` replies with `from: ProcessType::Storage` (line 240), a copy-paste bug. It should be `Scheduler`.
- `run` uses `tokio::select!` with `Err(err) = ...` patterns. If `message_consumer` returns `Ok` (the channel closed), that branch is disabled. In `Storage::run` and `ExecutorProcess::run` it is the **only** branch, so `select!` panics with "all branches are disabled and there is no else branch".
- The lookahead query (`LIMIT 10`, 5 min window) plus heap-only dedup means that when more than 10 jobs are due, the heap is refilled only as fast as the 10 s/30 s cycles allow. Throughput is capped at about 10 jobs per 10 s, unless the loop reloads right after draining the heap.
- `Job` implements `Ord` on `scheduled_at` only, while `PartialEq`/`Eq` are derived on all fields. That breaks the `Ord`/`Eq` consistency contract. Wrap it in a `Reverse((scheduled_at, id))` key instead.

### 4.6 Task repository (`src/repository/src/backend/local.rs`)
- `println!("{:?}", file_path)` debug leftover in `create` (line 51).
- `list()` **aborts the whole listing** on one badly named file, even though the error text says "Skipping". It should `continue` and log a warning.
- `split_once(".wasm")` also matches `foo.wasm.bak` and `x.wasmx`. Use `strip_suffix(".wasm")`.
- The namespace is not sanitized: `..` passes `TaskIdentifier::from_str`, so `POST /api/v0/tasks/../x/1.0.0` writes into `~/.wasmq/`. The damage is limited because the file name always ends in `@<ver>.wasm`, but namespace and name should still match `[a-z0-9_-]+`.
- Writes are not atomic: a failed `write_all` leaves a truncated file, and `create_new` then blocks any retry. Write to a temp file and `rename`.
- Upload does not check that the bytes are a valid component with a matching `handler` export (`Component::from_binary` plus a type check). Errors show up only at job run time.
- Axum's default 2 MB body limit applies to multipart uploads. Release builds of real tasks can go over it, and the error is not user-friendly.
- The hub and every executor each open their own `TaskRepository::local()`. This works only while all processes share one host and one home directory.

### 4.7 API correctness
- An invalid `TaskIdentifier` in `tasks/create.rs:21` returns **502 Bad Gateway**. It should be 400.
- `POST /jobs` does not check that the task exists, so a typo only fails at run time (and then goes through the retry loop).
- `Job::new` checks only spaces and empty names. There is no length limit, and no size limit on `args` besides the axum JSON limit.
- `ApiError::into_response` converts `StatusCode` to `u16` and back for nothing.
- `JobStatus` uses PascalCase in serde (`"Scheduled"`), lowercase in the DB and CLI (`"scheduled"`), and query-string deserialization accepts only PascalCase. Add `#[serde(rename_all = "lowercase")]`.

### 4.8 CLI UX
- `http://localhost:6283` is hardcoded in five commands. Add `--url` / `WASMQ_URL`.
- Every subcommand prints errors and **exits 0** (`job new`, `job list`, `job view`, `task list`, `task load`). Scripts and CI cannot detect failures. Return the error instead.
- `job view` error message says "Failed to list jobs".
- `task new` builds the target dir from `self.name.name`, and namespace and version are ignored. The generated `Cargo.toml` is not templated with the task name.

### 4.9 `wasmq_handler` proc-macro hygiene
`src/task/src/lib.rs:84-123`

- It injects `use anyhow::Result; use wit_bindgen;` at the call site. That clashes with user imports and **requires the user crate to depend on `anyhow`, `serde_json` and `wit_bindgen` directly**. Re-export them from a runtime crate (`wasmq_task::__private::*`) and use absolute paths.
- The generated `struct Mate` and `mod bindings` can collide with user items.
- The macro assumes an `anyhow::Result<T>`. `Result<T, MyError>` does not compile, because `Result<#output_type>` is rebuilt with one parameter.
- The macro does not check that the handler is `async`, or that it takes exactly one parameter.
- The WIT world is inlined in the macro and duplicated implicitly in the host (`HANDLER_FUNC_FQN = "handler"`). Move it to a shared `.wit` file and generate host bindings with `wasmtime::component::bindgen!` for type-checked calls.

### 4.10 Example tasks
- `task/fibonacci` names its handler `sum`.
- `task/http` calls `wstd::runtime::block_on` inside an `async` handler that already runs on the component-async runtime, and uses `.unwrap()`/`.expect()`. Guest errors become traps instead of `Err`.

---

## 5. Low / hygiene

### 5.1 Dead and uncompiled code
- `src/cli/src/cli/cmd/hub.rs` and `hub/start.rs` are not declared in `cmd.rs`, so they are never compiled. They would not compile anyway: `Hub::new(self.config.clone())` passes a `PathBuf`, and `run_server(hub.config(), ...)` passes `&Config` instead of `Arc<Config>`. Delete them or wire them in, since the Dockerfile expects a `start` command.
- `RunCmd` is an empty `enum` that is used only as a namespace for `run()`.
- Unused protocol variants: `JobAccepted`, `JobFailed` (the scheduler ignores replies), `Shutdown`/`ShutdownAck` (nothing sends them), `UpdateJobStatus`/`JobStarted` (not handled).
- Unused statuses: `pending`, `running`, `cancelled`. There is no cancel endpoint.
- `IpcServer::shutdown(&mut self)` cannot be called through the `Arc<IpcServer>` every component holds. `IpcServer::receiver` is `async` for no reason.
- `ExecutorId` is defined twice (`ipc/protocol.rs:6`, `wasmq/proto/job.rs:12`). `RetrieveJobsQuery` is defined in `client/api/v0/tasks.rs` and never used.
- The `clap` dependency in `wasmq-executor` is unused.

### 5.2 The rename to `wasmq` is incomplete
- Library names are still `mate_ipc`, `mate_executor`, `mate_storage`, `mate_scheduler`, `mate_config` and `mate_repository` (`[lib] name` in each `Cargo.toml`).
- Leftovers also remain in the socket dir `/tmp/mate_sys`, `MATE_SERVER_DEFAULT_PORT`, `let mate_exe`, `struct Mate` in the macro, the "Mate Client" doc, "Mate's Inter Process Communication Protocol", and the "Runs an instance of Mate's Hub" help text.

### 5.3 IPC design
- The transport opens a **new Unix-socket connection per message** (connect, write, shutdown). Use persistent connections or a framed stream (for example `tokio_util::codec::LengthDelimitedCodec`).
- Messages go through two unbounded channels: the transport's internal channel, then `IpcServer`'s. That doubles buffering, and neither channel applies backpressure.
- There is no read timeout in `handle_connection`, so a client that connects and never writes keeps a task alive forever.
- JSON on the wire is fine for debugging. A binary format (`postcard`/`bincode`) would be smaller and faster for `Job` payloads.
- `transport.rs:7` and `run.rs:19` TODO/FIXMEs note that components touch `Transport` directly.

### 5.4 Storage schema
- There is no index that covers `status IN ('scheduled','failed') AND attempts < max_attempts`. The existing `(status, scheduled_at)` index works for now.
- There is no retention or cleanup of completed jobs.
- `Storage::new` uses `home.to_str().unwrap()`, which panics on a non-UTF-8 home path. Use `SqliteConnectOptions::filename(&Path)` directly.
- `.env.example` refers to `DATABASE_URL`, but runtime never reads it. It is only needed by `sqlx prepare`.

### 5.5 Tests and CI
- Tests exist only for the SQLite claim, the Unix socket transport, config parsing and `TaskIdentifier`. **Nothing covers the scheduler, executor, API, retry logic, or an end-to-end run.** The bugs in 2.1, 2.2 and 3.1 would have been caught by one integration test that schedules a failing job.
- `test/smoke.sh` is out of date: it posts a `payload` field (the API expects `task`, `args` and `scheduled_at`), queries `status=Pending`, and calls `/jobs/{id}`, which does not exist. CI does not run it.
- The CI "Tasks Integrity" job depends on `httpbin.org`, which can make CI flaky.
- CI does not run `cargo sqlx prepare --check`, so `.sqlx` metadata can drift.
- The config test asserts `backend = "Memory"`, which does not reflect the actual SQLite backend.

### 5.6 Docs and README
- README links to `LICENSE.md` (the files are `LICENSE-MIT.md`/`LICENSE-APACHE.md`), has a typo in `githeub.com`, and says "usinc".
- The quick start tells users to `docker run` the image, which fails (2.5).
- There are no docs on the Wasm task interface (WIT world, input/output JSON contract), security model, or configuration file.

---

## 6. Suggested priorities

1. **Correctness of job execution:** make `ClaimJob` a request and gate `ExecuteJob` on success (2.1, 2.2). Handle `UpdateJobStatus`/`JobStarted` in Storage. Add a lease/reaper for `claimed` jobs, a `dead` terminal state and retry backoff (3.1, 3.9).
2. **Safety:** bind to the configured address, add API authentication, add Wasmtime epoch/fuel plus memory limits plus a per-job timeout, and restrict HTTP egress (2.3, 2.4). Use a private runtime dir for sockets (3.6).
3. **Make it run:** fix the Dockerfile entrypoint (2.5), the startup readiness race (3.4), the hardcoded executor count (3.2), and load config from a file (3.3).
4. **Performance:** share `Engine`/`Linker`, cache `InstancePre`, precompile on upload (4.1). Enforce `max_concurrent_jobs`. Handle Storage messages concurrently (3.10). Paginate queries (4.3).
5. **Hygiene:** finish the rename, delete dead code, return non-zero exit codes from the CLI, and add an end-to-end integration test.
