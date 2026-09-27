# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **A process can join its caller's trace.** A `ProcessCreationRequested` carrying a W3C context — `traceparent` (and optionally `tracestate`, `baggage`) as Kafka record headers, or in the new optional `traceContext` field — starts a process whose trace is the caller's: the engine keeps the context (`process_trace_context`, migration V35) and anchors the process to the caller's span instead of the id-derived anchor, so its step-overs, dispatches, recorded process and step spans all sit under the caller. Every record the engine publishes for the process — tasks, outbox-relayed events, `PUBLISH_EVENT`s, child creations — carries `traceparent`/`tracestate`/`baggage` headers (raw bytes) continuing that trace. `worker-kafka` runs each handler in a `CONSUMER` span under the task's headers and exposes them as `TaskContext.traceContext()`; `TaskDispatcher.dispatch(task, traceContext)` and the `TaskTracing` hook do the same for other transports. New span `eventconductor.create-process`; `step-over` is tagged with the workflow id, business key, resulting process status and the steps it moved, `dispatch-step` with the step and workflow ids. Only while tracing is on (no `Tracer`: nothing stored, looked up or added); `workflow.tracing.join-caller-trace=false` opts out. No change to any message for a producer that sends no context; synchronous invocations keep their linked-traces design. See *Observability → Joining the caller's trace*.

### Changed
- Mateu 3.0-alpha.362 → 3.0-alpha.368 (the engine UI and every app). Brings listing row details (`@Details`), UUID cells shortened to their last block, and Vaadin/Redwood layout fixes.
- The workflow definitions listing and the task contracts listing show a long description on one line, cut at a word with "…"; the whole description opens under the row when it is clicked.

### Fixed
- LOCK/UNLOCK steps and `processLock` failed on PostgreSQL at the first acquire: the lock row id joined the lock name and key with a NUL, which PostgreSQL refuses in any text value. The id is now `<name length>:<name>:<key>` (e.g. `7:booking:MRU01/ABC123`) — printable and still unambiguous whatever the name or key contain. Migration `V34` widens `process_lock.id` to 520; a lock an H2 database held under the old id is rewritten before the first lock operation, so it keeps excluding across the upgrade. Now covered on PostgreSQL by the distributed suite (DIST-30).

## [2.22.1] - 2026-09-25

### Security
- freemarker 2.3.34 → 2.3.35 in the orchestrator, forms, rules, worker and dev apps (CVE-2026-84939, critical: path traversal via a malformed locale identifier). It arrives through Mateu's annotation processor; the 2.22.0 images were stopped by the release's Trivy gate and never published — 2.22.1 is the same release with the fix.

## [2.22.0] - 2026-09-25

### Added
- **Moments** — "3 days before check-in, at 09:00 hotel time", from the process's data: `{date, offset, at, zone}` (or a date template). Offsets in days and up are calendar arithmetic in the zone, DST-safe. `workflow.time.zone` sets the zone for dates that carry none (default: the JVM's, as before).
- **TIMER `until`** — wait for a moment; `ifPast: fire | timeout` decides what a moment already past does (`timeout` follows `onTimeoutStepId` or fails, without retries).
- **`deadline` on any waiting step** (ACTION, USER_TASK, RULE, WAIT_FOR_MESSAGE, PROCESS, HTTP_CALL) — a moment by which it must finish; a timeout that retries do not extend; with `timeout`, the earlier wins.
- `until` timers and `deadline`s **follow the process's variables**: a moved check-in moves them (logged); a cleared date keeps the armed moment.
- The workflow graph shows a TIMER's moment, and a step's deadline.

### Changed
- Mateu 3.0-alpha.340 → 3.0-alpha.362 (the engine UI and every app). The apps' Keycloak bootstrap now refreshes the token and retries on a 401, and goes to the login when the session is gone.

### Fixed
- The process page showed `${state.invocation.message}` in a badge for every process not invoked synchronously (Mateu rendered a null `Status` field's template); fixed in Mateu 362.
- The process graph drew every step the run had not reached yet with the marching "waiting" border, as if the process were waiting on all of them; only steps that are really waiting (started, or queued for a retry) march now, and the rest are dimmed as not visited.

## [2.21.0] - 2026-09-24

### Added
- **`HTTP_CALL` step** — call an HTTP endpoint and map the response (`output`: JEXL over status, headers, body) into variables, without writing a worker. Named connections (`workflow.http.connections`) or absolute URLs; auth from the connection, a named profile (`basic`, `bearer`, `api-key`, `oauth2-client-credentials`) or inline with `${secret:NAME}` credentials only; `allowed-hosts` and an always-on block of internal addresses for absolute URLs; `retryOn` (default `[5xx, io]` — a 4xx is final); `Idempotency-Key` = the step execution id. Executed by the built-in task `http-call@1`.
- **`worker-http`** module (the `http-call@1` task, auto-configured) and **`http-worker-standalone-app`** (image) serving topic `http-calls` in kafka mode. Metric `eventconductor.http.calls`.
- **`PUBLISH_EVENT` step** — publish a domain event from the process's state through the outbox (published iff the step completed). Destinations are logical names mapped by `workflow.events.destinations`; CloudEvents 1.0 on Kafka (binary by default, structured or plain); embedded mode hands it to an `ExternalEventPublisher` bean (default: a Spring `ExternalEventPublished` application event).
- **Payload templates** — JSON/YAML with sandboxed `${JEXL}` leaves (typed single-expression leaves) or text; used by `REPLY` (`replyTemplate`), `PUBLISH_EVENT` and `HTTP_CALL`; validated at build time and on import.
- **Non-retryable failures** — a worker can mark a failure final (`TaskFailure(code, reason, false)`, or the reason prefix `[non-retryable] `); the engine skips the step's remaining retries.
- The workflow graph shows `HTTP_CALL` and `PUBLISH_EVENT` nodes.

### Fixed
- A step that times out and routes to its `onTimeoutStepId` no longer rolls the saga back: the process used to both follow the on-timeout route to its end **and** compensate the completed steps, ending `COMPENSATED` with its `END` cancelled.

## [2.20.0] - 2026-09-24

### Added
- **Synchronous invocation of durable processes — a caller starts a process over HTTP, waits up to a
  deadline and receives the reply the process itself emits.** New step type **`REPLY`** (from
  `replyVariables` or a JEXL `replyExpression`; engine-internal, recorded on the process in the
  transition that reaches it; before `END` it is the result, earlier the caller is answered while the
  process continues) and definition-level **`syncInvocation`** `{enabled, onFailure, onLockBusy,
  defaultDeadlineMs}`. `POST /workflow/api/definitions/{id}/invocations` (`Idempotency-Key` required,
  `Prefer: wait=N`) answers 200 with the reply, 202 + `Location` when the deadline passes first (the
  process carries on; `GET /workflow/api/invocations/{id}` serves the reply later), or 502 with a
  failure outcome — `FAILED` with the rollback `IN_PROGRESS` (default `REPLY_IMMEDIATELY`) or
  `COMPENSATED` / `COMPENSATION_FAILED` once it ends (`REPLY_AFTER_COMPENSATION`); `CANCELLED`;
  `COMPLETED_WITHOUT_REPLY` (200). A retry with the same key lands on the same process. `Accept:
  text/event-stream` streams `status`/`step`/`log` events and ends with `reply` (resumable with
  `Last-Event-ID`). `onLockBusy: FAIL` refuses 409 without creating an instance when the
  `processLock` is held. Validation (engine and Maven plugin, same code in the new
  `definition-analysis` module) guarantees at most one `REPLY` per run. **Fast path:** the receiving
  node drives the new process inline — every transition still written to the outbox, its rows claimed
  by that node so no relay races it; a node that dies mid-drive has its claims handed back to the
  relay. **Cross-node wake-up** via PostgreSQL `LISTEN/NOTIFY` issued in the reply's transaction, with
  a poll fallback on any database. Sharded: invocations are placed on the receiving shard. Metrics
  `eventconductor.sync.*`, span links between the caller's and the process's traces, a Synchronous
  badge and Reply tab in the process view, Grafana panels. Migrations V31–V33. Measured (one machine,
  PostgreSQL + Kafka in containers): two nodes with Kafka workers, p99 request → reply 251 ms → 22 ms.
  See `guides/synchronous-invocation` and `SYNC-INVOCATION-PLAN.md`.

### Fixed
- **A default step timeout no longer strips the process lock and the flow-authorization
  requirements from a process's definition snapshot.** `StepTimeoutDefaults` rebuilt the definition
  through a narrow constructor whenever `workflow.default-step-timeout-ms` was set, silently dropping
  `processLock`, `requiredScopes` and `requiredRoles`.

## [2.19.0] - 2026-09-24

### Added
- **The forms engine announces its human tasks.** Every task opened, completed or cancelled is
  published as a `HumanTaskChanged` (in `shared`: task, form and its name, process, step, status, the
  roles its form requires, who holds it) on the `humanTasks` binding — topic `human-tasks` in the
  standalone app, `FORMS_HUMAN_TASKS_TOPIC` to change it. It is for whoever keeps a person's work in
  one place — an inbox or notification centre that shows a task next to the other things waiting
  for that person — without polling the forms engine. Best effort, published after the task is
  saved: a broker that refuses it makes an inbox lag, never a task fail to open or close.

## [2.18.0] - 2026-09-20

### Added
- **Sharded message routing (phase 1): messages start to reach only the shards that can correlate
  them, instead of every shard.** Behind `workflow.sharding.message-routing.enabled` (default false;
  with it off, or with sharding off, behaviour is exactly as before). A single `MessageRouter` — used
  by both the external-message path (`MessageDispatcher`) and the `SEND_MESSAGE` relay — routes each
  message: a `MessageClassifier` derives, from the `WAIT_FOR_MESSAGE` steps of every imported
  definition, whether a message name correlates by business key or by expression; a business-key
  message is sent to the shard the placement store owns that key on (`ProcessPlacementRepository.find`,
  a new read-only lookup), and everything else — an expression key, an unplaced key (e.g. a child
  process, whose key is `parent:<id>` and never placed), or no placement store — falls through to the
  broadcast that exists today. Routing only filters; the receiving shard still runs the normal
  correlation, so a wrong guess costs a query or a broadcast, never a lost message. Metrics count
  messages by route and correlation queries per shard. The shared subscription table
  (`message_subscription`) and the router's layer-2 lookup for expression/unplaced keys are in place
  but **not yet populated** — the projection that fills them from waiting steps, and the residual
  per-shard Bloom filter, come next; until then those keys fall through to broadcast, unchanged.
- **Sharded message routing (phase 2): the subscription table is now populated, so layer-2 routing is
  live.** A `WAIT_FOR_MESSAGE` step emits a `MessageSubscriptionChanged` when it starts to wait (with
  the resolved correlation key) and again when it reaches any terminal status; an embedded projection
  (`MessageSubscriptionProjectionHandler`) stamps its own shard id onto the row and writes it to the
  shared routing database (the same datasource as the placement store, wired only when
  `workflow.sharding.placement.datasource.url` is set). The router's layer-2 lookup now resolves an
  expression key — or a business key the placement store does not own (e.g. a child process) — to the
  exact shard(s) whose step is waiting, and sends the message only there; all waiters are projected
  (business key and expression alike), so a message with several waiters across shards reaches each.
  Keyed by step execution, so a redelivered start upserts and a stop deletes idempotently; a stale row
  never loses a message, since the receiving shard still runs the normal correlation and an
  unresolved key still falls through to broadcast. Where no shared routing database is configured
  (a single-database engine) the projection is a no-op.
- **Sharded message routing (phase 3): a per-shard filter drops broadcast messages a shard cannot
  possibly correlate, without a query.** Behind `workflow.sharding.message-routing.filter.enabled`
  (default false). A residual message still reaches every shard by broadcast; each now checks an
  in-memory Bloom filter over the `(messageName, correlationKey)` pairs it has a step waiting for, and
  where the filter rules the pair out the shard drops the message instead of running the correlation
  query — cutting the S×M broadcast cost to roughly the shards that can actually match. The filter is
  shared-nothing (no placement store, no subscription table), so it helps even with layers 1–2 off. It
  is fed as steps start waiting and rebuilt wholesale from the authoritative waiting set at startup and
  every 30s (`WaitingMessageFilterRebuilder`, on a daemon thread that does not hold up boot or need the
  database to be up); the rebuild is what sheds pairs of steps that have stopped waiting. No false
  negatives by construction — a pair a step is waiting for is always present, and while disabled or not
  yet populated the filter says "maybe" so every message runs the query exactly as before; the only
  drop window (a step that just started waiting, not yet added) is the case the engine already handles
  by at-least-once redelivery. A `queries.skipped` counter meters the queries saved.
- **Sharded message routing (phase 4): the scaling is measured across all three layers, and the
  benchmark gained a message workload.** `MessageRoutingScalingBenchmarkTest` now drives real messages
  through the real router, placement store, subscription table and Bloom filter (on H2) and records
  the correlation queries each shard runs: for M = 100,000 messages the busiest shard is **M / S** —
  ~50k at 2 shards, ~25k at 4, ~12.5k at 8 — for business keys (layer 1), expression keys (layer 2),
  and pure broadcast filtered on the receiving side (layer 3), against a flat **M per shard** without
  routing. The filter recovers M/S with no shared store at all, at a false-positive cost of 24 extra
  queries in 100,000 (0.02%) at this load. The `workflow-benchmark` harness gained a
  `bench.workload=message` mode (an action → `WAIT_FOR_MESSAGE` → action definition, a driver that
  resumes each process by its business key over the shared `messages` topic) for the wall-clock
  transitions/s run on a multi-node sharded cluster. Results are in `guides/performance.md`.
- **Sharded message routing: a chaos test proves the message path loses nothing across a broker
  outage.** `Dist21` (real PostgreSQL + Kafka) drives 20 processes to a `WAIT_FOR_MESSAGE`, stops the
  broker, publishes each resume into the dead broker, and confirms nothing advances while it is down;
  when the broker returns all 20 correlate their resume and complete, the outbox drains to zero, and
  nothing is dead-lettered — with the per-shard filter on the correlation path, the end-to-end evidence
  that it drops no message it should have kept. Results in `guides/reliability.md`.
- **Sharded message routing: a chaos test proves the message path loses nothing across a broker outage
  on genuinely separate shards.** `Dist22` runs two full engines, each with its own database and its own
  `upstream`/`outbox` topics, sharing the one `messages` topic every shard consumes. Twelve processes are
  split across the shards and parked on a `WAIT_FOR_MESSAGE`; the broker is stopped; each resume is
  broadcast onto the shared topic; and when the broker returns both shards receive every resume, each
  completes exactly its own six, both outboxes drain to zero, and nothing is dead-lettered — the
  cross-shard fan-out, the per-shard filter ruling out the keys a shard does not hold, and recovery of
  the message path, shown to survive an outage. `DistInfra` gained per-shard database/topic helpers for
  it. Results in `guides/reliability.md`.

## [2.17.1] - 2026-09-20

<!-- 2.17.0 was tagged but never published (a transient Central 429, then a release-config fix);
     2.17.1 is the first published cut of this work. -->

### Added
- **UI: a Tasks view and one-click worker projects.** The engine UI gained a read-only **Tasks**
  view (in the full and admin menus) listing every task contract version, its group and topic, how
  many workflow definitions use it, and links to download a ready-to-build worker project. A plain
  HTTP endpoint (`/eventconductor/tasks/{group}/module.zip` and `…/service.zip`) serves the zip —
  the generated project skeleton (from `worker-codegen`, the same generator the Maven goal and IDE
  use) bundled with the group's `.ectask` contracts, so the download builds on its own — and the
  view shows the Maven dependency snippet for adding the module to an existing service.
- **Task worker code generation: a `.ectask` contract becomes the Java you implement against, and a
  whole project is just a parent and a few properties.** A new pure module `worker-codegen` (the
  single source the Maven goal, the IDE and the UI share — contracts and a little configuration in,
  files out, deterministically) turns each contract version into an `Input` and `Output` record with
  the attributes mapped to Java types (string→String, integer→Long, number→BigDecimal,
  boolean→Boolean, date→LocalDate, datetime→LocalDateTime, object→JsonNode, array→List; an attribute
  whose name is not a Java identifier keeps its wire name via `@JsonProperty`), a `<Id>V<n>Task`
  interface that specialises worker-api's `TaskHandler` with a nested `TaskFailure` subclass per
  declared error, and — one per `group` — an `@AutoConfiguration` that binds each implemented handler
  into a `TaskRegistration` bean, gated by `eventconductor.tasks.<id>.enabled` (a task declared but
  not implemented fails startup naming the interface, unless disabled). The version is part of every
  generated type name so a contract's versions coexist, and the generated `AutoConfiguration.imports`
  is written to a generated resources root so the registrations are found without component-scanning.
  For a standalone service it also emits the `@SpringBootApplication` class. The `workflow-maven-plugin`
  goal `generate-worker-sources` (bound to `generate-sources`) resolves the contracts from a versioned
  Maven artifact (`definitions`, the reproducible default), a git checkout pinned to a tag or commit
  (`repository`/`ref`; a branch is refused unless `allowBranch`), or the project's own
  `src/main/resources/tasks`, selects them by `group` or `tasks`, and writes to `generated-sources`.
  Two parents carry all the build logic (decisions 14/15): `task-module-parent` for a library added
  to an existing service, `task-service-parent` for a runnable Kafka service (Actuator, image build,
  `KAFKA_BROKERS`/group defaults) — a generated project is then only a `pom` with parent, coordinates
  and `ec.*` properties, plus a README generated from the contract. An in-repo example
  (`examples/greetings-tasks`) is exactly that pom and one handler, and it builds end to end. A
  developer now writes only the handler.
- **Docs: the task contract format and the worker protocol are documented for other-language
  generators (decision 17).** A new reference page (`reference/task-contracts.md`) specifies the
  `.ectask` fields, the attribute→JSON→Java type mapping, and the three-event wire protocol
  (`TaskExecutionRequested`, `TaskCancellationRequested`, `TaskStatusChanged`) with its at-least-once
  reply contract; `reference/maven-plugin.md` documents the `generate-worker-sources` goal; and
  `guides/workers.md` now leads with the generated flow, keeping `WorkerReply`/`EmbeddedTaskExecutor`
  as the documented low-level protocol beneath it.
- **IDE actions scaffold a worker from a task contract.** Both IDE plugins gained, on a `.ectask`,
  three actions: **Create task module** and **Create task service** (generate a `<group>-tasks` /
  `<group>-service` project — a `pom` inheriting the right parent, the contract, and for a service
  the `application.yaml`/`.gitignore`), and **Add task dependency** (insert a dependency on the
  `<group>-tasks` module into the nearest `pom.xml`). The IntelliJ plugin reuses `worker-codegen`
  directly (decision 16: one source of templates), resolving it from the local Maven build; the
  VS Code plugin mirrors the same output in TypeScript.
- **Worker runtime: implement a `TaskHandler` and a library speaks the engine's protocol for you,
  the same handler in Kafka and embedded mode.** Three modules split the concern: `worker-api` is
  the broker-free core a developer (and the generator) codes against — `TaskHandler<I,O>`,
  `TaskContext`, `TaskFailure` (a declared business error), `TaskRegistration`, and a `TaskDispatcher`
  that resolves the handler by the dispatched `taskId` (falling back to `stepId`), binds the process
  `Variable`s to the handler's typed input and its output back to variables with Jackson, checks
  cancellation before starting and before replying, and maps the outcome (return → completed,
  `TaskFailure` → failed with its code, anything else → failed with the message, so the engine
  retries per the step's `retries`). `worker-kafka` binds the `consumeWorkerEvent` function to the
  task topic and answers over the `upstream` topic via `WorkerReply` — its `EnvironmentPostProcessor`
  adds `consumeWorkerEvent` to `spring.cloud.function.definition` without dropping an application's
  own functions; a broker that refuses the reply after its retries leaves the offset uncommitted so
  the task is redelivered. `worker-embedded` supplies the engine's `EmbeddedTaskExecutor` and calls
  `UpdateStepExecutionUseCase` back directly, in `afterCommit` when a transaction is open. A reply is
  transaction-aware and worker-api never drags Spring Cloud Stream onto a handler's classpath. This
  is the runtime the task contracts are for; the code generator that turns a `.ectask` into the
  typed interfaces and registration comes next.
- **Task contracts: an ACTION step can declare the task it runs, so worker types can be generated
  from it.** A contract lives in a `.ectask` file under `definitions/tasks/` — `id`, `version`,
  `group`, `topic`, `description`, `input`, `output`, `errors` (see
  `urn:eventconductor:task-contract-schema:1`) — and a step references it with `task: <id>` or
  `task: <id>@<version>`. Contracts are **versioned**: several versions of one id coexist, so an
  in-flight process keeps the version it was pinned to while new definitions pick up the latest. On
  import the engine pins a bare `task: <id>` to `<id>@<latest>` (an explicit version is left as
  written) and defaults the step's topic from the contract, so a definition never changes contract
  without an edit; the pinned `<id>@<version>` is dispatched to the worker as the taskId.
  Contracts are imported from the classpath, from directories (`tasks.directory-import`) and from
  git (`tasks.git-import`) alongside workflows, forms and rules — ahead of workflows so references
  resolve; there is no pruning, because a contract a process still depends on must not disappear.
  The `workflow-maven-plugin` gained a `validateTasks` goal (schema, one group per id, unique
  `id@version`, and every referenced task exists), and the IDE plugins recognise `.ectask`. This is
  the contract-and-engine groundwork; the worker runtime and code generator come next.
- **Serialization locks: a definition can now make processes that touch the same entity run one at a
  time.** Two shapes, both keyed by a JEXL expression over the process variables (e.g. `bookingId`):
  - **Step-level critical section** — `LOCK` / `UNLOCK` steps carrying a `lockName` + `lockKey`.
    Only the stretch between them is serialized, so the lock is not held across long worker
    round-trips outside the section.
  - **Process-level** — a definition-level `processLock: { name, key }`. The whole instance is
    serialized against other instances resolving to the same key; a second instance makes no
    progress until the first finishes.

  Whoever cannot take the lock is queued and admitted in **arrival order (FIFO)**; a parked step sits
  in the new `WAITING_ON_LOCK` status (the process stays running, neither completing nor erroring
  around it). Locks are created on the fly, coordinate across pods through the database (a
  `SELECT … FOR UPDATE` on the held-lock row, no `ON CONFLICT`/`SKIP LOCKED`, portable across H2 /
  PostgreSQL / MariaDB), and are released by the `UNLOCK` step, on the process reaching a terminal
  state, or — the crash backstop — by a lease reaper (`workflow.lock.lease-ms`, default 15 min;
  scan `workflow.lock.lease-scan-interval-ms`, default 60 s). New tables `process_lock` /
  `process_lock_waiter` (`V29`). The graph draws `LOCK`/`UNLOCK` with a padlock glyph and a parked
  step with a distinct slate "waiting on a lock" border; the `.ec` schema and the IDE plugins gained
  the new step types and the `processLock` attribute. Wait order under identical `enqueued_at` ticks
  is not yet a strict FIFO tie-break (a monotonic sequence column is the hardening).

### Changed
- **`sample-worker` migrated to the generated runtime.** The canonical copyable worker no longer
  hand-wires a Spring Cloud Stream consumer: it declares `greet.ectask`, depends on `worker-kafka`,
  and its whole implementation is a `@Component` handler of the generated `GreetV1Task` interface.
  The hand-written consumer and its cancellation test are gone — that behaviour now lives in, and is
  tested in, `worker-kafka`.

<!--
  Next steps outside this repo (task-contract workers), noted per the plan:
  - ec-definitions: publish the versioned definitions zip so services resolve contracts by artifact.
  - ec-demo1: migrate the booking worker to a `booking-tasks` module, implemented in the `booking`
    service alongside its CRUD and MCP tools.
-->

## [2.16.5] - 2026-09-19

### Fixed
- **The Status filter now SHOWS as applied after a reload.** Opening the process listing by URL
  (`/workflow/processes?status=PAUSED,COMPLETED`) filtered correctly but the filter bar showed no
  chip — the filter looked unset. Fixed upstream in Mateu `3.0-alpha.340` (#502): the server's
  search-response re-render pushed a fresh state without the filters, so the bar lost them (the
  server kept filtering, reading them off the query); the URL's filters — the source of truth — are
  now re-applied on every re-render.

### Internal
- **Mateu 3.0-alpha.337 → 3.0-alpha.340.** Carries the filter-chip fix above and the changes since.

## [2.16.4] - 2026-09-13

### Fixed
- **The multi-select Status filter (2.16.3) actually works now.** 2.16.3 made the process listing's
  Status filter multi-valued, but applying it threw `ClassCastException: LinkedHashSet cannot be cast
  to List` and every search on the listing failed — a Mateu bug: its reflection coercer assumed a
  collection filter value was always a `List` and produced an `ArrayList`, so a `Set<enum>` filter
  field could neither be read nor built. Fixed upstream in Mateu `3.0-alpha.336` (#490) and picked up
  here. `?status=PAUSED,COMPLETED` now filters by both.

### Internal
- **Mateu 3.0-alpha.333 → 3.0-alpha.336.** Carries the coercer fix above, plus the embedded
  micro-frontend/background-trigger and GA-prep changes accumulated since.

## [2.16.3] - 2026-09-13

### Changed
- **The process listing's Status filter is now multi-select.** It was single-valued, so a link like
  `/workflow/processes?status=PAUSED,COMPLETED` silently returned the *unfiltered* list (a
  comma-joined value can't coerce to one enum, so it was dropped). Status is now a
  `Set<ProcessStatus>` end to end — the filter renders as a multi-select, the URL restores several
  values, and the store applies them as SQL `IN` (`ProcessFilters`, `ProcessListingFilter`,
  `ProcessEntityRepository` `p.status in :statuses`, the in-memory `ProcessRepository`, and the
  component-state parse in `SimpleProcessCrudAdapter`). One status still works; none narrows nothing.

## [2.16.2] - 2026-09-13

### Fixed
- **Embedded micro-frontends no longer flicker (reload + dim) on a shell re-render.** Behind a
  federated shell (the demo consoles), opening a screen showed a brief "the page is reloading"
  effect: content that had already painted got re-loaded with the loading veil on. Fixed upstream in
  Mateu (`3.0-alpha.333`, #483): an embedded `<mateu-ux>` was given a fresh `nanoid()` id on every
  render, so each shell re-render re-loaded its route; the id is now derived deterministically from
  what the micro-frontend points at, so a re-render reuses the same element instead of remounting.
  Verified A/B upstream against a live federated shell.

### Internal
- **Mateu 3.0-alpha.331 → 3.0-alpha.333.** Also carries a fix for a removed search/filter chip
  reappearing on a self-refreshing page (`3.0-alpha.332`, #482).

## [2.16.1] - 2026-09-12

### Fixed
- **Removing a filter chip removes it.** In a listing, clicking a filter chip's ✕ cleared the
  filtering from the results but left the chip on the bar, so the listing looked filtered when it was
  not. Fixed upstream in Mateu (`3.0-alpha.331`): the component's "keep edited field" defence used to
  restore a field from the value it held *before* the parent's re-bind, which for a **cleared** field
  is the stale pre-clear value — resurrecting what the user had just removed. It now restores the
  element's own last-committed value, so a clear stays cleared. Covered end-to-end by a new browser
  journey.

### Internal
- **Mateu 3.0-alpha.314 → 3.0-alpha.331.** Carries the fix above and the changes accumulated since.
  One migration: `@Route` folded into `@UI` (parameterized routes are `@UI("/x/:id")`).

## [2.16.0] - 2026-09-12

### Added
- **Cancelled and timed-out steps are badged on the graph.** Completed steps show a green check and
  errored ones a red cross in the bottom-right corner; cancelled steps now get a grey no-entry badge
  and timed-out steps an amber clock, so each terminal state reads at a glance. `TIMEOUT` gets its
  own overlay token (it was folded into `ERROR`), its own amber tint and tooltip chip — a step that
  ran out of time is not the same as one that failed.
- **The stopped/waiting heatmap toggle is always available in the definition view.** It used to
  appear only once some process was live or stopped, because it was gated on the heat overlay having
  data — backwards, since an operator turns it on precisely to look for stalls. With no data it just
  tints nothing.

### Changed
- **Leaner listings.** The overrides listing shows what identifies a rule, and listing columns were
  trimmed to what an operator scans.

### Internal
- Dependency bumps: grpc 1.84, protobuf-java, several Maven plugins, the Temurin base images, and a
  handful of npm/doc packages (js-yaml, Starlight).

## [2.15.0] - 2026-09-09

### Added
- **Filter the process listing by business key.** The listing could be narrowed by definition,
  status and creation dates, and the search box matched a process's name or business key together —
  but there was no way to narrow to the business key alone, the identifier an operator most often
  has in hand (an order number, a booking reference). A new **Business key** filter matches it, and
  it alone, as a case-insensitive substring, wired through both the in-memory store and the
  pushed-down JPA query.
- **Copy a process's state to the clipboard.** A **Copy state** action on the process detail
  serialises the process — identity, status, variables, steps and the message/error log — to JSON
  and copies it, with a "copied" toast, so it can be pasted into a ticket or a chat instead of
  screenshotting a screen a paste cannot carry.

### Fixed
- **The step's hover card on the diagram can be reached and scrolled.** In the monitoring view,
  hovering a step shows a card with its variables; when there were many, the card gave them a
  scrollbar that could never be reached — moving the pointer toward the card left the node and
  closed it, and the card itself was pointer-transparent. The card is now hoverable, with a short
  grace period bridging the 8px gap from the node to the card, so a long variable list can be read.
- **Starting a process from the browser works instead of throwing.** *Processes → New → Create*
  answered with an `UnsupportedOperationException`: a Crud renders its own Create submit for a
  creation form, and that submit fires the reserved `create` action, routed to a `create()` that
  only threw — the form's own action was never the button that submitted. Create now assembles the
  process from the submitted form and navigates to its detail. Covered by a new browser journey.

### Internal
- **De-flaked `TestWorkerJourneyTest`.** Dropped a pointless row click that raced the render.

## [2.14.1] - 2026-09-05

### Changed
- **Mateu bumped to `3.0-alpha.314`.** A dependency-only patch: every module's `mateu.version`
  — which had drifted apart at `.312` / `.309` / `.304` / `.288` — is unified on the latest
  published `3.0-alpha.314`. No engine behaviour changes; consumers that embed the workflow-admin
  or forms UI pick up the newer Mateu transitively.

### Internal
- **`mvnw.cmd` line endings renormalized.** The four `apps/*/mvnw.cmd` were stored with CRLF while
  `.gitattributes` declares `*.cmd text eol=crlf` (LF in-repo, CRLF on checkout), so git reported
  them as perpetually modified. Renormalized to the canonical form — an EOL-only change, no content
  difference, and Windows checkouts still get CRLF.

## [2.14.0] - 2026-09-04

### Added
- **The diagram remembers where you put its nodes.** A workflow's graph was laid out
  automatically every time anyone opened it; you could drag the nodes, and the drag went nowhere.
  Definitions now carry a `layout` — step id to `{x, y}` — written by the graph editor as you drag
  and read by everything that draws the graph, so the arrangement you made in the IDE is the one the
  console shows too. Absent means the automatic layout, so no existing file changes. Arranging is
  all or nothing (a partial layout would let the auto-layout drop the remaining steps on top of the
  ones you placed), it survives rewiring, "Re-layout" undoes it, and moving a node does not mint a
  new definition version. New nullable `layout_json` column (`V28`).
- **Cancel several processes at once.** The process listing already applied *Retry from failure* and
  *Restart from the beginning* to a selection; cancel was still one process per visit to a detail
  page. Asks before it runs, and each process is addressed by its own id so the request reaches the
  pod that owns it.
- **`started` and `finished` on the process detail's step list.** The status said a step was RUNNING
  and not since when, which is the whole question when deciding whether to wait or intervene — and
  on a finished process nothing on the row said which step took the time.

### Changed
- **A step that is working looks like it is working.** RUNNING and PENDING steps now carry a border
  of travelling dashes — running briskly, pending crawling — so a live process reads as live without
  comparing it against the previous refresh. The opacity pulse that faded a running node's whole
  content is gone; it took the title and id with it. `prefers-reduced-motion` stops the motion and
  keeps the distinction.
- **PENDING steps are no longer drawn as faintly as unreached ones.** A PENDING step has a step
  execution — the engine has scheduled it and a worker is about to take it — and at 30% opacity it
  looked the same as a step the run may never arrive at.
- **The operations consoles offer what an operator can act on.** *Steps* leaves
  `WorkflowOperationsMenu` (a step execution is diagnosis of the engine, and the process detail
  already lists the steps of the process in front of you), *Tasks v2* leaves `FormsOperationsMenu`,
  and the Forms detail loses its *Graph editor* button — a form definition is imported from git, so
  editing one on screen produces a form the next import silently replaces. `WorkflowMenu` and
  `FormsMenu` still carry all of it, so an application embedding the engine whole is unaffected and
  every route still resolves.

### Fixed
- **An END is reached by the first branch, not joined from all of them.** Eligibility ANDs a step's
  incoming links unless it is a JOIN declared XOR, so an END drawn with two exclusive routes into it
  — confirm if the payment arrived, cancel if it did not — waited for both, and one of them can
  never complete. Processes sat RUNNING at 60% forever, waiting for something that cannot happen.
  An END is not a join: it now takes the first satisfied link, as XOR does. The rest of the
  behaviour was already there — the untaken branch was already cancelled.

## [2.13.3] - 2026-09-03

### Fixed
- **The process detail drew the definition the engine holds today, not the one the process is
  actually running.** A definition is editable, and an instance that started before an edit keeps
  running the version it started with: the engine never re-reads the definition, because every step
  execution carries its own frozen `stepJson` and the process carries a `workflowDefinitionJson`
  snapshot of its own. That snapshot was written at creation and read by nobody — the diagram was
  built from `WorkflowDefinitionRepository.findById(...)` instead.

  So the screen painted a process that does not exist. Steps added since the instance started
  appeared as nodes that never ran; steps removed since vanished from under the executions that did
  run; a rewired precondition drew edges the instance never followed. Deleting a definition erased
  the diagram of every finished process that had used it. The diagram now comes from the process's
  own snapshot, and falls back to the stored definition only for a process that carries none.

- **Analytics did not show compensated processes.** The aggregation had them all along — it groups
  by status without filtering — but the report only ever painted COMPLETED, ERROR, CANCELLED and
  PENDING+RUNNING. A rolled-back saga was counted in "Instances" and shown in no column and no KPI,
  and so were `COMPENSATION_FAILED` and `PAUSED`. A definition whose instances had all compensated
  showed a row of instances with nothing under them.

  The report now accounts for every instance it counts: a **Compensated** KPI and column,
  a **Rollback failed** column (and a KPI when there is one — it is rare, and it is the outcome an
  operator must not miss), and PAUSED folded into "Active", which is everything not yet terminal.
  A test holds the columns to summing to the instance count.

- The workflow home's "completed processes" KPI counted every terminal status except
  `COMPENSATION_FAILED`, so a partially rolled-back saga counted as neither active nor finished.

### Security
- **`projector-standalone-app` was the one app of five that did not repeat the Tomcat security
  override**, so its image shipped the Boot BOM's `tomcat-embed-core` 11.0.24 and three CRITICAL
  CVEs (CVE-2026-65182, CVE-2026-65905, CVE-2026-68525; all fixed in 11.0.25). The release gate
  requires *every* image to be clean before *any* is pushed, so this one image is why the 2.13.2
  release published its artifacts to Maven Central and pushed no Docker image at all — the four
  clean ones included. The app parents off `spring-boot-starter-parent`, not the EventConductor
  root, so the root pom's overrides never reached it; it now carries the same block the other four
  do, next to the Postgres override that was there for exactly this reason.

### Changed
- **Mateu 3.0-alpha.313.** Redwood-renderer work: federated menu entries route back to the pod that
  serves them, the listing-to-detail path, the Page toolbar painted twice, header action
  distribution, tabs/embedded grids/web components in forms, and the page frame no longer rendered
  as a card. This engine's console runs the Vaadin renderer, so nothing here changes what it shows;
  the bump keeps the engine current.
- `orchestrator-standalone-app` had drifted back to `3.0-alpha.309` while the root pom moved to
  312. Both are on 313 now. That module is the image a deployment runs, so a Mateu fix that stops
  at the root pom reaches everything except the place anyone looks at it — the same drift the 2.13.1
  notes called out.

## [2.13.2] - 2026-09-03

Released without a changelog entry; recorded here after the fact.

### Changed
- The form-executions listing dropped two ids an operator does not read: the step-execution id is
  hidden from the list (`@HiddenInList`, so the editor keeps it) and the process id shows its last
  segment (`…a1b2c3d4`) instead of a full UUID that pushed the useful columns off the row. Display
  only — the stored value is untouched and nothing shortened is written back.
- **Mateu 3.0-alpha.312**, for the Vaadin listing-height fix: the grid no longer collapses on the
  way into a detail.

## [2.13.1] - 2026-09-02

### Changed
- **Mateu 3.0-alpha.309.** One fix, and it is one this engine's screens were showing: a page's
  width stuck to the SESSION rather than to the screen. Entering a console by url came up
  edge-to-edge on everything, and one navigation into a detail view then capped everything after
  it.

  The renderer stamps the resolved width on its host and the stylesheets size the content column
  from it — but it stamped only where a full structure arrived, and two of the three ways content
  reaches a view carry none: the client cache seeds a structure straight onto the fragment before
  the request goes out, and a server that recognises the echoed ETag replies state-only. So a
  reload into a cached route stamped nothing at all, and the first uncached route's width stuck to
  every cached screen visited after it. The structure cache lives in `localStorage`, which is why
  it survived reloads and read as the console remembering a decision.

  Vaadin only. The Redwood renderer has its own implementation and already carried the previous
  width forward only when a fragment had no component of its own.

- **`orchestrator-standalone-app` catches up on five Mateu releases.** Its pom pinned
  `3.0-alpha.304` while the root pom had moved to 307, with nothing saying why — the override sits
  directly above a `json-schema-validator` one that IS explained, which is how it read as drift
  rather than as a decision. It matters more than the number suggests: that module is the image a
  deployment runs, so it is what serves the workflow screens, and a fix released into the root pom
  reached everything except the place anyone would look at it.

## [2.13.0] - 2026-09-01

### Changed
- **Mateu 3.0-alpha.307.** Seven fixes, five of which this engine's own UI is the reason anyone
  noticed:

  - a save flashed the value the field had **before** the edit — measured on a deployment running
    this engine, the pre-edit value on screen at t+135ms and corrected at t+234ms. A form field
    reads its value out of the component state, and the parent re-binds that state on every render
    from a copy that predates the typing; saving exposes it because saving is two round-trips.
  - a crud action pushed a url that had lost its menu prefix: `/booking/bookings/77PXQQ` → Edit →
    `/77PXQQ/edit`, which 404s on reload. All four crud navigations were affected.
  - a filtered listing asked for from a chat agent filtered on a **doubled** query string — the
    view's own route came out as the query, was round-tripped and appended to a url that already
    carried it, and `?status=CANCELLED?status=CANCELLED` parses as one parameter whose value holds
    the second copy. The chip read `Status: CANCELLED?status=CANCELLED` and matched nothing. Only a
    GENERATED navigation shows it, which is why it survived: a person filtering through the filter
    bar never round-trips the query as a route.
  - a collection field the browser sent nothing for landed null rather than empty, throwing several
    layers below the form.
  - the listing collapsed to its natural height on the way into a detail view — 554px of grid down
    to 400px for a frame, on a table about to be replaced anyway.

  Two are cosmetic: the way back out of a crud page is a chevron before its title rather than a
  labelled button, and the chat panel's input bar is no longer clipped when opened from the FAB.

- **`ProcessDetailPage` locates the way back by ACCESSIBLE NAME**, not by visible text. The chevron
  above broke 15 of the 27 UI e2e tests here, all through one locator: `awaitLoaded` waited on
  `getByText("Back to list")` to decide a detail had loaded, so every journey that opens one
  failed. Nothing about the page regressed — the control is still there and still named "Back to
  list"; the label moved to `aria-label` and `title`.

  Keying on the name is the better locator regardless: it survives the control being drawn as an
  icon, and it fails when the NAME is dropped — which is when a screen reader user actually loses
  the button, and when the old locator would have kept passing.

## [2.12.0] - 2026-09-01

### Added
- **Each engine now serves its UI in two halves, on two base URLs.** A deployment with two consoles
  — one for using the product, one for administering the platform — could not split either engine's
  menu, because a `RemoteMenu` mounts the whole menu a pod declares and nothing less. Everything an
  engine offered therefore had to live on whichever console mounted it, so workflow definitions and
  the form editor sat next to a user's task list.

  ```
  /_workflow        Processes, Steps              /_workflow-admin   Definitions, Analytics
  /_forms           Executions, Tasks, Tasks v2   /_forms-admin      Forms
  ```

  Same pod, same page beans, a second front door. Not a menu filtered by role: an engine runs with
  its own security disabled behind something that authenticates, so a menu hiding entries would be
  hiding them from a request it cannot judge. A separate path is something a gateway *can* judge,
  with the rule written in one place instead of two.

  Routes are unchanged. Mateu derives a menu entry's route from the field name, and the new
  administration homes name their menu field the same as the home they were split from, so
  `/workflow/definitions` is still `/workflow/definitions` and `/forms/forms` is still
  `/forms/forms` — only the base URL they hang from is new. Deep links, and the `[NAVIGATE:]` blocks
  a chat agent may hold, keep working.

  New: `WorkflowAdminHome`, `WorkflowAdminMenu`, `WorkflowOperationsMenu`, `FormsAdminHome`,
  `FormsAdminMenu`, `FormsOperationsMenu`.

### Changed
- **`WorkflowHome` and `FormsHome` now mount the operations half only.** An application mounting
  `/_workflow` as a remote menu sees Processes and Steps; one mounting `/_forms` sees Executions,
  Tasks and Tasks v 2. To get the other half, mount the matching `-admin` base URL as a second
  remote menu.

  **`WorkflowMenu` and `FormsMenu` are deliberately unchanged and still offer all four sections.**
  They are the navigation contract an embedding application mounts — the testbenches and the UI e2e
  suite do exactly that — and narrowing them would have taken two sections away from every embedder
  to serve one deployment's layout. An embedder that wants everything keeps getting everything.

- **The standalone orchestrator shows both halves**, as two menu groups rather than one: a
  deployment with a single console splitting its UI in two would only be hiding half of it. Its
  administration routes are `/administration/*` there, because the inherited field already holds
  the operations half. Standalone mode only — behind a shell both halves keep their `/workflow/*`
  routes.

## [2.11.2] - 2026-08-31

### Fixed
- **Navigating to the screen already on display with different filters did nothing.** A Mateu
  listing keeps its filters in the query string — `mateu-table-crud` writes them there with
  `_syncStateToUrl` and reads them back with `_initStateFromUrl` — but `mateu-ui.routeChangedListener`
  decided whether to push a history entry by comparing **paths only**. So `/workflow/processes` and
  `/workflow/processes?status=RUNNING` were the same destination: no history entry, no re-read, no
  error and no log.

  It only shows where a navigation is generated rather than clicked, because a person filters
  through the filter bar and never notices. Asking the demo console's agent to show the RUNNING
  processes filters correctly from any other screen and does nothing at all from the process list
  itself.

  Fixed upstream in Mateu 3.0-alpha.306 (miguelperezcolom/mateu#429), which this release picks up:
  the comparison is over path and query together, extracted into `nextHistoryUrl` with a test of
  its own. An identical destination still pushes nothing, so the back button does not need
  pressing twice.

## [2.11.1] - 2026-08-31

### Fixed
- **Resuming a paused process that actually had somewhere to go rolled back, and left it paused.**
  The resume saved the process (PAUSED → RUNNING), then `StepOverProcessUseCase` mapped it and ran
  a derived query for its steps. On JPA that query auto-flushes — including the version bump the
  resume had just left pending — so the object mapped a moment earlier carried a version the flush
  superseded. Saving it afterwards failed optimistic locking, the whole handler rolled back, and
  the process was still PAUSED:

  ```
  ObjectOptimisticLockingFailureException: Row was already updated or deleted by another
  transaction for entity [ProcessEntity with id '…']
    at ProcessUpdateStepExecutionUpdateUseCase.apply(…:122)
    at ResumeProcessUseCase.handle(ResumeProcessUseCase.java:78)
  ```

  Only a resume that had work to release could fail: `StepOverProcessUseCase` saves the process
  only when a transition changed it, so a resume that advanced nothing left no pending write to
  collide with and committed happily. That is the shape that kept this invisible — and it is why
  the existing pause/resume coverage never saw it: those tests run on `AbstractE2eTest`, which
  boots the engine in **memory** mode, where there is no `@Version`, no persistence context and no
  auto-flush. A JPA e2e on H2 does not reach it either. `Dist20ResumeAdvancingTest` does, on real
  PostgreSQL and real Kafka, with the resume arriving as an upstream event.

  Fixed by loading a process's steps **before** mapping the process, at both call sites — the query
  flushes first, so the aggregate is mapped at the version the flush produced. `StepExecutionRepository`
  gains `findByProcessId(String)` for exactly this; `findByProcess(Process)` stays as a default for
  callers that hold the aggregate and are not about to save it.

- **A step's on-timeout branch was laid out before the start of the flow.** The graph editor's
  layout knew two kinds of edge, the flow and the rollback pointer. An on-timeout branch was drawn
  but never handed to ELK, so a step that exists only as somebody's timeout target — it declares no
  preconditions of its own — reached the layout as a node with no edges at all. ELK puts those in
  the first layer: the timeout of a step half-way through a definition was drawn to the left of the
  START, with its line coming back across the whole diagram.

  The layout now gets one edge per `onTimeoutStepId`, carrying the same low direction priority as
  the rollback edges, so the flow stays the straight line through the middle and it is the timeout
  that bends. On `booking-fase2-grabar-reserva`, the `Cancelar RQ` that a wait step falls back to
  moves from x=12 — above and left of the START — to x=570, beside the `Cobro confirmado` it is the
  timeout of. Ships in the VS Code (0.1.16) and IntelliJ (0.1.17) plugins, which embed the same
  component the engine serves.

### Changed
- **`POST /workflow/api/messages` declares its contract rather than checking it by hand.**
  `messageName` and `correlationKey` carry `@NotBlank` and the body is `@Valid`, so the rule is
  enforced at the HTTP boundary and visible in the schema instead of living as two `isBlank` checks
  inside the method.

  What a caller sees is unchanged, which took keeping: validation runs *before* the handler, so on
  its own it would have cost the endpoint both the message that names the offending field and the
  order of its own checks — a caller with no `X-Api-Key` would have been told what was wrong with a
  body the endpoint should never have read. A blank field is still a 400 naming the field, and a
  caller without the key is still turned away with a 401 first.

- **Mateu 3.0-alpha.305.** A plain dependency bump. Verified rather than read off the release: the
  browser suite was run against it in full alongside the engine, forms and worker suites, and the
  distributed suite on real PostgreSQL and Kafka. Nothing it carries is visible here.

## [2.11.0] - 2026-08-30

### Added
- **A read model for the analytics report, so it stops scanning the raw tables.** The report answered
  its `GROUP BY`s over `process_entity` and `step_execution_entity` in full — O(rows), which no index
  can help, because an aggregate over a window has to touch every row in it. On the reference
  deployment (388k processes, 2.7M step executions, 3.8 GB) the step aggregate alone was ~1.2s in the
  database on every page load, and grew linearly with history.

  It is answered from daily rollups now, opt-in behind `workflow.analytics.rollup=true` and jpa-only.
  A projector folds each immutable fact — a process created, a process finished, a step finished —
  exactly once behind a `(timestamp, id)` cursor, so it never re-scans history; the first run drains
  it as an implicit backfill. What is still in flight is counted live and merged at read time.
  Durations carry a mergeable histogram so a p95 can be read from summed buckets — the one figure
  that is not additive. Exact on counts, throughput, sample counts, totals and averages; the p95 is
  approximate (never below the true value, at most a bucket over) and the window is day-aligned. It
  plugs in behind the existing aggregate seam, so the memory-vs-jpa selection and its equivalence test
  are untouched. See `modules/workflow-engine/ANALYTICS-READ-MODEL.md`.

- **The test worker seeds an editable override the first time it meets a task.** The overrides table
  was write-only from the UI: a canned reply existed only if someone typed it in from nothing, which
  meant knowing the step ids up front. Now a task that resolves to the built-in default — no
  `TEST_CONFIG`, no override already matching — leaves a `TaskOverride` behind, keyed by workflow
  definition and step, copied from the default scenario and born enabled so it changes nothing until
  edited. Run a workflow, then open Overrides and retouch the ones that matter; the next run picks
  them up. Idempotent per signature, and seeding never fails a task.

## [2.10.0] - 2026-08-28

### Added
- **A workflow can say who may start it, and a form can say who may do its work.** Both declarations
  existed on the workflow side and neither was ever read: `requiredScopes`/`requiredRoles` parsed on a
  definition and on a step, `FlowAuthorizationService` evaluated them, `AuthorizationContext`
  described the snapshot it would evaluate against — and nothing called any of it. The property its
  own javadoc named, `workflow.security.flow-authorization.enabled`, did not exist. What shipped was a
  design with no enforcement, which reads from the outside exactly like a feature.

  It is enforced now, at two points and from two declarations:

  - **The workflow** decides who may *start* a process, checked in `CreateProcessUseCase` — one place
    rather than at each of the four doors (the page, an upstream record, an MCP call, cron), because a
    rule enforced in four places holds in three of them once a fifth door is added. What the engine
    starts for itself is not judged against a caller: cron says so with `AuthorizationContext.SYSTEM`
    and a PROCESS step by naming its parent step execution, since re-judging either would mean a
    scheduled definition could never run and a modelled child could never be spawned.
  - **The form** decides who may *see, claim and complete* a task, checked in all three places. The
    listing is narrowed in the query — filtering after it would turn a page of fifty into a short
    list — but the listing is a convenience and not the boundary: a task id travels in a URL, a log
    line, a pasted link, so claim and complete refuse on their own account.

  On the form rather than on the step, deliberately: this is a property of the work, not of the flow.
  The same "approve a refund" form is for the same people whichever workflow routes a task to it, so
  declaring it once is also what stops it drifting between the definitions that use the form.

  **Who the caller is** is a per-deployment question, so it is a port — `CallerResolver` — with a
  default that reads whichever of the two real sources is present: an application that authenticated
  the caller itself (Spring Security, which covers the standalone apps' HTTP Basic and any resource
  server) or a gateway that validated a token and forwarded it (opt-in, `trust-forwarded-token`,
  because reading an unverified token where nothing verified it is not weak authorization but none).
  Verified beats asserted, so the opt-in cannot quietly override a real login. A deployment that knows
  better defines its own bean.

  Requires-all and fail-closed throughout: a caller nobody could identify holds nothing, so anything
  required refuses them — which makes a misconfigured identity a locked door rather than an open one.

- `workflow.security.flow-authorization.enabled` (default `false`),
  `workflow.security.trust-forwarded-token` (default `false`), and
  `workflow.security.claims.subject` / `.scopes` / `.roles` for the claim names to read.
- `requiredScopes` / `requiredRoles` on a **form** definition, in the JSON schema and in the editor.

### Changed
- `FlowAuthorizationService` and its denial moved to `io.mateu.workflow.security` in `shared`, and the
  evaluation is now static. Both engines ask the same question and the answer has to be the same one;
  two copies of a rule like this drift.

### Fixed
- **The outbox relay backs off when the broker refuses, instead of retrying at the rate the pod is
  writing.** A send that the broker rejects is caught per message inside the drain — it never throws
  out of it — so the row stays `Pending` and the pass simply settles nothing. The outer loop then
  waited on `OutboxSignal`, which every commit raises; a commit is not news about the broker, so the
  relay went straight back at it. Under load with the broker down the retry cadence was the *write*
  cadence, not any configured interval: a connection attempt and an error line per message per pass.
  On a deployment polling at 50 ms that is a hot loop.

  A pass that claims rows and settles none of them is now paced by an exponential backoff that
  deliberately ignores the signal while it waits — honouring it is precisely what defeated the
  backoff. Recovery is immediate: the first pass that settles anything puts the wait back to the
  base.

  What is **not** a stall, and this is most of the correctness: an empty outbox (the healthy, common
  case — pacing it down would make an idle engine take seconds to relay the next message anyone
  wrote); a pass that settled some of what it claimed (a poisoned row is parked as `Error`, not
  retried, and must not slow the rest); and a relay that could not take the relay gate, which the
  chaos tests hold exclusively to freeze every relay at once.

  | property | default | |
  |---|---|---|
  | `workflow.outbox.relay-backoff-base-ms` | `100` | wait after the first stalled pass |
  | `workflow.outbox.relay-backoff-max-ms` | `5000` | the cap |
  | `workflow.outbox.relay-backoff-jitter` | `0.2` | ±20 %, so a fleet that stalled on one outage does not come back on one tick |

  The cap is seconds rather than a minute because nothing is relayed while the relay waits: it is
  also how long the engine stays quiet after the broker comes back, and steps here are paced in tens
  of milliseconds.

  New counter `eventconductor.outbox.relay.stalled`. It exists because the fix removes the symptom:
  the error flood *was* the signal that an outage was happening, and backing off would otherwise
  have removed the evidence along with the noise. Anything above zero means committed messages are
  not reaching the broker.

- `OutboxRelay` now stops on context shutdown (`@PreDestroy`). Its thread is a daemon, so it never
  held the JVM open, but it kept claiming rows and publishing while everything it depends on was
  being closed.

### Upgrading
- Nothing changes until `workflow.security.flow-authorization.enabled` is set. With it on, a
  definition or form that declares requirements refuses callers who do not hold them — including
  callers the deployment cannot identify at all, and including the MCP tools, which act with no
  identity unless given one through a custom `CallerResolver`.
- A step's `requiredScopes`/`requiredRoles` still parse and still do nothing. Enforcing them means
  checking when the step runs, which can be a week later on another pod, and that needs the caller's
  snapshot stored with the process — a column that does not exist yet. The javadoc now says so
  instead of implying otherwise.

### Added
- **A ceiling on what one caller may hand the engine.** Nothing asked how big anything was. The
  columns that hold variables are `TEXT` on purpose — a value cut to fit a column is worse than one
  refused, because the process then runs on data nobody sent — but that decision is about *a* large
  value and said nothing about how large. A caller could POST two hundred megabytes of JSON to the
  message API and the engine would parse it into heap, carry it through the outbox and write it to a
  row, on a thread that is also running everybody else's processes.

  `InputLimits` now draws the line between large and absurd, set where a real payload never reaches:
  a megabyte in one value (the number the forms engine already enforced, now applied to every
  channel), 500 variables per event, eight megabytes across them, and **255 for an identifier** — a
  business key, a correlation key, a message name. That last one is not about memory: it is the
  width of `process_entity.id`, `business_key` and `awaiting_correlation_key`, so without it an
  over-long key failed in the JDBC driver inside the transaction that was saving a running process,
  which is a far worse place for it than a check with a message.

  Enforced at one chokepoint — every event arriving from outside converges on
  `ProcessUpstreamEventUseCase`, which covers the `upstream` and `messages` topics, the embedded
  publisher, and therefore the REST endpoint and the MCP tools that publish through either. Events
  the engine generates for itself travel the outbox and are not re-judged, so a process that has
  legitimately grown large is not dead-lettered halfway through by a limit meant for its callers.

  **What happens to a refusal depends on which door it came through.** A Kafka record is parked on
  the dead-letter destination and the partition keeps moving — it will be exactly this size on every
  redelivery, so retrying it is a loop nobody reads. A REST caller is answered `400` with the reason
  by the controller itself, rather than `202` followed by a silent dead letter on the far side of
  the broker. A person filling in a form, and the process-creation page, are shown the message with
  what they typed still on screen.

- **A byte ceiling on the engine's own HTTP endpoints** (`workflow.rest.max-body-bytes`, default
  16 MiB). Refusing on content and refusing on size are different defences: the content check runs
  once the body has been read and parsed, so on its own it still lets a caller spend the memory
  first. `Content-Length` answers the ordinary case and the body is counted as it is read for the
  chunked one; both answer `413`. Registered on the message API and the git webhooks only — this
  ships inside a library, and a host application's own upload endpoint is not the engine's business.

- `workflow.rest.max-body-bytes` (default `16777216`), `workflow.forms.max-values`, and the
  `eventconductor.input.*` system properties: `max-identifier-length` (255), `max-value-length`
  (1048576), `max-variables` (500), `max-total-length` (8388608). `workflow.forms.max-value-length`
  now defaults to the engine-wide value rather than its own copy of it, so a value refused arriving
  over Kafka is refused arriving from a browser.

### Upgrading
- An event, message, submission or process creation carrying more than the limits above is now
  refused where it used to be accepted. The defaults are far above any real payload — a megabyte in
  a single variable still passes, and the existing tests that assert so are unchanged — but a
  deployment that genuinely sends more can raise any of them with the system properties named above
  without a rebuild.

## [2.9.0] - 2026-08-21

**A workflow could not be read as a workflow, and a workflow could be broken by a typo.** Tracing
described the code the engine was running rather than the process it was running it for, so Grafana
held a scatter of two-millisecond traces with nothing saying which process any of them belonged to.
And a definition file, a form, a rule or a business key was taken largely on trust — which is how a
misspelled key ran a step with no retries, and how two workflows that start each other spawned
processes for as long as the database would take rows.

Minor rather than patch: no public API moves, but two changes refuse input that used to pass in
silence — a definition file with a misspelled key, and a form with duplicate field ids or a
submission carrying fields the form does not declare. See Upgrading under each.

### Added
- **A process is one trace, shaped like the process.** Tracing described the code the engine was
  running — a span around a dispatch, one around a relay pass, one around a step-over — and each of
  those lasts a millisecond or two inside one method on one thread. The context that would tie them
  together does not survive the outbox row and the broker record in between, so what reached the
  backend was a scatter of unrelated two-millisecond traces with nothing saying which process any of
  them belonged to.

  A span cannot describe a workflow step anyway: a step starts in the transaction that dispatched it
  and ends wherever the worker's reply lands, minutes or days later, across a broker and possibly a
  pod restart. There is no call stack to wrap. What there *is* is `startedAt` and `finishedAt` on
  the step execution row, so the trace is now **built from what the engine durably records**: when a
  process reaches a terminal status it is written out as one trace, with a span for the process and
  one for each step that ran, each covering the time that step actually took. Steps that ran one
  after another read as consecutive siblings and steps that ran together as overlapping ones — the
  shape falls out of the timestamps rather than having to be inferred from causality.

  **Which trace a process belongs to is derived from its id**, not stored and not propagated: a hash
  every pod computes identically. That is what survives a rebalance, a restart, a redelivery from
  the dead-letter queue or a `TIMER` step that waits a week — a `traceparent` in a message header is
  gone the moment the message is replayed. No migration, no new column, and no OpenTelemetry SDK
  dependency: Micrometer's own `Span.Builder` already exposes the explicit start and end timestamps
  this needs.

  The live spans — `step-over`, `dispatch-step`, `correlate-message`, `outbox relay` — are still
  emitted and now join that trace instead of each starting one of their own, which is what makes a
  process visible while it is still running, before its own span exists.

  `management.tracing.sampling.probability` governs process traces, decided from the derived trace
  id by the same arithmetic OpenTelemetry's own ratio sampler uses. Per process rather than per
  span, deliberately: the engine's spans used to take their turn at the ratio independently, so at
  10% you got a tenth of the dispatches of a tenth of the processes and never a whole process to
  read. The volume is about the same now and arrives as whole processes. Nothing to configure.

- **An adversarial test suite over every layer that takes untrusted input**, specified test by test
  in [TESTING.md](TESTING.md) §8 and summarised in [SECURITY.md](SECURITY.md). A workflow, form or
  rule file arrives from a git import, a watched directory or an editor; a business key, a variable
  or a submitted form value arrives from an upstream record, the REST message API, an MCP call or a
  browser. The bar it holds is not "the engine rejects it" but **the engine survives it**: bad input
  is either refused with a message naming what is wrong, or accepted and unable to hurt a running
  process. Hostile definitions, the JEXL sandbox, injection-shaped keys and values against a real
  database, forms, rules, the REST and webhook surface through the HTTP layer, and stored-XSS
  journeys in a real browser. Several of the tests below were written against a defect and are green
  because it was fixed.

- `workflow.max-process-depth` (default `20`) and `workflow.forms.max-value-length`
  (default `1048576`).

### Fixed
- **An over-nested guard expression unwound the orchestration thread.** A few thousand brackets
  overflow JEXL's recursive-descent parser, and `StackOverflowError` is an `Error` — it passed
  straight through every fail-closed `catch (Exception)` around a guard evaluation and took out the
  Kafka consumer or the timer scheduler, rather than failing the one step whose definition was bad.
  Expressions now clear a length and nesting ceiling before a parser sees them, and a stack overflow
  is converted into an ordinary exception so the existing fail-closed handling engages. Shared by
  the workflow guards and the rule expressions, both of which now also state their denial list — no
  loops, lambdas, instantiation or global assignment — instead of relying on `createExpression` to
  refuse them.

- **Two workflows that start each other spawned processes without limit.** `checkInvariants`
  refuses a `PROCESS` step naming its own workflow, and that is the only spelling it can refuse: a
  definition is validated alone, so A starting B starting A validates cleanly and every generation
  started the next for as long as the store would take rows. A runtime depth limit
  (`workflow.max-process-depth`) now stops it and fails the step that asked for the child over the
  line, rather than leaving it waiting for a child that will never exist.

- **A malformed decision table failed anonymously.** A table missing its inputs, outputs or rows, or
  with a row carrying fewer cells than the table has columns, surfaced as a `NullPointerException`
  or `IndexOutOfBoundsException` from inside a loop, thrown into whichever step had asked for the
  rule, with nothing in it to say which rule or which row. It now fails by name, and in the
  evaluator rather than only at import — `RestRuleSource` and `GrpcRuleSource` fetch rules that
  passed through no validator at all.

- **A completed task could be completed again**, overwriting the values it was completed with and
  sending the engine a second reply. The engine ignores a status update for a step that already
  finished, so nothing broke; what was lost was the record of what the person actually submitted.

### Changed
- **A definition file with a misspelled key no longer imports silently.** The schema says
  `additionalProperties: false`, but validation ran against the parsed record re-serialised to JSON
  — by which point Jackson had dropped every key it did not recognise, so the check was applied to a
  document that could not contain an additional property. A file saying `"retires": 3` imported
  clean and ran with no retries at all, which is the exact failure a schema exists to prevent. The
  import paths now validate the document **as written**, before it is bound.

  **Upgrading:** a definition repository carrying a typo has been running with that field ignored,
  and that file will now be refused with the offending key named. The import reports it per file and
  carries on with the rest, so one bad file does not stop a catalogue — but check the import log
  after upgrading rather than after the first workflow behaves unexpectedly. The `$schema` line the
  README and the guides tell you to add for editor completion is explicitly allowed and stays so.

- **Forms are validated on the server.** Nothing compared a submission against the form that was
  asked for, so every posted name became a process variable — a task could be completed with fields
  its form does not have, and without the fields it says are required. `required` was a client-side
  hint and nothing more. Names the form does not declare are now dropped (dropped rather than
  refused: the page's own component state legitimately carries keys that are not fields), required
  fields are enforced, and a value longer than `workflow.forms.max-value-length` is refused.

  Duplicate field ids are refused outright. JSON Schema can require each id to be present and
  non-empty, but has no way to say they must differ from one another, so nothing checked — and two
  fields answering to one id make which value a downstream step reads arbitrary.

  **Upgrading:** a form with duplicate field ids will no longer import, and a client that submits
  fields the form does not declare will find them dropped rather than turned into process
  variables. If a downstream step reads a variable that no field declares, declare the field.

## [2.8.0] - 2026-08-21

**A saturated cluster drained 148 transitions/s with nothing busy** — the broker at 0.23 of two
cores, no threads waiting on JDBC, and 28 068 processes queued behind a relay that held six rows.
It was not behind on reading its backlog; it was slow at publishing, and the queue formed in front
of it.

Minor rather than patch: `workflow.outbox.relay-concurrency` changes meaning and default. No public
API moves, but a deployment that sets it explicitly keeps that value as a ceiling and sees no
change — see Upgrading below.

### Changed
- **The outbox relay publishes a batch into the broker's requests, not one message per round trip.**
  A saturated cluster drained 148 transitions/s with the broker idle at 0.23 of two cores and 28 068
  processes queued: nothing was busy, and the outbox held six rows, so the relay was not behind on
  reading its backlog — it was slow at publishing and the queue formed in front of it.

  The ack barrier was already there. What was not was enough keys in flight to fill a broker
  request: the send pool was `relay-concurrency` **platform** threads, so the producer never held
  more than a handful of records and amortised its round trip across four instead of across the
  batch. The sends are on **virtual threads** now, and `workflow.outbox.relay-concurrency` defaults
  to `0` — one per key in the batch, bounded by `batch-size`.

  Measured on a real broker, 2 000 messages: **449 ms → 207 ms**, 4 454 → 9 662 msg/s. The relay
  transaction got *shorter*, not longer, so the claim's row locks are held for less time rather than
  more — the cost this change was expected to have does not arrive, because the batch's acks were
  already awaited together.

  **Nothing about the guarantees moves.** Sends stay synchronous, so a refusal is knowable message
  by message; within a partition key they stay strictly sequential, so per-process ordering stays
  true by construction rather than becoming a property of `max.in.flight.requests.per.connection`.
  `Dist19` asserts both and was watched failing against each shortcut.

  **Upgrading:** a deployment that sets `WORKFLOW_OUTBOX_RELAYCONCURRENCY` explicitly keeps that
  value as a ceiling and sees no change. Remove it, or set it to `0`.

### Added
- **The process listing reads the read model, so a sharded fleet lists the fleet.** It queried the
  write-side database of whichever shard served the request, so *Workflow → Processes* showed one
  shard and no others — while the guide said the read model was what let a question span shards.
  It does now: with `workflow.projection.enabled` on, the listing pages, filters and orders against
  `process_index`, in both the JPA store and the JDBC one a remote projector deployment reads
  through. Off, it uses the write side exactly as before.

  This needed the process's **name** in the index, which `ProcessStatusChanged` did not carry: the
  listing shows it and searches by it, and an index without it would have answered a different
  question depending on which store answered. The event, the row, the two stores and the shard
  backfill all carry it now; `V3__process_index_name.sql` adds the column. Rows projected earlier
  have none and fall back to the business key rather than rendering a blank — a backfill fills them
  in.

## [2.7.0] - 2026-08-21

**`/workflow/analytics` did not return.** The request thread entered the route and never logged
again; the pod missed three liveness probes and was SIGKILLed. It was never the database —
PostgreSQL did the whole join in 247 ms — it was 383 215 rows materialised on the request thread to
put about fifty on a page.

Minor rather than patch: this changes the shape of the two repository ports. Anything implementing
`ProcessRepository` or `StepExecutionRepository` outside this repository inherits the new methods
as defaults and keeps working, but the interfaces are wider than they were.

### Changed
- **Analytics is aggregated by the database instead of folded in the JVM.** `/workflow/analytics`
  did not return: the request thread entered the route and never logged again, and the pod missed
  three liveness probes and was SIGKILLed. It was never the database — PostgreSQL did the whole join
  in 247 ms — it was 383 215 rows materialised on the request thread to put about fifty on a page.
  Every field of the report is a `GROUP BY`, so the two repository ports gained aggregate methods:
  the JPA store answers them in SQL, the in-memory store folds rows exactly as before, because
  memory mode is small by construction and that is what it is for. Measured on 20 000 processes and
  100 000 step executions against real PostgreSQL: **140 ms against 646 ms** row by row, and the
  reduction now returns tens of rows rather than hundreds of thousands. The `p95` stays a measured
  sample rather than an interpolation — `percentile_disc` in SQL is the same nearest-rank rule the
  Java implementation always applied, and an equivalence test holds the two to the same numbers.

## [2.6.2] - 2026-08-21

A deployment could not write a form execution at all — eleven columns were `varchar(255)` wherever
Hibernate built the schema, because the mappings declared no length and the migrations, which said
`TEXT`, never ran there. Reported from the sagas PoC, where a saga's variables are several kilobytes
and the task never reached the person waiting for it.

Also here: the graph browser journeys that had been failing at random on unrelated branches, and a
Mateu bump.

### Changed
- **Mateu 3.0-alpha.298.** A plain dependency bump. Verified rather than read off the release: the
  browser suite was run against it in full — 25 journeys — alongside the engine, forms and worker
  suites. Nothing it carries is visible here.

### Fixed
- **The graph browser journeys failed at random, on branches that touched nothing near the graph.**
  The page object read the graph's shadow DOM without checking it was attached yet, and the retry
  loop around it propagates an exception where it would have retried a false condition. Twice in one
  afternoon, on a listings change and on a persistence change.

- **Eleven columns were `varchar(255)` wherever Hibernate built the schema, and a real deployment
  could not write a form execution at all.** Reported from the sagas PoC:

      value too long for type character varying(255)
        ... insert into form_execution_entity (..., variables, ...)

  so the task never reached the person it was waiting for and the step timed out instead.

  None of these fields declared a length, so Hibernate mapped each to `varchar(255)` — narrower than
  the migrations intended in every case, and narrower than the content in several. It only bites
  where Hibernate builds the schema rather than Flyway, which is exactly how the demo and the PoC
  run (`ddl-auto=update`, migrations off), and it was invisible from the other side: the migrations
  had said `TEXT` all along.

  | entity | column | migration said | Hibernate built |
  |---|---|---|---|
  | `FormExecutionEntity` | `variables`, `values` | `TEXT` | `varchar(255)` |
  | `FieldEntity` | `options`, `optionsSource` | `TEXT` | `varchar(255)` |
  | `FieldEntity`, `FormEntity` | `description` | `VARCHAR(1024)` | `varchar(255)` |
  | `StepEntity` | `variables` | `VARCHAR(2048)` | `varchar(255)` |
  | `StepEntity` | `precondition`, `description` | `VARCHAR(1024)` | `varchar(255)` |
  | `LogMessageEntity` | `message` | `VARCHAR(2048)` | `varchar(255)` |
  | `ResourceEntity` | `url` | `VARCHAR(1024)` | `varchar(255)` |

  Both halves now say `TEXT`, so it no longer matters which one builds the schema. The migrations
  widen the capped columns to match rather than the mappings adopting their caps: in PostgreSQL
  `varchar(n)` and `text` are the same storage and the same speed, so a length on a JSON document,
  a log message or a URL buys nothing and costs an insert that fails in production. Widening is
  metadata-only there — no table rewrite, no long lock.

### Added
- **A test for the shape of this bug, in the one form that can fail.** The obvious test — save
  several kilobytes, read them back — is worthless here: **H2 does not enforce `VARCHAR` length**,
  so it passes against a 255-character column holding 2 KB. Verified rather than assumed, by
  reverting a mapping and watching the round trip stay green while `information_schema` reported
  `len=255`. That is the same blind spot that let this ship. The tests assert the *declared width*
  instead, which H2 reports faithfully even though it will not police it, and both were checked to
  discriminate: reverting a mapping fails exactly the column reverted.

## [2.6.1] - 2026-08-20

One fix, and the page it fixes is the one 2.6.0 was cut for. Analytics stopped killing the pod in
2.6.0 and started returning 500 instead, on every PostgreSQL deployment and for every window —
reported from a real deployment within hours of the release, reproduced, and covered by a test in
the suite that has a real database.

### Fixed
- **Analytics returned 500 on PostgreSQL, for every window.** Both analytics projections bound
  their window with the usual optional-parameter shape,
  `(:createdFrom is null or p.created >= :createdFrom)`. Hibernate emits a *separate* placeholder
  per occurrence of a named parameter, so what reaches the database is
  `(? is null or pe1_0.created >= ?)` and `$1` appears nowhere except in `$1 is null` — nothing to
  infer a type from, and PostgreSQL refuses to prepare the statement: `42P18, could not determine
  data type of parameter $1`. Casting the null check gives it the type back. Because the failure is
  at prepare time it did not depend on the values, so `/workflow/analytics` was broken with a window
  set exactly as it was without one — 2.6.0 turned a page that killed the pod into a page that
  always 500s.

  It survived release because nothing exercised these two queries against PostgreSQL: the engine's
  own tests run on H2, which infers the type happily and returns rows, and the service test above
  them mocks the repositories. The regression test is `Dist15AnalyticsWindowTest`, in the suite that
  has a real database — verified to discriminate, three failures with the cast removed, all of them
  the 42P18 above.

### Documentation
- **The test worker, driven from a deployment rather than a test.** The guide stated a scenario and
  then only ever started the process from JUnit, so the half of the audience holding a running
  cluster had no route in: the orchestrator UI, the `TEST_CONFIG` variable on a
  `ProcessCreationRequested`, and the escaping that a JSON-inside-a-string demands. Two failure modes
  are now written down and told apart — an event whose own JSON is broken never becomes an event at
  all, creates no process, is not parked in the dead-letter store because conversion fails before any
  handler runs, and leaves the producer exiting 0; a broken `TEST_CONFIG` *string* inside a valid
  event fails loudly on the process instead. Also documented: this worker and the forms engine both
  bind `downstream` out of the box, so the worker answers `USER_TASK`s meant for people until the
  human steps are given a topic of their own.

## [2.6.0] - 2026-08-20

**The admin UI read every row of the write side to paint ten of them**, and on the demo deployment
that had stopped being a slow page and become an outage: opening `/workflow/analytics` killed the
orchestrator pod.

Four pages shared one habit — load the whole table through the domain aggregate, then filter, sort
and paginate in Java. A process row carries its workflow definition JSON, 8 KB on average, so ten
rows cost 315 MB out of Postgres on every keystroke. Analytics was the same habit at a worse scale:
every process, plus the entire step-execution table re-read once per workflow definition, around
2.5 GB for one page. Two real requests: 61 s abandoned by the browser, and 38 s returning HTTP 500
with the pod SIGKILLed at that moment.

Everything below was measured against 37 651 processes and 345 564 step executions, not estimated.
The listings and analytics now read projections of the columns they actually show, page in SQL, and
answer in milliseconds.

### Changed
- **Mateu 3.0-alpha.297.** A plain dependency bump. Verified rather than assumed: the browser suite
  was run against it in full — 25 journeys, including the graph readability ones — alongside the
  engine and worker suites, and nothing it carries is visible here.

The admin UI read every row of the write side to paint ten of them. Four pages, one habit: load the
whole table through the domain aggregate, then filter, sort and paginate in Java. On the demo
deployment — 37 651 processes, 345 564 step executions — that was measured, not estimated.

### Fixed
- **Analytics did not just take a minute, it took the pod down.** `/workflow/analytics` loaded every
  process and then re-read the entire `step_execution_entity` table **once per workflow definition**
  — six times, step JSON and variables included. Around 2.5 GB of row data for one page. Two real
  requests: 61.3 s, abandoned by the browser; 38.5 s, HTTP 500. The orchestrator was SIGKILLed at
  the moment of the second (`exitCode: 137`), and the gateway then returned 500 on `/workflow/steps`
  because there was nothing behind it.

  Both ports gained an analytics projection (`findAnalyticsRows`) reading the seven and six columns
  the report actually uses, and the service now takes **one** snapshot per report instead of one
  re-read per definition — step executions are indexed by process once, so per-definition work is a
  lookup rather than a scan of the whole table. The window goes into the query, and step executions
  join their process so it narrows them too.

  Every number is unchanged: nearest-rank p95, averages, per-day throughput, the bottleneck flag and
  the flow ordering of steps are all still computed in Java over the same values. Measured on the
  same data: 6.5 MB + 36.6 MB instead of 315 MB + 6 × 360 MB, and 0.25 s of database work.

### Changed
- **The process and step listings page in SQL.** `SimpleProcessCrudAdapter.search` and
  `StepExecutionsCrudAdapter.search` called `findAll()` and paged the result in Java. A process row
  carries its workflow definition JSON — 8 KB on average — so painting ten rows moved 315 MB out of
  Postgres, on every keystroke in the search box and every page turn. Measured on the demo: 6.9 s,
  7.2 s, 7.6 s of server time to return between 454 and 6 111 bytes.

  Filtering, ordering and paging now happen in the store, over a projection of the columns the table
  shows — never `workflow_definition_json`, `variables`, `log` or `step_json`. The in-memory store
  keeps the old behaviour as the port's default; only the JPA store overrides it.

  With the new indexes the unfiltered listing is an index scan of twelve buffers, 0.047 ms.

- **Paging is one contract, in one place.** The page size and number a listing reports are decided
  by the store now (`ServedPage`), so the SQL and in-memory paths cannot drift: a size of zero or
  less still means everything on one page, and a page past the end is still answered with the last
  real one. Pushed down, that last part cannot be clamped after the fact — the store has to count
  before it knows which page exists — so it counts first and then asks for the page it can serve.

- **Received tasks page in the database too.** The test worker's listing went through mateu's
  default `CrudStore.find`, which is `findAll()` paged in memory: 64 006 rows for ten, 1.6–2.2 s.
  `JpaReceivedTaskStore` now overrides it. Text search is pushed down replicating mateu's own rule
  exactly — every whitespace-separated token must appear in the row's `toString()`. The filter form
  and column criteria are deliberately **not** reimplemented: their semantics live in private
  reflection inside `CrudStore`, so those paths still delegate to the default.

- **The demo shell compresses and lets its bundle be cached.** 3.5 MB of JavaScript went out
  uncompressed, under `Cache-Control: no-store` — Spring Security's default headers applied to
  `/assets/**`, so the cache was not missing, it was forbidden. Compression is on (817 KB, 4.3×) and
  a dedicated filter chain drops only the cache-control writer for static paths, leaving every other
  security header in place. The bundle revalidates rather than expiring, because these filenames
  carry no content hash and a long `max-age` would strand browsers on an old bundle after a release;
  a second load is now six 304s and no bytes. The bootstrap HTML stays uncached.

### Added
- **Indexes for the two listings** (`V22`, `V23`): `created` and `(status, created)` on
  `process_entity`, `started_at` on `step_execution_entity`. Each is declared `DESC NULLS LAST` to
  match the queries exactly — both columns are nullable, and a plain `DESC` index in Postgres is
  `NULLS FIRST`, which would leave the planner sorting anyway.
## [2.5.1] - 2026-08-20

Mateu 3.0-alpha.296, and nothing else. It compiles clean and changes nothing observable — cut so a
deployment can pin one version rather than a version plus a note about what is not in it.

Measured rather than read off the release: the three operator journeys that wait on a view model's
`@Toolbar` actions being rendered were enabled and run against it, and the toolbar still offers two
of the five declared. They stay disabled.

### Changed
- **Mateu 3.0-alpha.296.** A plain dependency bump: it compiles clean, and nothing it carries is
  visible here. Measured rather than read off the release — the three operator journeys that wait on
  a view model's `@Toolbar` actions being rendered were enabled and run against it, and the toolbar
  still offers two of the five it declares, so they stay disabled.

## [2.5.0] - 2026-08-20

Three defects that shared a failure mode: **the system said yes and did nothing.**

Tracing was configured, enabled, and exported not a single span — 2.3.0 had fixed the missing
auto-configuration, so a `Tracer` existed and the tests written for it passed, in CI and in the
image, while no trace ever reached a collector. The endpoint was set under a property Boot 4
deprecated at level *error*, which means it is no longer bound: it reads back perfectly from the
environment, nothing consumes it, and the exporter that depends on it is never created. Spans were
built by a real tracer, handed to a real processor, and dropped.

The test worker's `memory` profile — the one the guide recommends for CI — did not start at all,
and under `jpa` it ran tasks one at a time while looking concurrent on paper. Those two are one
problem seen twice: `memory` was the configuration in which the concurrency premise held, and the
one that would not run, so there was no shape in which this worker could be driven at load.

None of the three showed up as an error anywhere. That is what the tests added with them are for: a
span exporter bean, a context that starts under the profile, and a store call slow enough that
serialising it is visible.

### Fixed
- **Tracing was configured, enabled, and exported nothing.** 2.3.0 fixed the missing
  auto-configuration, so a `Tracer` was created and the two tests written for it passed — in CI and
  in the image. No span ever reached a collector, and nothing said why.

  The endpoint was set under `management.otlp.tracing.endpoint`, which Boot 4 deprecated at level
  **error**: the property is no longer bound, and the metadata entry survives only to say so. It
  reads back perfectly from the environment, so everything looked configured. What depends on it is
  the OTLP exporter bean, which was therefore never created — spans were built by a real tracer,
  handed to a real span processor, and dropped for want of anything to export them with.

  Now set under `management.opentelemetry.tracing.export.otlp.endpoint`, in all three apps that
  trace. `OTLP_TRACING_ENDPOINT` is unchanged, so no deployment has to move.

  The test asserts the **exporter bean exists**, which is the half nobody was asserting: a `Tracer`
  without an exporter is a tracing setup that passes every check and reports nothing. Reading the
  property back would have proved only that the yaml says what the yaml says — which is exactly
  what made this look right for two releases.

- **The worker's `memory` profile did not start.** The profile is documented as the shape a CI suite
  wants — one container, no volume — and it failed on an `entityManagerFactory` it had deliberately
  removed. The stores are conditional; the Spring Data repository interfaces they wrap are not and
  cannot be, because scanning is what finds them, and the application class turned scanning on
  unconditionally. Excluding the JPA auto-configurations does not help: excluding an
  auto-configuration does not stop repository scanning. `@EnableJpaRepositories` and `@EntityScan`
  now sit on a configuration conditional on `worker.persistence=jpa`, which is what the stores
  already switch on. A context test starts the profile, which is the only size of test that could
  have caught this — every unit in the worker was fine; the assembly was not.

- **The test worker did not run tasks concurrently under `jpa`.** The consumer uses `flatMap` and
  the simulator uses `Mono.delay`, so on paper it ran many at once. The store calls are blocking,
  and they were made on the Reactor thread carrying the task — a considered trade whose premise was
  that they interleave with a `delay` and starve nothing. True of the in-memory map, false of JPA,
  where the blocking call is a database round trip on the same small pool every other task shares.
  Measured at 5,000 processes against a deployed engine: 7.7 tasks/s at 200ms of simulated work
  each — about 1.5 genuinely in flight, with the worker at 50m CPU and PostgreSQL at 106m and one
  active connection. The store calls now run on `Schedulers.boundedElastic()`. The test puts the
  delay in the store, which is where JPA puts it: 0.6s with the fix, 1.8s without.

  Together these were the whole of it — `memory` was the configuration where the premise held and
  the one that would not start, so there was no shape in which this worker could be driven at load.


## [2.4.0] - 2026-08-20

The process diagram stops lying, rules stop being the odd one out, and an extension that existed
only on paper becomes real.

**The diagram an operator was watching was frozen** as of the moment the tab opened, for the life of
the tab, while the process ran to completion behind it. Nothing looked broken — every node was still
drawn, in the state it had on opening; only the colours were a lie, which is how it survived months
of use. It had a workaround that could not be shipped, because it traded the frozen diagram for a
status badge that lied about whether an operator's pause or cancel had taken effect. Mateu
3.0-alpha.294 supplied what was missing, and the fix here is to stop writing the graph as component
metadata and let it travel as what it is: data.

**`.ecrule` was declared in two lists and read by nothing** — so since 2.3.0 a build validated a
file the engine would then not load. Green light for something that does not work is worse than
either supporting the extension or refusing it. It is supported now, in the engine and in both IDE
plugins.

The cause of that was structural, and is fixed with it: workflows and forms each have one place that
decides what a definition file is, and rules did not. They do now — which also means **rules can be
imported from a directory**, as the other two already could.

### Added
- **Rules can be imported from a directory**, as workflows and forms already could:
  `rules.directory-import.directories`. This is where the drift above came from — workflows and
  forms each have one `ImportXFromDirectoryUseCase` that decides what a definition file is, and
  their Git imports delegate to it; rules had no such place, so their Git import carried its own
  walk, its own filter and its own parser. Now they have one too, and the Git import delegates. The
  next extension is added in one place rather than three.

### Changed
- **Mateu 3.0-alpha.295.** Carries the element-interpolation support that 294 introduced, and
  compiles clean — unlike 294, which moved `RestDataSource`. Still open upstream: a view model's
  `@Toolbar` actions are not all rendered (two of five), which is why three operator journeys stay
  disabled.
- **The process diagram follows the process again.** A page that refreshes itself answers with a
  `State`, which carries values and deliberately does not resend the component tree — and an
  `Element`'s attributes are part of that tree. So the diagram an operator was watching was frozen
  as of the moment the tab opened, for the life of the tab, while the process ran to completion
  behind it. Nothing looked broken: every node was still drawn, in the state it had on opening. Only
  the colours were a lie.

  Mateu 3.0-alpha.294 added what was missing — an element's attributes and content now accept
  `${...}` expressions, evaluated against the state — so the fix here is to stop writing the graph
  into metadata. The topology and the overlay are two plain `String` fields on the view model, which
  is what makes them data, and the attributes merely say where to read them. The update is applied
  in place, so the element repaints without being rebuilt and keeps its zoom, its selection and its
  computed layout.

  It had a workaround that could not be shipped: returning the view model instead of a `State` made
  the diagram follow the process and stopped the status badge updating, so an operator got a badge
  that lied about whether their pause or cancel had taken effect. The engine shipped the frozen
  diagram rather than that, and `DiagramStaysLiveJourneyTest` sat disabled carrying both
  measurements. It is enabled and passing, and so are the two operator journeys the workaround broke.

  The topology travels this way too, not only the overlay: a DYNAMIC step injects nodes while the
  process runs, so the graph's shape changes under a page that is already open.

### Fixed
- **`.ecrule` was a ghost extension.** It was declared in the engine's shared extension list and in
  the Maven plugin's copy of that list — which 2.3.0 fixed the validator to honour — and read by
  nothing: not the rule import, not either IDE plugin. So a build validated a file the engine would
  then not load, which is worse than either supporting the extension or refusing it: it gives green
  light to something that does not work.

  Both lists even met inside one call — the rule import handed its own three-extension filter to
  `DerivedIds.declaredUnder`, whose own list has six, so an id could be derived for a file the
  filter beside it would never let through.

  `.ecrule` is now read by the rule import and registered by both IDE plugins (VS Code 0.1.14,
  IntelliJ 0.1.15) as YAML with the rule schema attached. There is no visual rule editor and this
  does not pretend otherwise — what a `.ecrule` gets is highlighting, completion and validation
  against the schema the engine validates it with.


## [2.3.0] - 2026-08-20

A minor release: the process diagram answers a question it could not answer before, and two things
that were quietly answering nothing now answer properly.

The diagram numbers its steps in the order the run actually took. A workflow's shape is not a list
of what happened in it — branches drawn side by side ran in some order, a step drawn between two
others may have run before both — so the tick said a step ran and nothing said when. And the
conditions on the lines stopped hiding the steps they apply to: shorter, whole under the pointer,
and placed clear of the nodes.

The two that answered nothing are of a kind. `eventconductor:validate` collected only three of the
six extensions the engine imports, so a repository written by the editors was walked, nothing was
found, and the build passed — a validator that validates nothing looks exactly like one with
nothing to complain about. And an END step recorded no time at all, which read as a step that never
ran wherever the record is shown by time.

Both IDE plugins are rebuilt on the current graph — **VS Code 0.1.13, IntelliJ 0.1.14** — because
each ships its own copy of the component and a rebuild is the only thing that moves it.


### Added
- **The process diagram numbers its steps.** A node now carries its place in the order the run
  actually took, bottom-left, beside the tick that says it ran at all. The shape of a workflow does
  not answer "when": branches drawn side by side ran in some order, a step drawn between two others
  may have run before both, and one drawn on the path may have been skipped. A step that has not had
  its turn carries no number, which is as much part of the reading as the numbers are.

### Changed
- **The IDE plugins ship the current graph: VS Code 0.1.13, IntelliJ 0.1.14.** Both carry their own
  copy of `workflow-graph.js`, synced from the engine at build time and not tracked, so a change to
  the component reaches them only when they are rebuilt — and the published 0.1.12 / 0.1.13 predate
  the guard chips and the step numbers. Verified by unpacking the artifacts rather than by trusting
  the sync: the marks are in the bundle inside both, and were absent from the ones before.

- **Conditions on the lines no longer hide the steps they apply to.** A guard chip is drawn over its
  edge, which is right — it belongs to the way in, not to the step. But an expression is as long as
  its author needed it to be, and drawn in full it can be wider than the nodes it sits between. Two
  things now: the chip shows the first 16 characters and expands to the whole expression under the
  pointer, and it is placed clear of the nodes and of the other chips — searched along its own line
  first, since a chip that moved along its line is still obviously that line's, and only then
  stepped perpendicular to it. Pinned by a browser test that compares the drawn boxes; without the
  search, the fixture's chip covers two nodes.

- **Mateu 3.0-alpha.294.** Creating a task override in the browser works again: the null-numeric
  hang — three nullable numeric fields rendering as `vaadin-integer-field`, a null arriving as
  `NaN`, the field clearing itself and the clear counting as a change — is fixed upstream. The
  guide's caution is gone and `NewOverrideFormJourneyTest`, written as the reproduction, is enabled
  and passing.

  `RestDataSource` grew a `ref` component in this version, so the forms task page builds it by name
  rather than by position; a positional constructor turns every such addition into a compile error
  at best and a silently shifted argument at worst.

  The **process diagram is still frozen** in a page that refreshes itself, and re-measured rather
  than assumed: an `Element`'s attributes still do not travel in a `State` update, so it is the same
  trade as on 291 and 293 — `new State(loaded)` keeps the status badge correct and the diagram
  stale, returning the view model does the opposite and fails two operator journeys on the badge.
  The disabled test carries both measurements. Same for the toolbar rendering two of a view model's
  five `@Toolbar` actions, which is why three operator journeys stay disabled.

### Fixed
- **The Maven plugin validated nothing in a repository written by the editors.** `eventconductor:validate`
  collected only `*.json`, `*.yaml` and `*.yml`, and the graph editor and both IDE plugins write
  `.ec`, `.ecform` and `.ecrule` — which the engine imports without complaint. Pointing the goal at
  a directory of them walked the tree, found no files, and passed: a validator that validates
  nothing is indistinguishable from one with nothing to complain about. Verified against the
  published 2.2.2, not only a local build. It now collects all six, and reads anything that is not
  `.json` with the YAML parser, which reads JSON too — so an `.ec` holding either parses.

- **An END step recorded no time at all.** It is completed straight from the orchestrator's end
  transition, which never went through the path that stamps `finishedAt`, so the one execution that
  by definition ran last was the one with nothing to say when — against what the field itself
  documents. It read as a step that never ran wherever the record is shown by time.

## [2.2.2] - 2026-08-19

A patch release for five defects found by running 2.2.1 in a real Kubernetes deployment, and four
of the five failed the same way: **by doing nothing, quietly**. Tracing that traced nothing because
no `Tracer` was ever created; a Prometheus endpoint that answered 404 because exposing an endpoint
is not the same as having one; a `directory` setting that relaxed binding accepted and nothing read;
and a malformed message dropped without a log line, a dead letter, or a metric. Nothing was broken
in a way that shows: each one looked exactly like a working system with nothing to do.

The fifth was the opposite — it did too much, inserting another copy of every definition file that
declared no `id`, on every import, without bound.

The lesson is in the tests rather than the fixes. Each one now has a test that fails without it,
because every one of these could return the same way it arrived: by a dependency moving, a property
being renamed, or a default changing under us, with nothing to say so.

### Fixed
- **An unreadable message was dropped in complete silence.** A record whose bytes could not be
  turned into an event — JSON that does not parse, a `type` this version does not know — never
  reached a handler, so none of the engine's handling applied to it. Spring Cloud Stream's converter
  failed, and the binder's default helper answers "do not fail" and then does nothing but re-align
  the `kafka_*` header lists. The record was dropped, the batch committed and the offset advanced:
  no log line at any level, no dead letter, no metric, lag back to zero.

  That combination is undiagnosable from outside, which is what makes it worse than the failure
  itself. A producer that sent 1,500 malformed messages saw a healthy engine that had created
  nothing — indistinguishable from messages that never arrived, so the search starts at the
  producer, then the topic, then the consumer group, and the payload is the last thing anyone looks
  at.

  Such a record is now logged at ERROR with an excerpt of the payload and the reason it could not be
  read — derived by re-reading the bytes, because the batch hook is handed no cause at all and
  "could not be converted" without saying why is most of the way back to silence. It is parked on
  the `dead-letter` topic as the original bytes, with `x-dead-letter-unreadable: true`, and counted
  by `eventconductor.events.dead.lettered`.

  It is still skipped rather than retried: bytes that cannot be parsed now cannot be parsed on
  redelivery either, so failing the batch would stall the partition for every process behind it, for
  ever. What changed is that the skip says so. DIST-14 drives it through a real broker and asserts
  both halves — the record is parked, and the records around it still finish.

- **A definition file with no `id` was duplicated on every import.** The importers gave such a file
  a fresh `UUID.randomUUID()` each time, so nothing connected the definition an import created to the
  one the previous import had created from that same file: every import inserted another copy, and
  none of them could be pruned — the code said so itself, tracking only explicit ids for pruning.
  With a git webhook wired up, every push added a copy, without bound, and nothing warned. All three
  engines did it: workflows, forms, and rules (through `SaveRuleUseCase`, which generates an id, as
  it should for a rule somebody saves in the UI and should not for a file).

  Such a file now gets an id **derived from its path** relative to the scan root —
  `sagas/onboarding.ec` becomes `sagas.onboarding`, dots rather than slashes because an id travels
  in URLs and in event payloads. The property that matters is that it is the same next time, which
  is exactly what reconciliation and pruning needed, and both now work for these files.

  Two consequences, chosen rather than incurred: **moving or renaming a file is a delete plus a
  create** (the old path is pruned, the new one arrives new), and the id is relative to the scan
  root, so changing `directory` changes the ids of files that declare none. A definition that must
  survive a move should declare an `id` — that is what declaring one is for.

  An explicit `id` still wins and a derived one never takes it: the ids a scan declares are read
  before anything is imported, because the collision is one of order — the file declaring the id may
  be walked second, and by then the derived one would already have been saved over it. A file whose
  path collides is reported as an error and skipped.

  **Upgrading:** definitions already inserted under generated UUIDs are attributable to no file, so
  the upgrade cannot clean them up. The next import creates one stable definition per file and
  leaves the old copies where they are; archive or delete them once, by hand. After that the count
  stays put.

- **Distributed tracing did nothing at all.** The orchestrator, forms and rule apps declared
  `micrometer-tracing-bridge-otel` and `opentelemetry-exporter-otlp`, and their yaml mapped
  `TRACING_SAMPLING` and `OTLP_TRACING_ENDPOINT` — but those are the OpenTelemetry *libraries*, and
  what creates a `Tracer` from them is Spring Boot's tracing *auto-configuration*, which Boot 4
  split out of `spring-boot-starter-actuator` into its own module. Nothing on the classpath owned
  either property, no `Tracer` bean was created, `WorkflowTracingAutoConfiguration` resolved its
  provider to nothing, and every call ran untraced.

  It failed silently in both directions, which is why it shipped: relaxed binding accepts a
  property nobody owns without complaint, and the engine's tracing bridge is designed to degrade to
  a no-op rather than refuse to start. A deployment setting `TRACING_SAMPLING=1.0` got no spans and
  no error.

  Fixed by adding `spring-boot-micrometer-tracing-opentelemetry`, which brings
  `spring-boot-micrometer-tracing` with it, so one dependency owns both properties. The existing
  names keep working — Boot 4 also offers `management.opentelemetry.tracing.export.otlp.endpoint`,
  but `management.otlp.tracing.endpoint` is still bound, so no deployment has to change. Each of the
  three apps has a test asserting an OpenTelemetry-backed `Tracer` is really there, because nothing
  would have caught its return.

- **The worker and projector answered 404 on `/actuator/prometheus`.** Both list `prometheus` in
  `management.endpoints.web.exposure.include` and neither declared `micrometer-registry-prometheus`.
  Exposure is a permission, not a creation: with no registry on the classpath the endpoint does not
  exist, and naming one you have no implementation for is accepted in silence.

  Anything scraping those pods had a target that was permanently down, which is worse than no
  target — it is an alert about the wrong thing. The other three apps declared the registry all
  along, so this was an omission rather than a decision. Each of the two now has a test that scrapes
  the endpoint over HTTP and asserts it answers with metrics, rather than looking for a registry
  bean: the bean is not the promise.

- **`directory` was accepted and silently ignored by the forms and rule engines.** The workflow
  engine's `GitImportProperties.GitRepository` has a `directory` field, so
  `WORKFLOW_GITIMPORT_REPOSITORIES_0_DIRECTORY` scopes the scan to a subdirectory of the clone. The
  forms and rule copies of that class did not, so their equivalents were accepted by relaxed
  binding — which never complains about a property nobody owns — and changed nothing.

  Pointed at a repository that is not exclusively definitions, both engines walked the entire clone:
  a parse error per unrelated YAML file, and — the real risk — anything that happened to look like a
  definition imported as one.

  Both now carry the field and the `resolveScanRoot` / `pruneKey` handling their workflow
  counterpart already had, including the guard that refuses a directory escaping the repository
  root. That guard matters more than its size: the root it resolves against is the throwaway clone
  the import deletes afterwards, so a `directory: ../..` that normalised instead of being refused
  would walk, prune against, and then delete whatever it found outside.

  Three copies of one properties class is what produced this, and they are still three.

### Notes
- **Known issue: `/_worker/taskOverrides/new` hangs the browser.** Mateu renders `TaskOverride`'s
  three nullable numeric fields — `durationMs`, `failuresBeforeSuccess`, `replyTimes` — as
  `vaadin-integer-field`, and a null arrives as `NaN`: the field clears itself, the clear counts as
  a change, and it is set again, without end.

  Isolated both ways on Mateu 3.0-alpha.291: making those three fields primitive opens the form,
  restoring them hangs it again. Not worked around, because the nullability *is* the meaning —
  `durationMs` null means "inherit from `default`" and `0` means "finish instantly", and a primitive
  cannot express the first.

  Until it is fixed, create overrides through the API or in the `task_override` table. Everything
  else on both worker pages works. The guide says so, and a disabled browser test carries the
  reproduction and passes against a build with those fields made primitive.

## [2.2.1] - 2026-08-19

A patch release, and every entry is something that shipped broken in 2.2.0 or earlier. Two of them
were invisible until something was finally pointed at them: a feature that could not answer a
request, and an image whose vulnerabilities nothing had ever scanned.

### Fixed
- **The worker image shipped four CRITICAL CVEs, and 2.2.0 published it anyway.** The worker was
  the only one of the five standalone apps missing both halves of what keeps the others clean: it
  parented off `spring-boot-starter-parent` **4.0.4** while orchestrator, forms, rules and projector
  are on 4.0.7, and it declared none of the security overrides they each carry. So it went out with
  `tomcat-embed-core` 11.0.18 (three CRITICAL), `jackson` 2.21.1 and `micrometer-core` 1.16.4 —
  25 HIGH and 4 CRITICAL in total, every one of them transitive.

  Neither absence was deliberate. That pom predates the app being published as an image, and until
  2.2.0 added it to the release workflow nothing had ever scanned it: the image had been pushed by
  hand, unscanned, for months. Adding the scan is what found this, on its first run.

  Now aligned with the other four — parent 4.0.7 and the same overrides — and verified with Trivy
  against the built image, with the flags the workflow uses: 0 findings, in the Alpine base and in
  the jar. **`worker-standalone-app:2.2.0` and the `latest` it moved should be treated as
  vulnerable and replaced by this release.**

- **Git webhooks never worked in a released build.** Every `POST` to
  `/workflow/webhooks/{provider}`, `/forms/webhooks/{provider}` or `/rules/webhooks/{provider}`
  answered 500 with `Name for argument of type [java.lang.String] not specified`, before the
  signature was verified and whatever the payload said. The three controllers declare `provider`
  as a bare `@PathVariable String`, so Spring binds it by parameter name — and the reactor
  compiled without `-parameters`, so no `MethodParameters` attribute reached any jar it published.
  There is no Spring Boot parent here to switch that on, and configuring `maven-compiler-plugin`
  at all is what silenced javac's own default, so the omission was invisible: the feature was
  documented, unit-tested and shipped, and the only thing it could not do was answer a request.

  The unit tests could not have caught it. They call `controller.webhook(...)` as a plain object,
  which is the right shape for the routing and verification logic but never reaches the mapping
  layer, and the mapping layer is the whole of the bug. There is now a MockMvc test for the
  workflow engine's webhook that goes through the dispatcher, so the flag cannot be lost silently
  again.

  Fixed for the class rather than the three sites: `<parameters>true</parameters>` on the root
  pom's compiler plugin, which is what the Boot parent would have contributed.

### Changed
- **The release scans its images before publishing them, not after.** The five images were pushed
  as they were built and scanned afterwards, which makes the scan a report rather than a gate —
  2.2.0 published all five and *then* failed on the worker's CVEs, so the bad image was on Docker
  Hub under both its version tag and `latest`, and the projector's scan never ran at all because
  the failing step skipped it. Each image is now built into the runner's daemon, all five are
  scanned, and only then are they pushed — in a single step, because five push steps failing
  part-way through would leave exactly the half-published state this ordering exists to prevent.

  Maven Central still publishes before any image is built, so a failing scan still leaves a
  published version behind on the immutable side. That is a separate decision, not an oversight.

### Notes
- `forms-engine` rose from 44.0% line coverage to 75.7%, and `rule-engine` from 42.8% to 75.8%,
  with their floors raised to match. Nothing was chosen to move a number: the classes covered are
  the adapters at each engine's edge — the JPA repositories, the Kafka consumer, the MCP tools, the
  REST and gRPC read APIs, and the Git import — each the only implementation of a contract somebody
  else depends on, and each at or near zero. `TESTING.md` records the figures and what they mean.

## [2.2.0] - 2026-08-19

A release about what a definition can say, and how it reaches the engine. Every change here is
additive: a definition written for 2.1.x means the same thing to 2.2.0, with the one exception
called out under *Changed*.

### Added
- **The worker app is now a test instrument: it plays back whatever scenario you ask for.** Testing
  a workflow meant answering its tasks, and answering its tasks meant writing a worker — one per
  scenario, or one with a branch per scenario, until the scaffolding outnumbered the workflow under
  test. The worker app now does no work at all. A process states what its tasks should do in a
  `TEST_CONFIG` variable, and the worker plays it:

  ```json
  {
    "default": { "durationMs": 200, "outcome": "COMPLETED" },
    "tasks": {
      "reserve-seat": { "durationMs": 500, "variables": [{ "name": "seatId", "value": "12A" }] },
      "charge-card":  { "outcome": "ERROR", "reason": "card declined" },
      "notify":       { "outcome": "NO_REPLY" }
    }
  }
  ```

  The keys are step ids — the engine sends an empty task id for every `ACTION` step, filling that
  field only for `USER_TASK` and `RULE`. A task can state its duration, its outcome, the reason it
  failed, the log lines it emits and when, the variables it hands back, how many of the first
  attempts to fail before succeeding, how many times to send its reply, and whether to ignore a
  cancellation. Three of those exist for states no
  ordinary worker can be asked to produce on demand: `NO_REPLY` is a worker that took the task and
  hung, which is what the step timeout is written for; a duplicate reply and a reply after
  cancellation are workers misbehaving, and what the engine does with them is a property of the
  engine worth being able to point at.

  Unknown properties and malformed JSON fail the task with the parse error as its reason, on the
  process you started. A misspelled `durationMS` quietly meaning "two seconds" would turn a test
  that proves nothing into a test that looks like it passed.

  It also records every task it is given, and offers a UI at `/_worker` for browsing them and
  canning a different reply for next time — for the processes you cannot edit. `TEST_CONFIG` always
  wins over a stored override: a suite whose result depends on a table someone edited by hand last
  Tuesday is not a suite. Every recorded row says which of the two answered it, because that is the
  first question anyone asks when a run surprises them.

  New module `modules/test-worker`, deliberately outside the `io.mateu.workflow` package tree so
  that everything which scans the engine's packages does not sweep up a worker binding and two JPA
  stores from any classpath that happens to contain it. `modules/sample-worker` is untouched and
  stays what it was — the hundred-line worker people copy.

  `DIST-13` (`Dist13TestWorkerScenariosTest`) drives the whole thing against a real orchestrator
  over real PostgreSQL and Kafka: a saga completed, a saga rolled back, a flaky step retried, a
  silent step timed out, and two processes disagreeing about the same task at once. It earned its
  place twice on the first run — the worker had assumed a retry arrives as a new task execution
  (the engine re-dispatches the same one, so the count never left 1 and a flaky step failed
  forever), and had led with task ids in a protocol that sends them empty.

- **Definitions can be imported from a directory on disk, not only from Git.**
  `workflow.directory-import.directories` and `forms.directory-import.directories` (or
  `WORKFLOW_DEFINITIONS_DIRS` / `FORMS_DEFINITIONS_DIRS` in the standalone apps) name directories
  scanned at startup:

  ```yaml
  workflow:
    directory-import:
      directories:
        - /definitions/workflows
  ```

  Git import reads what is **committed**, which is what a deployment wants and exactly what the
  loop where someone is *writing* a definition does not: edit, commit, restart, discover the commit
  was the step you forgot. A mounted volume of definitions had no other way in — forms have no
  classpath loading at all, and the workflow one needs the files inside the jar.

  Pruning, provenance and the per-file error handling are the same as for a repository, because it
  is the same code: importing a directory is what the git import already did once the clone
  finished, and it now lives in `ImportWorkflowDefinitionsFromDirectoryUseCase` /
  `ImportFormsFromDirectoryUseCase`, which the git import calls. A directory that is not there is
  reported as an error rather than passed over — a typo in a mount path should not look like an
  empty definition list.

- **A form field can declare its choices, as value/label pairs.** A field that picks from a fixed
  list — `radio`, `select`, `combobox`, `listBox`, `choice` — had no way to say what the list is,
  so a definition either left the picker empty or spelled the choices out in the field's
  description and hoped:

  ```yaml
  - id: decision
    label: Decision
    dataType: string
    stereotype: radio
    options:
      - value: WALK
        label: Walk the guest to another hotel
      - value: REFUND
        label: Refund the reservation
      - value: REJECT          # no label → the user sees "REJECT"
  ```

  The value is what the form submits and what the process variable ends up holding; the label is
  what the user reads. Keeping them apart is the point: the workflow's guards stay written against
  stable codes (`decision == 'REFUND'`) while the wording changes freely. The visual form editor
  edits the pairs and previews the real choices, and `form-definitions.md` documents the field
  format — which until now described a `type: SELECT` / `options: ["A","B"]` shape the schema has
  never accepted.

- **…or fetch them from a REST endpoint, with `optionsSource`.** A list written into a definition
  says what the choices were when it was authored; when they are a catalogue or a directory, what
  you want is what they are now:

  ```yaml
  - id: country
    label: Country
    dataType: string
    stereotype: select
    optionsSource:
      url: https://restcountries.com/v3.1/all?fields=cca2,name
      valuePath: cca2
      labelPath: name.common
  ```

  The engine only carries the descriptor — it never calls the endpoint. The fetch is the renderer's,
  through mateu's `RestDataSource` / `@RestOptions`: a form talking to any REST API without the
  backend in the middle. `url`, `headers` and `body` interpolate `${state.x}`, so one field's
  choices can depend on another's answer and refetch when it changes. A field declares `options` or
  `optionsSource`, never both, and the schema rejects one that declares both.

  `proxy: true` moves the fetch from the browser to the server — no CORS, and a `${secret.X}`
  placeholder resolved server-side instead of shipped to the client. Mateu never takes a proxied
  endpoint from the request (it would be an open relay), so the task pages declare what they carry
  through mateu's `RestSourceSupplier` (3.0-alpha.291): what the server fetches is only ever what
  the stored definition declares. Without it, the fetch is the browser's and the endpoint must be
  reachable from there.

- **`onFalse` on a precondition link: `WAIT` (the default) or `DISCARD`.** What a false condition
  means used to be decided by where it was written — a guard on a link held the step and kept the
  process open around it, a step-level `preconditionExpression` discarded the step and let the
  process finish. Those are two different statements, and both are worth making about one route
  in, so the link says which:

  ```json
  "preconditions": [
    { "stepId": "validate", "expression": "ratePlan == 'NON_REFUNDABLE'", "onFalse": "DISCARD" }
  ]
  ```

  This is what an optional or exclusive branch wants and what a link could not say before: the
  step is skipped when the condition does not hold, without the condition also having to apply to
  every other route into the step. `WAIT` is the default, so every existing link keeps its
  behaviour, and the property is written back out only when it is `DISCARD`.

### Fixed
- **Annotation processing was on by accident, and pruning a dependency turned it off.** Since JDK 23
  javac no longer runs processors found on the classpath, and maven-compiler-plugin follows it, so
  whether a module got Lombok and Mateu's UI registrations was decided by whether some dependency
  happened to pull a processor in. `sample-worker` is where it surfaced — dropping the Mateu UI
  dependencies it never used cost its `@Slf4j` classes their `log` field, reported as a missing
  symbol rather than as "nothing was processed" — but every module was one dependency change away
  from the same silence. The parent pom now says `<proc>full</proc>`.
- **The worker image could never have been built from its Dockerfile.** Two independent reasons:
  the builder stage ran `mvn dependency:go-offline` against a pom depending on
  `io.mateu.workflow:*:1.0-SNAPSHOT`, which is published nowhere, so it failed on the first line it
  ran; and the `COPY` named `app-*.jar` while the artifact is `worker-standalone-app-*.jar`, left
  over from the template the file was copied from. Neither had ever run — the image on Docker Hub
  was pushed by hand from a laptop. It is now runtime-only, from a jar the reactor built, which is
  the same shape as the `demo/*/Dockerfile.runtime` files the images that do get built come from.
- **The worker's healthcheck probed an endpoint the app did not serve.** `HEALTHCHECK` called
  `/actuator/health` with no actuator on the classpath, on a hardcoded `8080` rather than
  `${SERVER_PORT}`, and the entrypoint activated a `prod` profile that has never existed.
- **The worker's context test tested a different application.** It declared an H2 datasource and
  `workflow.persistence=jpa` — a property that app does not read — while the app itself ran with no
  database at all, so the context it proved could start was not the application's. It now starts the
  real configuration, and asserts the binding, the stores and the pages are actually there.
- **`failuresBeforeSuccess` could never let a step through, and DIST-13 is how that was found.** The
  worker counted a step's attempts by counting its rows within the process, on the assumption that
  the engine retries by issuing a new task execution. It does not: it re-dispatches the *same*
  `taskExecutionId` and keeps the count itself. So the retry overwrote the very row being counted,
  every attempt looked like the first, and a step told to fail twice failed until the engine ran out
  of retries. It now counts deliveries of the task execution, which is what the engine's model
  actually offers.
- **`sample-worker` pulled 70 MB of native library into every image built from it.** It depended on
  `spring-cloud-stream-binder-kafka-streams`, which brings `rocksdbjni`, and on Mateu's UI — none of
  it imported by any class in the module, and nothing anywhere in the repository uses Kafka Streams.
  The worker app's jar went from 176 MB to 54 MB.

- **Forms saved as `.ecform` were never imported from git.** Both IDE plugins register `.ecform` as
  the form file type — it is what the visual editor writes and what the schema follows the user
  into — but the git scan looked only at `.json`, `.yaml` and `.yml`, so those forms were skipped
  in silence. Its workflow twin has always accepted `.ec`. Parsed as YAML, which reads JSON too.
- **The Maven plugin rejected every definition whose preconditions are declared as links.** Its
  structural checks read only `preconditionStepIds`/`preconditionStepId`, so a step declaring
  `preconditions` looked like it waited for nothing and the entry-point rule reported it as a step
  nothing would ever start — failing the build on a valid workflow. The same blindness quietly
  narrowed the dangling-reference, self-reference and cycle checks, which never saw those edges.
  It now resolves all three spellings in the engine's own order.
- **The conditions on `preconditions` links are JEXL-checked at build time**, as
  `preconditionExpression` and `correlationExpression` already were. They are the preferred place
  to write a condition, and an unparseable one there fails closed at runtime just as silently.

### Changed
- **A step-level `preconditionExpression` is folded into the step's incoming links.** A condition
  is about a route into a step; a step-level one is the special case where every route asks the
  same thing. `Step.resolvedPreconditions()` now ANDs it onto each link, so a guard has one home
  and the engine has one place that evaluates it — `shouldRunStep` no longer asks two questions.

  Behaviour is preserved, including the part that was never about *where* the condition was
  written: a false step-level expression **skips** the step (the branch was not taken, and the
  process may finish around it), while a false condition on a link **holds** it. That axis is now
  explicit as `GuardMode` on the folded link — `WAIT`, what a link condition has always done, or
  `DISCARD`, what a step-level one has always done — instead of being decided by the spelling.

  One consequence is a fix, and it can change which branch a flow takes: a `CHOICE` picks its
  successor by the condition **on the link**, and was blind to one written at step level. Such a
  successor looked unguarded — i.e. like the else branch — so a `CHOICE` could take the default,
  or lose a tie-break, in a case its author had guarded. It now reads that condition, picks the
  branch that matches, and no longer reports the successor as the default in the "CHOICE has no
  default branch" warning. Definitions whose `CHOICE` successors are guarded at step level should
  be re-checked: they now branch as written.

## [2.1.1] - 2026-08-18

### Fixed
- **The `projector-standalone-app` image shipped a vulnerable PostgreSQL driver.** 2.1.0's image
  bundled `org.postgresql:postgresql` 42.7.11 (CVE-2026-54291, HIGH — SCRAM-SHA-256-PLUS downgrade
  defeating man-in-the-middle protection) instead of the 42.7.12 every other artifact here uses.

  The app parents off `spring-boot-starter-parent` rather than the EventConductor root, so the root's
  security overrides never reached it and the Boot BOM's own version won. Every other app repeats
  those overrides for exactly this reason; the new one did not, and the release pipeline scans images
  *after* pushing them, so 2.1.0's projector image reached Docker Hub before the gate caught it.

  **If you pulled `projector-standalone-app:2.1.0` or `:latest` before this release, re-pull.** No
  other image and no Maven Central artifact is affected: the driver is bundled only in that
  application's fat jar, and the application itself is not published to Central.

## [2.1.0] - 2026-08-18

Finishes the read side of sharding. Sharding shipped in 2.0.0 with the write side proven on a
cluster and the query side openly unfinished: each shard ran its own in-process projector, so the
fleet had as many partial answers to "what is running" as it had shards, and none for the whole.

It also closes a **correctness** gap that a fleet-wide read model does not close, and cannot — see
the placement claim below. Everything here is opt-in and additive: a single-cluster deployment, and
a sharded one that does not turn any of it on, behave exactly as they did.

### Added
- **A standalone projector (`projector-standalone-app`) and `workflow.projection.mode=remote`.** In
  remote mode the outbox relay diverts `ProcessStatusChanged` to a shared `process-index` topic
  instead of the shard's own `outbox`, the in-process projector is not created, and the engine reads
  the index from a read database. `listInFlightProcesses`, `countProcessesByStatus`,
  `findByBusinessKey` and the command router's `processId → shardId` lookup answer for the fleet for
  the first time.

  The channel is shared rather than per-shard for the same reason `messages` is: **no shard count
  appears in it**, so adding or draining a shard changes nothing about projection. The projector
  does not depend on the engine — `ProcessStatusChanged` carries the whole projected shape precisely
  so a projector needs no entities, no write schema and no engine beans.

  Diverted, not duplicated. A second copy of the index in the shard's own database would be a
  partial index that looks like a complete one.

- **A synchronous placement claim (`workflow.sharding.placement.datasource.*`).** A business key must
  be placed on exactly one shard, and every redelivery of that creation must come back to it, or the
  per-shard creation guard cannot collapse the duplicate and the fleet runs two processes for one key
  — two sets of side effects, on two shards, that nobody is watching for. The ingress router used to
  answer that from the process-index, which is eventually consistent: a creation redelivered before
  the projection catches up finds nothing and is placed again, somewhere else.

  Placement is now claimed in one atomic statement whose winner and every loser read back the same
  answer. It **fails closed**: a creation that cannot be claimed fails rather than being routed
  anyway, because a failed creation is retryable at its source and a duplicated process is not
  repairable. A sharded deployment without a placement store still works and warns at startup, in
  the terms of the damage rather than of the setting.

  It does not reintroduce the bottleneck sharding removed: sharding exists because one database
  cannot absorb the per-*step* write stream, and a claim is one small insert per *process*.

- **A cutover backfill.** The projector image doubles as a Job (`--backfill.shards=0,1`) that seeds
  the read database from each shard's write tables — the index, so the fleet view is complete from
  the first query, and the placements, so the claim knows where existing business keys already live.
  Idempotent and safe on a live fleet. This is the only step in the design that needs the shard list.

- **`process-index`, a module of its own**, so the projector can depend on the read model without
  depending on the engine. Its JDBC store upserts atomically on PostgreSQL
  (`INSERT … ON CONFLICT … WHERE`): one round-trip instead of two, and the staleness guard stops
  depending on the caller being serialised per process.

- **Fleet checks in the benchmark** (`bench.fleet.jdbc.url`): the index is complete per shard (R8a),
  agrees with the shards on status (R8b — where an ordering bug shows up and nowhere else), and no
  business key is running on two shards (R8c). Added to the per-shard verdict rather than replacing
  it: a read model verified by reading the read model proves nothing, so the two sides are reached by
  different paths.

- **k8s manifests for the fleet half** — the shared database, the compacted `process-index` topic,
  the projector and the backfill Job, plus `deploy-shard.sh fleet` and `backfill`.

### Notes
- **The projection topic must be compacted.** Compaction is what makes the read database rebuildable:
  it keeps the last event per process forever at bounded size, so a projector replaying from the
  earliest offset reconstructs the whole index. Under the default time retention the same replay
  silently loses every process older than the window — a rebuild that appears to succeed. The shipped
  manifest sets `cleanup.policy=compact` before anything produces to the topic.
- **The index is derived and disposable; the placement table is not.** They share a database for
  operational convenience and have completely different durability requirements. Back it up for the
  placements. And do not prune placements casually: a row must outlive the window in which a
  duplicate creation can still arrive, or housekeeping reintroduces the very duplicate the table
  exists to prevent.
- **Not yet cluster-validated.** The write-side sharding in 2.0.0 was proven on a live two-shard run;
  this read side is proven by tests only. Getting sharding onto a cluster surfaced five deployment
  bugs, all configuration, and there is no reason to think this half is different.

## [2.0.0] - 2026-08-17

The first MAJOR since 1.0, for one reason: a field that did nothing now does what it always said it
did, and a definition that names a `topic` other than `downstream` will be dispatched somewhere
else than before. See **Migration** below — it is short, and it only concerns definitions that set
the field.

The **definition format is unchanged and stays at schema version 1**
(`urn:eventconductor:workflow-definition-schema:1`). Every document that validated against 1.3.0
still validates: `topic` went from required to optional, which only relaxes. The MAJOR is about
behaviour, not format, and nothing needs rewriting to be *accepted* — only steps that named a topic
need looking at to keep going where they were going.

### Added
- **`topic` routes a step to a worker pool of its own.** The field has been in the definition
  schema since the beginning, described as "the Kafka topic to dispatch the task to" — and nothing
  read it. `Step.topic()` had no callers anywhere in the engine, and `KafkaDownstreamEventPublisher`
  sent every task to a hard-coded `"downstream"`, so a step naming `order-validator` went exactly
  where a step naming nothing went. It now goes to `order-validator`.

  A topic with no binding of its own is a dynamic destination, created by Spring Cloud Stream on
  first use, so naming one costs the application no configuration. A step that names none keeps
  going to `downstream`, which is the default and the overwhelmingly common case.

  **The step's cancellation follows its task.** `TaskCancellationRequested` — sent when a process is
  cancelled, when a step is stepped over, and when a task times out — goes to the topic that task
  was dispatched to, not to the default. Sent to the default while the task ran on a pool of its
  own it would reach nobody, and the step would run to its `timeout` instead of stopping: a failure
  with no error in it, which is why `KafkaDownstreamEventPublisherTest` asserts the address rather
  than only the payload. The destination is read from the step frozen on the `StepExecution`, not
  from the current definition, so a task already at a worker is cancelled where it actually went
  even if the definition has been re-imported since.

  `DownstreamEventPublisher.publish` takes the destination as a parameter rather than defaulting it,
  so a new call site has to say where its event goes. Embedded mode ignores it: there is one
  in-process `EmbeddedTaskExecutor` and no transport to route over, so it takes every task whatever
  the step says — routing to several in-process pools would be a different feature from naming a
  destination, and `java-api` no longer claims the bean name is matched against the topic, which it
  never was.

  **No migration.** The field already survives the database: steps are stored as JSON
  (`workflow_definition_entity.steps_json`, `step_execution_entity.step_json`), and both already
  carried `topic`. The `step_entity` table, which has no `topic` column, turned out to have no
  writers at all — `StepEntityRepository` has no callers — so it played no part in this.

- **A worker in kafka mode can finally say *why* a task failed.** `1.0-beta.022` fixed "a failing
  step recorded that it failed, and nothing about why" — but only on one side. In embedded mode the
  engine catches the worker's exception on its behalf and fills the `log` field of
  `UpdateStepExecutionCommand`, so the reason lands on the process. In kafka mode the worker answers
  with a `TaskStatusChanged`, which carries a status and variables and **no message**: there was
  nowhere to put the reason. So every kafka-mode failure reached the process log as "Task status
  changed to ERROR" and the explanation existed only in the worker's own stdout, if it logged at
  all. Whoever opened a rolled-back saga saw that it rolled back and nothing about what went wrong.

  `WorkerReply.failed(streamBridge, task, variables, reason)` publishes the reason as a
  `TaskLogEmitted` alongside the failure, so a failure reads the same in both modes. The reason goes
  **first**, and that ordering is the point: both sends are on the existing retry-or-throw path, so a
  broker that will not take the log line throws before anything has been reported at all and the
  task is simply redelivered. Reporting a failure and then losing its explanation would leave the
  engine acting on something nobody can account for, which is the state this exists to end. A null
  or blank reason sends nothing extra and behaves exactly like the three-argument overload, which
  stays — this is purely additive.

  The event was already accepted from `upstream` and recorded by `TaskLogEmittedEventHandler`; what
  was missing was any way to reach it from the API workers actually use. `WorkerReply.send(…,
  TaskLogEmitted)` is now public too, for a worker that wants to log progress rather than a failure.

### Changed
- **`topic` is no longer required on an `ACTION`.** It was required by the schema while nothing read
  it, which made it a field every author had to write on every ACTION step for no effect. The
  requirement did not even hold where it mattered: `WorkflowDefinitionValidator` validates the
  **serialised** definition, and Jackson writes `"topic": null`, which satisfies a JSON Schema
  `required`. A definition omitting it imported cleanly and only the IDE plugins complained — the
  rule nagged the author and protected nothing. It is now optional with a documented default of
  `downstream`, in the engine schema and in the copies the VSCode and IntelliJ plugins bundle.

- **The worker guide no longer teaches the bug.** Its example caught `Exception e` and called the
  three-argument `failed(...)`, discarding `e` without so much as logging it locally — the exact
  shape that makes a failed step unexplainable, copied from the documentation into real workers. It
  now logs and passes the reason.

### Migration
- **A step that names a `topic` other than `downstream` now goes there.** Until this release its
  value was ignored and its task arrived on `downstream` regardless, so a definition carrying a
  decorative topic — written against the documentation, or copied from the example in the AI
  reference, which gave two steps two different topics — has been working only because the field
  did nothing. Such a step will now be dispatched to a destination nobody consumes, and a task sent
  where no worker listens fails silently: nothing refuses it, and the step sits until its `timeout`.
  Before upgrading, either point each `topic` at a destination a worker really consumes, or remove
  it so the step goes to `downstream` as it has been doing. Steps with no `topic` are unaffected.

## [1.3.0] - 2026-08-14

### Added
- **`onTimeoutStepId` — route a step's timeout forward instead of failing.** By default a step that
  times out (after any retries) is a failure: it ends `TIMEOUT` and the process errors. A step may
  now name an `onTimeoutStepId` — its own on-timeout branch — and the flow routes there instead; the
  timed-out step ends `TIMEOUT` but is **not** counted as a process failure. It is the forward-routing
  dual of compensation: compensation rolls *backward* over steps that already **succeeded**, while
  on-timeout routes *forward* because a step that timed out never succeeded (there is nothing of its
  own to compensate). This is the native way to say "if nobody actions this human task in 30s, cancel
  the booking" — previously only expressible by racing the step against a parallel `TIMER`/`FORK`. In
  the graph editor an on-timeout line is drawn with **shift+alt+drag** (from a task step), rendered
  amber with a ⏱ clock chip showing the timeout, and the token-flow animation follows it. See
  [On-timeout routing](https://eventconductor.io/guides/retries-timeouts-compensation/#on-timeout-routing).

### Changed
- **A `USER_TASK` (and `RULE`) can now declare a compensation.** The editor's compensation gesture was
  restricted to `ACTION`/`PROCESS`/`WAIT_FOR_MESSAGE`/`DYNAMIC`; it now covers every **task step** —
  `ACTION`, `USER_TASK`, `RULE`, `WAIT_FOR_MESSAGE`, `PROCESS`, `DYNAMIC` — since a completed human task
  or rule has an effect worth undoing. The same set is what may carry an `onTimeoutStepId`.

## [1.2.0] - 2026-08-13

### Added
- **`CHOICE` — an exclusive-split gateway.** Where a `FORK` takes every eligible branch, a `CHOICE`
  takes exactly one: the first successor whose per-link guard holds, evaluating them from the longest
  guard expression to the shortest (most specific first), with an unguarded successor as the default
  (`else`), tried last. The pick **latches** — once a branch has started, a later change to the
  variables a guard reads cannot hand the split to another — and a `CHOICE` whose guards are all false
  with no default takes no branch and lets the process complete (a build-time warning flags the
  missing default). It is the split counterpart of the `XOR` join, which is how its branches should
  reconverge, and renders as the amber "×" gateway. See the
  [step types reference](https://eventconductor.io/reference/step-types/#choice).

### Changed
- **The graph editor is now a proper diagramming surface.** A left palette carries one item per step
  type, each drawn as the node it drops. Drag a palette item onto the canvas to create a node, or onto
  an existing node to create it connected as a successor. A step's **type is fixed at creation** (edit
  the YAML to change it). Preconditions and compensation are wired on the graph rather than in a form:
  **shift+drag** draws a precondition line, **alt+drag** draws a compensation line (only from a
  compensable step — `ACTION`/`PROCESS`/`WAIT_FOR_MESSAGE`/`DYNAMIC`), and selecting a connection edits
  that link's precondition — and a `?` **Help** button in the toolbar lists these gestures so they are
  discoverable. Connections are drawn as arrows. The token-flow animation opens paused; a retrying step
  pulses red once per failed attempt before it succeeds, and a slow step (a human task, a wait, an
  AND-join) lingers with a single ping instead of repeating. The same editor ships in the VSCode and
  IntelliJ plugins.

## [1.1.0] - 2026-08-10

### Added
- **Dynamic workflows: a step that grows the running process.** A new `DYNAMIC` step type is
  dispatched to a worker like an `ACTION`, but its reply may return a batch of new steps to add to
  the process it is running in — so a flow can decide its own shape at runtime (a fan-out whose
  width is only known once the work starts, a plan a worker computes and then executes). The worker
  sends them with [`WorkerReply.inject(...)`](https://eventconductor.io/reference/java-api/) (or
  `injectAndComplete(...)`); the message is `StepsInjected`. Injection is **add-only** — it never
  rewrites or removes existing steps — and the worker supplies each step *with its own
  preconditions*, because there is no default wiring: an injected step with no precondition is
  simply unreachable, a visible bug in the graph rather than something the engine papers over. The
  batch is validated engine-side as a whole (unique ids that do not collide with the process's
  steps, every precondition reference resolved, no cycle introduced) and, if anything fails, the
  **whole batch is rejected and the `DYNAMIC` step is failed** with the reason — so a bad injection
  is a failed step, not a silently dropped one. Only a `DYNAMIC` step may inject. Recursion is
  allowed (an injected step may itself be `DYNAMIC`) but bounded by two step-budget guards: the
  engine-wide `workflow.dynamic.max-steps-per-process` (default `500`) and a per-definition
  `maxSteps` override. Injected steps are marked in the process diagram — a dashed accent border and
  a ⚡ corner badge — and carry their provenance (`injectedByStepExecutionId`, the `DYNAMIC` step
  execution that added them), which also makes a re-delivered injection exactly idempotent. See the
  [Dynamic Workflows guide](https://eventconductor.io/guides/dynamic-workflows/).

## [1.0.1] - 2026-08-10

### Added
- **Failed steps are badged in the process diagram.** A completed step carries a green check in its
  bottom-right corner; a failed (`ERROR`) step used to be marked only by its red border. It now gets
  a matching badge — a white cross in a red circle — so a failure reads as clearly as a success at a
  glance. Cancelled steps stay unbadged, and a compensation keeps its undo arrow.

## [1.0.0] - 2026-08-09

First stable release. The formats, configuration and metrics documented here are the frozen 1.0
public API.

### Changed
- **The step property `rollbackable` is now `compensable`.** The engine calls it compensation, so
  the field that turns it on now matches. Definitions and in-flight state written with `rollbackable`
  keep working unchanged — the field is read through a `@JsonAlias`, so old JSON still deserialises,
  and a migration renames the definition table's column — but new definitions should use
  `compensable`, and the JSON schema, examples and docs now use it throughout.

### Fixed
- **Compensation no longer runs for the step that failed.** A saga rolls back by undoing the work
  that *committed*, in reverse — so it compensates the steps that completed successfully before the
  failure, and not the one that failed, which by definition committed nothing to undo. The engine
  used to compensate the failed step as well (treating "it attempted its work" as enough), which
  could reverse work that never happened. Now only `COMPLETED` steps are compensated; a step that
  ends in `ERROR` or `TIMEOUT` triggers the rollback but is not part of it and keeps its terminal
  status as the record of why the process rolled back.

- **A step's timeout no longer counts the time it spent queued.** The clock started when the
  orchestrator marked the step started and wrote its dispatch to the outbox, so everything before a
  worker saw it — outbox residence, the relay, the broker — was charged against the step's own
  execution budget. Under load that turns a backlog into failures rather than latency: once the
  dispatch backlog outgrows the timeout, steps expire before anyone could have run them, their
  retries expire with them, and their sagas roll back. A deliberate 120/s saturation run produced
  12,517 `ERROR` and 3,035 `COMPENSATION_FAILED` that way — business state left half-undone, none of
  it a worker doing anything wrong, while the outbox itself had lost nothing and drained cleanly
  afterwards at ~240/s.

  The clock now starts when the dispatch has actually been relayed out. Two published contracts
  already described it this way and neither was true: `timeout` is "Maximum execution time" in the
  definition schema, and `eventconductor.step.duration` is "from dispatch to final status" in the
  observability reference. Both derive from the same field, so one fix makes both so — and step
  duration now measures execution rather than execution plus queueing.

  A queue is capacity, not failure: a step waiting for a worker now simply waits, and shows up in
  `eventconductor.steps.stalled` if it waits too long, which is the signal built for it.

### Added
- **Tests for the paths whose failure mode is silence.** The outbox drain (`OutboxDrain`), where
  delivering before marking Sent is what makes the at-least-once guarantee, and a refused delivery
  must leave its row Pending — the shape of the defect that lost 71 of 642,912 messages during a
  broker outage. The boot-time rearm (`InFlightStepRearmRunner`), where a missed step is not
  un-timed-out but invisible: never scanned again, its process stopped for ever with nothing logged.
  And the tracing bridge, whose entire contract is that tracing failures never reach the work.
- **The 1.0 public surfaces are pinned as literals.** Meter names and tags for all four metric
  adapters, and the MCP tool names for the workflow, forms and rules engines. Both are listed as
  stable in `versioning.md` and both have consumers outside this repository — a Grafana dashboard,
  an alert rule, someone's assistant configuration — so a rename broke them and broke nothing here.
  Asserting the constants would not have caught it; the tests assert the published strings.
- **The filesystem half of Git import**, including the guard that stops a configured
  `directory: ../..` from walking out of the throwaway clone, and the rule that one unparseable
  definition costs that file rather than aborting the import of every file after it.

### Changed
- **The coverage gate now measures the code that carries the risk.** It asked for 85% of lines and
  passed, over a bundle that excluded the outbox, the schedulers, the JPA repositories, the message
  REST API, the MCP tools, the autoconfiguration and the Git import. Two exclusions remain —
  generated gRPC stubs and the Vaadin view classes — and the gate is now a per-module floor set just
  under what each module measures, so no module can regress. Over the honest scope the repository is
  at 65.7% per module and 76.4% aggregated; `TESTING.md` records both and how to reproduce them.
- `InFlightStepRearmRunner.rearmOnce` and two Git-import helpers widened from private to
  package-private so a single pass can be driven without the retry thread or a repository to clone.

### Fixed
- **The engine's own migrations never ran in any of the three standalone applications.** The
  initializer that applies them was guarded by `@ConditionalOnSingleCandidate(DataSource.class)`,
  which asks whether the data source's *bean definition* is visible at the moment the condition
  runs — and in a real boot of these apps it is not. The condition reported "did not find any beans"
  while Hikari's own autoconfiguration matched in the same report, so the bean was never registered
  and not one migration was applied. Nothing failed, because `ddl-auto` defaults to `update` and
  Hibernate built the schema instead — without a single index, which is the exact slow failure
  1.0-beta.025 exists to prevent. Under the chart's `DDL_AUTO=validate` the apps did not start at
  all. The data source is now resolved when the bean is *created* rather than when it is *defined*,
  which takes autoconfiguration ordering out of the picture; an ambiguous set of data sources now
  says so instead of silently skipping. The autoconfiguration's own test could never have caught
  this — an `ApplicationContextRunner` orders the autoconfigurations by construction — so each app
  now has a `@SpringBootTest` that boots it the way it really boots.
- **`forms-standalone-app` kept every form definition and task execution on the heap.** It set
  `workflow.persistence`, which the forms engine does not read, and never set `forms.persistence` —
  so an application wired to PostgreSQL, shipping forms migrations and running under
  `DDL_AUTO=validate` lost all of it on restart. It also meant the task list was permanently empty:
  `Tasks`, `TasksV2`, `TasksWidget` and `CancelTaskUseCase` read the JPA repository directly, so
  they queried a table nothing was writing to. Now `forms.persistence: ${FORMS_PERSISTENCE:jpa}`,
  as `rules.persistence` already was.
- **A form silently lost a field to any other form that reused its id.** `field_entity` was keyed on
  the field id alone, but a field id is unique only *within* its form — `form-schema.json` says so,
  and git-imported definitions use human slugs (`approved`, `comment`) that repeat freely. Saving
  the second form re-parented the row and the first form's field was gone. The key is now
  `(form_id, id)`. Three more that had never run in production alongside it: a field dropped from a
  definition survived the save that dropped it, deleting a form orphaned its fields (which
  git-import's prune path hits), and field order was whatever the database returned — now stored,
  because a form renders its fields in order. A field that omits the optional `stereotype` no longer
  throws on save; it defaults to `regular`, as the schema documents.

## [1.0-beta.025] - 2026-08-06

The schema travels with the engine.

A workflow engine you can embed has to bring its own tables, and this one did not: the migrations
shipped with the standalone applications, so embedding it meant running on whatever `ddl-auto`
built — which has no indexes at all. It was invisible, because nothing fails; it just gets slow.
The engine now carries its schema in its own jar and applies it itself, into a history table of its
own, and says so out loud when it cannot.

### Fixed
- **The engine now ships and applies its own schema.** The Flyway migrations lived in
  `apps/*-standalone-app`, not in the engine modules, so they existed only for the deployment shape
  we happen to ship. Anyone **embedding** the engine got no migrations at all: `ddl-auto=update`
  built tables and primary keys and no indexes — Hibernate's update path emits no index DDL — so
  every deadline scan, outbox claim and message correlation ran as a sequential scan, silently. The
  documented advice for embedders (`spring.flyway.enabled=true`) could not have worked either: the
  SQL was not on their classpath. The migrations now travel in `workflow-engine`, `forms-engine` and
  `rule-engine`, and each engine runs its own at startup, embedded exactly as in the standalone apps.
- **The engine's migration history is kept away from the host application's.** A host's migrations
  are numbered from V1 and so are the engine's; one shared `flyway_schema_history` collides the two
  numbering spaces and the second to run fails validation on a checksum mismatch. Each engine
  records its history in a table of its own (`eventconductor_schema_history`, `…_forms`, `…_rules`)
  and never touches `flyway_schema_history`. `spring.flyway.*` keeps meaning what it meant: the
  application's own migrations.

### Added
- `workflow.schema.enabled` / `workflow.schema.table` (and the `forms.schema.*` / `rules.schema.*`
  equivalents) — whether an engine applies its own migrations, and where it records them.
- **A startup warning when the engine runs on a schema it is not managing.** Reached by running
  `workflow.persistence=jpa` without `flyway-core` on the classpath, or with
  `workflow.schema.enabled=false`. Both are legitimate; running indexless without knowing it is not.
- Tests for the case that was never covered: an application with a data source and no
  `spring.flyway.*` configuration of its own. They immediately caught an ordering defect — the
  schema autoconfiguration evaluated its condition before the data source existed, so an embedder
  would still have got nothing.

### Changed
- The standalone apps no longer carry the migrations or configure Boot's Flyway for them. They keep
  their historical history-table names through `FLYWAY_TABLE`, so **an existing deployment upgrades
  in place with no manual step**. The Helm chart sets `WORKFLOW_SCHEMA_TABLE` / `FORMS_SCHEMA_TABLE`
  / `RULES_SCHEMA_TABLE` in place of `SPRING_FLYWAY_TABLE`; its `*.flywayTable` values are unchanged.
- `flyway-core` is an optional dependency of the engine modules: a run with
  `workflow.persistence=memory` needs no database and should not carry a migration tool. Embedders
  using `jpa` add it (plus the driver module, e.g. `flyway-database-postgresql`).

## [1.0-beta.024] - 2026-08-06

The gate earns its keep.

The Trivy image gate that landed in beta.023 immediately did the job it was added for: it caught
fixable HIGH/CRITICAL CVEs in core dependencies that would otherwise have shipped silently — Spring
Boot and Spring Framework (security bypass, DoS, XSS), Tomcat (HTTP/2, authentication bypass),
Jackson 2.x and 3.x (arbitrary code execution), Spring Data, Spring for Kafka, the PostgreSQL JDBC
driver, and Alpine OS packages. This release remediates all of them to a clean image.

### Security
- **Spring Boot 4.0.4 → 4.0.7**, which bundles the coordinated Spring fixes (Framework 7.0.8,
  Data Commons 4.0.6, Spring for Kafka 4.0.6, Jackson 3.1.4, Tomcat 11.0.22). Applied to the engine
  library modules (via the reactor root) and to the orchestrator/forms/rules apps (which parent off
  `spring-boot-starter-parent`, so each app pom carries the bump).
- Explicit pins for what the Boot BOM does not cover: **Jackson 2.x → 2.21.4** (arrives via
  `json-schema-validator`; Boot 4 manages only Jackson 3.x) and **PostgreSQL JDBC → 42.7.12**.
- **`apk upgrade`** on the container base images (Alpine → 3.23.5) for the OS package fixes.
- The orchestrator image now scans clean under the release Trivy gate (alpine 0, app.jar 0).

## [1.0-beta.023] - 2026-08-05

The last blockers before the promise.

The hardening a workflow engine cannot go to 1.0 without: a rule can no longer reach out of its
sandbox, a fast-failing step can no longer hammer the thing it depended on, and a rollback that
cannot finish can no longer do so in silence. This beta also writes down the 1.0 compatibility
promise before making it — see [Versioning & Compatibility](doc/src/content/docs/reference/versioning.md)
for exactly what will, and will not, be covered from 1.0.0.

### Security
- **Rule expressions now run sandboxed.** The `rule-runtime` JEXL evaluator built its engine
  without `JexlPermissions.RESTRICTED`, unlike the workflow-engine's precondition evaluator. Rule
  `when`/`then` expressions come from untrusted sources (imported from Git, edited in the UI), so an
  attacker who controlled a rule could reach `Runtime.exec`, `System.exit` or reflection — arbitrary
  code execution on the rules service. The evaluator is now restricted to match the engine's.
- **Docker image release now fails on HIGH/CRITICAL CVEs.** The three Trivy image scans in the
  release workflow ran with `exit-code: '0'`, so vulnerabilities were reported but never blocked a
  publish. They now gate the release (`exit-code: '1'`, `ignore-unfixed` retained).
- **`rule-engine` upgraded jgit 6.10.1 → 7.7.0**, matching `workflow-engine`/`forms-engine`.
  rule-engine also clones remote repositories, so it carried the same fetch/parse CVEs the other
  modules had already been bumped away from.
- **Standalone apps fail closed on the DB password.** The orchestrator/forms/rules configs defaulted
  `password` to `${DB_PASSWORD:user_password}`, so a bare-jar boot with the env var unset came up on
  a known password (contradicting the Dockerfiles, which state there is no default). The default is
  removed; Helm and docker-compose supply `DB_PASSWORD` as before.

### Changed
- **In-memory persistence warns loudly at startup.** `workflow.persistence=memory` (the default)
  keeps all state in the JVM heap and loses every running process on restart. The engine now logs a
  prominent warning so a non-durable store is a deliberate choice, not a silent one.
- **The bundled Grafana engine dashboard charts the engine's own metrics.** It previously showed
  only infrastructure (Kafka lag, executors, Hikari, HTTP). Added process running / outbox pending /
  stalled steps / compensations-failed stat panels (the alert-worthy gauges) and process-outcome and
  step-retry/compensation/dead-letter rate timeseries.

### Fixed
- **Auto-retry now backs off instead of hot-looping.** A failed step with retries left was
  re-dispatched immediately, so a worker that failed fast burned the entire retry budget in
  milliseconds and hammered the failing dependency. Failed steps are now parked in a new
  `AWAITING_RETRY` status for an exponential, jittered backoff (`workflow.retry.backoff-*`) and the
  scheduler re-dispatches them when the delay elapses. Manual retries still reset immediately.
- **A failed compensation is no longer silent.** When a compensation step itself failed after its
  retries, the saga rollback halted and the process was left in `ERROR`, half-rolled-back, with no
  terminal state, metric or alert — indistinguishable from a plain failure. Such a process now
  reaches the distinct, sticky terminal `COMPENSATION_FAILED`, increments the new
  `eventconductor.compensations.failed` counter and logs loudly.

### Added
- `eventconductor.compensations.failed` metric and the `COMPENSATION_FAILED` process status.
- `workflow.retry.backoff-base-ms` / `-multiplier` / `-max-ms` / `-jitter` configuration and the
  `AWAITING_RETRY` step-execution status.
- **A documented versioning & compatibility policy** (`reference/versioning`): what 1.0 keeps stable
  (definition formats, configuration, worker/Kafka contract, embedded entry points, HTTP/MCP surface,
  metric names) and what it explicitly does not (the internal Java model classes — program against
  the definition format, not the `WorkflowDefinition`/`Step` types).
- **Versioned definition-format schemas.** The workflow/rule/form JSON Schemas now carry a versioned
  `$id` (`…-schema:1`), so a future backward-incompatible format change is a distinct version rather
  than a silent break.

### Plugins — IntelliJ 0.1.3, VS Code 0.1.2
- **The editors ship the current graph.** Neither plugin's sources have changed since the last
  plugin release; what has changed is the component they carry, and they only carry a new one when
  they are released. Installed alongside `1.0-beta.022`, the editor was laying a definition out
  differently from the web viewer — it had the bundle from 4 August, before the rollback edges
  reached the layout. These builds bring compensation steps drawn amber once they have run, the
  rollback edges laid out (so a compensation sits to the right of the step it undoes rather than at
  the far left with its line drawn back across the graph), and a paused animation that stays paused
  when a node is selected.

## [1.0-beta.022] - 2026-08-05

A failure that says why.

An exception escaping an embedded worker has failed its step since the last release, and the
process recorded that it had failed and nothing about why — the reason existed only in the
application's stdout. Two separate things were dropping it, and the second one had been hiding
the first for as long as there has been an Errors tab: it was empty however badly a process had
gone, because it looked for a string nothing writes.

### Fixed
- **A failing step recorded that it failed, and nothing about why.** `UpdateStepExecutionCommand`
  has always carried a log line — a worker's message, or the exception the engine catches on its
  behalf in embedded mode — and the use case dropped it: the process log said "Task status changed
  to ERROR" and the reason existed only in the application's stdout. It is written to the process
  now, typed by outcome, so a failure lands where failures are read.
- **And where they are read was reading the wrong thing.** Log messages are stored as
  `MessageType.name()` — "Error" — and all four readers compared against the lowercase literal, so
  the Errors tab of a process was empty however badly it had gone, its errors appeared in the
  Messages tab instead, and the graph's hover card never had a reason to show. Matched
  case-insensitively now, which covers the rows already written.
- **Selecting a node started the animation the operator had paused.** Focusing belongs to the
  simulation — it picks the paths the token will take and lights them while the rest of the graph
  falls back — so on a paused graph a click now selects the node and leaves the picture neutral,
  clearing any focus left over from before the pause. The token itself was already staying
  stopped; it was the lighting that read as it starting again.

## [1.0-beta.021] - 2026-08-05

The mode we were the only ones running.

Kafka mode worked in the standalone application, in the benchmark and in the distributed tests —
everywhere we run the engine ourselves — and could not work anywhere else, because the one
property that makes its consumers function was in each of those configurations and in none of the
documentation. An application that followed the README to the letter got a `ClassCastException` on
the first event of its first process, and its whole outbox dead-lettered behind it. The engine
brings its own bindings now.

The rest is a compensated saga finally looking like what it is: a process that finished.

### Fixed
- **Kafka mode did not work outside our own applications.** The engine's consumers take a batch —
  `Consumer<Message<List<DomainEvent>>>` — because a poll batch is committed as one transaction
  per process. A binding without `consumer.batch-mode` delivers one record and does not convert
  it, so the payload arrives as a `byte[]` and the first event dies with `ClassCastException:
  class [B cannot be cast to class java.util.List`; retries exhaust and every outbox event is
  dead-lettered. That property was set in the standalone application's YAML, in the benchmark and
  in the distributed tests, and appeared in no documentation at all — every snippet in the README,
  the configuration reference, the AI references and the scaffold skill wired the destinations and
  left it out. Those same snippets gave both consumers one shared group, which is the
  range-assignor trap fixed for our own configuration in `1.0-beta.015` and never fixed in the
  docs. The engine contributes its own bindings now — destinations, a group per binding,
  batch-mode — as the lowest-precedence property source, so anything an application declares still
  wins. It does not contribute `spring.cloud.function.definition`: that lists the functions the
  application composes, and a default would silently drop a worker's or the forms engine's. Also
  `spring.cloud.stream.function.definition`, in the README and the scaffold skill: that property
  has not been read since Boot 2.x.
- **A rolled-back process looked like one that had stopped halfway.** Reaching `COMPENSATED` only
  set the status: the steps the flow never reached stayed `CREATED` — indefinitely, on a process
  that is over — and the completion bar stayed frozen wherever the failure happened. A finished
  saga went on showing steps that looked like they were waiting their turn, at 43%. The steps that
  can no longer run are cancelled now, the same ones an END transition cancels, and the process is
  100% complete: the rollback ran to the end. The step that failed keeps its `ERROR`.
- **`COMPENSATED` is drawn green**, like `COMPLETED`. It was amber, which said "something here
  needs looking at" about a process that had already cleaned up after itself, and sat it next to
  the `ERROR` processes that do.
- **The rollback edges were drawn but never laid out.** ELK was given the flow and nothing else,
  so a compensation step — which declares no preconditions — was a node with no edges at all: it
  went to the first layer, at the far left, and its rollback line was then drawn from the middle
  of the flow back across everything in front of it, through whatever nodes were in the way. The
  layout gets those edges now, which puts each compensation in the layer after the step it undoes
  — to its right, where the eye looks for it, stacked above or below the flow rather than beyond
  it — and routes around what is already there. Definitions written either way lay out the same.

### Added
- **The demo images are published from CI**, by a `workflow_dispatch` job that builds the demo
  reactor once and pushes all seven, each labelled with the revision it was built at. They used to
  be built by hand with buildx: the ones on Docker Hub had been pushed three hours before the
  Dockerfiles they came from were committed, and nothing recorded which commit any of them
  contained. Their tag is `demo-0.1.0` now — the demo does not ship with the engine, and a tag
  shaped like an engine version reads as stale the moment the engine moves.

### Fixed (build)
- **The demo did not build from a clean checkout.** Five of its services depend on
  `io.mateu.workflow:shared:1.0-SNAPSHOT`, which nothing publishes; on any machine that had built
  the repository it was in `~/.m2`, so it appeared to build on its own.

## [1.0-beta.020] - 2026-08-05

One worker is not the engine — and a step with nothing to wait for is not a step that may run.

Both came out of the same afternoon with a proof-of-concept saga. A hotel service that accepted
the connection and never answered stopped every process in the JVM, not just its own, and the
symptom named nothing: the processes created afterwards sat with every step in `CREATED`, which
the UI describes as "waiting for its preconditions". And the compensation steps of that saga ran
in the happy path, because an anchor written without its guard is an ordinary edge — an anchor
they only needed because "no preconditions" meant "start immediately".

### Fixed
- **An embedded worker that blocked stopped the engine, and one that threw stopped its process.**
  Embedded dispatch calls the worker and waits for it to return, on the calling thread — which
  under `jpa` persistence is `embedded-outbox-relay`, the single thread draining the outbox and so
  the only one advancing every process in the JVM. One HTTP call to a service that accepted the
  connection and never answered froze all of them: no step-over ran, and processes created
  afterwards sat with every step in `CREATED`, which the UI describes as "waiting for its
  preconditions" and which looks nothing like a stuck worker.

  And an exception escaping a worker left its `StepExecution` exactly as it was, `PENDING`,
  waiting for a reply that was never coming; only the outbox row was parked as `Error`. Without a
  `timeout` on the step nothing would ever look at it again. The engine now records the throw as
  the step's failure, so retries and compensation engage. Reporting `ERROR` yourself is still the
  contract — a throw carries no variables and no message of your choosing.

### Changed
- **A step with nothing to wait for no longer runs.** Eligibility asked whether every
  precondition was satisfied, and every precondition of none is satisfied — so "no preconditions"
  meant "start immediately". That is why a compensation step had to be anchored to some step it
  had no relationship with and guarded with `"preconditionExpression": "false"`: a fiction whose
  only job was to keep the dataflow away from it, that had to be written correctly every single
  time, and that turned into a live branch of the happy path when it was not. A compensation is
  declared on the step it undoes and started by the rollback pipeline; it needs no way in of its
  own, and now it may have none.

  Only a flow's entry points run with no preconditions: `START`, and a `WAIT_FOR_MESSAGE` that
  begins a flow rather than sitting inside one. The roots rule becomes a reachability rule —
  a step with no preconditions that is neither of those nor another step's `compensationStepId`
  is rejected at load, because nothing would ever start it. Definitions using the false-guarded
  anchor keep working unchanged, in the engine and in the Maven plugin's build-time validation;
  the anchor was never real flow and is still ignored by the topology warnings.

### Added
- **A step that ran as a compensation is drawn amber in the process diagram**, with an undo badge
  instead of the green tick and a `COMPENSATION` chip on its hover card. A compensation that
  succeeds is `COMPLETED` like any other step, so it was drawn in the same green as the work it
  had just undone: a fully rolled-back saga read as a successful one with a few extra boxes. The
  graph already knew which steps those were — it draws their rollback edges — so nothing new is
  sent to it.
- **`workflow.embedded.worker-threads`** (plus `worker-queue-capacity` and
  `worker-shutdown-grace-ms`): hands embedded tasks to a bounded pool instead of running them on
  the dispatching thread. Off by default, because turning it on changes what delivery means —
  `Sent` becomes handed-off rather than finished, as it already is in `kafka` mode, so a task lost
  to a crash is recovered by the step's `timeout` rather than by redelivery, and two tasks of one
  process can be in flight at once. A full pool rejects, and the rejection is classified retryable
  so the outbox holds the message and offers it again rather than dead-lettering a queue for being
  busy.

## [1.0-beta.019] - 2026-08-05

The first time the whole thing was deployed to a real cluster — the engine and the seven demo
services, on Kubernetes, against one database — and it did not come up. Four separate reasons, none
of which a test or a lint could have found, because each of them only exists once the three
applications are pointed at one database and at one another. Two of them had been shipping in
releases for weeks: the rule engine's gRPC server has not started since `1.0-beta.009`, and the
chart named an image tag that was never published.

Nothing here changes how the engine behaves. It is the difference between software that passes its
tests and software that starts.

### Fixed
- **The orchestrator and the rule engine could not start next to the forms engine.** They share a
  database, each ships its own migrations numbered from V1, and all three wrote to Flyway's default
  history table: whichever started first put its V1 there and the other two refused to start on a
  checksum mismatch. Forms and rules now keep their own history tables, and all three baseline at
  version 0 rather than 1 — `baseline-on-migrate` reads a non-empty schema as "my tables are
  already here", which in a shared database means somebody else got there first, so the app skipped
  the baseline that creates its own tables and then failed on the first migration that touched one.
- **The rule engine's gRPC server never started**, in every release since `1.0-beta.009`: the stubs
  were generated with protoc 4.35.1 while grpc-java and Spring Boot's dependency management resolve
  protobuf-java 4.32.0, and generated code refuses to load on a runtime older than itself. The
  failing bean took the whole application context down with it. protoc is now pinned to the runtime
  the hosts actually get.
- **The database ran out of connections.** PostgreSQL allows 100 by default and the chart's own
  defaults ask for 96 — three apps, two replicas each, sixteen connections apiece — so anything
  else sharing it, including the demo's own services, was refused with "sorry, too many clients
  already". The chart now sets `postgres.maxConnections` (200), and the demo services take small
  pools out of it.
- **The chart deployed images that do not exist**: `appVersion` was the placeholder `0.0.0`, so
  installing straight from the repository left every pod in `ImagePullBackOff`. It names a real
  release, and the comment claiming CI pinned it is gone — nothing did.

### Added
- **A chart for the demo** (`charts/eventconductor-demo/`): the seven services, deployed as
  ordinary applications against an existing EventConductor release. Runtime images for the five
  that lacked them, and the gateway's routes, which point at localhost in the image and so answer
  "connection refused" to everything until a deployment tells them the real addresses.

### Documentation
- The Helm section of the deployment guide describes the chart as it is: the rule engine it also
  deploys, the password it requires to install at all, the values that matter, and the demo chart.


## [1.0-beta.018] - 2026-08-04

### Added
- **A process is one trace, not one per hop.** OpenTelemetry over OTLP was already wired and the
  documentation said the context propagated across the engine's asynchronous boundaries. It did
  not, for the boundary that matters: that boundary is a database row, not a network call. An event
  is written to the outbox inside the transaction that produced it and published by a relay thread
  afterwards, so auto-instrumentation saw a write in one trace and, later, a Kafka send belonging
  to nothing — and the consumer started a fresh trace. The outbox row now carries the producing
  trace's W3C `traceparent` (`V17`, null when nothing is being traced) and the relay publishes as a
  continuation of it.
- **Engine spans**: `eventconductor.step-over`, `eventconductor.dispatch-step` and
  `eventconductor.correlate-message`, so a trace shows what the engine was doing rather than only
  the queries it made along the way.
- **Metrics over OTLP** (`OTLP_METRICS_ENABLED`), for deployments that already run a collector.
  Off by default, and it does not turn Prometheus scraping off.

All of it through a `WorkflowTracing` port with a no-op default, wired to Micrometer only when the
host brings a `Tracer` — the engine libraries still run with zero observability dependencies, and
a tracing failure is logged at debug rather than being allowed to fail a workflow.

### Documentation
- The observability reference describes the outbox propagation, the engine's spans and the OTLP
  metrics export; the AI reference gained the definition status, per-link preconditions,
  `restartProcess` and the tracing summary; the IDE guide documents the workflow status in the
  graph's Settings panel and that a new `.ec` is written as YAML.

## [1.0-beta.017] - 2026-08-04

Two questions a definition could not answer before: which route into a step a condition belongs to,
and whether the workflow is meant to be running at all.

### Added
- **A precondition carries its own condition.** `preconditions: [{stepId, expression}]` puts a
  guard on the link rather than on the step, so a step reached from two places can require
  something different of each. A link whose condition is false is not satisfied, so the step
  **waits** — the literal reading of "wait for all of them", chosen over quietly dropping the
  branch, which would let a step run having waited for less than its author wrote. Documented
  including the cost: a condition that never becomes true is a process that never finishes, and one
  the stalled-step gauge cannot see. The step-level `preconditionExpression` is unchanged and still
  skips rather than holds.

### Changed
- **A definition says whether it may run, and the runtime cannot overrule it.** `status: ACTIVE |
  DISABLED | ARCHIVED` in the `.ec` is a floor: an operator can take a workflow out of service, but
  cannot put one into service that its own definition closes — which is what lets a definition live
  in a repository without being live. It replaces the `disabled`/`archived` booleans, which between
  them said what one word says; both are still read, in files and in the database.

### Fixed
- **Process creation ignored a disabled workflow.** Only the cron scheduler checked, so anything
  creating a process directly — the UI, an upstream event, an MCP call — walked straight past a
  workflow that had been taken out of service.
- **A git import silently put back into service anything an operator had disabled**, because the
  file and the runtime wrote the same field and whoever wrote last won. They are stored apart now:
  an import replaces the declaration and leaves the runtime decision alone.
- **The graph editor wrote a field the engine could not read.** Its Status dropdown produced
  `status: DRAFT|ACTIVE|DISABLED|ARCHIVED`, which the schema does not define and the importer's
  parser rejects — so such a file did not merely fail to disable anything, it stopped importing
  altogether. The editor now writes the real status, and the importer adopts the older spellings
  rather than choking on them.

### Plugins — IntelliJ 0.1.2, VS Code 0.1.1
- **Delete (or Backspace) removes the selected node or connection**, and deleting a node clears
  every reference to it — preconditions in either spelling, and the compensation pointer that used
  to be left dangling. Connections are selectable at all for the first time.
- **The animation follows the graph you are editing.** Its paths were derived only when the host
  pushed a value in, so an edit made in the editor left it walking the graph as it was before:
  new nodes on no path, deleted ones still on theirs. Selecting a node no longer restarts a paused
  simulation, and a node with nothing wired to it is not animated as a path of its own.
- Readable arrowheads, no expand button in an editor pane that is already the whole surface, a
  Settings panel whose rows line up with their labels, and a new `.ec` written as YAML rather than
  JSON.

### Migration
- `V15` and `V16` add the definition status columns; both are idempotent and run over a `ddl-auto`
  schema. No action beyond the `flyway repair` that 1.0-beta.016 already called for.

## [1.0-beta.016] - 2026-08-04

Tagged without release notes at the time; written down here after the fact.

### Added
- **Definition version history owned by the engine**, with per-version process statistics (#145),
  and a per-repository git subdirectory plus a stopped/waiting heatmap in the definition viewer
  (#144).
- **Run a stopped process again, two ways.** *Retry from failure* picks it up where it stopped;
  *restart from the beginning* re-runs every step, including the ones that succeeded, with the
  variables the process was created with. Both from the process list, the process detail and MCP,
  and both now accept a CANCELLED process and not only a failed one.

### Fixed
- **The engine's own workers were dropping replies.** The forms engine answering a USER_TASK and
  the rule runtime answering a RULE step still called `streamBridge.send` and discarded the
  boolean — the line that left 3 356 processes stuck. Both now reply through `WorkerReply`, and
  the synchronous-producer default that makes a refusal detectable moved to `shared`, so it
  reaches every module that can answer the engine rather than only those that embed it. The demo
  workers too.
- **`DB_POOL_SIZE` and `DB_CONNECTION_TIMEOUT` did nothing.** Every shipped configuration put the
  Hikari settings under `spring.hikari.*`, a prefix Boot binds nothing from, so every deployment
  ran HikariCP's default of 10 connections whatever the environment said. Moving them inside
  `spring.datasource:` makes the documented values real — **the effective pool changes on deploy**.
- **The stalled-step gauge counted people.** `eventconductor.steps.stalled` counted every live step
  with no deadline, which is what a human task is by design, so any deployment with USER_TASK steps
  reported permanent stalled work and warned about it every minute. Only ACTION and RULE steps —
  the ones a worker owes an answer for — are counted now.
- **The documented upgrade path did not run.** Adopting Flyway over a schema `ddl-auto` had already
  built failed at `V11`, which added columns the entity already declares and dropped columns
  Hibernate never created; the application did not start. Both shapes migrate now, and
  `DdlAutoToFlywayUpgradeTest` runs the whole chain over a Hibernate-built schema.

  :warning: **Action required on databases where `V11` already ran successfully.** Editing an
  applied migration changes its checksum, and Flyway validates checksums at startup: those
  databases need a one-off `flyway repair` before the application will start. Databases that never
  got past `V11` — the broken path this fixes — need nothing.
- Path focus (click and alt+click) works again in the process view of the workflow graph.

## [1.0-beta.015] - 2026-08-03

### Fixed
- **The transactional outbox was not transactional: 71 messages in 642 912 were marked `Sent` and
  never reached the broker.** The relay delivers before marking the row, which is the right order
  and bought nothing, because the send was asynchronous — `StreamBridge.send` returns `true` the
  moment the record is buffered, so a broker that was down still produced a row marked `Sent` and
  a process that never moved again. Producer sends are now synchronous by default, contributed by
  the engine itself (`SynchronousProducerDefaults`) rather than left to each application's YAML,
  and a refused send throws so the row stays `Pending`. Measured cost: **0.6 ms per transition and
  4.4% of peak throughput**. Set `spring.cloud.stream.kafka.default.producer.sync=false` to opt
  out, deliberately.

- **Workers were dropping replies, in the pattern this project teaches.** Every worker here called
  `streamBridge.send(...)` and discarded the `false` it returns on refusal, so a reply the broker
  would not take vanished, the consumer committed the offset anyway, and the task was never
  reported as done. During a 90-second broker outage that lost **3 352 replies and left 3 356
  processes permanently stuck, with no error logged anywhere**. New `WorkerReply` retries and then
  throws, so Kafka redelivers the task — worker handlers must be idempotent, as at-least-once
  delivery always required.

- **A step with no timeout was invisible, not merely un-timed-out.** The deadline scan is an index
  range over the deadline column, so a step without one was never looked at again: if its dispatch
  or its reply was lost, the process stopped forever and nothing reported it. Two changes — the
  `eventconductor.steps.stalled` gauge counts live steps with no deadline that have waited past
  `workflow.stalled-step-after-ms`, and `workflow.default-step-timeout-ms` gives ACTION and RULE
  steps a fallback deadline that hands them to the existing retry path. Off by default; never
  applied to USER_TASK, PROCESS or WAIT_FOR_MESSAGE, whose waiting is unbounded by design.

- **A rollback the database made impossible was treated as a defective event.** Stopping
  PostgreSQL mid-transaction produced `JpaSystemException: Unable to rollback against JDBC
  Connection`, which the classifier did not recognise as retryable, and two process creations were
  dead-lettered for it. Connection-level `SQLException`s are now matched on SQLState — class `08`,
  class `53`, PostgreSQL `57P01`–`57P03` — as well as by type. A constraint violation stays
  non-retryable: retrying that forever is a poison pill.

- **`forms` and `rules` still shipped with Flyway off.** Only the orchestrator had been fixed, so
  both ran the schema `ddl-auto` builds, which has no indexes. Both now default `FLYWAY_ENABLED`
  to `true`. `DB_POOL_SIZE` defaults to 16 across the three apps.

- **One consumer thread per pod meant a deployment could never use more partitions than replicas.**
  `KAFKA_CONCURRENCY` now defaults to 3. Measured on a three-pod cluster with six partitions: four
  consumed, two unread, and no pod anywhere near CPU-bound.

- **PostgreSQL and Redpanda declared no resource requests**, which makes them BestEffort and the
  first pods the kubelet evicts. Scaling the orchestrator from 3 to 6 replicas evicted the broker
  and stopped the engine completely. Both now have requests and limits in the chart.

- **Redpanda's shard count is pinned (`--smp`).** It is written into the data directory as an
  invariant, so a broker rescheduled onto a smaller node refuses to start — permanently — with
  "Decreasing redpanda core count is not allowed". On any cluster with a node autoprovisioner that
  turns a routine reschedule into an outage.

- **Two migrations shared version 11**, which makes Flyway refuse to start and the app die at boot
  with no schema. Both branches had merged green because nothing on the build path reads these
  filenames. A test does now.

### Added
- **A reliability harness** (`modules/workflow-benchmark/k8s/reliability`): a soak driver, seven
  chaos scenarios — pod kill, whole-tier kill, rolling redeploy, broker outage, database outage,
  node drain, and replacing the workflow definition mid-flight — and the invariants that decide
  whether anything was lost. The verdict is computed from the engine's own tables against a count
  the driver writes to the database, because the harness is inside the blast radius.

  Results after the fixes: **zero stuck processes** (3 356 before), a full drain in 144 s (it never
  drained before), zero duplicate step executions across 230 417 dispatches including 44 genuine
  redeliveries, and the definition swap producing exactly two shapes and no hybrids. See the new
  [Reliability guide](https://eventconductor.io/guides/reliability/).

### Fixed
- **Flyway runs by default.** `FLYWAY_ENABLED` defaulted to `false` while `DDL_AUTO` defaulted to
  `update`, so the out-of-the-box standalone app built its schema with Hibernate and never ran the
  migrations — and the migrations are the only place the engine's indexes come from, since
  `ddl-auto=update` emits no index DDL. The default deployment therefore ran every deadline scan,
  outbox claim and message correlation as a sequential scan.

  Enabling it is safe on a schema `ddl-auto` already created: it baselines at V1 and every later
  migration is written with `IF NOT EXISTS`. **Existing installs that override
  `flywayEnabled: false` should turn it back on** — the Helm chart already defaults it to true, so
  this only bites deployments that overrode it or run the app without the chart. Turn Flyway on
  first, confirm the indexes appear, and only then move `ddlAuto` to `validate`.

- **The demo services had the shared-consumer-group defect too.** `content-service` and
  `users-service` used one group for their `upstream` and `outbox` bindings, same as the
  orchestrator did.

- **A consumer group per binding. Sharing one left Kafka partitions with no consumer at all.**
  The orchestrator's two bindings — `upstream` and `outbox` — used the same group, so its members
  had different topic subscriptions. The default range assignor handles that badly: observed on a
  cluster, 12 members held 7 of 12 partitions between them, and the 5 unassigned ones were simply
  never read. Every process whose next event landed there stopped, and the pods sat near idle
  while work piled up.

  It was never visible locally because the suite is short and a rebalance eventually reshuffles
  the gap onto a consumer. It was costing throughput the whole time: DIST-05 goes from ~60 to
  **78–91 PI/s** with nothing changed but the group names.

  Fixed in the standalone app, the distributed harness and the benchmark. Anyone running their own
  orchestrator wiring should check the same thing: a consumer group's members must subscribe to
  the same topics.

- **Indexes are declared on the entities, not only in the migrations.** They only ever existed in
  Flyway, so a schema built by `ddl-auto` had primary keys and nothing else, and every deadline
  scan, outbox claim and correlation lookup became a sequential scan. On a cluster with ~9k live
  step rows that pinned PostgreSQL at 750m of CPU.

  **`ddl-auto=update` still will not create them** — Hibernate's update path emits no index DDL,
  which is worth knowing before relying on it. `create` and `create-drop` do, so the test
  harnesses now exercise the same plan as production. For a real deployment the migrations remain
  the answer, and `spring.jpa.hibernate.ddl-auto=update` is not a substitute for running them.

### Changed
- **The relay is woken by the write instead of finding out on its next poll.** A pod that commits
  an outbox row raises a signal its own relay is waiting on; the poll interval stays as the
  fallback for rows written by other pods, which a pod has no way to hear about directly.

  The poll interval was never a scheduling preference — it was latency added to every step, and a
  transition crosses the relay twice. Measured on the benchmark harness at 40 instances/s, p50 per
  transition:

  | `workflow.outbox-poll-interval-ms` | before | after |
  |---|---|---|
  | 500 (the shipped default) | 508,8 ms | **10,1 ms** |
  | 20 | 28,6 ms | **10,7 ms** |

  Fifty times at the default, and still nearly three times better than the most aggressive polling
  worth running — which also stops mattering, since the interval no longer sets the latency.

  The signal fires **after the commit**, not at the write: raised inside the transaction it would
  wake the relay to look for a row not yet visible, which finds nothing, goes back to waiting, and
  spends the wakeup — leaving exactly the latency it exists to remove, with an extra query on top.
  It carries one permit rather than a count, because the relay drains until empty and only needs
  to know that something arrived.

### Fixed
- **An event that lost an optimistic race is now redelivered instead of disappearing.** The
  rejection was caught, counted, logged as "the event will be redelivered" — and then the handler
  returned normally, the consumer committed its offset, and the event was gone. Nothing redelivered
  it. The log line was simply wrong. A rejection now propagates as
  `ConcurrentProcessAccessException` all the way out of the consumer, which is what actually stops
  the offset from advancing over work that never happened. It is deliberately the one failure that
  is not swallowed: the event is not defective, it just lost a race, so retrying is the right
  answer rather than a log line.

- **An event the engine cannot process is parked instead of vanishing into a log line.** It used
  to be caught inside the event use cases, logged once, and forgotten: never retried, never
  visible, and invisible to anyone not reading that log. Those catches are gone. A failure now
  reaches whoever delivered the event, which is the only place that can decide what to do with it,
  because the answer depends on how it arrived.

  **The policy is retry forever, or park at once — never "retry N times, then give up".** N
  attempts is a guess about how long an outage lasts, and getting the guess wrong drops events
  that would have worked. `EventFailures` decides: only failures known to be about the
  environment — database unreachable, lock not obtained, a lost race — are retryable, and the
  classification is deliberately narrow because being wrong towards parking is the safer
  direction. A parked event can be replayed once someone understands it; an event retried for
  ever is a loop nobody reads.

  Where it parks depends on the topology, because the two have different queues. In `kafka` mode
  it goes to a **dead-letter topic** (`deadLetter` binding), payload untouched so replaying is
  just republishing it on the destination its `x-dead-letter-source` header names, with the
  failure in headers and the process still as the key. In `embedded` mode the outbox table *is*
  the queue, so the resting place is its existing `Error` status — visible in the table, replayed
  by setting the row back to `Pending`. That path also fixes a quieter bug: a failed message was
  left `Pending`, so an event that could never succeed was retried every cycle for ever.

  New counter `eventconductor.events.dead.lettered` — the one that should make somebody look, since
  a retry is the engine coping and a dead letter is the engine giving up. Specs `DIST-12` and
  `E2E-DLQ-01`.

### Changed
- **A Kafka poll batch is committed as one transaction per process, not one per event.** A busy
  batch carries several events for the same process, and collapsing those into a single commit is
  most of the write cost. Measured on DIST-05, three runs: **56,8 / 60,3 / 56,8 PI/s against
  50,8 / 51,0 / 53,7** before — about 12% on wall clock and 20% on the engine-side window.
  Consumers switch to batch mode (`batch-mode: true` on the two orchestrator bindings; turning it
  off falls back to one transaction per event).

  **Per process, not per batch — and that distinction is the whole design.** One transaction for
  the entire batch is the obvious shape and it is a trap: an event that fails inside its own
  participating transaction marks the shared one rollback-only *even when the failure is caught*,
  so every other event in the batch loses the commit it believed it had. The failure that makes
  this concrete is an optimistic conflict, which is precisely what a consumer-group rebalance
  produces — the moment the engine is least settled would be the moment it threw away the most
  work. Measured both ways: the two perform the same, so the batch-wide version buys nothing for
  its blast radius. `BatchTransactionScopeTest` pins the trap; `DIST-12` covers the behaviour.

  Events of a process keep their arrival order, and an event belonging to no process gets a
  transaction of its own so it neither drags others down nor is dragged down by them.
- **No per-process lock in `kafka` mode: exclusivity is inherited from the partition.** Events are
  keyed by process and a consumer group gives each partition to exactly one consumer, so a process
  already has a single writer. `PartitionOwnedProcessLockService` therefore only opens the
  transaction the work commits in; the optimistic version is what guards the rebalance window.

  Measured on DIST-05, three runs each: **44,0 / 46,1 / 50,7 PI/s with the row lock against
  50,8 / 51,0 / 53,7 without** — about 10% on the mean, and a visibly tighter spread, since the
  lock was the part that could wait. The lowest run without it beat the highest run with it.

  **`embedded` mode keeps the row lock**, deliberately: nothing partitions processes across pods
  there, so two of them can still reach the same process, and a version guards a *row* rather than
  a *decision* — two step-overs that read the same state and then write different rows collide on
  no version and would dispatch a step twice.

  That same reasoning left one hole in kafka mode, and it is closed rather than documented away:
  a worker on an older shared module reports back **unkeyed**, so its event can land on a pod that
  does not own the process. `UnkeyedEventRouter` now sends such an event back out with the process
  it belongs to instead of handling it where it landed — one indexed lookup and one extra hop,
  paid only by workers that do not echo the process, and inert outside kafka mode.
- **Operator actions travel as events, so they run on the pod that owns the process.** Pausing,
  resuming, cancelling and retrying (a whole process or one step) used to execute wherever the UI
  click or the MCP call landed — which, under partition ownership, is not the pod that owns the
  process. They are now published keyed by the process: `PauseProcessRequested`,
  `ResumeProcessRequested`, `RetryProcessRequested`, `RetryStepExecutionRequested`, and the
  long-declared but never-used `ProcessCancellationRequested`, which gains a `processId` because a
  process with no business key cannot be addressed by one. An operator action goes through the
  same single writer as everything else instead of being the one path that needs a lock to be safe.

  **In embedded mode nothing changes:** the upstream publisher dispatches in-process, so a UI
  action is still carried out synchronously before the call returns. It is only in `kafka` mode
  that these become routed — and there the MCP tools now answer "requested… query the process to
  see the outcome" rather than claiming the work is done.

  There are no REST endpoints for these operations; the entry points are the UI and MCP.
  Specs `E2E-OPS-01..05`.
- **Optimistic locking on `Process` and `StepExecution`.** Both aggregates now carry a `version`,
  checked on every write (migration `V11`, existing rows backfilled to 0 — a null version is how
  Spring Data recognises a row it has never persisted, so leaving them null would turn updates
  into failed inserts).

  This is the fence for the one hole in ownership. Keying events by process gives each process to
  a single pod, but a consumer group guarantees which consumer is *assigned* a partition, not
  which is still *in flight*: during a rebalance the outgoing pod can be finishing a record the
  incoming one has just been handed. A stale writer's update now matches no row at its version
  and is rejected, rather than quietly overwriting the new owner's work.

  It costs nothing when there is no conflict — no waiting, no lock held, no connection parked —
  which is what makes it able to replace the pessimistic lock rather than sit beside it.

  **Rejections are counted, not just logged**: `eventconductor.process.concurrent.writes.rejected`.
  That metric is the point of this change as much as the safety is. Outside a rebalance it must be
  flat at zero; anything else means something is reaching a process from outside its partition —
  exactly what has to be true before the pessimistic lock can be removed. Specs `E2E-LOCK-01..03`.
- **Events are keyed by process, so a process belongs to one pod.** Every event that concerns a
  process now carries it as the Kafka message key (`DomainEvent.partitionKey()`), so all of a
  process's events hash to the same partition — and a consumer group gives each partition to
  exactly one consumer. Per-process serialization stops being something the engine arranges after
  the fact and becomes a property of how events are addressed.

  **This also fixes ordering, which is a correctness matter rather than a performance one.** On an
  unkeyed topic two events of the same process land on different partitions and are handled
  concurrently by different pods, in whatever order they arrive; the per-process lock serialized
  access but never ordered it. What made that survivable was the terminal-status guards and the
  re-reads inside the lock. Keyed, the order is the order they were produced in.

  Two events that mutate process state carried only a task id and now carry the process too:
  `TaskStatusChanged` (a worker's reply, echoed from the `TaskExecutionRequested` it received) and
  `StepExecutionStatusChanged`. Both keep their previous constructor, which leaves the key null —
  so a third-party worker built against an older shared module still compiles, still deserializes,
  and simply falls back to the unrouted behaviour it has today. Events that write only their own
  independent row (`TaskLogEmitted`, `TaskResourceCreated`) stay unkeyed on purpose: pinning them
  to a partition would cost balance and buy nothing.

  Nothing is removed yet — the per-process lock stays as the safety net, since ownership is only
  a Kafka *assignment* guarantee and a rebalance can still put two pods on one process briefly.
  New spec `DIST-11` verifies the keys by reading the topics, not by trusting that `send` set one.
- **BREAKING (SPI): per-process exclusion is a row lock, not an advisory lock.**
  `ProcessLockService` loses `tryLock`/`unlock` and gains a single
  `runExclusively(processId, action)`. In JPA mode the action now runs in a transaction that opens
  by taking `SELECT … FOR UPDATE` on the process row, and the commit releases it. Anyone who
  implemented this port has to follow; nothing else about the engine's concurrency semantics
  changes.

  The reason is not elegance. An advisory lock is session-scoped, so acquiring it took a
  connection out of the pool and **held it for the whole critical section**, while the work inside
  needed a second one. Two connections per in-flight process made the pool size — not the database
  — the ceiling on concurrency, and past that point the failure was a **wedge, not a slowdown**:
  lock holders waiting for a connection to do the work they held the lock for. New spec `DIST-10`
  pins this down: 40 concurrent processes complete through a 3-connection pool, and restoring the
  two-connection shape exhausts it and stalls the processes outright.

  What else falls out: the stale-lock watchdog is gone, and with it the chance of force-releasing
  exclusivity from an operation still running; `ProcessLocks.lockWithRetry` is gone, because
  waiting is now the database's row-lock queue rather than a sleep-and-retry loop that reopened a
  connection on every attempt; exclusivity is reentrant within a transaction; and the lock key is
  the process id itself rather than a 64-bit fold of its UUID. Waiting is bounded by
  `workflow.process-lock-timeout-seconds` (default 10), applied as a statement timeout, which is
  portable in a way that per-vendor lock-timeout settings are not.

  `UpdateStepExecutionUseCase` drops its own `TransactionTemplate`: exclusivity and the
  transaction are now the same scope, so the inner one only joined the outer.
- **Every pod relays the outbox now — it is no longer a leader-elected singleton.** In `kafka`
  mode one pod drained the whole outbox while the others idled, and since every state transition
  passes through the relay, that put a ceiling on the distributed topology that adding pods could
  not lift. Relays now claim bounded batches with `FOR UPDATE SKIP LOCKED`, so N orchestrators
  drain N disjoint slices with no leader between them. New spec `DIST-09` proves the claims are
  disjoint, non-blocking and complete, against real PostgreSQL.

  The at-least-once contract is unchanged: a batch is claimed, published and marked `Sent` in one
  transaction, publishing still happens *before* the rows are marked, and a crash anywhere in
  between rolls back and releases the locks so another pod redelivers.

  Three related fixes in the same path: the relay **no longer loads the entire pending outbox**
  on every cycle (bounded batch, new `workflow.outbox.batch-size`, default 100) but keeps draining
  until it is empty so a backlog is not paced by the poll interval; per-message logging drops from
  `INFO` to `DEBUG`; and `workflow.outbox-poll-interval-ms` **defaults to 500 ms instead of
  5000** — it used to add up to five seconds of idle latency to every step hop in `kafka` mode,
  and the published DIST-05 throughput baseline was only ever reachable by overriding it.
  Migration `V10` indexes `(status, timestamp)`, which the claim's ordering needs.

  The **embedded** relay deliberately keeps its leader lock: its "delivery" is the engine running
  a step synchronously, taking the process lock and its own connections, and holding row locks and
  a transaction across all of that would mean long transactions and a plausible deadlock against
  the very work being dispatched. It does take the bounded fetch and the log-level fix.

  The relay's old advisory lock survives as a **shared** gate that every relay holds while
  draining: shared holders do not block each other, so this costs nothing, but a single exclusive
  holder still freezes every relay at once — the deterministic crash window DIST-02 and DIST-08
  are built on.
- **An arriving message finds its waiting steps by index.** A live `WAIT_FOR_MESSAGE` step now
  stores the subscription it represents — `awaiting_message_name` and `awaiting_correlation_key`
  — so correlating a `MessageReceived` is a lookup on those two columns instead of a walk over
  every step waiting anywhere in the engine, loading each one's process and evaluating its JEXL
  expression. Indexing the message name alone would not have helped: the case that hurts is many
  processes parked on the *same* message, where the key is the only selective part.

  **The correlation contract is unchanged**, and that is what most of the work went into. The key
  derives from process variables, and those move while a step waits — a parallel branch can write
  the very variable the expression reads. Evaluating on arrival made that free; storing it does
  not, so both paths that update process variables now rearm the subscriptions of that one
  process (`MessageSubscriptionService`), writing only the keys that actually moved. A message
  still correlates against the process as it is *now*. Spec `E2E-MSG-06` covers it. Fail-closed
  survives too: an expression that will not evaluate stores a null key, and null matches nothing.
  `CompleteMessageStepHandler` still re-checks the correlation against the live process under the
  process lock — the query is the filter, that check remains the decision.

  Steps already waiting when this version is deployed are armed at the next boot by
  `InFlightStepRearmRunner` (which also covers the deadline below). Migration `V9` adds the
  columns and `idx_step_exec_awaiting_message`.
- **The scheduler no longer walks every live step to find the due ones.** Each step execution now
  carries a **materialised deadline** (`deadline_at`) — a `TIMER`'s due moment or a step's timeout
  — armed when the step starts, from the `startedAt`, variables and step JSON that are frozen at
  that instant. The scheduler asks for `deadline_at <= now` over a new index instead of loading
  every PENDING/RUNNING step and re-evaluating each one on every tick. The cost of a tick now
  tracks *what is due* — normally nothing — rather than *what is waiting*, which is what the
  engine's own use case is made of: a check-in reminder is a `TIMER` sitting PENDING for weeks,
  and it used to be re-examined every ten seconds for all of them.

  The deadline is derived state, so it is recomputed by every path that moves the clock;
  `withDeadlineAt` is suppressed on the aggregate so it cannot be set on its own, and pause/resume
  (which shifts `startedAt` by the pause duration) moves both together. Steps already in flight
  when this version is deployed are armed at the next boot by `InFlightStepRearmRunner`, which
  recomputes them from the state they already carry — one query at startup, idempotent, a no-op
  from then on. Migration `V8` adds the column and `idx_step_exec_deadline`.

  No behaviour change, with one millisecond-scale exception: a step timeout falling exactly on the
  tick now fires on that tick instead of the next, matching what `TIMER` already did.

### Fixed
- **Timer and timeout checks no longer load every live step in the system.** `CheckTimerUseCase`
  and `CheckTimeoutUseCase` listed *all* PENDING/RUNNING step executions and filtered them by
  process in memory. Because the scheduler scan fans out one check event per due process, a
  single scan tick that found N due processes triggered N full loads of the live-step table, on
  top of its own. Both now query only the process they were commanded for, through a new
  `StepExecutionRepository.findPendingOrRunningByProcessId(processId)`. The cost matters most on
  the workloads the engine is built for — long waits, where tens of thousands of `TIMER` steps
  sit PENDING for weeks. New composite index `idx_step_exec_process_status` on
  `step_execution_entity (process_id, status)` replaces the process-only index it subsumes
  (migration `V7`). No behaviour change.

## [1.0-beta.014] - 2026-08-01

### Added
- **IDE plugins for VS Code and IntelliJ IDEA.** New editor plugins (under `plugins/`) open a
  workflow definition as an **interactive graph** or as plain **YAML/JSON**, both views editing the
  same file, with schema validation, and they embed the exact graph web component the app renders.
  VS Code ships a custom editor (graph by default, *Show YAML/JSON side-by-side*); IntelliJ
  (2024.2+) ships a split graph/text editor via JCEF. Published to the VS Code and JetBrains
  marketplaces. See [IDE Plugins](/guides/ide-plugins/).
- **`.ec` — a first-class workflow-definition extension.** An `.ec` file holds a definition as
  **JSON or YAML** (detected from the content and preserved on save). The git import and the
  classpath importer now read `.ec` alongside `.json` / `.yaml` / `.yml`.
- **JOIN gains an AND/XOR type.** A `JOIN` now carries a `joinType`: **`AND`** (default) is a
  synchronizing join that waits for all incoming branches; **`XOR`** is an exclusive join that
  proceeds as soon as any one completes. Null/absent = `AND`, so existing definitions are
  unaffected. The precondition check honours it (all-match for AND, any-match for XOR).
- **Live process state on the in-app graphs (read-only monitoring).** The workflow-definition view
  badges each node with how many live process instances currently sit on it; the process view gains
  a *Diagram* tab that shows the graph with the active step highlighted and the parts not yet
  reached dimmed. Driven by a new `overlay` property on the graph component.
- **Workflow graph editor — major UX pass.** Zoom/pan with fit-to-view and a minimap; dark-mode
  support outside Lumo (for the IDE webviews); BPMN-style event/gateway shapes with an
  exclusive-gateway (`×`) glyph for XOR joins; a token-flow simulation that dwells on long-running
  steps and, on an AND-join, lights up all its incoming branches to show it synchronising;
  node-avoiding, non-overlapping, shape-fitting edge routing; and drawing precondition lines by
  Shift+drag. The same built bundle is reused by both IDE plugins.
- **Whole-process saga rollback in reverse execution order.** When any step fails or times out
  after exhausting its retries, the engine now compensates **every executed compensable step**
  (completed steps plus the one that just failed) — not only the failed step's own
  compensation — running them **sequentially, in reverse execution order**: the latest-executed
  step is undone first, and each compensation starts only once the previous one completes. The
  next compensation is derived purely from persisted step-execution state (new
  `CompensationService`), so it is idempotent under redelivery and across restarts. A single
  failed compensable step is just the degenerate case of this cascade. See the new
  `COMPENSATED` terminal state under Changed.
- **Git reload webhook: multiple providers, targeted reload and pruning.** The
  `POST /{engine}/webhooks/{provider}` endpoint (workflow, forms and rule engines) now accepts
  `github`, `gitlab`, `bitbucket` and `generic` (`/github` keeps its behaviour), each
  authenticated with the configured `webhook-secret` — GitHub/Bitbucket HMAC-SHA256
  (`X-Hub-Signature-256`/`X-Hub-Signature`), GitLab (`X-Gitlab-Token`) and generic
  (`X-Webhook-Token`) tokens; a blank secret skips verification. The push payload is parsed to
  reload **only the repository and branch that changed** (an unmatched push is acknowledged and
  ignored; an unparseable payload falls back to reloading everything). Shared, reusable helpers
  live in a new `io.mateu.workflow.webhook` package. See pruning under Changed.
- **Workflow graph editor: multiple incoming preconditions per step.** The graph now renders
  and edits several `preconditionStepIds` into a single step (one edge per precondition, a
  multi-select editor) — the engine already honoured them; only the editor had modelled a
  single incoming edge.
- **Workflow graph editor: restyled, more expressive SVG.** The graph is redrawn in a richer
  visual language on the same Lit + ELK stack: per-type node cards with a corner glyph and an
  uppercase caption (`→ topic`, `👤 form`, `ƒ rule`, `⨝ JOIN`, `✉→ message`, …), dashed
  `FORK`/`JOIN` gateways, orthogonal rounded edges with arrowheads, a themeable palette
  (`--ec-*` custom properties, with a Lumo dark-mode mapping), and a step's
  `preconditionExpression` guard shown as a chip on its incoming edge.

### Changed
- **At most one `START` per workflow.** More than one `START` step is now rejected at validation.
  Multiple `END` steps remain valid — a flow may finish through several distinct outcomes.
- **Gateway-model guidance (warnings, not errors).** The validator now logs non-fatal warnings
  nudging a multi-input step toward a `JOIN` (with explicit AND/XOR semantics) and a multi-output
  step toward a `FORK`. Compensation anchors are excluded and conditional (guarded) splits stay
  allowed; it never blocks a definition.
- **BREAKING: a fully compensated process ends `COMPENSATED`, not `ERROR`.** A failed process
  that runs its saga rollback to completion now reaches the new terminal
  `ProcessStatus.COMPENSATED` instead of remaining `ERROR`; if a compensation itself fails
  after its retries, the chain halts and the process stays `ERROR`. `COMPENSATED` is a sticky
  terminal failure state (like `ERROR`), distinguished by whether the side effects were undone.
  Consumers, queries and dashboards that treat `ERROR` as the only failure terminal — and a
  parent `PROCESS` step, which now also errors on a `COMPENSATED` child — should account for
  the new state.
- **BREAKING: the git reload webhook reloads a subset and prunes removed definitions.**
  Previously every webhook call re-imported **all** configured repositories and only
  added/updated definitions. It now reloads only the repository and branch named in the push,
  and definitions that were removed from a repo are **pruned** — workflow definitions are
  archived (`ARCHIVED`), forms and rules are deleted (git-imported definitions only, never
  classpath or hand-authored ones; tracked per running instance). Only definitions with an
  explicit `id` are prune-tracked.

## [1.0-beta.013] - 2026-07-31

Cumulative since `1.0-beta.010` — releases `1.0-beta.011` and `1.0-beta.012` were cut
without changelog sections, so their contents are included below.

### Added
- **Pause/play for processes and workflow definitions.** New `ProcessStatus.PAUSED`:
  `PauseProcessUseCase` pauses a `PENDING`/`RUNNING` process and `ResumeProcessUseCase`
  puts it back to `RUNNING`. Pause freezes the frontier, not in-flight work — running
  workers finish and their reports are accepted (steps complete, variables merge), and
  messages still complete `WAIT_FOR_MESSAGE` steps — but successors do not start until
  resume. Clocks freeze too: timeout and TIMER scanning skip paused processes and, on
  resume, every non-terminal started step's `startedAt` is shifted forward by the pause
  duration (recorded in the new `Process.pausedAt`), so step timeouts and timer
  due-moments resume where they left off; blocking-error handling is deferred the same
  way, and cancelling from `PAUSED` works. At the definition level, a new runtime
  `paused` flag (orthogonal to the `DRAFT`/`ACTIVE`/... lifecycle; in the schema as
  nullable boolean, default `false`, so exports round-trip): `PauseWorkflowUseCase` sets
  it and pauses all the definition's `PENDING`/`RUNNING` processes, while new instances —
  cron occurrences included — are still accepted and created **born-`PAUSED`**;
  `ResumeWorkflowUseCase` clears it and resumes all its `PAUSED` processes. Surface:
  **Pause**/**Resume** toolbar actions on the process detail and on the definition detail
  (plus a **Paused** row), and four MCP tools — `pauseProcess`, `resumeProcess`,
  `pauseWorkflow`, `resumeWorkflow`. Flyway migration V6 in the orchestrator app
  (`process_entity.paused_at`, `workflow_definition_entity.paused`).
- **`START` step type — explicit workflow entry points.** A no-worker node that completes
  instantly at process creation, fanning the flow out to its successors. A `START` must have
  no preconditions, and declaring several gives the process concurrent entry branches. Every
  flow must now enter through a `START` or a `WAIT_FOR_MESSAGE` (see the roots rule under
  Changed).
- **`FORK` and `JOIN` are now implemented, on multi-preconditions.** Steps gain
  `preconditionStepIds` (array): ALL the listed steps must complete before the step starts;
  the singular `preconditionStepId` remains valid (the plural wins when both are set). `FORK`
  and `JOIN` are no-worker nodes that complete instantly — `FORK` is the explicit fan-out
  (all its successors start concurrently when it completes), and `JOIN`'s barrier is exactly
  its multiple preconditions (the converge point of parallel branches). Precondition-cycle
  detection is now a DFS over the multi-edge graph. The Maven plugin emits a build-time
  warning (never a failure) when a `JOIN` waits directly on a guarded step — if the guard is
  false the join never fires and the flow beyond it is cancelled.
- **`PROCESS` step type — child workflows are now implemented.** A `PROCESS` step (required
  `childWorkflowDefinitionId`, which must differ from the workflow's own id) starts a child
  process carrying ALL parent variables under the deterministic businessKey
  `parent:<stepExecutionId>` — idempotent, duplicate creation events are deduped. The parent
  step waits `PENDING`; when the child completes, the parent step completes and copies back
  only the child variables named in the new `outputVariables` step field (absent/empty =
  none); a child ending `ERROR` or `CANCELLED` puts the parent step in `ERROR` (normal
  retry/compensation pipeline), and `timeout` bounds the wait. `Process` gains
  `parentStepExecutionId` (Flyway migration V5 in the orchestrator app). Cancellation
  propagates parent→child: a parent `PROCESS` step ending `CANCELLED`, `ERROR` or `TIMEOUT`
  (retries exhausted) cancels a still-running child, cascading to grandchildren; while
  retries remain the child keeps running and a retried step re-attaches to it through the
  deterministic business key.
- **Graph and build-time validation support for the new model.** The workflow graph renders
  multi-precondition edges and `START`/`FORK`/`JOIN` nodes, and the Maven plugin's
  `SpecValidator` mirrors all the new invariants (roots rule, START-without-preconditions,
  plural precondition references, multi-edge DFS cycle detection, the PROCESS child id).
- **`SEND_MESSAGE` step type — fire-and-forget in-engine messaging.** The throw side of
  `WAIT_FOR_MESSAGE`: on start the engine evaluates the step's `correlationExpression` (JEXL,
  same context as preconditions), emits a `MessageReceived(messageName, correlationKey,
  variables)` through the outbox and completes the step immediately — no worker, no `ACTION`
  step needed for process-to-process signaling. The new optional `messageVariables` field
  (array of process-variable names) selects which variables the outgoing message carries;
  empty or absent means none — process state is never sent implicitly. Delivery is not
  acknowledged and a message matching no waiting process is discarded (not buffered). A
  missing `messageName`/`correlationExpression` or an unevaluable correlation key puts the
  step in `ERROR` (fail loud, normal retry/compensation pipeline) — deliberately not the
  silent fail-closed of precondition guards. The Maven plugin's `SpecValidator` now also
  checks `correlationExpression` JEXL syntax at build time.
- **`message-received` deliverable via the Kafka `upstream` topic.** `MessageReceived` is now a
  registered `DomainEvent` subtype (`"type": "message-received"`), so external producers can
  resume waiting `WAIT_FOR_MESSAGE` steps by publishing raw JSON to `upstream` — previously
  only REST (`POST /workflow/api/messages`), the MCP `sendMessage` tool or the Java API could.
- **Engine observability — metrics parity across all engines.** The Micrometer metrics pattern
  that already existed in the workflow engine (an `application/out` port with no-op defaults, a
  Micrometer implementation in `autoconfigure`, and an autoconfiguration guarded on a
  `MeterRegistry` bean) has been mirrored into the other engines. Metrics stay inert unless the
  host application provides a `MeterRegistry` (e.g. via Spring Boot Actuator), so libraries still
  run with zero observability dependencies.
  - **forms-engine** (`FormsMetrics`): `eventconductor.forms.task.created`,
    `eventconductor.forms.task.completed`, `eventconductor.forms.task.cancelled`,
    `eventconductor.forms.task.duration`, `eventconductor.forms.imported`.
  - **rule-engine** (`RuleCatalogMetrics`): `eventconductor.rule.catalog.saved`,
    `eventconductor.rule.catalog.deleted`, `eventconductor.rule.catalog.imported`,
    `eventconductor.rule.catalog.served`.
  - **rule-runtime** (`RuleRuntimeMetrics`): `eventconductor.rule.evaluation.count`,
    `eventconductor.rule.evaluation.duration`, `eventconductor.rule.evaluation.cache`. The
    runtime keeps working as a plain (non-Spring) library — the metrics port defaults to a no-op
    via overloaded constructors.
- **Distributed tracing (OpenTelemetry over OTLP).** The `orchestrator`, `forms` and `rule`
  standalone apps now ship `micrometer-tracing-bridge-otel` + `opentelemetry-exporter-otlp` and
  expose `management.tracing` / `management.otlp.tracing` configuration. Tracing is **off by
  default** (`TRACING_SAMPLING=0.0`); set `TRACING_SAMPLING` and `OTLP_TRACING_ENDPOINT` to emit
  spans. Because it is enabled at the app layer, HTTP, Kafka (Spring Cloud Stream) and JDBC calls
  are auto-instrumented and trace context propagates across the engines' async boundaries without
  any engine-code changes — consistent with the metrics' "optional, host-activated" design.
- **Observability docs** — the docs site gains a dedicated Reference → Observability page
  (`doc/src/content/docs/reference/observability.md`) covering the metrics of every engine
  (workflow, forms, rule catalog, rule runtime) plus a Distributed tracing section (how to
  enable Prometheus scraping and OTLP tracing); the Configuration reference keeps a short
  pointer to it.
- **Boot without the database and resume when it appears.** `DbLockDialectFactory` was the only
  startup-time database access left: it now falls back to inferring the lock dialect from the
  JDBC url when the database is unreachable (the pollers already retry every tick), so an
  orchestrator configured for resilience (lazy pool, `ddl-auto: none`, explicit dialect) boots
  in seconds with PostgreSQL down, and a process parked mid-flight with pending outbox rows
  completes once the database returns. Proven by a new distributed chaos test (DIST-08) that
  pauses/resumes PostgreSQL with `docker pause`, like the Kafka chaos tests.
- **REST endpoint to deliver messages to `MESSAGE` steps.** `POST /workflow/api/messages`
  publishes a `MessageReceived` through the same upstream surface as Kafka, the embedded
  publisher and the `sendMessage` MCP tool, so webhooks and SaaS callbacks that cannot produce
  to Kafka can resume waiting `MESSAGE` steps. Fire-and-forget (`202 Accepted`), with an
  optional `X-Api-Key` guard configured via `workflow.message-api.api-key`.
- **Read-only workflow definition detail view with graph.** Selecting a definition in the CRUD
  now opens a dedicated `WorkflowDefinitionDetailView` (the "view" action) instead of the
  editor: name as the title, lifecycle status as a header badge, a compact property list and a
  read-only ELK graph side by side, with the steps as a full-width band below. The editor
  layouts were tidied (5-column definition grid; the Step form's Main / Precondition /
  Execution / Reliability sections sit side by side), a `PROCESS` step can no longer pick the
  workflow it belongs to as its child, and the graph component gains a read-only mode (toolbar
  and node-detail panel hidden, capped height with a full-screen expand button).
- **Lifecycle actions on the detail view + Export YAML.** The read-only detail view now carries
  the full lifecycle toolbar (*Promote to production*, *Create working copy*, *Disable*,
  *Enable*, *Reactivate*, *Archive*) with the same visibility rules as before, so an `ACTIVE`
  definition can again be disabled or copied from the UI. Any `DRAFT` is now promotable (a
  standalone draft is activated in place), and *Export YAML* downloads the definition in the
  exact shape the git/classpath importers read back.
- **Definition-level guardrails against runaway loops.** `WorkflowDefinition.checkInvariants()`
  now rejects precondition cycles (a step waits for its `preconditionStepId` to complete, so a
  cycle deadlocks; the error names the cycle), and the model and JSON schema gain per-step
  execution caps — `Step.maxSuccessfulExecutions` and
  `WorkflowDefinition.defaultMaxStepExecutions` (`0` = inherit/unbounded) — carried through
  working copies, imports and persistence, to be enforced when step re-execution lands.

### Changed
- **BREAKING: the execution model is now pure dataflow.** A step starts when it is `CREATED`,
  ALL its preconditions have `COMPLETED` and its `preconditionExpression` is truthy — and
  every eligible step starts **concurrently**. The old scheduling semantics are gone: array
  order no longer matters, an active step no longer serializes independent chains, and
  `parallel: true` no longer opts into concurrency — the flag is **deprecated and ignored**
  (it still deserializes, so persisted stepJson and old files keep loading, but it has no
  effect). Parallelism is expressed structurally: shared preconditions fan out (`FORK` makes
  it explicit), multiple preconditions form a barrier (`JOIN`).
- **BREAKING: roots rule — every flow must enter through a `START` or `WAIT_FOR_MESSAGE`.**
  A step with no preconditions must be one of those two types; definitions violating this are
  rejected at load (and by the Maven plugin at build time). Migration: add one `START` step
  and point your old first steps at it; anchor compensation steps to the step they compensate
  with `"preconditionExpression": "false"` (the compensation pipeline starts them directly,
  ignoring the guard).
- **BREAKING: `MESSAGE` step type renamed to `WAIT_FOR_MESSAGE`, and `correlationExpression`
  is now required on both message step types.** New or reimported definitions must use the
  new name and declare `correlationExpression` explicitly — the old defaults-to-`businessKey`
  behavior is gone; add `"correlationExpression": "businessKey"` to keep it. A legacy
  deserialization alias keeps old persisted stepJson and definition files loading (`MESSAGE`
  maps to `WAIT_FOR_MESSAGE`, and those legacy steps retain the businessKey fallback), so
  in-flight processes survive the upgrade. To support the idiom, `businessKey` is now seeded
  as a plain variable into every JEXL evaluation context (correlation **and** precondition
  expressions) — property access such as `process.businessKey` was never evaluable, because
  the JEXL engine deliberately runs with `RESTRICTED` permissions on untrusted expressions.
- **Upgraded Mateu to `3.0-alpha.271`.** `3.0-alpha.271` is a breaking release that removed the
  UI CRUD API (`CrudRepository`, `CrudAdapter`, `CrudEditorForm`, `CrudCreationForm`,
  `ListingBackend`, the `core.infra.declarative.Listing` base and `AutoNamedView`) and split
  `Searchable` into a marker interface plus a new `SearchableText`. The admin-UI layer of all
  engines was migrated:
  - `CrudRepository` → `CrudStore` (the method set is unchanged) across ~24 ports/pages/adapters.
  - Pages whose detail view differs from the row (`Processes` → `SimpleProcessViewModel`,
    `WorkflowDefinitions` → `WorkflowDefinitionDetailView`) and the read-only `Tasks` listing now
    extend `Crud<View, …>` directly instead of `AutoCrud`/`FilteredAutoCrud` (which pin
    `View = Row`), because `Navigable.view()` is now generically typed.
  - `rule-engine` was pinned to Mateu `3.0-alpha.222`; it now tracks `${mateu.version}` like the
    rest of the reactor.
  - In the demo services (booking, content, control-plane, shell, users) the `CrudAdapter` layer
    was dissolved into the crud itself: every crud now implements the whole lifecycle
    (`search(SearchRequest)`, `view`, `edit`, `creationForm`, `save`, `create`, `deleteAllById`)
    in a single `Crud`/`AutoCrud`/`FilteredAutoCrud` subclass, and the demo `*CrudAdapter`
    classes were deleted. The workflow engine keeps its adapters (`SimpleProcessCrudAdapter`,
    `StepExecutionsCrudAdapter` and the process child-crud adapters) as collaborators of the
    migrated pages.
  - Testbench UI apps declare `spring-boot-starter-webmvc` explicitly: the Mateu MVC annotation
    processor now generates SSE-capable controllers and the engines only carry the starter as
    `optional`.
  - Everything builds against the released `3.0-alpha.271` — the engine reactor and the
    standalone demo and testbench apps (which have their own poms outside the reactor) alike.
- **The Workflow Definitions admin page is now read-only** (list + rich detail view). Definitions
  are authored as YAML and loaded from the classpath, Git or the database, and were never created
  or edited from this page; the write actions are disabled rather than reimplemented on the new
  API.

### Removed
- Dead `ProcessCrudAdapter` (superseded by `SimpleProcessCrudAdapter`; only referenced by an
  unused import) was deleted as part of the Mateu 271 migration.

### Fixed
- **`END` no longer records co-eligible sibling steps as `COMPLETED` without running them.**
  When an `END` became eligible in the same transition as another executable step, that step
  was silently marked `COMPLETED` even though it never ran; it is now `CANCELLED` like every
  other in-flight step the `END` terminates.
- **`MessageReceived` was missing from `DomainEvent`'s `@JsonSubTypes`**, so a raw
  `message-received` event published on the Kafka `upstream` topic could not be deserialized.
  It is now registered as `message-received`.
- **Startup failure on databases with existing workflow definitions**: the new NOT NULL
  `default_max_step_executions` column could not be added by `ddl-auto=update` (Postgres rejects
  it without a default). `@ColumnDefault("0")` fixes Hibernate-managed schemas and a V4 Flyway
  migration covers Flyway-managed deployments; `0` = unbounded.
- **Process detail view**: explicit tab names keep *steps*, *log*, *errors* and *resources* as
  separate tabs (consecutive bare `@Tab` fields now merge into one tab in Mateu), and
  `CreateProcessForm` marks the form clean on create — creating *is* the save, so no
  "save before leaving?" prompt.
- **Workflow definition editor**: removed the backward-compatible `WorkflowDefinition`/`Step`
  constructors that made Mateu's `ReflectionInstanceFactory` build empty objects on save
  (`name=null`, `steps=[]`), and the precondition/compensation step lookups now exclude the step
  being edited, so a step can no longer be offered as its own precondition or compensation.

## [1.0-beta.010] - 2026-07-24

### Fixed
- **Kafka broker resilience.**
  - Corrected the default Kafka broker address to `localhost:9092` (was a `9192` typo), so
    a standalone app started from the IDE without `KAFKA_BROKERS` set connects to the local
    dev broker instead of rebootstrapping in an endless loop.
  - Standalone apps now **boot gracefully when the broker is unavailable at startup**. The
    Spring Cloud Stream binder's provisioning/admin timeouts are bounded and binding retry
    is enabled, so the context no longer blocks ~2 minutes (and loses its AdminClient) — it
    starts promptly and binds its consumers as soon as the broker is up.

### Added
- **Distributed chaos tests for Kafka broker outages** (`workflow-dist-e2e`): a process
  recovers when the broker disappears mid-flight and returns — driven by the transactional
  outbox (DIST-06) — and the orchestrator boots and processes normally when the broker is
  unavailable at startup and later returns (DIST-07).

## [1.0-beta.009] - 2026-07-23

### Added
- **Workflow definition lifecycle management** in the admin UI. Definitions move
  through `DRAFT` → `ACTIVE` → `DISABLED` → `ARCHIVED` with per-status toolbar
  actions: *Promote to production* (working copies only), *Create working copy*,
  *Disable*/*Enable*, *Archive* and *Reactivate*. An `ACTIVE` definition is
  read-only (edited through a working copy) and must be disabled before it can be
  archived; *Reactivate* returns an archived definition to `DRAFT`. New
  definitions are created as `DRAFT` and the status is never editable. Documented
  with a state diagram in `/guides/workflow-definitions`.
- EventConductor now **owns the workflow graph component** (moved out of mateu),
  served as a self-contained web component from the engine jar.

### Changed
- **Migrated to Spring Boot 4** (from 3.5): starter/autoconfig relocations,
  Jackson 3, Spring Cloud 2025.1, Spring AI 2.0 and networknt
  json-schema-validator 3.0.
- Adopted **mateu 3.0-alpha.263** (new `CrudStore` API). This brings the UI
  behaviour the lifecycle work relies on: the top navigation renders as a Vaadin
  menu-bar, an intermediate menu route (e.g. `/workflow`) shows a section index
  instead of "Not found", and the built-in *Edit* action is hidden on `ACTIVE`
  definitions.
- Coordinated **gRPC 1.83.0 + protobuf 4.35.1** bump; commons-jexl3 3.7.0; and
  Spring Cloud dependency updates.

## [1.0-beta.008] - 2026-07-17

### Added
- `workflow-maven-plugin`: a Maven plugin (goal `eventconductor:validate`, bound
  to `process-resources`) that validates workflow, form and rule definitions
  (JSON/YAML) against the engine's published specifications at build time and
  fails the build on any violation. It bundles the canonical schemas straight
  from the sibling engine modules so it can never drift, and adds the semantic
  checks a schema cannot express: workflow duplicate/dangling/self-referencing
  step references, cron-expression validity and JEXL parseability of
  preconditions; decision-table row arity and JEXL parseability of rule
  expressions. Configurable directories, per-type toggles, `failOnError`,
  `failOnMissing` and `skip`.

## [1.0-beta.007] - 2026-07-16

### Added
- `RULE` step type backed by a new rule-engine catalog and embeddable
  rule-runtime: a step references a `ruleId` and the engine dispatches it for
  evaluation.
- Forms **Tasks v2**: the pending-tasks listing renders the completion form
  inline in each row, with an optimized query behind it.
- `TIMER` step type: durably pauses a process for an ISO 8601 duration
  (`duration: "PT72H"`) or until an absolute date taken from a process variable
  (`untilVariable`), fired by the timeout scheduler in both memory and JPA modes.
- `MESSAGE` step type with correlation: a step waits (durably, dispatching no
  task) for a `MessageReceived` correlated by the process `businessKey` or a
  `correlationExpression`; the message variables merge into the process.
- Cron-scheduled process starts: a definition can declare a `cronExpression`
  and the engine creates instances at each occurrence with deterministic
  business keys so multiple pods never duplicate an occurrence. New
  `workflow.cron-scan-interval-ms` and `workflow.cron-enabled` properties.
- Built-in process analytics (`ProcessAnalyticsService`): per definition and
  time window, instance counts by status, completion/error/cancellation rates,
  throughput per day and average/p95 durations per process and per step with a
  bottleneck flag — in all deployment modes. Surfaced in an **Analytics** UI
  page and the `getWorkflowAnalytics` / `findBottleneck` MCP tools. Step
  executions record `finishedAt` on terminal status (Flyway `V3`).
- Public test specification ([TESTING.md](./TESTING.md)) and the matching
  suites: an embedded/JPA end-to-end suite (`workflow-e2e`) covering
  orchestration semantics, failure handling, timers, message correlation,
  cron starts, idempotency, the JEXL sandbox and analytics; a JPA durability
  suite driving the real outbox relay and JDBC advisory locks on H2; a
  single-JVM crash-recovery test; and a Docker-gated distributed suite
  (`workflow-dist-e2e`, `dist-e2e` profile) over real Kafka + PostgreSQL via
  Testcontainers — distributed happy path, crash recovery, two-pod dispatch
  exclusivity, worker-crash redelivery and a 500-instance load smoke.

### Changed
- Bumped the Mateu UI dependency to `3.0-alpha.243` and centralized it as a
  `mateu.version` property.

### Fixed
- A process whose step exhausted its retries is now reported as `ERROR`
  instead of being falsely marked `COMPLETED`, and successors of a failed
  step no longer run.
- Precondition (JEXL) expressions are evaluated in a sandbox
  (`JexlPermissions.RESTRICTED`, blocking reflection and system access) and
  now fail closed — an evaluation error means the guarded step does not run.
- Outbox relays publish before marking a message `Sent` (at-least-once);
  messages that cannot be loaded are parked as `Error` rather than retried
  forever, and message types are validated against an `io.mateu.*` allowlist.
- Process creation deduplicates by `businessKey`, so a redelivered creation
  event no longer creates a duplicate process.
- Cancellation marks the process `CANCELLED` first so the orchestration loop
  cannot dispatch new steps mid-cancellation, and late worker reports for
  steps already in a terminal state are ignored.
- Step timeouts now fire in memory-persistence mode, not only in JPA mode.
- Definition validation rejects duplicate step ids and dangling
  `preconditionStepId` / `compensationStepId` references.
- Relay and scheduler threads are daemon threads with configurable poll
  intervals.

## [1.0-beta.006] - 2026-07-02

### Added
- Spring Boot Actuator with Prometheus, health, and info endpoints in
  `workflow-engine` and `forms-engine`.
- Custom Micrometer metrics for processes by status, step executions and
  retries.
- Spring Security with HTTP Basic on REST endpoints, configurable via
  application properties. Actuator health/info remain public.
- Flyway migrations for the workflow and forms schemas. Default `ddl-auto`
  is now `validate` in production profiles.
- Logstash-encoder structured JSON logging via `application-prod.yaml`.
- LICENSE (MIT), SECURITY.md, CONTRIBUTING.md, CODE_OF_CONDUCT.md and
  CHANGELOG.md.
- Dependabot configuration for Maven, GitHub Actions, Docker and npm.
- Build status, CodeQL and license badges in the README.
- Helm chart hardening: external secrets, NetworkPolicy, PodDisruptionBudget,
  Ingress template and Actuator-based probes.
- New PR CI workflow that runs `mvn verify` (unit + integration tests +
  JaCoCo coverage check) and Trivy container scans.
- Renamed misspelled `buid-and-publish.yml` to `build-and-publish.yml` and
  enabled running the test suite during release builds.

### Changed
- `workflow.mode` now defaults to `embedded` (previously `kafka`), mirroring
  `workflow.persistence` which defaults to `memory`. Apps start fully
  in-process with no external dependencies and opt into JPA/Kafka as they
  scale. The standalone distributed apps set `kafka`/`jpa` explicitly.
- Bumped the Mateu UI dependency to `3.0-alpha.222`.
- Dockerfiles run as a non-root `app` user (UID 10001).
- `docker-compose.yml` split into `docker-compose.dev.yml` (with default
  passwords) and `docker-compose.yml` (with required env vars).

### Fixed
- Quote the reserved SQL column `values` in `FormExecutionEntity` so schema
  creation succeeds on H2 and PostgreSQL.

### Security
- All REST endpoints in the orchestrator and forms standalone apps now
  require authentication by default.
- Bumped Eclipse JGit to `6.10.1.202505221210-r` to fix the XXE vulnerability
  CVE-2025-4949 (GHSA-vrpq-qp53-qv56).

## [Earlier]

EventConductor pre-1.0 snapshots. See the `git log` for individual commits.
