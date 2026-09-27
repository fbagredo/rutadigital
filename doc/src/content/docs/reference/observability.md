---
title: Observability
description: Metrics (Micrometer/Prometheus) and distributed tracing (OpenTelemetry/OTLP) for every EventConductor engine.
---

EventConductor exposes **metrics** and **distributed tracing** through
[Micrometer](https://micrometer.io/). Both are *optional and host-activated*: the engine libraries
carry no hard observability dependency and behave as no-ops until the host application provides the
relevant infrastructure (a `MeterRegistry` for metrics, a tracer for tracing). The `orchestrator`,
`forms` and `rule` standalone apps wire both.

## Metrics

All engines (workflow, forms, rule catalog and rule runtime) publish engine-level metrics through
Micrometer. They activate automatically when a `MeterRegistry` bean is present in the host
application — no property is needed. The easiest way to get one (plus a Prometheus scrape endpoint)
is:

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-actuator</artifactId>
</dependency>
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-registry-prometheus</artifactId>
</dependency>
```

```properties
management.endpoints.web.exposure.include=health,prometheus
```

Metrics are then available at `GET /actuator/prometheus`. Without a `MeterRegistry` each engine
falls back to a no-op implementation with zero overhead — Micrometer is an optional dependency of
every engine module.

Instrumentation happens at the use-case layer, so the same metrics are emitted in **all deployment
modes** (embedded + memory, embedded + jpa, kafka + jpa).

### Workflow engine

| Metric | Type | Tags | Description |
|---|---|---|---|
| `eventconductor.process.started` | Counter | `workflowDefinitionId` | Processes started |
| `eventconductor.process.completed` | Counter | `workflowDefinitionId` | Processes completed successfully |
| `eventconductor.process.errored` | Counter | `workflowDefinitionId` | Processes finished in error (retries exhausted) |
| `eventconductor.process.cancelled` | Counter | `workflowDefinitionId` | Processes cancelled |
| `eventconductor.process.duration` | Timer | `workflowDefinitionId`, `outcome` | Process duration from start to final status (`COMPLETED` \| `ERROR` \| `CANCELLED`) |
| `eventconductor.step.executions` | Counter | `workflowDefinitionId`, `outcome` | Step executions finished, by outcome (`COMPLETED` \| `ERROR` \| `TIMEOUT`) |
| `eventconductor.step.duration` | Timer | `workflowDefinitionId`, `outcome` | Step execution duration, from dispatch to final status |
| `eventconductor.step.retries` | Counter | `workflowDefinitionId`, `trigger` | Retries performed (`auto` = retry policy, `manual` = user-initiated) |
| `eventconductor.step.compensations` | Counter | `workflowDefinitionId` | Compensation steps triggered after retries were exhausted |
| `eventconductor.compensations.failed` | Counter | `workflowDefinitionId` | Saga rollbacks that could **not** complete — a compensation step itself failed, leaving the process partially rolled back (`COMPENSATION_FAILED`). Alert on any non-zero rate: it is business state left inconsistent for a human to resolve |
| `eventconductor.process.running` | Gauge | — | Processes currently in `RUNNING` status |
| `eventconductor.outbox.pending` | Gauge | — | Outbox messages waiting to be relayed (only with `workflow.persistence=jpa`; the in-memory mode has no outbox). Rises during a broker outage and must return to zero |
| `eventconductor.steps.stalled` | Gauge | — | ACTION and RULE steps with **no deadline** that have been waiting on a worker longer than `workflow.stalled-step-after-ms`. The one gauge to alert on — see below |

**Alert on `eventconductor.steps.stalled`.** Every other metric here counts something happening;
this counts work that has stopped happening where nothing in the engine will notice. A step that
declares no timeout has no deadline, and the timeout scan is an index range over the deadline — so
if that step's dispatch or its worker's reply is lost, the process stops permanently and silently.
Any sustained non-zero value is work that will never finish. Give those steps a timeout, or set
`workflow.default-step-timeout-ms`. See [Reliability](/guides/reliability/).

Only ACTION and RULE steps count, because only they are owed an answer by a worker. A `USER_TASK`
waits for a person, a `WAIT_FOR_MESSAGE` waits for a message that may never arrive, and a `PROCESS`
waits for its child — none of them has a deadline, and none of them is stalled. Counting those made
the gauge permanently non-zero in every deployment with human tasks.

The number is **cluster-wide and reported by every pod**: it counts rows in a shared table rather
than one pod's share of the work. Alert on `max()` across replicas, not `sum()`, which multiplies
it by the replica count. It reads zero in `workflow.persistence=memory`, which keeps no such
table — there it means "not measured", not "nothing stalled".

#### Synchronous invocation

| Meter | Type | Tags | Meaning |
|---|---|---|---|
| `eventconductor.sync.invocations` | counter | `workflowDefinitionId` | Invocations that started a process (retries of an existing one are not counted) |
| `eventconductor.sync.answers` | counter | `workflowDefinitionId`, `outcome` | Callers answered, by outcome — `DEADLINE` means a 202 |
| `eventconductor.sync.response.latency` | timer (histogram) | `workflowDefinitionId`, `outcome` | Request arriving → answer ready |
| `eventconductor.sync.deadline.expired` | counter | `workflowDefinitionId` | Callers whose deadline passed before the reply |
| `eventconductor.sync.rejected` | counter | `workflowDefinitionId`, `reason` | Invocations refused before creating anything |
| `eventconductor.sync.waiting` | gauge | — | Callers waiting on this node |
| `eventconductor.sync.wakeups` | counter | `via` | How replies reached waiting callers: `registration`, `local`, `notify`, `poll` — a rising `poll` share means NOTIFY is not working |
| `eventconductor.sync.inline.steps` | summary | — | Outbox messages handled per inline drive (the fast path) |
| `eventconductor.sync.inline.fallbacks` | counter | `reason` | Inline drives that stopped early or never started: `saturated`, `budget`, `claim_lost`, `failure` |

#### HTTP calls (`worker-http`)

| Meter | Type | Tags | Meaning |
|---|---|---|---|
| `eventconductor.http.calls` | timer | `target` (connection or host), `status` (code, `timeout`, `io`) | Every HTTP_CALL request and how long it took |

Tracing: the request's span `eventconductor.sync.invoke` carries a **link** to the process's anchored trace, and the `eventconductor.sync.reply` span in the process trace links back to the caller's — so each can be reached from the other without the process becoming a child of one request.

### Forms engine

| Metric | Type | Tags | Description |
|---|---|---|---|
| `eventconductor.forms.task.created` | Counter | `formId` | User tasks created |
| `eventconductor.forms.task.completed` | Counter | `formId` | User tasks completed |
| `eventconductor.forms.task.cancelled` | Counter | `formId` | User tasks cancelled |
| `eventconductor.forms.task.duration` | Timer | `formId`, `outcome` | Task duration (recorded only when the form execution carries usable timestamps) |
| `eventconductor.forms.imported` | Counter | — | Form definitions imported (e.g. from Git) |

### Rule engine (catalog)

| Metric | Type | Tags | Description |
|---|---|---|---|
| `eventconductor.rule.catalog.saved` | Counter | `ruleId` | Rules created/updated in the catalog |
| `eventconductor.rule.catalog.deleted` | Counter | `ruleId` | Rules deleted |
| `eventconductor.rule.catalog.imported` | Counter | — | Rules imported (e.g. from Git) |
| `eventconductor.rule.catalog.served` | Counter | `ruleId`, `source` | Rules served to the runtime (e.g. `source=grpc`) |

### Rule runtime (evaluation)

| Metric | Type | Tags | Description |
|---|---|---|---|
| `eventconductor.rule.evaluation.count` | Counter | `ruleId`, `ruleType`, `outcome` | Rule evaluations, by outcome (`matched` \| `nomatch` \| `error`) |
| `eventconductor.rule.evaluation.duration` | Timer | `ruleId`, `ruleType` | Evaluation duration |
| `eventconductor.rule.evaluation.cache` | Counter | `ruleId`, `result` | Rule-source cache lookups (`result=hit` \| `miss`) |

The forms, rule-catalog and rule-runtime metrics follow the same activation rules as the workflow
metrics above (a `MeterRegistry` bean enables them; otherwise a zero-overhead no-op is used). The
rule runtime also works as a plain, non-Spring library — its metrics port then defaults to the
no-op.

### Notes

- With the Prometheus registry, names are exported in Prometheus form: `eventconductor.process.started` becomes `eventconductor_process_started_total`, timers become `_seconds_count` / `_seconds_sum` / `_seconds_max` families.
- Each step retry that fails again increments `eventconductor.step.executions{outcome="ERROR"}`, so the counter reflects attempts, not distinct steps.
- Counters are per-node and reset on restart, as usual with Prometheus counters — use `rate()`/`increase()` over them.
- To customize (percentile histograms, common tags, renaming), use standard Micrometer `MeterFilter` beans, or replace the implementation entirely by defining your own metrics bean (e.g. `WorkflowMetrics`).

## Distributed tracing

The `orchestrator`, `forms` and `rule` standalone apps ship distributed tracing over
[OpenTelemetry](https://opentelemetry.io/) (OTLP). It is **disabled by default** (sampling `0.0`)
so there is no overhead until you opt in — set the sampling probability and point it at your
collector:

```properties
# fraction of traces to sample, 0.0 (off) .. 1.0 (all)
management.tracing.sampling.probability=${TRACING_SAMPLING:0.0}
# OTLP HTTP traces endpoint (OTel Collector / Tempo / Jaeger)
management.opentelemetry.tracing.export.otlp.endpoint=${OTLP_TRACING_ENDPOINT:http://localhost:4318/v1/traces}
```

:::caution[Not `management.otlp.tracing.endpoint`]
Boot 4 deprecated that name at level **error**, which means it is no longer bound — the metadata
entry survives only to say so. Set under the old name it reads back perfectly from the environment,
nothing consumes it, **no span exporter is created**, and every span is built and thrown away. A
`Tracer` exists, the collector is reachable, and the trace store stays empty; there is nothing in
any log to say why.

EventConductor shipped it that way through 2.4.0. `OTLP_TRACING_ENDPOINT` is unchanged, so a
deployment setting the environment variable needs no change.
:::

```bash
export TRACING_SAMPLING=1.0
export OTLP_TRACING_ENDPOINT=http://localhost:4318/v1/traces
```

With tracing on, HTTP requests, Kafka (Spring Cloud Stream) publish/consume and JDBC calls are
auto-instrumented. The standalone apps bundle `micrometer-tracing-bridge-otel` and
`opentelemetry-exporter-otlp`; enabling tracing needs only the two properties above.

### Across the outbox

Auto-instrumentation alone does **not** give you one trace per process, because this engine's
asynchronous boundary is not a network call — it is a database row. A domain event is written to
the outbox inside the transaction that produced it, and a relay thread publishes it some time
afterwards. The instrumentation sees a write in one trace and, later, a Kafka send belonging to
nothing, so the consumer on the other side starts a fresh trace: a trace per hop rather than a
trace per process.

The engine therefore carries the context itself. Each outbox row stores the W3C `traceparent` of
whatever produced the event (column `trace_parent`, null when nothing was being traced), and the
relay publishes **as a continuation of that trace**. A process started by an HTTP request and
finished by a worker three hops later reads as one trace.

### One trace per process

A trace of a workflow should read the way the workflow ran: this step, then that one, then those
two at the same time. That picture cannot be produced by instrumenting the code as it executes. A
span is started and ended by one object on one thread, and a workflow step obliges on neither
count — it starts in the transaction that dispatched it and ends wherever the worker's reply
lands, minutes or days later, across a broker and possibly a pod restart. Wrapping the running
code can only ever describe the hop it is inside, which is why a trace made of dispatches and
relay passes reads as a scatter of two-millisecond fragments.

So the engine does not reconstruct the process from its hops. **It writes the trace out from what
it durably recorded.** A step execution row already carries `startedAt` and `finishedAt`; when a
process reaches a terminal status the engine emits a span for the process and one for each step
that ran, each covering the time that step actually took. Steps that ran one after another come
out as consecutive siblings, steps that ran together as overlapping ones, and the shape falls out
of the timestamps instead of having to be inferred from causality.

```
Order fulfilment ─────────────────────────────────────────────  (the process)
  ├── Reserve stock ────────
  ├── Charge card                  ─────────────
  ├── Print label                  ──────────            (these two ran in parallel)
  └── Notify customer                            ──────
```

The process span carries `eventconductor.process.id`, `.businessKey`, `.status` and the workflow
id and version; each step span carries `eventconductor.step.id`, `.executionId`, `.status`,
`.attempts`, `.type` and, for an ACTION, `.topic`.

**Which trace a process belongs to is derived from its id**, not stored and not propagated — the
anchor is a hash of the process id, so every pod computes the same trace id for the same process
with nothing to carry and nothing to lose. That is what makes this survive the things that defeat
ordinary context propagation: a rebalance, a restart, a redelivery from the dead-letter queue, a
`TIMER` step that waits a week. A `traceparent` in a message header is gone the moment the message
is replayed; a derived anchor was never held.

### Joining the caller's trace

A process started from inside somebody else's trace — a booking a CRS sent, whose trace already
holds the CRS, the connector and the broker hop — can join that trace instead of starting its own,
so the booking reads end to end in one waterfall: caller → engine → every worker → back.

The caller hands over its W3C context in either of two ways:

- **Kafka record headers** on the `ProcessCreationRequested` record: `traceparent`, and optionally
  `tracestate` and `baggage`, named as the W3C specs name them, values as UTF-8 bytes.
- **The payload**: `ProcessCreationRequested.traceContext` — `{"traceparent": …, "tracestate": …,
  "baggage": …}` — for a producer whose outbox relay cannot set headers. Optional and left off the
  wire when null; an engine that predates it ignores it. The payload wins when both are present.

The engine keeps the context with the process (table `process_trace_context`, one row per process
that was given one) and uses the caller's span **as the process's anchor** in place of the derived
one. Everything described below still holds — the live spans, the recorded process and step spans,
one trace for the whole life of the process on every pod — it simply sits under the caller's span,
and it is stored rather than derived, so it survives restarts and redeliveries the same way. The
caller's sampled flag decides whether the process is traced.

**Every record the engine publishes for the process carries the trace on**: tasks
(`TaskExecutionRequested`, straight from `dispatch-step`), everything relayed through the outbox,
`PUBLISH_EVENT` events, child-process creations — each with a `traceparent` of the engine span that
sent it, plus the caller's `tracestate` and `baggage` passed through. Headers are written as raw
bytes so any W3C propagator on the other side reads them. A worker built on `worker-kafka` runs the
handler in a `CONSUMER` span `eventconductor.task <stepId>` under that `traceparent` (when it has a
tracing bridge), and exposes the context to the handler as `TaskContext.traceContext()`. A worker
with its own Spring Cloud Stream consumer joins by turning on the binder's observation
(`spring.cloud.stream.kafka.binder.enable-observation=true`) with a tracing bridge on the classpath.
Replies need nothing: the engine continues the process's trace from what it stored.

Only while tracing is on: with no `Tracer`, nothing is stored, nothing is looked up, and no header is
added. `workflow.tracing.join-caller-trace=false` keeps every process on its derived trace even when
a context arrives. Synchronous invocations keep their own design — the request and the process in
separate traces, linked both ways (see [Synchronous invocation](#synchronous-invocation)).

### Sampling

`management.tracing.sampling.probability` governs process traces exactly as it governs everything
else. The decision is taken **from the derived trace id**, by the same arithmetic the OpenTelemetry
SDK's own `TraceIdRatioBased` sampler uses — so the rate comes out as configured, and a process is
either traced or not traced.

That last part is the point, and it matters more than the rate. The decision belongs to the
process, not to the span: every pod that touches it reaches the same answer, over its whole life,
across restarts and redeliveries. Sampling per span would at 10% have given you a tenth of the
dispatches of a tenth of the processes — scattered fragments, and never a whole process to read.
Here a traced process is traced end to end, and an untraced one costs nothing at all.

:::note[What changes for a deployment that was already tracing]
The engine's spans — `step-over`, `dispatch-step`, `correlate-message` — used to be roots of their
own, so each one took its own turn at the ratio independently. At 10% that meant roughly a tenth of
the dispatches of roughly a tenth of the processes, which is why they read as unrelated fragments.
The volume is about the same now; what changes is that it arrives as whole processes instead of
scattered spans. Nothing needs configuring for that — the property means what it says.
:::

Steps that never ran — a branch abandoned when another reached `END` — draw nothing, so the
waterfall shows what happened rather than what did not.

### The engine's own spans

The live spans are still emitted, and they now join the trace of the process they are working on
instead of each starting one of their own. They are what makes a process visible in the trace
store **while it is still running**, before its own span exists:

| Span | What it covers |
|---|---|
| `eventconductor.create-process` | Creating a process that joined a caller's trace |
| `eventconductor.step-over` | Advancing a process: deciding what may run now and dispatching it; tagged with the workflow id, business key, the process's status after it and the steps it moved (`eventconductor.steps.moved`) |
| `eventconductor.dispatch-step` | Handing one step to a worker; tagged with the step and workflow ids |
| `eventconductor.correlate-message` | Matching an arriving message to the steps waiting for it |
| `outbox relay` | Publishing one outbox row, as a continuation of the trace that produced it |

`correlate-message` is anchored only after the match: which process an arriving message belongs to
is precisely what the lookup answers, so the lookup itself runs untraced and each match is traced
inside the process it turned out to belong to.

Both the propagation and the spans go through a port with a no-op default
(`WorkflowTracing`), wired to Micrometer only when the host application brings a `Tracer` — the
same shape as metrics. The engine libraries still run with **zero** observability dependencies, and
nothing here can fail a workflow: a tracing error is logged at debug and the work carries on.

## Metrics over OTLP

Metrics are exposed for Prometheus scraping by default. A deployment that already runs an
OpenTelemetry collector can push them instead, over the same protocol as the traces:

```properties
management.otlp.metrics.export.enabled=${OTLP_METRICS_ENABLED:false}
management.otlp.metrics.export.url=${OTLP_METRICS_ENDPOINT:http://localhost:4318/v1/metrics}
management.otlp.metrics.export.step=${OTLP_METRICS_INTERVAL:60s}
```

Off by default, and independent of the Prometheus endpoint — turning it on does not turn scraping
off.
