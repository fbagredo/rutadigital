---
title: "Task contracts and the worker protocol"
description: The .ectask task contract format and the wire protocol between the engine and a worker, documented so workers — and worker generators — can be built in any language.
---

A **task contract** is the shape an `ACTION` step commits to when it references a task by id. It
lives in a `.ectask` file (JSON or YAML) and is shared by everything that needs it: the engine
pins its version onto a step at import and routes by its default topic, the
[Maven plugin](/reference/maven-plugin/) validates it, and the code generator turns it into typed
interfaces. This page documents the format and the underlying wire protocol so a worker — or a
generator for another language — can be built against them directly. The canonical schema is
`task-contract-schema.json` (`urn:eventconductor:task-contract-schema:1`).

## The `.ectask` format

```yaml
id: greet                 # stable identifier, unique across all groups
version: 1                # a backward-incompatible change is a new version
group: greetings          # free-form grouping; one group = one generated module
topic: sample-greetings   # default transport destination for steps using this task (optional)
description: Greet a person by name.
input:                    # variables the task expects, by name (declaration order matters)
  name:
    type: string
    required: true
output:                   # variables the task returns
  message:
    type: string
errors:                   # business errors the task may report
  - code: EMPTY_NAME
    description: the name was blank
```

- **`id`** — stable, unique across every group. A step references it with `task: <id>` or
  `task: <id>@<version>`.
- **`version`** — an integer; a backward-incompatible change (a removed or renamed field, a
  tightened type, a moved group) is a new version. Contracts are **append-only**: a version a
  running process is pinned to is never removed. A service may implement several versions at once.
- **`group`** — free-form; an `id` belongs to exactly one group for its whole life (moving it is a
  breaking change). One group maps to one generated worker module.
- **`topic`** — the default transport destination (Kafka topic) for steps that use the task; an
  explicit `topic` on the step overrides it.
- **`input` / `output`** — maps of attribute name to `{type, required, description}` (and `items`
  for an array). Declaration order is significant: it is the order of the generated record's
  components.
- **`errors`** — each has a `code` (a valid Java identifier) and an optional `description`. A
  handler throws one to fail the step with that code as the reason.

### Attribute types

| `type` | JSON on the wire | Java type generated |
|---|---|---|
| `string` | string | `String` |
| `integer` | number (integral) | `Long` |
| `number` | number | `BigDecimal` |
| `boolean` | boolean | `Boolean` |
| `date` | string (ISO-8601 date) | `LocalDate` |
| `datetime` | string (ISO-8601 date-time) | `LocalDateTime` |
| `object` | object | `JsonNode` (opaque, no nested contract) |
| `array` | array | `List<items>` (`items` gives the element type) |

## Versioning and the taskId

On import the engine pins a bare `task: <id>` to `<id>@<latest>` (an explicit `<id>@<version>` is
left as written), so a definition never changes contract without an edit. It then dispatches
`<id>@<version>` as the `taskId`, and defaults the step's topic from the contract. See
[versioning](/reference/versioning/) for how pinned versions and in-flight processes coexist.

### How a worker finds the handler

The Java runtime resolves a dispatched task in this order:

1. **`taskId` with a version** (`greet@2`) — exactly that registration. A version the worker does
   not serve resolves to nothing (a warning, or `ERROR` with `eventconductor.worker.strict=true`);
   it is never swapped for another version, whose input or output may differ.
2. **`taskId` without a version** (`greet`) — the highest version the worker registers for that id.
3. **`stepId`, only when the `taskId` is blank** — an `ACTION` with no `task:` reference. The step
   id is matched like a `taskId`: `<id>@<version>` exactly, otherwise as a contract id (highest
   version). So a step `id: greet` with no `task:` is served by the `greet` handler.

A `taskId` that is set but not served never falls back to the `stepId`.

## The wire protocol

A worker communicates with the engine through three integration events. In Kafka mode they travel
on topics; in embedded mode the engine calls the worker in-process and the worker calls back a use
case — the payloads are the same.

### Engine → worker

- **`TaskExecutionRequested`** — run this task.

  ```
  taskExecutionId       the engine's step-execution id; the reply and any cancellation are keyed by it
  processId             the parent process
  workflowDefinitionId
  stepId                the workflow step
  taskId                <id>@<version> for a contract-backed ACTION (empty for a legacy ACTION)
  variables             the process variables at this point, as name/value string pairs
  ```

- **`TaskCancellationRequested`** — stop the task with this `taskId` (the step-execution id). The
  engine only sends it while the step is in flight at a worker.

### Worker → engine

- **`TaskStatusChanged`** — the outcome.

  ```
  taskExecutionId
  status                RUNNING | COMPLETED | ERROR
  variables             output variables, merged into the process
  processId             echoed back for routing
  ```

  `RUNNING` reports progress and resets the step's timeout clock; `COMPLETED` finishes it with the
  output variables; `ERROR` fails it (with an accompanying log line for the reason).

### Binding variables

`variables` are name/value **string** pairs, and a task receives every process variable, not only
its contract's inputs. How a value is read is decided by the **declared type** of the input
attribute it binds to — never by what the value looks like:

| Declared type | Value bound as |
|---|---|
| `string`, `date`, `datetime` (Java `String`, `LocalDate`, `LocalDateTime`, enums, `UUID`, …) | the raw string, unchanged — a locator `12E45` or a code `007` stays itself |
| `integer`, `number`, `boolean`, `object`, `array` (numbers, booleans, `JsonNode`, lists, maps, records) | read as JSON; a value that is not JSON is passed as a string and coerced (or reported as a binding error) |
| no declared type (an `Object` component, a `Map<String, Object>` input) | the raw string |

Variables the input does not declare are ignored. Output values are written back as strings
as-is, and everything else as JSON (`java.time` values as ISO-8601 strings).

The Java runtime binds with a Jackson mapper private to the SDK (JSR-310 enabled, unknown
properties ignored). It does not use, and does not register, an application `ObjectMapper` bean.

### Reliability

Delivery is **at-least-once**, so a worker must be **idempotent**. A reply that the broker refuses
must not be silently dropped: the reference runtime retries and then throws, leaving the inbound
message uncommitted so the task is redelivered rather than lost. See
[implementing workers](/guides/workers/#do-not-drop-the-reply) and
[reliability](/guides/reliability/) for why this matters and the one producer setting it depends
on.

## Building a worker in another language

Everything above is language-agnostic: the `.ectask` schema, the type mapping, the three events and
their topics, and the at-least-once reply contract. A generator for another language would read the
same `.ectask` files, emit that language's equivalents of the input/output types, the handler
interface and the typed errors, and provide a small runtime that consumes `TaskExecutionRequested`,
binds the variables, invokes the handler, and publishes `TaskStatusChanged` with the same
retry-or-fail discipline. The JVM implementation (`worker-api`, `worker-kafka`, `worker-embedded`)
is the reference.
