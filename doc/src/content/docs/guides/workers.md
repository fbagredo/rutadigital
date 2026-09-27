---
title: Implementing Workers
description: How to implement workers that handle ACTION steps in your workflows.
---

A worker performs the business logic of an `ACTION` step: it is handed the process variables, does its work, and reports the outcome and any output variables back to the engine. Workers are **stateless** — the orchestrator handles retries, timeouts and error tracking.

There are two ways to write one. The **recommended** way is to declare a *task contract* and let EventConductor generate the typed interface you implement — you write only the business logic, and the same handler runs unchanged over Kafka or embedded in the engine. Underneath it sits a **low-level protocol** (`WorkerReply`, `EmbeddedTaskExecutor`) that you can still use directly when you need to; it is documented in full further down.

## Generating a worker from a task contract

### 1. Declare the contract

A task contract is a `.ectask` file under `src/main/resources/tasks/` (or in your definitions repository, next to the workflows). It is the shape an `ACTION` step commits to when it references the task by id:

```yaml
id: greet
version: 1
group: greetings
topic: sample-greetings
description: Greet a person by name.
input:
  name:
    type: string
    required: true
output:
  message:
    type: string
errors:
  - code: EMPTY_NAME
    description: the name was blank
```

A workflow step uses it with `task: greet` (pinned to the latest version at import) or `task: greet@1`. See [versioning](/reference/versioning/) for how versions and in-flight processes coexist, and [the task contract format](/reference/task-contracts/) for every field.

### 2. Generate the types

Run the `generate-worker-sources` goal of the [workflow-maven-plugin](/reference/maven-plugin/). The simplest projects inherit a parent that wires it for you:

- **`task-module-parent`** — a library you add to any Spring Boot service;
- **`task-service-parent`** — a runnable standalone Kafka service (the `@SpringBootApplication`, Actuator and image build are set up for you).

Either way the generated project is just a `pom` with the parent, coordinates and a few properties:

```xml
<parent>
  <groupId>io.mateu.workflow</groupId>
  <artifactId>task-module-parent</artifactId>
  <version>...</version>
</parent>
<artifactId>greetings-tasks</artifactId>
<properties>
  <ec.group>greetings</ec.group>
  <ec.basePackage>com.example</ec.basePackage>
</properties>
```

For each contract version the generator writes, into `target/generated-sources` (never edited), a `GreetV1Input` and `GreetV1Output` record, a `GreetV1Task` interface, a `TaskFailure` subclass per declared error, and a per-group `@AutoConfiguration` that registers your handler.

### 3. Implement the handler

Implement the generated interface as a Spring bean. That is the entire worker:

```java
@Component
public class GreetHandler implements GreetV1Task {

    @Override
    public GreetV1Output handle(GreetV1Input input, TaskContext context) {
        if (input.name() == null || input.name().isBlank()) {
            throw new GreetV1Task.EmptyName("a name is required");   // a declared business error
        }
        return new GreetV1Output("Hello, " + input.name() + "!");
    }
}
```

Return normally to complete the step; throw a generated `TaskFailure` subclass (one per `errors` entry) to fail it with that business code; any other exception fails it with the exception's text and the engine retries it per the step's `retries`. `TaskContext` gives you the ids, `isCancelled()` and `progress(...)`.

**Declaring a task obliges you to implement it**: if the handler bean is missing, the application fails to start with a message naming the interface — unless you opt out with `eventconductor.tasks.<id>.enabled=false`.

### 4. Choose the transport

The host decides the transport; the handler does not change:

- add **`worker-kafka`** for a service that talks to the engine over Kafka (the `consumeWorkerEvent` binding, the `upstream`/task topics and cancellation are wired automatically);
- add **`worker-embedded`** to run the handler in-process inside an embedded engine.

`worker-api`, which the generated code depends on, never puts Spring Cloud Stream on your classpath. The in-repo `modules/sample-worker` is a full Kafka example, and `examples/greetings-tasks` is the minimal task module.

### What the runtime adds to your application

- **Beans, namespaced and backing off.** Every bean the SDK contributes has an `eventconductor…`
  name — `eventconductorTaskRegistry`, `eventconductorTaskDispatcher`, `eventconductorWorkerReplySink`,
  `eventconductorWorkerCancellations`, `eventconductorCancelledTasks`, `eventconductorTaskTracing`
  (Kafka), and `eventconductorEmbeddedReplySink`, `eventconductorEmbeddedCancellations`,
  `eventconductorEmbeddedTaskExecutor` (embedded) — so your own `taskDispatcher` or `taskRegistry`
  never clashes with them. Each backs off (`@ConditionalOnMissingBean` by type) when you define a
  bean of the same type, e.g. your own `TaskDispatcher`. The Kafka binding function keeps its name,
  `consumeWorkerEvent`.
- **No `ObjectMapper` bean.** Variables are bound with a Jackson mapper private to the SDK
  (JSR-310 enabled, unknown properties ignored). Your application's JSON configuration neither
  affects task binding nor is affected by having the SDK on the classpath.
- **Handler lookup** is by contract reference, with or without version, falling back to the step id
  only for an `ACTION` with no `task:` — see
  [how a worker finds the handler](/reference/task-contracts/#how-a-worker-finds-the-handler).
- **Typing** follows the contract's declared input types, not what a value looks like — a
  `string` attribute holding `12E45` arrives as `"12E45"` — see
  [binding variables](/reference/task-contracts/#binding-variables).
- **Commit semantics (Kafka): at least once.** The handler runs, and its reply is published
  synchronously, on the listener thread; the binder commits the task's offset only after that
  returns. A crash before the reply is out leaves the offset uncommitted and the task is
  redelivered — handlers must be idempotent. A reply the broker still refuses after the runtime's
  own retries is rethrown, so the binder retries it (`max-attempts`) and then applies its error
  handling; set `enable-dlq` on the `consumeWorkerEvent-in-0` binding if a task must survive a
  prolonged broker outage. Tasks run one at a time per consumer thread: raise
  `spring.cloud.stream.bindings.consumeWorkerEvent-in-0.consumer.concurrency` (up to the task
  topic's partition count) for parallelism, and keep a handler under the consumer's
  `max.poll.interval.ms` (5 minutes by default) — use an [asynchronous worker](#asynchronous-workers)
  for longer work. A cancellation is honoured mid-task only when it is consumed by another thread,
  so in-flight cancellation also needs a concurrency above one.

## The low-level worker protocol

Everything above is built on the protocol below. Use it directly when you are not generating from a contract, or to understand what the runtime does on your behalf.

## Kafka worker

### Receiving tasks

Subscribe to the `downstream` Kafka topic. The event payload:

```java
record TaskExecutionRequested(
    String taskExecutionId,   // unique ID for this execution
    String processId,          // parent process ID
    String workflowDefinitionId,
    String stepId,
    String taskId,
    List<Variable> variables   // process variables at this point
) {}
```

### Reporting completion

Publish a `TaskStatusChanged` to the `upstream` topic:

```java
record TaskStatusChanged(
    String taskExecutionId,
    TaskStatus status,         // COMPLETED | ERROR | RUNNING
    List<Variable> variables,  // output variables merged into the process
    String log                 // optional log message
) {}
```

### Example (Spring Cloud Stream)

Reply through `WorkerReply`, not through `StreamBridge` directly — see
[Do not drop the reply](#do-not-drop-the-reply) below.

```java
@Component
public class MyWorker {

    @Bean
    public Consumer<TaskExecutionRequested> myWorkerTopic(
            StreamBridge streamBridge) {
        return request -> {
            try {
                String result = doBusinessLogic(request.variables());
                WorkerReply.completed(streamBridge, request,
                    List.of(new Variable("result", result)));
            } catch (Exception e) {
                log.error("Step {} failed", request.stepId(), e);
                WorkerReply.failed(streamBridge, request, List.of(), e.toString());
            }
        };
    }
}
```

### Say why it failed

The four-argument `failed(...)` publishes the reason as a log line on the process, next to the
failure itself. Use it. The three-argument overload reports the status and nothing else, so the
process log reads "Task status changed to ERROR" and whoever is looking at a rolled-back saga has
no idea what went wrong — the reason lives only in the worker's own stdout, and only if the worker
bothered to log it.

Embedded mode never had this problem: the engine catches the exception on the worker's behalf and
records it. In kafka mode the worker is the only one who knows, so it has to say.

The reason is sent **before** the failure, and both are on the retry-or-throw path below — so a
broker that will not take the reason throws before anything has been reported at all, and the task
is simply redelivered. Reporting a failure and then losing its explanation is the outcome worth
avoiding.

### Do not drop the reply

`StreamBridge.send` reports failure by **returning `false`**, and the obvious one-liner throws that
away:

```java
// Wrong, and it is the version everyone writes first.
streamBridge.send("upstream", new TaskStatusChanged(...));
```

When the broker refuses the message the listener still returns normally, the consumer commits the
offset, and the task your worker actually performed is never reported. The engine's step stays in
`PENDING` waiting for an answer that was never published, and if that step declares no timeout it
waits forever. This is not hypothetical: during a ninety-second broker outage on a test cluster,
workers written this way lost 3,352 replies and left 3,356 processes permanently stuck, with no
error logged anywhere. See [Reliability](/guides/reliability/).

`WorkerReply` retries a refused send and then **throws**, which is the point: the offset is not
committed, so Kafka redelivers the task and your worker does it again. **Worker handlers must be
idempotent** — they always had to be, because at-least-once delivery was always the contract.

It rests on one setting — an asynchronous producer returns `true` the moment the record is
buffered, so the refusal `WorkerReply` checks for never arrives:

```yaml
spring:
  cloud:
    stream:
      kafka:
        default:
          producer:
            sync: true      # without it, send() returns true before the broker has seen anything
```

You do not have to declare it. It ships with the `shared` module — the one every worker already
depends on to build a `TaskStatusChanged` — and applies wherever a Kafka producer exists. Set it
yourself only if you want the opposite, and know why: it is contributed at the lowest precedence,
so an explicit value always wins.

## Embedded worker (mode: embedded)

When running in embedded mode, register a single Spring bean of type `EmbeddedTaskExecutor`. All ACTION steps are routed to that bean regardless of the `topic` field in the workflow definition. The bean receives the full `TaskExecutionRequested` — use `request.stepId()` to branch between steps. Each branch calls `updateStepExecution` independently with its own output variables.

Output variables produced by a step are automatically included in `request.variables()` for all subsequent steps, so later steps can read values written by earlier ones.

```java
@Bean
public EmbeddedTaskExecutor taskExecutor(UpdateStepExecutionUseCase updateStepExecution) {
    return request -> {
        switch (request.stepId()) {
            case "greet" -> {
                String name = request.variables().stream()
                    .filter(v -> "name".equals(v.name()))
                    .map(v -> v.value())
                    .findFirst().orElse("World");
                System.out.println("Hello, " + name + "!");
                updateStepExecution.handle(new UpdateStepExecutionCommand(
                    request.taskExecutionId(),
                    List.of(new Variable("greeting", "Hello, " + name + "!")),
                    "",
                    StepExecutionStatus.COMPLETED
                ));
            }
            case "farewell" -> {
                // variables() includes outputs from previous steps (e.g. greeting)
                String name = request.variables().stream()
                    .filter(v -> "name".equals(v.name()))
                    .map(v -> v.value())
                    .findFirst().orElse("World");
                System.out.println("Goodbye, " + name + "!");
                updateStepExecution.handle(new UpdateStepExecutionCommand(
                    request.taskExecutionId(),
                    List.of(new Variable("farewell", "Goodbye, " + name + "!")),
                    "",
                    StepExecutionStatus.COMPLETED
                ));
            }
            default -> updateStepExecution.handle(new UpdateStepExecutionCommand(
                request.taskExecutionId(),
                List.of(),
                "Unknown step: " + request.stepId(),
                StepExecutionStatus.ERROR
            ));
        }
    };
}
```

A working example with two sequential steps is available in `demo/embedded-headless`.

### The thread the worker runs on

By default the engine calls the bean and waits, **on the thread that dispatched the task**. With
`workflow.persistence=jpa` that is `embedded-outbox-relay`, the single thread draining the outbox
and therefore the only one advancing every process in the JVM. A worker that blocks there stops
all of them — and the symptom does not look like a stuck worker: processes created afterwards sit
with every step in `CREATED`, described in the UI as "waiting for its preconditions".

So an embedded worker must not block indefinitely. Two ways out, and they compose:

- Give every outbound call a timeout. A `RestClient` built with `builder.baseUrl(url).build()` has
  none — the connect and read timeouts have to be set on the request factory.
- Set `workflow.embedded.worker-threads` above zero to hand tasks to a pool, or do the handoff
  yourself (see [Asynchronous workers](#asynchronous-workers)). Read
  [the configuration reference](/reference/configuration/#where-an-embedded-worker-runs) first:
  through a pool, a task lost to a crash is recovered by the step's `timeout` rather than by
  redelivery, so give ACTION steps one.

An exception that escapes the bean fails the step — the engine reports the `ERROR` you did not.
Prefer reporting it yourself: a throw carries no output variables and no message of your choosing.

## Reporting intermediate progress

Workers can report `RUNNING` status to indicate they are still working. This resets the timeout clock and updates the step execution status:

```java
// Kafka mode
WorkerReply.running(streamBridge, request);

// Embedded mode
updateStepExecution.handle(new UpdateStepExecutionCommand(
    request.taskExecutionId(),
    List.of(),
    "Processing batch 3 of 10...",
    StepExecutionStatus.RUNNING
));
```

## Injecting steps (DYNAMIC workers)

A worker handling a [`DYNAMIC`](/reference/step-types/) step can do one thing an `ACTION` worker
cannot: return new steps to add to the running process. Reply through `WorkerReply.inject(...)` (or
`injectAndComplete(...)`) with a JSON array of steps in the workflow-definition step schema:

```java
String stepsJson = """
    [
      {"id":"task-a","type":"ACTION","name":"Task A","topic":"work","preconditionStepId":"plan"},
      {"id":"task-b","type":"ACTION","name":"Task B","topic":"work","preconditionStepId":"plan"},
      {"id":"merge","type":"JOIN","name":"Merge","preconditionStepIds":["task-a","task-b"]}
    ]
    """;
WorkerReply.injectAndComplete(streamBridge, request, stepsJson, List.of());
```

Injection is add-only, the worker supplies each step's preconditions (there is no default wiring),
and the engine validates the whole batch and fails the `DYNAMIC` step if it is rejected. See
[Dynamic Workflows](/guides/dynamic-workflows/) for the full picture and the runaway guards.

## Asynchronous workers

For long-running tasks, the worker can return immediately and report completion later. `UpdateStepExecutionUseCase` is a Spring bean available anywhere in the application context:

```java
@Component
@RequiredArgsConstructor
public class AsyncWorkerBean {

    private final UpdateStepExecutionUseCase updateStepExecution;

    public void startAsync(TaskExecutionRequested request) {
        CompletableFuture.runAsync(() -> {
            // ... long-running work ...
            updateStepExecution.handle(new UpdateStepExecutionCommand(
                request.taskExecutionId(),
                List.of(new Variable("result", "done")),
                "",
                StepExecutionStatus.COMPLETED
            ));
        });
    }
}
```

## Output variables

Output variables reported by a worker are **merged into the process variables**. They overwrite any existing variable with the same name and are available to all subsequent steps.

```java
// These variables will be accessible in later JEXL precondition expressions
List.of(
    new Variable("approved", "true"),
    new Variable("approvedBy", "manager@example.com")
)
```
