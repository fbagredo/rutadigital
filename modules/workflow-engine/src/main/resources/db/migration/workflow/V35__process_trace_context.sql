-- The W3C trace context a process joined when it was created, and the tracestate/baggage every
-- outbox row carries on alongside its traceparent.
--
-- A service that starts a process from inside its own trace hands over its context (Kafka record
-- headers, or the traceContext field of ProcessCreationRequested). Kept here, one row per process
-- that was given one, so every pod continues that trace for the life of the process instead of the
-- one derived from the process id. Processes started without a context have no row, which is all of
-- them until a caller sends one, and all of them whenever tracing is off.
--
-- Plain portable DDL with IF NOT EXISTS, as V29-V33 (V34 is the lock row id fix): it also runs over a schema ddl-auto created
-- from the entities (ProcessTraceContextEntity, OutboxMessageEntity).
CREATE TABLE IF NOT EXISTS process_trace_context (
    process_id    VARCHAR(255) NOT NULL PRIMARY KEY,
    trace_parent  VARCHAR(64)  NOT NULL,
    trace_state   VARCHAR(512),
    baggage       VARCHAR(2048)
);

ALTER TABLE outbox_message_entity ADD COLUMN IF NOT EXISTS trace_state VARCHAR(512);
ALTER TABLE outbox_message_entity ADD COLUMN IF NOT EXISTS baggage VARCHAR(2048);
