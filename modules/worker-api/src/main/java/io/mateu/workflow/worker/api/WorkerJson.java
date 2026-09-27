package io.mateu.workflow.worker.api;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * The SDK's own Jackson mapper, used to bind variables to and from task records.
 *
 * <p>It is deliberately <b>private to the SDK</b> and never registered as a Spring bean. Worker-api
 * used to contribute a fallback {@code ObjectMapper} bean, which either won over (or competed with)
 * the application's and its libraries' mappers — "expected single matching bean but found 2" — or,
 * as a plain {@code new ObjectMapper()}, became the application's mapper and broke its
 * {@code java.time} handling. Binding a task contract must not depend on how an application
 * configures JSON, and an application's JSON must not depend on the SDK being on the classpath.
 *
 * <p>Configured for contracts: {@code java.time} types (contract {@code date}/{@code datetime}) read
 * and written as ISO strings, and unknown properties ignored (a task receives every process
 * variable, not only its contract's inputs).
 */
final class WorkerJson {

    private static final ObjectMapper MAPPER = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .build();

    private WorkerJson() {
    }

    /** The shared SDK mapper (thread-safe; never mutated after construction). */
    static ObjectMapper mapper() {
        return MAPPER;
    }
}
