package io.mateu.workflowdist.support;

import io.mateu.workflowdist.support.orchestrator.DistOrchestratorApp;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.mateu.workflow.ddd.DomainEvent;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Singleton infrastructure for the distributed suite: one PostgreSQL and one Kafka
 * container (Testcontainers) shared by every test class, plus factories for orchestrator
 * and worker instances. Orchestrator "pods" are separate Spring application contexts in
 * this JVM wired to the real containers — each has its own Kafka consumers, connection
 * pool and background threads, so killing one (abruptly closing its context) leaves
 * durable state exactly as a process kill would: unsent rows stay {@code Pending} in the
 * outbox table and unconsumed events stay on the topics.
 */
public final class DistInfra {

    /** Advisory lock id used by the engine's OutboxRelay (see OutboxRelay.LOCK_ID). */
    public static final long OUTBOX_RELAY_LOCK_ID = 111222333L;

    static final int PARTITIONS = Integer.getInteger("tune.partitions", 6);

    private static PostgreSQLContainer<?> postgres;
    private static KafkaContainer kafka;
    private static ConfigurableApplicationContext workerContext;
    private static ConfigurableApplicationContext testWorkerContext;
    private static KafkaProducer<String, String> producer;
    private static JdbcTemplate jdbc;
    private static int instanceCounter;

    private static final ObjectMapper eventWriter = new ObjectMapper();

    private DistInfra() {
    }

    public static synchronized void ensureStarted() {
        if (postgres != null) {
            return;
        }
        postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                .withDatabaseName("workflow")
                .withUsername("workflow")
                .withPassword("secret");
        kafka = new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"));
        postgres.start();
        kafka.start();
        createTopics();

        var dataSource = new DriverManagerDataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        jdbc = new JdbcTemplate(dataSource);

        var producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(producerProps);
    }

    /** Multiple partitions so two orchestrator pods in the same consumer group genuinely share traffic. */
    private static void createTopics() {
        try (var admin = AdminClient.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(
                    new NewTopic("upstream", PARTITIONS, (short) 1),
                    new NewTopic("downstream", PARTITIONS, (short) 1),
                    // The test worker's own topic. Separate from `work` because both workers run
                    // in this JVM in different consumer groups, and a shared topic would hand every
                    // task to both of them.
                    new NewTopic("sim-work", PARTITIONS, (short) 1),
                    new NewTopic("outbox", PARTITIONS, (short) 1))).all().get();
        } catch (Exception e) {
            throw new IllegalStateException("Could not create Kafka topics", e);
        }
    }

    /**
     * Boots an orchestrator instance (kafka + jpa mode) against the shared containers.
     * The caller owns the context: close it to "kill the pod".
     */
    public static synchronized ConfigurableApplicationContext startOrchestrator(Map<String, Object> overrides) {
        ensureStarted();
        var props = new HashMap<String, Object>();
        props.put("spring.application.name", "dist-orchestrator-" + (++instanceCounter));
        props.put("spring.main.web-application-type", "none");
        props.put("spring.main.banner-mode", "off");
        props.put("workflow.mode", "kafka");
        props.put("workflow.persistence", "jpa");
        props.put("spring.cloud.stream.bindings.consumeUpstream-in-0.consumer.batch-mode", "true");
        props.put("spring.cloud.stream.bindings.consumeOutbox-in-0.consumer.batch-mode", "true");
        // Tunables, not constants. These numbers were picked to make the suite run quickly and
        // never measured; overriding them from the command line is how we find out which of them
        // the throughput actually depends on:
        //   mvn -Pdist-e2e ... -Dtune.outbox-poll-ms=20 -Dtune.consumer-concurrency=6
        props.put("workflow.outbox-poll-interval-ms", tune("outbox-poll-ms", "200"));
        props.put("workflow.timeout-scan-interval-ms", tune("timeout-scan-ms", "250"));
        props.put("workflow.outbox.batch-size", tune("outbox-batch-size", "100"));
        props.put("spring.datasource.url", postgres.getJdbcUrl());
        props.put("spring.datasource.username", postgres.getUsername());
        props.put("spring.datasource.password", postgres.getPassword());
        props.put("spring.datasource.hikari.maximum-pool-size", tune("pool-size", "10"));
        props.put("spring.jpa.hibernate.ddl-auto", "update");
        props.put("spring.jpa.open-in-view", "false");
        props.put("spring.cloud.function.definition", "consumeOutbox;consumeUpstream");
        props.put("spring.cloud.stream.kafka.binder.brokers", kafka.getBootstrapServers());
        props.put("spring.cloud.stream.bindings.consumeOutbox-in-0.destination", "outbox");
        props.put("spring.cloud.stream.bindings.consumeOutbox-in-0.group", "orchestrator-outbox");
        props.put("spring.cloud.stream.bindings.consumeOutbox-in-0.consumer.concurrency", tune("consumer-concurrency", "3"));
        props.put("spring.cloud.stream.bindings.consumeUpstream-in-0.destination", "upstream");
        props.put("spring.cloud.stream.bindings.consumeUpstream-in-0.group", "orchestrator-upstream");
        props.put("spring.cloud.stream.bindings.consumeUpstream-in-0.consumer.concurrency", tune("consumer-concurrency", "3"));
        props.put("spring.cloud.stream.kafka.binder.configuration.linger.ms", tune("producer-linger-ms", "0"));
        props.put("spring.cloud.stream.kafka.binder.configuration.fetch.min.bytes", tune("fetch-min-bytes", "1"));
        props.put("spring.cloud.stream.kafka.binder.configuration.max.poll.records", tune("max-poll-records", "500"));
        props.put("spring.cloud.stream.bindings.upstream.destination", "upstream");
        props.put("spring.cloud.stream.bindings.downstream.destination", "downstream");
        props.put("spring.cloud.stream.bindings.outbox.destination", "outbox");
        props.put("spring.cloud.stream.bindings.deadLetter.destination", "dead-letter");
        // These tests assert distributed behaviour, not metrics, so the engine's Micrometer
        // instrumentation is switched off — which also keeps this suite as the standing proof that
        // excluding it is survivable. It was not: Micrometer *is* on this classpath (spring-cloud-
        // stream pulls micrometer-core in transitively), the no-op fallback was gated on the class
        // being absent, and so nothing provided WorkflowMetrics at all. Every one of these tests
        // failed to boot a context, for days, on an exclusion that reads like a preference.
        props.put("spring.autoconfigure.exclude",
                "io.mateu.workflow.autoconfigure.WorkflowMetricsAutoConfiguration");
        props.put("logging.level.io.mateu.workflow", "WARN");
        props.putAll(overrides);
        return new SpringApplicationBuilder(DistOrchestratorApp.class)
                .web(WebApplicationType.NONE)
                .properties(props)
                .run();
    }

    /** Boots (once) the shared Kafka worker context programmed through {@link WorkerStub}. */
    public static synchronized void ensureWorkerStarted() {
        ensureStarted();
        if (workerContext != null) {
            return;
        }
        var props = new HashMap<String, Object>();
        props.put("spring.application.name", "dist-worker");
        props.put("spring.main.web-application-type", "none");
        props.put("spring.main.banner-mode", "off");
        // The worker talks to Kafka via Spring Cloud Stream. Without this, workflow.mode
        // defaults to "embedded" and EmbeddedModeAutoConfigurationExcluder strips the
        // stream autoconfiguration (BindingServiceConfiguration/FunctionConfiguration),
        // so StreamBridge is never created and consumeWorkerEvent fails to wire.
        // The worker has no @ComponentScan, so this pulls in no orchestrator/outbox beans.
        props.put("workflow.mode", "kafka");
        props.put("spring.cloud.function.definition", "consumeWorkerEvent");
        props.put("spring.cloud.stream.kafka.binder.brokers", kafka.getBootstrapServers());
        // "work", not "downstream": every definition in src/test/resources/workflows names
        // `"topic": "work"` on its ACTION steps, and the engine now dispatches to the topic a step
        // names. Pointing the worker at what the fixtures actually ask for is also the only
        // end-to-end cover per-step routing has — it is exercised here over a real broker, on every
        // one of these tests, rather than only in a unit test of the publisher.
        //
        // The suite found this itself: with the worker on "downstream" and the definitions naming
        // "work", eleven of twelve tests timed out at 120s with nothing completing. That is exactly
        // the failure a topic pointed at a destination nobody consumes produces in production, and
        // exactly why the change ships with a migration note.
        props.put("spring.cloud.stream.bindings.consumeWorkerEvent-in-0.destination", "work");
        props.put("spring.cloud.stream.bindings.consumeWorkerEvent-in-0.group", "worker-group");
        props.put("spring.cloud.stream.bindings.consumeWorkerEvent-in-0.consumer.concurrency", "3");
        props.put("spring.cloud.stream.bindings.upstream.destination", "upstream");
        workerContext = new SpringApplicationBuilder(DistWorkerApp.class)
                .web(WebApplicationType.NONE)
                .properties(props)
                .run();
    }

    /**
     * Boots (once) the real test worker — {@code modules/test-worker} — against the shared
     * containers, on its own topic and its own consumer group.
     *
     * <p>{@code worker.persistence=jpa} against the same PostgreSQL the engine uses, so its two
     * tables land in the same schema and a test can read what the worker recorded with the same
     * {@link #jdbc()} it reads the engine's state with. It is also the only place the JPA stores
     * are exercised against a real PostgreSQL rather than H2.
     *
     * <p>It sets {@code workflow.mode}/{@code workflow.persistence} even though the shipped worker
     * has neither and needs neither. That is an artifact of this suite, not of the worker: the
     * engine is on <em>this</em> JVM's classpath, so its
     * {@code EmbeddedModeAutoConfigurationExcluder} runs for every context started here, and at its
     * defaults (embedded + memory) it strips both the Cloud Stream autoconfiguration and the JPA
     * one — leaving this context with no {@code StreamBridge} and no {@code EntityManagerFactory}.
     * The properties are here to tell it to keep its hands off. In {@code worker-standalone-app}
     * the engine is not on the classpath at all, so the excluder does not exist and the worker
     * needs to declare nothing.
     */
    public static synchronized void ensureTestWorkerStarted() {
        ensureStarted();
        if (testWorkerContext != null) {
            return;
        }
        var props = new HashMap<String, Object>();
        props.put("spring.application.name", "dist-test-worker");
        props.put("spring.main.web-application-type", "none");
        props.put("spring.main.banner-mode", "off");
        props.put("workflow.mode", "kafka");
        props.put("workflow.persistence", "jpa");
        props.put("worker.persistence", "jpa");
        // Zero, so a scenario that says nothing about duration does not add two seconds to every
        // step of every test. Any scenario that cares states its own durationMs.
        props.put("worker.task-duration", "0s");
        props.put("spring.cloud.function.definition", "consumeWorkerEvent");
        props.put("spring.cloud.stream.kafka.binder.brokers", kafka.getBootstrapServers());
        props.put("spring.cloud.stream.bindings.consumeWorkerEvent-in-0.destination", "sim-work");
        props.put("spring.cloud.stream.bindings.consumeWorkerEvent-in-0.group", "sim-worker-group");
        props.put("spring.cloud.stream.bindings.consumeWorkerEvent-in-0.consumer.concurrency", "3");
        props.put("spring.cloud.stream.bindings.upstream.destination", "upstream");
        props.put("spring.datasource.url", postgres.getJdbcUrl());
        props.put("spring.datasource.username", postgres.getUsername());
        props.put("spring.datasource.password", postgres.getPassword());
        props.put("spring.jpa.hibernate.ddl-auto", "update");
        props.put("spring.jpa.open-in-view", "false");
        props.put("logging.level.io.mateu.workflow", "WARN");
        testWorkerContext = new SpringApplicationBuilder(DistTestWorkerApp.class)
                .web(WebApplicationType.NONE)
                .properties(props)
                .run();
    }

    /** Publishes a domain event onto the given topic exactly as an external system would. */
    public static void publishUpstream(DomainEvent event) {
        try {
            var json = eventWriter.writerFor(DomainEvent.class).writeValueAsString(event);
            producer.send(new ProducerRecord<>("upstream", json));
            producer.flush();
        } catch (Exception e) {
            throw new IllegalStateException("Could not publish upstream event", e);
        }
    }

    /** {@link #publishUpstream(DomainEvent)} with record headers, written as UTF-8 bytes. */
    public static void publishUpstream(DomainEvent event, Map<String, String> headers) {
        try {
            var json = eventWriter.writerFor(DomainEvent.class).writeValueAsString(event);
            var record = new ProducerRecord<String, String>("upstream", json);
            headers.forEach((name, value) ->
                    record.headers().add(name, value.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
            producer.send(record);
            producer.flush();
        } catch (Exception e) {
            throw new IllegalStateException("Could not publish upstream event", e);
        }
    }

    /** Fire-and-forget variant for bulk publishing (call {@link #flushProducer()} afterwards). */
    public static void publishUpstreamAsync(DomainEvent event) {
        try {
            var json = eventWriter.writerFor(DomainEvent.class).writeValueAsString(event);
            producer.send(new ProducerRecord<>("upstream", json));
        } catch (Exception e) {
            throw new IllegalStateException("Could not publish upstream event", e);
        }
    }

    /**
     * Publishes bytes onto the upstream topic without asking whether they are an event.
     *
     * <p>{@link #publishUpstream} cannot express this: it serialises a real event, so everything it
     * writes is by construction readable. A producer outside the engine has no such guarantee — it
     * writes whatever it built, and a template that interpolates an unescaped value writes JSON that
     * only looks valid.
     */
    public static void publishRawUpstream(String payload) {
        producer.send(new ProducerRecord<>("upstream", payload));
        producer.flush();
    }

    public static void flushProducer() {
        producer.flush();
    }

    /** Direct JDBC access to the shared PostgreSQL for state assertions. */
    /** A fresh, independent session — used to act as a second pod against the same database. */
    public static Connection newSession() {
        ensureStarted();
        try {
            var connection = DriverManager.getConnection(
                    postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
            connection.setAutoCommit(false);
            return connection;
        } catch (Exception e) {
            throw new IllegalStateException("Could not open a database session", e);
        }
    }

    /** Bootstrap servers of the shared Kafka container, for tests that inspect the topics. */
    public static String kafkaBootstrapServers() {
        ensureStarted();
        return kafka.getBootstrapServers();
    }

    // ---- Sharded-topology helpers (DIST-22) ---------------------------------------------------
    // A real shard is a full stack with its OWN database and its own per-shard topics, plus the one
    // shared `messages` topic. These give a test that shape inside the shared containers: extra
    // databases in the one PostgreSQL (so each shard's rows are genuinely separate) and the per-shard
    // and shared topics on the one Kafka.

    public static String postgresUsername() {
        ensureStarted();
        return postgres.getUsername();
    }

    public static String postgresPassword() {
        ensureStarted();
        return postgres.getPassword();
    }

    /** The base JDBC url with its database swapped for {@code database} — another database in the same
     *  container, standing in for a shard's or the fleet's own database. */
    public static String jdbcUrlFor(String database) {
        ensureStarted();
        var url = postgres.getJdbcUrl();
        var pathStart = url.indexOf('/', "jdbc:postgresql://".length());
        var query = url.contains("?") ? url.substring(url.indexOf('?')) : "";
        return url.substring(0, pathStart) + "/" + database + query;
    }

    /** Creates a database in the shared container if it is not already there. */
    public static void createDatabase(String name) {
        ensureStarted();
        try (var connection = DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             var statement = connection.createStatement()) {
            try (var rs = statement.executeQuery("SELECT 1 FROM pg_database WHERE datname = '" + name + "'")) {
                if (rs.next()) {
                    return;
                }
            }
            statement.executeUpdate("CREATE DATABASE " + name);
        } catch (Exception e) {
            throw new IllegalStateException("Could not create database " + name, e);
        }
    }

    /** A JdbcTemplate against one of the per-shard / fleet databases. */
    public static JdbcTemplate jdbcFor(String database) {
        return new JdbcTemplate(new DriverManagerDataSource(
                jdbcUrlFor(database), postgres.getUsername(), postgres.getPassword()));
    }

    /** Creates the given Kafka topics; topics that already exist in the shared container are ignored. */
    public static void createTopics(List<String> names) {
        ensureStarted();
        try (var admin = AdminClient.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
            var topics = names.stream().map(n -> new NewTopic(n, PARTITIONS, (short) 1)).toList();
            admin.createTopics(topics).all().get();
        } catch (Exception alreadyExistsOrRace) {
            // Shared container across classes: the topics may already be there, which is fine.
        }
    }

    /** Publishes a domain event onto an arbitrary topic, fire-and-forget (call {@link #flushProducer()}). */
    public static void publishToAsync(String topic, DomainEvent event) {
        try {
            var json = eventWriter.writerFor(DomainEvent.class).writeValueAsString(event);
            producer.send(new ProducerRecord<>(topic, json));
        } catch (Exception e) {
            throw new IllegalStateException("Could not publish to " + topic, e);
        }
    }

    /** A harness knob, overridable with -Dtune.&lt;name&gt; so a sweep needs no recompile. */
    private static String tune(String name, String fallback) {
        return System.getProperty("tune." + name, fallback);
    }

    /** The names and values actually in force, so a measurement can say what produced it. */
    public static String tuning() {
        return "outbox-poll-ms=" + tune("outbox-poll-ms", "200")
                + " outbox-batch-size=" + tune("outbox-batch-size", "100")
                + " consumer-concurrency=" + tune("consumer-concurrency", "3")
                + " max-poll-records=" + tune("max-poll-records", "500")
                + " producer-linger-ms=" + tune("producer-linger-ms", "0")
                + " pool-size=" + tune("pool-size", "10")
                + " partitions=" + tune("partitions", "6");
    }

    public static JdbcTemplate jdbc() {
        ensureStarted();
        return jdbc;
    }

    /**
     * Freezes every orchestrator's outbox relay by taking the relay gate <b>exclusively</b>.
     * The relays hold that same advisory lock in shared mode while draining — shared holders do
     * not block each other, so all pods relay concurrently, but this exclusive holder shuts them
     * all down at once. While held, domain events pile up as {@code Pending} rows — the
     * deterministic "crashed between committing a step and dispatching the next" window DIST-02
     * needs. Returns the holding connection; pass it to {@link #unblockOutboxRelay}.
     */
    public static Connection blockOutboxRelay() {
        ensureStarted();
        try {
            var connection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
            try (var statement = connection.createStatement()) {
                statement.execute("SELECT pg_advisory_lock(" + OUTBOX_RELAY_LOCK_ID + ")");
            }
            return connection;
        } catch (Exception e) {
            throw new IllegalStateException("Could not acquire outbox relay lock", e);
        }
    }

    public static void unblockOutboxRelay(Connection connection) {
        try (connection; var statement = connection.createStatement()) {
            statement.execute("SELECT pg_advisory_unlock(" + OUTBOX_RELAY_LOCK_ID + ")");
        } catch (Exception e) {
            throw new IllegalStateException("Could not release outbox relay lock", e);
        }
    }

    // ── Kafka chaos: make the broker unresponsive and responsive again at the SAME address ──
    // We PAUSE/UNPAUSE the container rather than stop/start: a KRaft broker restarted with
    // docker stop/start does not re-advertise its listeners (the mapped port is only wired at
    // first boot), so it never becomes reachable again. Pausing (SIGSTOP) freezes the broker —
    // clients' requests time out exactly like a network partition — and unpausing (SIGCONT)
    // brings it back instantly with the same listeners, port and KRaft state, so bound consumers
    // and producers simply resume. Not synchronized on purpose (see startOrchestrator).

    /** Freezes the Kafka broker: in-flight and new requests hang/time out, as in an outage. */
    public static void pauseKafka() {
        DockerClientFactory.instance().client().pauseContainerCmd(kafka.getContainerId()).exec();
    }

    /** Resumes the Kafka broker (same address) and blocks until it accepts admin requests again. */
    public static void resumeKafka() {
        var docker = DockerClientFactory.instance().client();
        var paused = Boolean.TRUE.equals(
                docker.inspectContainerCmd(kafka.getContainerId()).exec().getState().getPaused());
        if (paused) { // idempotent: an @AfterEach safety net after a passing test is a no-op
            docker.unpauseContainerCmd(kafka.getContainerId()).exec();
        }
        awaitKafkaReady();
    }

    // ── PostgreSQL chaos: same pause/unpause technique as Kafka (a stopped container would
    // come back on a different mapped port, but SIGSTOP/SIGCONT keeps address and state). ──

    /** Freezes PostgreSQL: connection attempts and in-flight statements hang/time out. */
    public static void pausePostgres() {
        DockerClientFactory.instance().client().pauseContainerCmd(postgres.getContainerId()).exec();
    }

    /** Resumes PostgreSQL (same address) and blocks until it accepts connections again. */
    public static void resumePostgres() {
        var docker = DockerClientFactory.instance().client();
        var paused = Boolean.TRUE.equals(
                docker.inspectContainerCmd(postgres.getContainerId()).exec().getState().getPaused());
        if (paused) { // idempotent: an @AfterEach safety net after a passing test is a no-op
            docker.unpauseContainerCmd(postgres.getContainerId()).exec();
        }
        awaitPostgresReady();
    }

    private static void awaitPostgresReady() {
        var deadline = System.currentTimeMillis() + 60_000;
        Exception last = null;
        DriverManager.setLoginTimeout(3);
        while (System.currentTimeMillis() < deadline) {
            try (var connection = DriverManager.getConnection(
                    postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
                 var statement = connection.createStatement()) {
                statement.execute("SELECT 1");
                return;
            } catch (Exception e) {
                last = e;
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted waiting for PostgreSQL", ie);
                }
            }
        }
        throw new IllegalStateException("PostgreSQL did not become ready after resume", last);
    }

    private static void awaitKafkaReady() {
        var deadline = System.currentTimeMillis() + 60_000;
        RuntimeException last = null;
        while (System.currentTimeMillis() < deadline) {
            try (var admin = AdminClient.create(Map.of(
                    "bootstrap.servers", kafka.getBootstrapServers(),
                    "default.api.timeout.ms", "3000",
                    "request.timeout.ms", "2000"))) {
                admin.listTopics().names().get();
                return;
            } catch (Exception e) {
                last = new IllegalStateException("Kafka not ready yet", e);
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted waiting for Kafka", ie);
                }
            }
        }
        throw new IllegalStateException("Kafka did not become ready after restart", last);
    }
}
