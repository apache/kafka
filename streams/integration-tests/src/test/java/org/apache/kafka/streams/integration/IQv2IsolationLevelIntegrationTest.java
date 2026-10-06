/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.integration;

import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.MultiVersionedKeyQuery;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.RangeQuery;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.query.VersionedKeyQuery;
import org.apache.kafka.streams.query.WindowKeyQuery;
import org.apache.kafka.streams.query.WindowRangeQuery;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.VersionedRecord;
import org.apache.kafka.streams.state.VersionedRecordIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.kafka.streams.integration.IsolationLevelIntegrationFixtures.COMMIT_WAIT;
import static org.apache.kafka.streams.integration.IsolationLevelIntegrationFixtures.StallGate;
import static org.apache.kafka.streams.integration.IsolationLevelIntegrationFixtures.TEST_KEY;
import static org.apache.kafka.streams.integration.IsolationLevelIntegrationFixtures.TEST_TIMESTAMP;
import static org.apache.kafka.streams.integration.IsolationLevelIntegrationFixtures.TEST_VALUE;
import static org.apache.kafka.streams.integration.IsolationLevelIntegrationFixtures.baseStreamsConfig;
import static org.apache.kafka.streams.integration.IsolationLevelIntegrationFixtures.sendOne;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * IQv2 counterpart to {@link IQv1IsolationLevelIntegrationTest}. Two things to prove, both within a single
 * stall window:
 * <ol>
 *   <li>{@link StateQueryRequest#withIsolationLevel(IsolationLevel)} takes precedence over
 *       {@link StreamsConfig#DEFAULT_INTERACTIVE_QUERY_ISOLATION_LEVEL_CONFIG}.</li>
 *   <li>Omitting the per-query override falls back to the configured default.</li>
 * </ol>
 *
 * <p>Each parameter case runs against one configured default and asserts three resolutions:
 * implicit default, explicit {@code READ_UNCOMMITTED}, explicit {@code READ_COMMITTED}. This exercises both
 * directions of override in the same run.
 *
 * <p>The isolation level is resolved independently by each query runner in {@code StoreQueryUtils} — every
 * runner makes its own {@code readOnly(config.getIsolationLevel())} call against the terminal store. So the
 * {@link Scenario}s below cover all seven isolation-aware runners: {@code KeyQuery} and {@code RangeQuery}
 * over key-value stores; {@code WindowKeyQuery} and {@code WindowRangeQuery} over window stores;
 * {@code WindowRangeQuery} over session stores; and {@code VersionedKeyQuery} and
 * {@code MultiVersionedKeyQuery} over versioned stores. Both in-memory and persistent backings are covered
 * where the store shape has both.
 */
@Tag("integration")
@Timeout(600)
public class IQv2IsolationLevelIntegrationTest {

    private static final int NUM_BROKERS = 1;
    private static final String INPUT_TOPIC = "iq-iso-v2-input";
    private static final String KV_STORE = "iq-iso-v2-kv";
    private static final String WINDOW_STORE = "iq-iso-v2-window";
    private static final String SESSION_STORE = "iq-iso-v2-session";
    private static final String VERSIONED_STORE = "iq-iso-v2-versioned";
    private static final Duration WINDOW_SIZE = Duration.ofSeconds(1);
    private static final Duration WINDOW_RETENTION = Duration.ofHours(1);
    private static final Duration SESSION_RETENTION = Duration.ofHours(1);

    public static final EmbeddedKafkaCluster CLUSTER =
        new EmbeddedKafkaCluster(NUM_BROKERS, IsolationLevelIntegrationFixtures.singleBrokerEosOverrides());

    private static final AtomicInteger APP_COUNTER = new AtomicInteger();

    /**
     * A single (store shape × query type) combination. Each constant maps to exactly one isolation-aware
     * runner in {@code StoreQueryUtils}, and knows how to build the StoreBuilder, stage a write into its
     * store, produce the query, and turn a StateQueryResult into an observed value — keeping the test body
     * shape-free.
     */
    enum Scenario {
        IN_MEMORY_KV_POINT(KV_STORE, Shape.IN_MEMORY_KV, Scenario::kvQuery, Scenario::extractPoint),
        PERSISTENT_KV_POINT(KV_STORE, Shape.PERSISTENT_KV, Scenario::kvQuery, Scenario::extractPoint),
        IN_MEMORY_KV_RANGE(KV_STORE, Shape.IN_MEMORY_KV, Scenario::kvRangeQuery, Scenario::extractKeyValueIterator),
        PERSISTENT_KV_RANGE(KV_STORE, Shape.PERSISTENT_KV, Scenario::kvRangeQuery, Scenario::extractKeyValueIterator),
        IN_MEMORY_WINDOW_POINT(WINDOW_STORE, Shape.IN_MEMORY_WINDOW, Scenario::windowKeyQuery, Scenario::extractWindowIterator),
        PERSISTENT_WINDOW_POINT(WINDOW_STORE, Shape.PERSISTENT_WINDOW, Scenario::windowKeyQuery, Scenario::extractWindowIterator),
        IN_MEMORY_WINDOW_RANGE(WINDOW_STORE, Shape.IN_MEMORY_WINDOW, Scenario::windowRangeQuery, Scenario::extractWindowedIterator),
        PERSISTENT_WINDOW_RANGE(WINDOW_STORE, Shape.PERSISTENT_WINDOW, Scenario::windowRangeQuery, Scenario::extractWindowedIterator),
        IN_MEMORY_SESSION(SESSION_STORE, Shape.IN_MEMORY_SESSION, Scenario::sessionQuery, Scenario::extractWindowedIterator),
        PERSISTENT_SESSION(SESSION_STORE, Shape.PERSISTENT_SESSION, Scenario::sessionQuery, Scenario::extractWindowedIterator),
        PERSISTENT_VERSIONED_POINT(VERSIONED_STORE, Shape.PERSISTENT_VERSIONED, Scenario::versionedKeyQuery, Scenario::extractVersioned),
        PERSISTENT_VERSIONED_MULTI(VERSIONED_STORE, Shape.PERSISTENT_VERSIONED, Scenario::multiVersionedKeyQuery, Scenario::extractVersionedIterator);

        final String storeName;
        final Shape shape;
        final QueryFactory queryFactory;
        final ResultExtractor extractor;

        Scenario(final String storeName,
                 final Shape shape,
                 final QueryFactory queryFactory,
                 final ResultExtractor extractor) {
            this.storeName = storeName;
            this.shape = shape;
            this.queryFactory = queryFactory;
            this.extractor = extractor;
        }

        StoreBuilder<?> store() {
            return shape.builder.apply(storeName);
        }

        void stage(final ProcessorContext<Void, Void> ctx, final Record<Integer, Integer> record) {
            shape.writeStep.stage(ctx, record, storeName);
        }

        /** Align an event timestamp to the start of its WINDOW_SIZE window. */
        private static long windowStartFor(final long timestamp) {
            return timestamp - (timestamp % WINDOW_SIZE.toMillis());
        }

        private static Query<?> kvQuery() {
            return KeyQuery.<Integer, Integer>withKey(TEST_KEY);
        }

        private static Query<?> kvRangeQuery() {
            return RangeQuery.<Integer, Integer>withRange(TEST_KEY, TEST_KEY);
        }

        private static Query<?> windowKeyQuery() {
            final Instant t = Instant.ofEpochMilli(windowStartFor(TEST_TIMESTAMP));
            return WindowKeyQuery.<Integer, Integer>withKeyAndWindowStartRange(TEST_KEY, t, t);
        }

        private static Query<?> windowRangeQuery() {
            final Instant t = Instant.ofEpochMilli(windowStartFor(TEST_TIMESTAMP));
            return WindowRangeQuery.<Integer, Integer>withWindowStartRange(t, t);
        }

        private static Query<?> sessionQuery() {
            return WindowRangeQuery.<Integer, Integer>withKey(TEST_KEY);
        }

        private static Query<?> versionedKeyQuery() {
            return VersionedKeyQuery.<Integer, Integer>withKey(TEST_KEY);
        }

        private static Query<?> multiVersionedKeyQuery() {
            return MultiVersionedKeyQuery.<Integer, Integer>withKey(TEST_KEY);
        }

        private static Integer extractPoint(@SuppressWarnings("rawtypes") final StateQueryResult result) {
            return (Integer) onlyResult(result);
        }

        @SuppressWarnings("unchecked")
        private static Integer extractKeyValueIterator(@SuppressWarnings("rawtypes") final StateQueryResult result) {
            final Object raw = onlyResult(result);
            if (raw == null) {
                return null;
            }
            try (KeyValueIterator<Integer, Integer> it = (KeyValueIterator<Integer, Integer>) raw) {
                return it.hasNext() ? it.next().value : null;
            }
        }

        @SuppressWarnings("unchecked")
        private static Integer extractWindowIterator(@SuppressWarnings("rawtypes") final StateQueryResult result) {
            final Object raw = onlyResult(result);
            if (raw == null) {
                return null;
            }
            try (WindowStoreIterator<Integer> it = (WindowStoreIterator<Integer>) raw) {
                return it.hasNext() ? it.next().value : null;
            }
        }

        @SuppressWarnings("unchecked")
        private static Integer extractWindowedIterator(@SuppressWarnings("rawtypes") final StateQueryResult result) {
            final Object raw = onlyResult(result);
            if (raw == null) {
                return null;
            }
            try (KeyValueIterator<Windowed<Integer>, Integer> it = (KeyValueIterator<Windowed<Integer>, Integer>) raw) {
                return it.hasNext() ? it.next().value : null;
            }
        }

        @SuppressWarnings("unchecked")
        private static Integer extractVersioned(@SuppressWarnings("rawtypes") final StateQueryResult result) {
            final Object raw = onlyResult(result);
            return raw == null ? null : ((VersionedRecord<Integer>) raw).value();
        }

        @SuppressWarnings("unchecked")
        private static Integer extractVersionedIterator(@SuppressWarnings("rawtypes") final StateQueryResult result) {
            final Object raw = onlyResult(result);
            if (raw == null) {
                return null;
            }
            try (VersionedRecordIterator<Integer> it = (VersionedRecordIterator<Integer>) raw) {
                return it.hasNext() ? it.next().value() : null;
            }
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        private static Object onlyResult(final StateQueryResult result) {
            final QueryResult qr = result.getOnlyPartitionResult();
            if (qr == null || qr.isFailure()) {
                return null;
            }
            return qr.getResult();
        }
    }

    /**
     * The distinct terminal store implementations. Each carries the store builder and the write step; a
     * {@link Scenario} pairs a shape with the query type that targets it.
     */
    enum Shape {
        IN_MEMORY_KV(Shape::buildInMemoryKvStore, Shape::stageKvWrite),
        PERSISTENT_KV(Shape::buildPersistentKvStore, Shape::stageKvWrite),
        IN_MEMORY_WINDOW(Shape::buildInMemoryWindowStore, Shape::stageWindowWrite),
        PERSISTENT_WINDOW(Shape::buildPersistentWindowStore, Shape::stageWindowWrite),
        IN_MEMORY_SESSION(Shape::buildInMemorySessionStore, Shape::stageSessionWrite),
        PERSISTENT_SESSION(Shape::buildPersistentSessionStore, Shape::stageSessionWrite),
        PERSISTENT_VERSIONED(Shape::buildPersistentVersionedStore, Shape::stageVersionedWrite);

        final Function<String, StoreBuilder<?>> builder;
        final WriteStep writeStep;

        Shape(final Function<String, StoreBuilder<?>> builder, final WriteStep writeStep) {
            this.builder = builder;
            this.writeStep = writeStep;
        }

        private static StoreBuilder<?> buildInMemoryKvStore(final String name) {
            return Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(name), Serdes.Integer(), Serdes.Integer());
        }

        private static StoreBuilder<?> buildPersistentKvStore(final String name) {
            return Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(name), Serdes.Integer(), Serdes.Integer());
        }

        private static StoreBuilder<?> buildInMemoryWindowStore(final String name) {
            return Stores.windowStoreBuilder(
                Stores.inMemoryWindowStore(name, WINDOW_RETENTION, WINDOW_SIZE, false),
                Serdes.Integer(),
                Serdes.Integer());
        }

        private static StoreBuilder<?> buildPersistentWindowStore(final String name) {
            return Stores.windowStoreBuilder(
                Stores.persistentWindowStore(name, WINDOW_RETENTION, WINDOW_SIZE, false),
                Serdes.Integer(),
                Serdes.Integer());
        }

        private static StoreBuilder<?> buildInMemorySessionStore(final String name) {
            return Stores.sessionStoreBuilder(
                Stores.inMemorySessionStore(name, SESSION_RETENTION), Serdes.Integer(), Serdes.Integer());
        }

        private static StoreBuilder<?> buildPersistentSessionStore(final String name) {
            return Stores.sessionStoreBuilder(
                Stores.persistentSessionStore(name, SESSION_RETENTION), Serdes.Integer(), Serdes.Integer());
        }

        private static StoreBuilder<?> buildPersistentVersionedStore(final String name) {
            return Stores.versionedKeyValueStoreBuilder(
                Stores.persistentVersionedKeyValueStore(name, Duration.ofHours(1), Duration.ofMinutes(10)),
                Serdes.Integer(),
                Serdes.Integer());
        }

        private static void stageKvWrite(final ProcessorContext<Void, Void> ctx,
                                         final Record<Integer, Integer> record,
                                         final String storeName) {
            final KeyValueStore<Integer, Integer> store = ctx.getStateStore(storeName);
            store.put(record.key(), record.value());
        }

        private static void stageWindowWrite(final ProcessorContext<Void, Void> ctx,
                                             final Record<Integer, Integer> record,
                                             final String storeName) {
            final WindowStore<Integer, Integer> store = ctx.getStateStore(storeName);
            store.put(record.key(), record.value(), Scenario.windowStartFor(record.timestamp()));
        }

        private static void stageSessionWrite(final ProcessorContext<Void, Void> ctx,
                                              final Record<Integer, Integer> record,
                                              final String storeName) {
            final SessionStore<Integer, Integer> store = ctx.getStateStore(storeName);
            store.put(new Windowed<>(record.key(), new SessionWindow(record.timestamp(), record.timestamp())),
                record.value());
        }

        private static void stageVersionedWrite(final ProcessorContext<Void, Void> ctx,
                                                final Record<Integer, Integer> record,
                                                final String storeName) {
            final VersionedKeyValueStore<Integer, Integer> store = ctx.getStateStore(storeName);
            store.put(record.key(), record.value(), record.timestamp());
        }
    }

    @FunctionalInterface
    interface WriteStep {
        void stage(ProcessorContext<Void, Void> ctx, Record<Integer, Integer> record, String storeName);
    }

    @FunctionalInterface
    interface QueryFactory {
        Query<?> create();
    }

    @FunctionalInterface
    interface ResultExtractor {
        Integer extract(@SuppressWarnings("rawtypes") StateQueryResult result);
    }

    private KafkaStreams streams;
    private StallGate gate;

    @BeforeAll
    public static void startCluster() throws Exception {
        CLUSTER.start();
    }

    @AfterAll
    public static void stopCluster() {
        CLUSTER.stop();
    }

    @BeforeEach
    public void resetFixtures() throws Exception {
        CLUSTER.deleteTopic(INPUT_TOPIC);
        CLUSTER.createTopic(INPUT_TOPIC, 1, 1);
        gate = new StallGate();
    }

    @AfterEach
    public void stopStreams() {
        if (streams != null) {
            streams.close(Duration.ofSeconds(30));
            streams.cleanUp();
        }
    }

    static Stream<Arguments> cases() {
        return Stream.of(Scenario.values())
            .flatMap(scenario -> Stream.of(
                Arguments.of(scenario, IsolationLevel.READ_UNCOMMITTED),
                Arguments.of(scenario, IsolationLevel.READ_COMMITTED)
            ));
    }

    /**
     * Parameterised on (scenario × configured default). For every case we pin the write in the transaction
     * buffer, then fire three queries: implicit default, explicit UNCOMMITTED, explicit COMMITTED. Visibility
     * must track the effective level — not the configured default — in all three.
     *
     * <p>After releasing the stall, the test waits for the next commit interval and verifies both
     * levels converge on the now-durable value.
     */
    @ParameterizedTest(name = "{0} default={1}")
    @MethodSource("cases")
    public void perQueryOverrideBeatsConfiguredDefault(final Scenario scenario,
                                                       final IsolationLevel configured) throws Exception {
        startPipeline(scenario, configured);

        sendOne(CLUSTER.bootstrapServers(), INPUT_TOPIC, TEST_KEY, TEST_VALUE, TEST_TIMESTAMP);
        gate.awaitStalled();

        // Pre-commit: effective level alone determines visibility, whether chosen via the config default or
        // the per-query override. The configured default is irrelevant once the override is set.
        assertStagedVisibility(scenario, Optional.empty(),                             configured);
        assertStagedVisibility(scenario, Optional.of(IsolationLevel.READ_COMMITTED),   IsolationLevel.READ_COMMITTED);
        assertStagedVisibility(scenario, Optional.of(IsolationLevel.READ_UNCOMMITTED), IsolationLevel.READ_UNCOMMITTED);

        gate.release();
        gate.awaitExit();
        waitForCondition(
            () -> Integer.valueOf(TEST_VALUE).equals(readValue(scenario, Optional.of(IsolationLevel.READ_COMMITTED))),
            COMMIT_WAIT.toMillis(),
            "READ_COMMITTED never observed the committed value for " + scenario
        );
        assertEquals(Integer.valueOf(TEST_VALUE), readValue(scenario, Optional.empty()));
        assertEquals(Integer.valueOf(TEST_VALUE), readValue(scenario, Optional.of(IsolationLevel.READ_UNCOMMITTED)));
    }

    private void assertStagedVisibility(final Scenario scenario,
                                        final Optional<IsolationLevel> override,
                                        final IsolationLevel effective) {
        final Integer observed = readValue(scenario, override);
        if (effective == IsolationLevel.READ_UNCOMMITTED) {
            assertEquals(Integer.valueOf(TEST_VALUE), observed,
                () -> "Expected " + scenario + " override=" + override + " effective=" + effective
                    + " to expose the staged write, saw " + observed);
        } else {
            assertNull(observed,
                () -> "Expected " + scenario + " override=" + override + " effective=" + effective
                    + " to hide the staged write, saw " + observed);
        }
    }

    private Integer readValue(final Scenario scenario, final Optional<IsolationLevel> override) {
        StateQueryRequest<?> request =
            StateQueryRequest.inStore(scenario.storeName).withQuery(scenario.queryFactory.create());
        if (override.isPresent()) {
            request = request.withIsolationLevel(override.get());
        }
        final StateQueryResult<?> result = IntegrationTestUtils.iqv2WaitForResult(streams, request);
        assertTrue(result.getPartitionResults().size() >= 1, "query returned no partitions");
        return scenario.extractor.extract(result);
    }

    private void startPipeline(final Scenario scenario, final IsolationLevel configured) throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(scenario.store());

        final StallGate runtimeGate = gate;
        final Scenario runtimeScenario = scenario;
        builder.<Integer, Integer>stream(INPUT_TOPIC)
            .process(() -> new Processor<Integer, Integer, Void, Void>() {
                private ProcessorContext<Void, Void> ctx;

                @Override
                public void init(final ProcessorContext<Void, Void> context) {
                    this.ctx = context;
                }

                @Override
                public void process(final Record<Integer, Integer> record) {
                    runtimeScenario.stage(ctx, record);
                    runtimeGate.spinHere();
                }
            }, scenario.storeName);

        final Properties config = baseStreamsConfig();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "iq-iso-v2-" + APP_COUNTER.incrementAndGet());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        config.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        config.put(StreamsConfig.DEFAULT_INTERACTIVE_QUERY_ISOLATION_LEVEL_CONFIG, configured.name());

        streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp();
        startApplicationAndWaitUntilRunning(streams);
    }
}
