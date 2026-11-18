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
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
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

/**
 * End-to-end check that {@link StreamsConfig#DEFAULT_INTERACTIVE_QUERY_ISOLATION_LEVEL_CONFIG} takes
 * effect for IQv1 composite stores.
 *
 * <p>The test exercises the full wiring path:
 * <ol>
 *   <li>{@link KafkaStreams#store(StoreQueryParameters)} returns a composite read-only store.</li>
 *   <li>That composite resolves each underlying terminal store through {@code readOnly(configuredDefault)}
 *       — for an in-memory transactional store the view either consults the transaction buffer or reads
 *       directly from the backing {@code NavigableMap}; for a persistent RocksDB store it selects the
 *       committed vs pending {@code DBAccessor}.</li>
 *   <li>The terminal store's view honours the requested level, hiding or exposing writes that have been
 *       staged in the transaction buffer but not yet flushed by a Streams commit.</li>
 * </ol>
 *
 * <p>Every IQv1 composite store family that carries a {@code readOnly(IsolationLevel)} override is covered,
 * across both in-memory and persistent backings: {@link ReadOnlyKeyValueStore}, {@link ReadOnlyWindowStore}
 * and {@link ReadOnlySessionStore} (see {@link Shape}). Versioned stores have no IQv1
 * {@code QueryableStoreType}, so they are exercised by the IQv2 companion test only.
 */
@Tag("integration")
@Timeout(600)
public class IQv1IsolationLevelIntegrationTest {

    private static final int NUM_BROKERS = 1;
    private static final String INPUT_TOPIC = "iq-iso-v1-input";
    private static final String KV_STORE = "iq-iso-v1-kv";
    private static final String WINDOW_STORE = "iq-iso-v1-window";
    private static final String SESSION_STORE = "iq-iso-v1-session";
    private static final Duration WINDOW_SIZE = Duration.ofSeconds(1);
    private static final Duration WINDOW_RETENTION = Duration.ofHours(1);
    private static final Duration SESSION_RETENTION = Duration.ofHours(1);

    public static final EmbeddedKafkaCluster CLUSTER =
        new EmbeddedKafkaCluster(NUM_BROKERS, IsolationLevelIntegrationFixtures.singleBrokerEosOverrides());

    private static final AtomicInteger APP_COUNTER = new AtomicInteger();

    /**
     * Discriminates the IQv1 composite store families and backings. Each constant knows how to build the
     * StoreBuilder, stage a write into its store, and read a single value back through a composite view at a
     * given isolation level — keeping the test body shape-free.
     */
    enum Shape {
        IN_MEMORY_KV(KV_STORE, Shape::buildInMemoryKvStore, QueryableStoreTypes.keyValueStore(),
                Shape::stageKvWrite, Shape::readKv),
        PERSISTENT_KV(KV_STORE, Shape::buildPersistentKvStore, QueryableStoreTypes.keyValueStore(),
                Shape::stageKvWrite, Shape::readKv),
        IN_MEMORY_WINDOW(WINDOW_STORE, Shape::buildInMemoryWindowStore, QueryableStoreTypes.windowStore(),
                Shape::stageWindowWrite, Shape::readWindow),
        PERSISTENT_WINDOW(WINDOW_STORE, Shape::buildPersistentWindowStore, QueryableStoreTypes.windowStore(),
                Shape::stageWindowWrite, Shape::readWindow),
        IN_MEMORY_SESSION(SESSION_STORE, Shape::buildInMemorySessionStore, QueryableStoreTypes.sessionStore(),
                Shape::stageSessionWrite, Shape::readSession),
        PERSISTENT_SESSION(SESSION_STORE, Shape::buildPersistentSessionStore, QueryableStoreTypes.sessionStore(),
                Shape::stageSessionWrite, Shape::readSession);

        final String storeName;
        final Function<String, StoreBuilder<?>> builder;
        final QueryableStoreType<?> queryableType;
        final WriteStep writeStep;
        final Reader reader;

        Shape(final String storeName,
              final Function<String, StoreBuilder<?>> builder,
              final QueryableStoreType<?> queryableType,
              final WriteStep writeStep,
              final Reader reader) {
            this.storeName = storeName;
            this.builder = builder;
            this.queryableType = queryableType;
            this.writeStep = writeStep;
            this.reader = reader;
        }

        StoreBuilder<?> store() {
            return builder.apply(storeName);
        }

        /** Align an event timestamp to the start of its WINDOW_SIZE window. */
        private static long windowStartFor(final long timestamp) {
            return timestamp - (timestamp % WINDOW_SIZE.toMillis());
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
            store.put(record.key(), record.value(), windowStartFor(record.timestamp()));
        }

        private static void stageSessionWrite(final ProcessorContext<Void, Void> ctx,
                                              final Record<Integer, Integer> record,
                                              final String storeName) {
            final SessionStore<Integer, Integer> store = ctx.getStateStore(storeName);
            store.put(new Windowed<>(record.key(), new SessionWindow(record.timestamp(), record.timestamp())),
                record.value());
        }

        @SuppressWarnings("unchecked")
        private static Integer readKv(final Object composite, final Optional<IsolationLevel> override) {
            ReadOnlyKeyValueStore<Integer, Integer> view = (ReadOnlyKeyValueStore<Integer, Integer>) composite;
            if (override.isPresent()) {
                view = view.readOnly(override.get());
            }
            return view.get(TEST_KEY);
        }

        @SuppressWarnings("unchecked")
        private static Integer readWindow(final Object composite, final Optional<IsolationLevel> override) {
            ReadOnlyWindowStore<Integer, Integer> view = (ReadOnlyWindowStore<Integer, Integer>) composite;
            if (override.isPresent()) {
                view = view.readOnly(override.get());
            }
            final Instant t = Instant.ofEpochMilli(windowStartFor(TEST_TIMESTAMP));
            try (WindowStoreIterator<Integer> it = view.fetch(TEST_KEY, t, t)) {
                return it.hasNext() ? it.next().value : null;
            }
        }

        @SuppressWarnings("unchecked")
        private static Integer readSession(final Object composite, final Optional<IsolationLevel> override) {
            ReadOnlySessionStore<Integer, Integer> view = (ReadOnlySessionStore<Integer, Integer>) composite;
            if (override.isPresent()) {
                view = view.readOnly(override.get());
            }
            try (KeyValueIterator<Windowed<Integer>, Integer> it = view.fetch(TEST_KEY)) {
                return it.hasNext() ? it.next().value : null;
            }
        }
    }

    @FunctionalInterface
    interface WriteStep {
        void stage(ProcessorContext<Void, Void> ctx, Record<Integer, Integer> record, String storeName);
    }

    @FunctionalInterface
    interface Reader {
        Integer read(Object composite, Optional<IsolationLevel> override);
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
        return Stream.of(Shape.values())
            .flatMap(shape -> Stream.of(
                Arguments.of(shape, IsolationLevel.READ_UNCOMMITTED),
                Arguments.of(shape, IsolationLevel.READ_COMMITTED)
            ));
    }

    /**
     * Two things to prove at each configured default level:
     * <ol>
     *   <li>A bare composite ({@code streams.store(...)}) resolves the level from the configured default
     *       — staged writes visible iff {@code READ_UNCOMMITTED}.</li>
     *   <li>Calling {@code composite.readOnly(otherLevel)} on the returned view routes subsequent reads at
     *       that level instead — the per-query escape hatch.</li>
     * </ol>
     *
     * <p>After releasing the stall, the test also waits for the next Streams commit interval to fire
     * and verifies that the committed value becomes visible through {@code READ_COMMITTED} — proving
     * that transactional state stores are flushed on the normal commit cycle in EOS mode.
     */
    @ParameterizedTest(name = "{0} default={1}")
    @MethodSource("cases")
    public void shouldHonourConfiguredDefaultAndPerQueryOverride(final Shape shape,
                                                                 final IsolationLevel configured) throws Exception {
        startPipeline(shape, configured);

        // Drive a single record through: the processor writes it, then parks in the stall gate before the
        // StreamThread can reach its next commit point. Writes are now live in the transaction buffer.
        sendOne(CLUSTER.bootstrapServers(), INPUT_TOPIC, TEST_KEY, TEST_VALUE, TEST_TIMESTAMP);
        gate.awaitStalled();

        // The bare composite resolves the level from the configured default. Explicit per-query overrides
        // route reads at the requested level regardless of the default. These three assertions together
        // also prove the processor ran and staged the write: only the READ_UNCOMMITTED view can see a
        // value that hasn't flushed yet.
        assertPreCommitVisibility(shape, Optional.empty(),                             configured);
        assertPreCommitVisibility(shape, Optional.of(IsolationLevel.READ_UNCOMMITTED), IsolationLevel.READ_UNCOMMITTED);
        assertPreCommitVisibility(shape, Optional.of(IsolationLevel.READ_COMMITTED),   IsolationLevel.READ_COMMITTED);

        // Release the stall. On the next commit.interval.ms tick, postCommit → maybeCheckpoint →
        // stateMgr.commit flushes the transaction buffer into the base store; both isolation levels
        // must converge on the now-durable value.
        gate.release();
        gate.awaitExit();
        waitForCondition(
            () -> Integer.valueOf(TEST_VALUE).equals(readValue(shape, Optional.of(IsolationLevel.READ_COMMITTED))),
            COMMIT_WAIT.toMillis(),
            "IQv1 READ_COMMITTED view never observed the committed value for " + shape + "; commit did not propagate"
        );
        assertEquals(Integer.valueOf(TEST_VALUE), readValue(shape, Optional.of(IsolationLevel.READ_UNCOMMITTED)));
    }

    private void assertPreCommitVisibility(final Shape shape,
                                           final Optional<IsolationLevel> override,
                                           final IsolationLevel effective) throws Exception {
        final Integer observed = readValue(shape, override);
        if (effective == IsolationLevel.READ_UNCOMMITTED) {
            assertEquals(Integer.valueOf(TEST_VALUE), observed,
                () -> "Expected " + shape + " override=" + override + " effective=" + effective
                    + " to expose the staged value, saw " + observed);
        } else {
            assertNull(observed,
                () -> "Expected " + shape + " override=" + override + " effective=" + effective
                    + " to hide the staged value, saw " + observed);
        }
    }

    private Integer readValue(final Shape shape, final Optional<IsolationLevel> override) throws Exception {
        final Object composite = IntegrationTestUtils.getStore(
            streams,
            StoreQueryParameters.fromNameAndType(shape.storeName, shape.queryableType)
        );
        return shape.reader.read(composite, override);
    }

    private void startPipeline(final Shape shape, final IsolationLevel configured) throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(shape.store());

        final StallGate runtimeGate = gate;
        final Shape runtimeShape = shape;
        builder.<Integer, Integer>stream(INPUT_TOPIC)
            .process(() -> new Processor<Integer, Integer, Void, Void>() {
                private ProcessorContext<Void, Void> ctx;

                @Override
                public void init(final ProcessorContext<Void, Void> context) {
                    this.ctx = context;
                }

                @Override
                public void process(final Record<Integer, Integer> record) {
                    runtimeShape.writeStep.stage(ctx, record, runtimeShape.storeName);
                    runtimeGate.spinHere();
                }
            }, shape.storeName);

        final Properties config = baseStreamsConfig();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "iq-iso-v1-" + APP_COUNTER.incrementAndGet());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        config.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        config.put(StreamsConfig.DEFAULT_INTERACTIVE_QUERY_ISOLATION_LEVEL_CONFIG, configured.name());

        streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp();
        startApplicationAndWaitUntilRunning(streams);
    }
}
