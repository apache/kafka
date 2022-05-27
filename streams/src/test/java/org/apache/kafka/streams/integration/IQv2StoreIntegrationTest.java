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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.RangeQuery;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.query.WindowKeyQuery;
import org.apache.kafka.streams.query.WindowRangeQuery;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.StoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.streams.query.StateQueryRequest.inStore;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Category({IntegrationTest.class})
@RunWith(value = Parameterized.class)
public class IQv2StoreIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(IQv2StoreIntegrationTest.class);

    private static final long SEED = new Random().nextLong();
    private static final Random RANDOM = new Random(SEED);

    private static final int NUM_BROKERS = 1;
    public static final Duration WINDOW_SIZE = Duration.ofMinutes(5);
    private static int port = 0;
    private static final String INPUT_TOPIC_NAME = "input-topic";
    private static final Position INPUT_POSITION = Position.emptyPosition();
    private static final String STORE_NAME = "kv-store";

    private static final long RECORD_TIME = System.currentTimeMillis();
    private static final long WINDOW_START =
        (RECORD_TIME / WINDOW_SIZE.toMillis()) * WINDOW_SIZE.toMillis();

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private static final Position POSITION_0 =
        Position.fromMap(mkMap(mkEntry(INPUT_TOPIC_NAME, mkMap(mkEntry(0, 1L)))));

    public static class UnknownQuery implements Query<Void> { }

    private final StoresToTest storeToTest;
    private final String kind;
    private final boolean cache;
    private final boolean log;

    private KafkaStreams kafkaStreams;

    public enum StoresToTest {
        GLOBAL_IN_MEMORY_KV {
            @Override
            public StoreSupplier<?> supplier() {
                return Stores.inMemoryKeyValueStore(STORE_NAME);
            }

            @Override
            public boolean global() {
                return true;
            }

            @Override
            public boolean keyValue() {
                return true;
            }
        },
        GLOBAL_IN_MEMORY_LRU {
            @Override
            public StoreSupplier<?> supplier() {
                return Stores.lruMap(STORE_NAME, 100);
            }

            @Override
            public boolean global() {
                return true;
            }

            @Override
            public boolean keyValue() {
                return true;
            }
        },
        GLOBAL_ROCKS_KV {
            @Override
            public StoreSupplier<?> supplier() {
                return Stores.persistentKeyValueStore(STORE_NAME);
            }

            @Override
            public boolean timestamped() {
                return false;
            }

            @Override
            public boolean global() {
                return true;
            }

            @Override
            public boolean keyValue() {
                return true;
            }
        },
        GLOBAL_TIME_ROCKS_KV {
            @Override
            public StoreSupplier<?> supplier() {
                return Stores.persistentTimestampedKeyValueStore(STORE_NAME);
            }

            @Override
            public boolean global() {
                return true;
            }

            @Override
            public boolean keyValue() {
                return true;
            }
        },
        IN_MEMORY_KV {
            @Override
            public StoreSupplier<?> supplier() {
                return Stores.inMemoryKeyValueStore(STORE_NAME);
            }

            @Override
            public boolean keyValue() {
                return true;
            }
        },
        IN_MEMORY_LRU {
            @Override
            public StoreSupplier<?> supplier() {
                return Stores.lruMap(STORE_NAME, 100);
            }

            @Override
            public boolean keyValue() {
                return true;
            }
        },
        ROCKS_KV {
            @Override
            public StoreSupplier<?> supplier() {
                return Stores.persistentKeyValueStore(STORE_NAME);
            }

            @Override
            public boolean timestamped() {
                return false;
            }

            @Override
            public boolean keyValue() {
                return true;
            }
        },
        TIME_ROCKS_KV {
            @Override
            public StoreSupplier<?> supplier() {
                return Stores.persistentTimestampedKeyValueStore(STORE_NAME);
            }

            @Override
            public boolean keyValue() {
                return true;
            }
        },
        IN_MEMORY_WINDOW {
            @Override
            public StoreSupplier<?> supplier() {
                return Stores.inMemoryWindowStore(STORE_NAME, Duration.ofDays(1), WINDOW_SIZE,
                                                  false
                );
            }

            @Override
            public boolean isWindowed() {
                return true;
            }
        },
        ROCKS_WINDOW {
            @Override
            public StoreSupplier<?> supplier() {
                return Stores.persistentWindowStore(STORE_NAME, Duration.ofDays(1), WINDOW_SIZE,
                                                    false
                );
            }

            @Override
            public boolean isWindowed() {
                return true;
            }

            @Override
            public boolean timestamped() {
                return false;
            }
        },
        TIME_ROCKS_WINDOW {
            @Override
            public StoreSupplier<?> supplier() {
                return Stores.persistentTimestampedWindowStore(STORE_NAME, Duration.ofDays(1),
                                                               WINDOW_SIZE, false
                );
            }

            @Override
            public boolean isWindowed() {
                return true;
            }
        },
        IN_MEMORY_SESSION {
            @Override
            public StoreSupplier<?> supplier() {
                return Stores.inMemorySessionStore(STORE_NAME, Duration.ofDays(1));
            }

            @Override
            public boolean isSession() {
                return true;
            }
        },
        ROCKS_SESSION {
            @Override
            public StoreSupplier<?> supplier() {
                return Stores.persistentSessionStore(STORE_NAME, Duration.ofDays(1));
            }

            @Override
            public boolean isSession() {
                return true;
            }
        };

        public abstract StoreSupplier<?> supplier();

        public boolean timestamped() {
            return true; // most stores are timestamped
        }

        public boolean global() {
            return false;
        }

        public boolean keyValue() {
            return false;
        }

        public boolean isWindowed() {
            return false;
        }

        public boolean isSession() {
            return false;
        }
    }

    @Parameterized.Parameters(name = "cache={0}, log={1}, supplier={2}, kind={3}")
    public static Collection<Object[]> data() {
        LOG.info("Generating test cases according to random seed: {}", SEED);
        final List<Object[]> values = new ArrayList<>();
        for (final boolean cacheEnabled : Arrays.asList(true, false)) {
            for (final boolean logEnabled : Arrays.asList(true, false)) {
                for (final StoresToTest toTest : StoresToTest.values()) {
                    for (final String kind : Arrays.asList("DSL", "PAPI")) {
                        values.add(new Object[]{cacheEnabled, logEnabled, toTest.name(), kind});
                    }
                }
            }
        }
        // Randomizing the test cases in case some orderings interfere with each other.
        // If you wish to reproduce a randomized order, copy the logged SEED and substitute
        // it for the constant at the top of the file. This will cause exactly the same sequence
        // of pseudorandom values to be generated.
        Collections.shuffle(values, RANDOM);
        return values;
    }

    public IQv2StoreIntegrationTest(
        final boolean cache,
        final boolean log,
        final String storeToTest,
        final String kind) {
        this.cache = cache;
        this.log = log;
        this.storeToTest = StoresToTest.valueOf(storeToTest);
        this.kind = kind;
    }

    @BeforeClass
    public static void before()
        throws InterruptedException, IOException, ExecutionException, TimeoutException {

        CLUSTER.start();
        CLUSTER.deleteAllTopicsAndWait(60 * 1000L);
        final int partitions = 2;
        CLUSTER.createTopic(INPUT_TOPIC_NAME, partitions, 1);

        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);

        final List<Future<RecordMetadata>> futures = new LinkedList<>();
        try (final Producer<Integer, Integer> producer = new KafkaProducer<>(producerProps)) {
            for (int i = 0; i < 4; i++) {
                final Future<RecordMetadata> send = producer.send(
                    new ProducerRecord<>(
                        INPUT_TOPIC_NAME,
                        i % partitions,
                        RECORD_TIME,
                        i,
                        i,
                        null
                    )
                );
                futures.add(send);
                Time.SYSTEM.sleep(1L);
            }
            producer.flush();

            for (final Future<RecordMetadata> future : futures) {
                final RecordMetadata recordMetadata = future.get(1, TimeUnit.MINUTES);
                assertThat(recordMetadata.hasOffset(), is(true));
                INPUT_POSITION.withComponent(
                    recordMetadata.topic(),
                    recordMetadata.partition(),
                    recordMetadata.offset()
                );
            }
        }

        assertThat(INPUT_POSITION, equalTo(
            Position
                .emptyPosition()
                .withComponent(INPUT_TOPIC_NAME, 0, 1L)
                .withComponent(INPUT_TOPIC_NAME, 1, 1L)
        ));
    }

    @Before
    public void beforeTest() {
        final StoreSupplier<?> supplier = storeToTest.supplier();
        final Properties streamsConfig = streamsConfiguration(
            cache,
            log,
            storeToTest.name(),
            kind
        );

        final StreamsBuilder builder = new StreamsBuilder();
        if (Objects.equals(kind, "DSL") && supplier instanceof KeyValueBytesStoreSupplier) {
            setUpKeyValueDSLTopology((KeyValueBytesStoreSupplier) supplier, builder);
        } else if (Objects.equals(kind, "PAPI") && supplier instanceof KeyValueBytesStoreSupplier) {
            setUpKeyValuePAPITopology((KeyValueBytesStoreSupplier) supplier, builder);
        } else if (Objects.equals(kind, "DSL") && supplier instanceof WindowBytesStoreSupplier) {
            setUpWindowDSLTopology((WindowBytesStoreSupplier) supplier, builder);
        } else if (Objects.equals(kind, "PAPI") && supplier instanceof WindowBytesStoreSupplier) {
            setUpWindowPAPITopology((WindowBytesStoreSupplier) supplier, builder);
        } else if (Objects.equals(kind, "DSL") && supplier instanceof SessionBytesStoreSupplier) {
            setUpSessionDSLTopology((SessionBytesStoreSupplier) supplier, builder);
        } else if (Objects.equals(kind, "PAPI") && supplier instanceof SessionBytesStoreSupplier) {
            setUpSessionPAPITopology((SessionBytesStoreSupplier) supplier, builder);
        } else {
            throw new AssertionError("Store supplier is an unrecognized type.");
        }

        // Don't need to wait for running, since tests can use iqv2 to wait until they
        // get a valid response.

        kafkaStreams =
            IntegrationTestUtils.getStartedStreams(
                streamsConfig,
                builder,
                true
            );
    }

    private void setUpSessionDSLTopology(final SessionBytesStoreSupplier supplier,
                                         final StreamsBuilder builder) {
        final Materialized<Integer, Integer, SessionStore<Bytes, byte[]>> materialized =
            Materialized.as(supplier);

        if (cache) {
            materialized.withCachingEnabled();
        } else {
            materialized.withCachingDisabled();
        }

        if (log) {
            materialized.withLoggingEnabled(Collections.emptyMap());
        } else {
            materialized.withLoggingDisabled();
        }

        builder
            .stream(INPUT_TOPIC_NAME, Consumed.with(Serdes.Integer(), Serdes.Integer()))
            .groupByKey()
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(WINDOW_SIZE))
            .aggregate(
                () -> 0,
                (key, value, aggregate) -> aggregate + value,
                (aggKey, aggOne, aggTwo) -> aggOne + aggTwo,
                materialized
            );
    }

    private void setUpWindowDSLTopology(final WindowBytesStoreSupplier supplier,
                                        final StreamsBuilder builder) {
        final Materialized<Integer, Integer, WindowStore<Bytes, byte[]>> materialized =
            Materialized.as(supplier);

        if (cache) {
            materialized.withCachingEnabled();
        } else {
            materialized.withCachingDisabled();
        }

        if (log) {
            materialized.withLoggingEnabled(Collections.emptyMap());
        } else {
            materialized.withLoggingDisabled();
        }

        builder
            .stream(INPUT_TOPIC_NAME, Consumed.with(Serdes.Integer(), Serdes.Integer()))
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(WINDOW_SIZE))
            .aggregate(
                () -> 0,
                (key, value, aggregate) -> aggregate + value,
                materialized
            );
    }

    private void setUpKeyValueDSLTopology(final KeyValueBytesStoreSupplier supplier,
                                          final StreamsBuilder builder) {
        final Materialized<Integer, Integer, KeyValueStore<Bytes, byte[]>> materialized =
            Materialized.as(supplier);

        if (cache) {
            materialized.withCachingEnabled();
        } else {
            materialized.withCachingDisabled();
        }

        if (log) {
            materialized.withLoggingEnabled(Collections.emptyMap());
        } else {
            materialized.withLoggingDisabled();
        }

        if (storeToTest.global()) {
            builder.globalTable(
                INPUT_TOPIC_NAME,
                Consumed.with(Serdes.Integer(), Serdes.Integer()),
                materialized
            );
        } else {
            builder.table(
                INPUT_TOPIC_NAME,
                Consumed.with(Serdes.Integer(), Serdes.Integer()),
                materialized
            );
        }
    }

    private void setUpKeyValuePAPITopology(final KeyValueBytesStoreSupplier supplier,
                                           final StreamsBuilder builder) {
        final StoreBuilder<?> keyValueStoreStoreBuilder;
        final ProcessorSupplier<Integer, Integer, Void, Void> processorSupplier;
        if (storeToTest.timestamped()) {
            keyValueStoreStoreBuilder = Stores.timestampedKeyValueStoreBuilder(
                supplier,
                Serdes.Integer(),
                Serdes.Integer()
            );
            processorSupplier = () -> new ContextualProcessor<Integer, Integer, Void, Void>() {
                @Override
                public void process(final Record<Integer, Integer> record) {
                    final TimestampedKeyValueStore<Integer, Integer> stateStore =
                        context().getStateStore(keyValueStoreStoreBuilder.name());
                    stateStore.put(
                        record.key(),
                        ValueAndTimestamp.make(
                            record.value(), record.timestamp()
                        )
                    );
                }
            };
        } else {
            keyValueStoreStoreBuilder = Stores.keyValueStoreBuilder(
                supplier,
                Serdes.Integer(),
                Serdes.Integer()
            );
            processorSupplier =
                () -> new ContextualProcessor<Integer, Integer, Void, Void>() {
                    @Override
                    public void process(final Record<Integer, Integer> record) {
                        final KeyValueStore<Integer, Integer> stateStore =
                            context().getStateStore(keyValueStoreStoreBuilder.name());
                        stateStore.put(record.key(), record.value());
                    }
                };
        }
        if (cache) {
            keyValueStoreStoreBuilder.withCachingEnabled();
        } else {
            keyValueStoreStoreBuilder.withCachingDisabled();
        }
        if (log) {
            keyValueStoreStoreBuilder.withLoggingEnabled(Collections.emptyMap());
        } else {
            keyValueStoreStoreBuilder.withLoggingDisabled();
        }
        if (storeToTest.global()) {
            builder.addGlobalStore(
                keyValueStoreStoreBuilder,
                INPUT_TOPIC_NAME,
                Consumed.with(Serdes.Integer(), Serdes.Integer()),
                processorSupplier
            );
        } else {
            builder.addStateStore(keyValueStoreStoreBuilder);
            builder
                .stream(INPUT_TOPIC_NAME, Consumed.with(Serdes.Integer(), Serdes.Integer()))
                .process(processorSupplier, keyValueStoreStoreBuilder.name());
        }

    }

    private void setUpWindowPAPITopology(final WindowBytesStoreSupplier supplier,
                                         final StreamsBuilder builder) {
        final StoreBuilder<?> windowStoreStoreBuilder;
        final ProcessorSupplier<Integer, Integer, Void, Void> processorSupplier;
        if (storeToTest.timestamped()) {
            windowStoreStoreBuilder = Stores.timestampedWindowStoreBuilder(
                supplier,
                Serdes.Integer(),
                Serdes.Integer()
            );
            processorSupplier = () -> new ContextualProcessor<Integer, Integer, Void, Void>() {
                @Override
                public void process(final Record<Integer, Integer> record) {
                    final TimestampedWindowStore<Integer, Integer> stateStore =
                        context().getStateStore(windowStoreStoreBuilder.name());
                    stateStore.put(
                        record.key(),
                        ValueAndTimestamp.make(
                            record.value(), record.timestamp()
                        ),
                        WINDOW_START
                    );
                }
            };
        } else {
            windowStoreStoreBuilder = Stores.windowStoreBuilder(
                supplier,
                Serdes.Integer(),
                Serdes.Integer()
            );
            processorSupplier =
                () -> new ContextualProcessor<Integer, Integer, Void, Void>() {
                    @Override
                    public void process(final Record<Integer, Integer> record) {
                        final WindowStore<Integer, Integer> stateStore =
                            context().getStateStore(windowStoreStoreBuilder.name());
                        stateStore.put(record.key(), record.value(), WINDOW_START);
                    }
                };
        }
        if (cache) {
            windowStoreStoreBuilder.withCachingEnabled();
        } else {
            windowStoreStoreBuilder.withCachingDisabled();
        }
        if (log) {
            windowStoreStoreBuilder.withLoggingEnabled(Collections.emptyMap());
        } else {
            windowStoreStoreBuilder.withLoggingDisabled();
        }
        if (storeToTest.global()) {
            builder.addGlobalStore(
                windowStoreStoreBuilder,
                INPUT_TOPIC_NAME,
                Consumed.with(Serdes.Integer(), Serdes.Integer()),
                processorSupplier
            );
        } else {
            builder.addStateStore(windowStoreStoreBuilder);
            builder
                .stream(INPUT_TOPIC_NAME, Consumed.with(Serdes.Integer(), Serdes.Integer()))
                .process(processorSupplier, windowStoreStoreBuilder.name());
        }

    }

    private void setUpSessionPAPITopology(final SessionBytesStoreSupplier supplier,
                                          final StreamsBuilder builder) {
        final StoreBuilder<?> sessionStoreStoreBuilder;
        final ProcessorSupplier<Integer, Integer, Void, Void> processorSupplier;
        sessionStoreStoreBuilder = Stores.sessionStoreBuilder(
            supplier,
            Serdes.Integer(),
            Serdes.Integer()
        );
        processorSupplier = () -> new ContextualProcessor<Integer, Integer, Void, Void>() {
            @Override
            public void process(final Record<Integer, Integer> record) {
                final SessionStore<Integer, Integer> stateStore =
                    context().getStateStore(sessionStoreStoreBuilder.name());
                stateStore.put(
                    new Windowed<>(record.key(), new SessionWindow(WINDOW_START, WINDOW_START)),
                    record.value()
                );
            }
        };
        if (cache) {
            sessionStoreStoreBuilder.withCachingEnabled();
        } else {
            sessionStoreStoreBuilder.withCachingDisabled();
        }
        if (log) {
            sessionStoreStoreBuilder.withLoggingEnabled(Collections.emptyMap());
        } else {
            sessionStoreStoreBuilder.withLoggingDisabled();
        }
        if (storeToTest.global()) {
            builder.addGlobalStore(
                sessionStoreStoreBuilder,
                INPUT_TOPIC_NAME,
                Consumed.with(Serdes.Integer(), Serdes.Integer()),
                processorSupplier
            );
        } else {
            builder.addStateStore(sessionStoreStoreBuilder);
            builder
                .stream(INPUT_TOPIC_NAME, Consumed.with(Serdes.Integer(), Serdes.Integer()))
                .process(processorSupplier, sessionStoreStoreBuilder.name());
        }

    }


    @After
    public void afterTest() {
        // only needed because some of the PAPI cases aren't added yet.
        if (kafkaStreams != null) {
            kafkaStreams.close();
            kafkaStreams.cleanUp();
        }
    }

    @AfterClass
    public static void after() {
        CLUSTER.stop();
    }

    @Test
    public void verifyStore() {
        try {
            if (storeToTest.global()) {
                // See KAFKA-13523
                globalShouldRejectAllQueries();
            } else {
                shouldRejectUnknownQuery();
                shouldCollectExecutionInfo();
                shouldCollectExecutionInfoUnderFailure();

                if (storeToTest.keyValue()) {
                    if (storeToTest.timestamped()) {
                        final Function<ValueAndTimestamp<Integer>, Integer> valueExtractor =
                            ValueAndTimestamp::value;
                        shouldHandleKeyQuery(2, valueExtractor, 2);
                        shouldHandleRangeQueries(valueExtractor);
                    } else {
                        final Function<Integer, Integer> valueExtractor = Function.identity();
                        shouldHandleKeyQuery(2, valueExtractor, 2);
                        shouldHandleRangeQueries(valueExtractor);
                    }
                }

                if (storeToTest.isWindowed()) {
                    if (storeToTest.timestamped()) {
                        final Function<ValueAndTimestamp<Integer>, Integer> valueExtractor =
                            ValueAndTimestamp::value;
                        shouldHandleWindowKeyQueries(valueExtractor);
                        shouldHandleWindowRangeQueries(valueExtractor);
                    } else {
                        final Function<Integer, Integer> valueExtractor = Function.identity();
                        shouldHandleWindowKeyQueries(valueExtractor);
                        shouldHandleWindowRangeQueries(valueExtractor);
                    }
                }

                if (storeToTest.isSession()) {
                    // Note there's no "timestamped" differentiation here.
                    // Idiosyncratically, SessionStores are _never_ timestamped.
                    shouldHandleSessionKeyQueries();
                }
            }
        } catch (final AssertionError e) {
            LOG.error("Failed assertion", e);
            throw e;
        }
    }


    private <T> void shouldHandleRangeQueries(final Function<T, Integer> extractor) {
        shouldHandleRangeQuery(
            Optional.of(1),
            Optional.of(3),
            extractor,
            mkSet(1, 2, 3)

        );
        shouldHandleRangeQuery(
            Optional.of(1),
            Optional.empty(),
            extractor,
            mkSet(1, 2, 3)

        );
        shouldHandleRangeQuery(
            Optional.empty(),
            Optional.of(1),
            extractor,
            mkSet(0, 1)

        );
        shouldHandleRangeQuery(
            Optional.empty(),
            Optional.empty(),
            extractor,
            mkSet(0, 1, 2, 3)

        );
    }

    private <T> void shouldHandleWindowKeyQueries(final Function<T, Integer> extractor) {

        // tightest possible start range
        shouldHandleWindowKeyQuery(
            2,
            Instant.ofEpochMilli(WINDOW_START),
            Instant.ofEpochMilli(WINDOW_START),
            extractor,
            mkSet(2)
        );

        // miss the window start range
        shouldHandleWindowKeyQuery(
            2,
            Instant.ofEpochMilli(WINDOW_START - 1),
            Instant.ofEpochMilli(WINDOW_START - 1),
            extractor,
            mkSet()
        );

        // miss the key
        shouldHandleWindowKeyQuery(
            999,
            Instant.ofEpochMilli(WINDOW_START),
            Instant.ofEpochMilli(WINDOW_START),
            extractor,
            mkSet()
        );

        // miss both
        shouldHandleWindowKeyQuery(
            999,
            Instant.ofEpochMilli(WINDOW_START - 1),
            Instant.ofEpochMilli(WINDOW_START - 1),
            extractor,
            mkSet()
        );
    }

    private <T> void shouldHandleWindowRangeQueries(final Function<T, Integer> extractor) {
        final long windowSize = WINDOW_SIZE.toMillis();
        final long windowStart = (RECORD_TIME / windowSize) * windowSize;

        shouldHandleWindowRangeQuery(
            Instant.ofEpochMilli(windowStart),
            Instant.ofEpochMilli(windowStart),
            extractor,
            mkSet(0, 1, 2, 3)
        );

        // miss the window start
        shouldHandleWindowRangeQuery(
            Instant.ofEpochMilli(windowStart - 1),
            Instant.ofEpochMilli(windowStart - 1),
            extractor,
            mkSet()
        );

        // Should fail to execute this query on a WindowStore.
        final WindowRangeQuery<Integer, T> query = WindowRangeQuery.withKey(2);

        final StateQueryRequest<KeyValueIterator<Windowed<Integer>, T>> request =
                inStore(STORE_NAME)
                        .withQuery(query)
                        .withPartitions(mkSet(0, 1))
                        .withPositionBound(PositionBound.at(INPUT_POSITION));

        final StateQueryResult<KeyValueIterator<Windowed<Integer>, T>> result =
                IntegrationTestUtils.iqv2WaitForResult(kafkaStreams, request);

        if (result.getGlobalResult() != null) {
            fail("global tables aren't implemented");
        } else {
            final Map<Integer, QueryResult<KeyValueIterator<Windowed<Integer>, T>>> queryResult =
                result.getPartitionResults();
            for (final int partition : queryResult.keySet()) {
                final QueryResult<KeyValueIterator<Windowed<Integer>, T>> partitionResult =
                    queryResult.get(partition);
                final boolean failure = partitionResult.isFailure();
                if (!failure) {
                    throw new AssertionError(queryResult.toString());
                }
                assertThat(partitionResult.getFailureReason(), is(FailureReason.UNKNOWN_QUERY_TYPE));
                assertThat(partitionResult.getFailureMessage(), matchesPattern(
                    "This store"
                        + " \\(class org.apache.kafka.streams.state.internals.Metered.*WindowStore\\)"
                        + " doesn't know how to execute the given query"
                        + " \\(WindowRangeQuery\\{key=Optional\\[2], timeFrom=Optional.empty, timeTo=Optional.empty}\\)"
                        + " because WindowStores only supports WindowRangeQuery.withWindowStartRange\\."
                        + " Contact the store maintainer if you need support for a new query type\\."
                ));
            }
        }
    }

    private <T> void shouldHandleSessionKeyQueries() {
        shouldHandleSessionRangeQuery(
            2,
            mkSet(2)
        );

        // not preset, so empty result iter
        shouldHandleSessionRangeQuery(
            999,
            mkSet()
        );

        // Should fail to execute this query on a SessionStore.
        final WindowRangeQuery<Integer, T> query =
            WindowRangeQuery.withWindowStartRange(
                Instant.ofEpochMilli(0L),
                Instant.ofEpochMilli(0L)
            );

        final StateQueryRequest<KeyValueIterator<Windowed<Integer>, T>> request =
            inStore(STORE_NAME)
                .withQuery(query)
                .withPartitions(mkSet(0, 1))
                .withPositionBound(PositionBound.at(INPUT_POSITION));

        final StateQueryResult<KeyValueIterator<Windowed<Integer>, T>> result =
            IntegrationTestUtils.iqv2WaitForResult(kafkaStreams, request);

        if (result.getGlobalResult() != null) {
            fail("global tables aren't implemented");
        } else {
            final Map<Integer, QueryResult<KeyValueIterator<Windowed<Integer>, T>>> queryResult =
                result.getPartitionResults();
            for (final int partition : queryResult.keySet()) {
                final QueryResult<KeyValueIterator<Windowed<Integer>, T>> partitionResult =
                    queryResult.get(partition);
                final boolean failure = partitionResult.isFailure();
                if (!failure) {
                    throw new AssertionError(queryResult.toString());
                }
                assertThat(partitionResult.getFailureReason(), is(FailureReason.UNKNOWN_QUERY_TYPE));
                assertThat(partitionResult.getFailureMessage(), is(
                    "This store"
                        + " (class org.apache.kafka.streams.state.internals.MeteredSessionStore)"
                        + " doesn't know how to execute the given query"
                        + " (WindowRangeQuery{key=Optional.empty, timeFrom=Optional[1970-01-01T00:00:00Z], timeTo=Optional[1970-01-01T00:00:00Z]})"
                        + " because SessionStores only support WindowRangeQuery.withKey."
                        + " Contact the store maintainer if you need support for a new query type."
                ));
            }
        }
    }

    private void globalShouldRejectAllQueries() {
        // See KAFKA-13523

        final KeyQuery<Integer, ValueAndTimestamp<Integer>> query = KeyQuery.withKey(1);
        final StateQueryRequest<ValueAndTimestamp<Integer>> request =
            inStore(STORE_NAME).withQuery(query);

        final StateQueryResult<ValueAndTimestamp<Integer>> result = kafkaStreams.query(request);

        assertThat(result.getGlobalResult().isFailure(), is(true));
        assertThat(
            result.getGlobalResult().getFailureReason(),
            is(FailureReason.UNKNOWN_QUERY_TYPE)
        );
        assertThat(
            result.getGlobalResult().getFailureMessage(),
            is("Global stores do not yet support the KafkaStreams#query API."
                   + " Use KafkaStreams#store instead.")
        );
    }

    public void shouldRejectUnknownQuery() {

        final UnknownQuery query = new UnknownQuery();
        final StateQueryRequest<Void> request = inStore(STORE_NAME).withQuery(query);
        final Set<Integer> partitions = mkSet(0, 1);

        final StateQueryResult<Void> result =
            IntegrationTestUtils.iqv2WaitForPartitions(kafkaStreams, request, partitions);

        makeAssertions(
            partitions,
            result,
            queryResult -> {
                assertThat(queryResult.isFailure(), is(true));
                assertThat(queryResult.isSuccess(), is(false));
                assertThat(
                    queryResult.getFailureReason(),
                    is(FailureReason.UNKNOWN_QUERY_TYPE)
                );
                assertThat(
                    queryResult.getFailureMessage(),
                    matchesPattern(
                        "This store (.*)"
                            + " doesn't know how to execute the given query"
                            + " (.*)."
                            + " Contact the store maintainer if you need support for a new query type."
                    )
                );
                assertThrows(IllegalArgumentException.class, queryResult::getResult);

                assertThat(queryResult.getExecutionInfo(), is(empty()));
            }
        );
    }

    public <V> void shouldHandleKeyQuery(
            final Integer key,
            final Function<V, Integer> valueExtactor,
            final Integer expectedValue) {

        final KeyQuery<Integer, V> query = KeyQuery.withKey(key);
        final StateQueryRequest<V> request =
                inStore(STORE_NAME)
                        .withQuery(query)
                        .withPartitions(mkSet(0, 1))
                        .withPositionBound(PositionBound.at(INPUT_POSITION));

        final StateQueryResult<V> result =
                IntegrationTestUtils.iqv2WaitForResult(kafkaStreams, request);

        final QueryResult<V> queryResult = result.getOnlyPartitionResult();
        final boolean failure = queryResult.isFailure();
        if (failure) {
            throw new AssertionError(queryResult.toString());
        }
        assertThat(queryResult.isSuccess(), is(true));

        assertThrows(IllegalArgumentException.class, queryResult::getFailureReason);
        assertThrows(
                IllegalArgumentException.class,
                queryResult::getFailureMessage
        );

        final V result1 = queryResult.getResult();
        final Integer integer = valueExtactor.apply(result1);
        assertThat(integer, is(expectedValue));
        assertThat(queryResult.getExecutionInfo(), is(empty()));
        assertThat(queryResult.getPosition(), is(POSITION_0));
    }   

    public <V> void shouldHandleRangeQuery(
        final Optional<Integer> lower,
        final Optional<Integer> upper,
        final Function<V, Integer> valueExtactor,
        final Set<Integer> expectedValue) {

        final RangeQuery<Integer, V> query;
        if (lower.isPresent() && upper.isPresent()) {
            query = RangeQuery.withRange(lower.get(), upper.get());
        } else if (lower.isPresent()) {
            query = RangeQuery.withLowerBound(lower.get());
        } else if (upper.isPresent()) {
            query = RangeQuery.withUpperBound(upper.get());
        } else {
            query = RangeQuery.withNoBounds();
        }

        final StateQueryRequest<KeyValueIterator<Integer, V>> request =
            inStore(STORE_NAME)
                .withQuery(query)
                .withPartitions(mkSet(0, 1))
                .withPositionBound(PositionBound.at(INPUT_POSITION));
        final StateQueryResult<KeyValueIterator<Integer, V>> result =
            IntegrationTestUtils.iqv2WaitForResult(kafkaStreams, request);

        if (result.getGlobalResult() != null) {
            fail("global tables aren't implemented");
        } else {
            final Set<Integer> actualValue = new HashSet<>();
            final Map<Integer, QueryResult<KeyValueIterator<Integer, V>>> queryResult = result.getPartitionResults();
            for (final int partition : queryResult.keySet()) {
                final boolean failure = queryResult.get(partition).isFailure();
                if (failure) {
                    throw new AssertionError(queryResult.toString());
                }
                assertThat(queryResult.get(partition).isSuccess(), is(true));

                assertThrows(
                    IllegalArgumentException.class,
                    queryResult.get(partition)::getFailureReason
                );
                assertThrows(
                    IllegalArgumentException.class,
                    queryResult.get(partition)::getFailureMessage
                );

                try (final KeyValueIterator<Integer, V> iterator = queryResult.get(partition).getResult()) {
                    while (iterator.hasNext()) {
                        actualValue.add(valueExtactor.apply(iterator.next().value));
                    }
                }
                assertThat(queryResult.get(partition).getExecutionInfo(), is(empty()));
            }
            assertThat("Result:" + result, actualValue, is(expectedValue));
            assertThat("Result:" + result, result.getPosition(), is(INPUT_POSITION));
        }
    }

    public <V> void shouldHandleWindowKeyQuery(
        final Integer key,
        final Instant timeFrom,
        final Instant timeTo,
        final Function<V, Integer> valueExtactor,
        final Set<Integer> expectedValue) {

        final WindowKeyQuery<Integer, V> query = WindowKeyQuery.withKeyAndWindowStartRange(
            key,
            timeFrom,
            timeTo
        );

        final StateQueryRequest<WindowStoreIterator<V>> request =
            inStore(STORE_NAME)
                .withQuery(query)
                .withPartitions(mkSet(0, 1))
                .withPositionBound(PositionBound.at(INPUT_POSITION));

        final StateQueryResult<WindowStoreIterator<V>> result =
            IntegrationTestUtils.iqv2WaitForResult(kafkaStreams, request);

        if (result.getGlobalResult() != null) {
            fail("global tables aren't implemented");
        } else {
            final Set<Integer> actualValue = new HashSet<>();
            final Map<Integer, QueryResult<WindowStoreIterator<V>>> queryResult = result.getPartitionResults();
            for (final int partition : queryResult.keySet()) {
                final boolean failure = queryResult.get(partition).isFailure();
                if (failure) {
                    throw new AssertionError(queryResult.toString());
                }
                assertThat(queryResult.get(partition).isSuccess(), is(true));

                assertThrows(
                    IllegalArgumentException.class,
                    queryResult.get(partition)::getFailureReason
                );
                assertThrows(
                    IllegalArgumentException.class,
                    queryResult.get(partition)::getFailureMessage
                );

                try (final WindowStoreIterator<V> iterator = queryResult.get(partition).getResult()) {
                    while (iterator.hasNext()) {
                        actualValue.add(valueExtactor.apply(iterator.next().value));
                    }
                }
                assertThat(queryResult.get(partition).getExecutionInfo(), is(empty()));
            }
            assertThat("Result:" + result, actualValue, is(expectedValue));
            assertThat("Result:" + result, result.getPosition(), is(INPUT_POSITION));
        }
    }

    public <V> void shouldHandleWindowRangeQuery(
            final Instant timeFrom,
            final Instant timeTo,
            final Function<V, Integer> valueExtactor,
            final Set<Integer> expectedValue) {

        final WindowRangeQuery<Integer, V> query = WindowRangeQuery.withWindowStartRange(timeFrom, timeTo);

        final StateQueryRequest<KeyValueIterator<Windowed<Integer>, V>> request =
                inStore(STORE_NAME)
                        .withQuery(query)
                        .withPartitions(mkSet(0, 1))
                        .withPositionBound(PositionBound.at(INPUT_POSITION));

        final StateQueryResult<KeyValueIterator<Windowed<Integer>, V>> result =
                IntegrationTestUtils.iqv2WaitForResult(kafkaStreams, request);

        if (result.getGlobalResult() != null) {
            fail("global tables aren't implemented");
        } else {
            final Set<Integer> actualValue = new HashSet<>();
            final Map<Integer, QueryResult<KeyValueIterator<Windowed<Integer>, V>>> queryResult = result.getPartitionResults();
            for (final int partition : queryResult.keySet()) {
                final boolean failure = queryResult.get(partition).isFailure();
                if (failure) {
                    throw new AssertionError(queryResult.toString());
                }
                assertThat(queryResult.get(partition).isSuccess(), is(true));

                assertThrows(
                        IllegalArgumentException.class,
                        queryResult.get(partition)::getFailureReason
                );
                assertThrows(
                        IllegalArgumentException.class,
                        queryResult.get(partition)::getFailureMessage
                );

                try (final KeyValueIterator<Windowed<Integer>, V> iterator = queryResult.get(partition).getResult()) {
                    while (iterator.hasNext()) {
                        actualValue.add(valueExtactor.apply(iterator.next().value));
                    }
                }
                assertThat(queryResult.get(partition).getExecutionInfo(), is(empty()));
            }
            assertThat("Result:" + result, actualValue, is(expectedValue));
            assertThat("Result:" + result, result.getPosition(), is(INPUT_POSITION));
        }
    }

    public <V> void shouldHandleSessionRangeQuery(
            final Integer key,
            final Set<Integer> expectedValue) {

        final WindowRangeQuery<Integer, V> query = WindowRangeQuery.withKey(key);

        final StateQueryRequest<KeyValueIterator<Windowed<Integer>, V>> request =
                inStore(STORE_NAME)
                        .withQuery(query)
                        .withPartitions(mkSet(0, 1))
                        .withPositionBound(PositionBound.at(INPUT_POSITION));
        final StateQueryResult<KeyValueIterator<Windowed<Integer>, V>> result =
                IntegrationTestUtils.iqv2WaitForResult(kafkaStreams, request);

        if (result.getGlobalResult() != null) {
            fail("global tables aren't implemented");
        } else {
            final Set<Integer> actualValue = new HashSet<>();
            final Map<Integer, QueryResult<KeyValueIterator<Windowed<Integer>, V>>> queryResult = result.getPartitionResults();
            for (final int partition : queryResult.keySet()) {
                final boolean failure = queryResult.get(partition).isFailure();
                if (failure) {
                    throw new AssertionError(queryResult.toString());
                }
                assertThat(queryResult.get(partition).isSuccess(), is(true));

                assertThrows(
                        IllegalArgumentException.class,
                        queryResult.get(partition)::getFailureReason
                );
                assertThrows(
                        IllegalArgumentException.class,
                        queryResult.get(partition)::getFailureMessage
                );

                try (final KeyValueIterator<Windowed<Integer>, V> iterator = queryResult.get(partition).getResult()) {
                    while (iterator.hasNext()) {
                        actualValue.add((Integer) iterator.next().value);
                    }
                }
                assertThat(queryResult.get(partition).getExecutionInfo(), is(empty()));
            }
            assertThat("Result:" + result, actualValue, is(expectedValue));
            assertThat("Result:" + result, result.getPosition(), is(INPUT_POSITION));
        }
    }

    public void shouldCollectExecutionInfo() {

        final KeyQuery<Integer, ValueAndTimestamp<Integer>> query = KeyQuery.withKey(1);
        final Set<Integer> partitions = mkSet(0, 1);
        final StateQueryRequest<ValueAndTimestamp<Integer>> request =
            inStore(STORE_NAME)
                .withQuery(query)
                .enableExecutionInfo()
                .withPartitions(partitions)
                .withPositionBound(PositionBound.at(INPUT_POSITION));

        final StateQueryResult<ValueAndTimestamp<Integer>> result =
            IntegrationTestUtils.iqv2WaitForResult(
                kafkaStreams,
                request
            );

        makeAssertions(
            partitions,
            result,
            queryResult -> assertThat(queryResult.getExecutionInfo(), not(empty()))
        );
    }

    public void shouldCollectExecutionInfoUnderFailure() {

        final UnknownQuery query = new UnknownQuery();
        final Set<Integer> partitions = mkSet(0, 1);
        final StateQueryRequest<Void> request =
            inStore(STORE_NAME)
                .withQuery(query)
                .enableExecutionInfo()
                .withPartitions(partitions)
                .withPositionBound(PositionBound.at(INPUT_POSITION));

        final StateQueryResult<Void> result =
            IntegrationTestUtils.iqv2WaitForResult(
                kafkaStreams,
                request
            );

        makeAssertions(
            partitions,
            result,
            queryResult -> assertThat(queryResult.getExecutionInfo(), not(empty()))
        );
    }

    private <R> void makeAssertions(
        final Set<Integer> partitions,
        final StateQueryResult<R> result,
        final Consumer<QueryResult<R>> assertion) {

        if (result.getGlobalResult() != null) {
            assertion.accept(result.getGlobalResult());
        } else {
            assertThat(result.getPartitionResults().keySet(), is(partitions));
            for (final Integer partition : partitions) {
                assertion.accept(result.getPartitionResults().get(partition));
            }
        }
    }

    private static Properties streamsConfiguration(final boolean cache, final boolean log,
                                                   final String supplier, final String kind) {
        final String safeTestName =
            IQv2StoreIntegrationTest.class.getName() + "-" + cache + "-" + log + "-" + supplier
                + "-" + kind + "-" + RANDOM.nextInt();
        final Properties config = new Properties();
        config.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-" + safeTestName);
        config.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:" + (++port));
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        config.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        config.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 200);
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 1000);
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        return config;
    }
}