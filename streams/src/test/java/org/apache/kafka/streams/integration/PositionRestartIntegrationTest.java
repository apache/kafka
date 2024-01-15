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
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
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
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.RangeQuery;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.query.WindowKeyQuery;
import org.apache.kafka.streams.query.WindowRangeQuery;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
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
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.streams.query.StateQueryRequest.inStore;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@Category({IntegrationTest.class})
@RunWith(value = Parameterized.class)
public class PositionRestartIntegrationTest {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(600);
    private static final Logger LOG = LoggerFactory.getLogger(PositionRestartIntegrationTest.class);
    private static final long SEED = new Random().nextLong();
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
    private final StoresToTest storeToTest;
    private final String kind;
    private final boolean cache;
    private final boolean log;
    private KafkaStreams kafkaStreams;
    private final Properties streamsConfig;

    public enum StoresToTest {
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
                    // We don't need to test if non-persistent stores without logging
                    // survive restarts, since those are by definition not durable.
                    if (logEnabled || toTest.supplier().get().persistent()) {
                        for (final String kind : Arrays.asList("DSL", "PAPI")) {
                            values.add(new Object[]{cacheEnabled, logEnabled, toTest.name(), kind});
                        }
                    }
                }
            }
        }
        return values;
    }

    public PositionRestartIntegrationTest(
        final boolean cache,
        final boolean log,
        final String storeToTest,
        final String kind) {
        this.cache = cache;
        this.log = log;
        this.storeToTest = StoresToTest.valueOf(storeToTest);
        this.kind = kind;
        this.streamsConfig = streamsConfiguration(
            cache,
            log,
            storeToTest,
            kind
        );
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
        beforeTest(true);
    }

    public void beforeTest(final boolean cleanup) {
        final StoreSupplier<?> supplier = storeToTest.supplier();

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

        kafkaStreams =
            IntegrationTestUtils.getStartedStreams(
                streamsConfig,
                builder,
                cleanup
            );
    }

    @After
    public void afterTest() {
        afterTest(true);
    }

    public void afterTest(final boolean cleanup) {
        if (kafkaStreams != null) {
            kafkaStreams.close();
            if (cleanup) {
                kafkaStreams.cleanUp();
            }
        }
    }

    @AfterClass
    public static void after() {
        CLUSTER.stop();
    }

    public void reboot() {
        afterTest(false);
        beforeTest(false);
    }

    @Test
    public void verifyStore() {
        final Query<?> query;
        if (storeToTest.keyValue()) {
            query = RangeQuery.withNoBounds();
        } else if (storeToTest.isWindowed()) {
            query = WindowKeyQuery.withKeyAndWindowStartRange(
                2,
                Instant.ofEpochMilli(WINDOW_START),
                Instant.ofEpochMilli(WINDOW_START)
            );
        } else if (storeToTest.isSession()) {
            query = WindowRangeQuery.withKey(2);
        } else {
            throw new AssertionError("Unhandled store type: " + storeToTest);
        }
        shouldReachExpectedPosition(query);

        reboot();

        shouldReachExpectedPosition(query);
    }

    private void shouldReachExpectedPosition(final Query<?> query) {
        final StateQueryRequest<?> request =
            inStore(STORE_NAME)
                .withQuery(query)
                .withPartitions(mkSet(0, 1))
                .withPositionBound(PositionBound.at(INPUT_POSITION));

        final StateQueryResult<?> result =
            IntegrationTestUtils.iqv2WaitForResult(kafkaStreams, request);

        assertThat(result.getPosition(), is(INPUT_POSITION));
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

        builder.table(
            INPUT_TOPIC_NAME,
            Consumed.with(Serdes.Integer(), Serdes.Integer()),
            materialized
        );
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
        builder.addStateStore(keyValueStoreStoreBuilder);
        builder
            .stream(INPUT_TOPIC_NAME, Consumed.with(Serdes.Integer(), Serdes.Integer()))
            .process(processorSupplier, keyValueStoreStoreBuilder.name());

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
        builder.addStateStore(windowStoreStoreBuilder);
        builder
            .stream(INPUT_TOPIC_NAME, Consumed.with(Serdes.Integer(), Serdes.Integer()))
            .process(processorSupplier, windowStoreStoreBuilder.name());

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
        builder.addStateStore(sessionStoreStoreBuilder);
        builder
            .stream(INPUT_TOPIC_NAME, Consumed.with(Serdes.Integer(), Serdes.Integer()))
            .process(processorSupplier, sessionStoreStoreBuilder.name());
    }

    private static Properties streamsConfiguration(final boolean cache, final boolean log,
                                                   final String supplier, final String kind) {
        final String safeTestName =
            PositionRestartIntegrationTest.class.getName() + "-" + cache + "-" + log + "-"
                + supplier + "-" + kind;
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
        config.put(InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED, true);
        return config;
    }
}