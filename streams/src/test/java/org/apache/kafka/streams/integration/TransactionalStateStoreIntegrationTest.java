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

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.cleanStateBeforeTest;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.getRunningStreams;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.purgeLocalStreamsState;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.quietlyCleanStateAfterTest;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.apache.kafka.streams.state.QueryableStoreTypes.keyValueStore;
import static org.apache.kafka.streams.state.QueryableStoreTypes.sessionStore;
import static org.apache.kafka.streams.state.QueryableStoreTypes.windowStore;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/*
 * Tests in this suite ensure that transactional state stores:
 * 1. discard uncommitted data
 * 2. replay changelog correctly.
 *
 * This test processes 2 records by inserting them into the state store and them forwarding them.
 * The processor correctly processes the first record. On the second record, it inserts it into
 * the state store and then crashes. The Long.MAX_VALUE commit interval ensures that we don't
 * read the first records because it was committed.
 *
 * The tests assert that:
 * 1. Both records were inserted into the state store before the crash
 * 2. After the recovery only the first record is available. This happens because the
 * the txn state store wipes both uncommitted records, then replays the first one from the
 * changelog topic.
 */
@RunWith(Parameterized.class)
@Category(IntegrationTest.class)
public class TransactionalStateStoreIntegrationTest {

    @Rule
    public Timeout globalTimeout = Timeout.seconds(600);

    @Rule
    public TestName testName = new TestName();

    @SuppressWarnings("deprecation")
    @Parameterized.Parameters(name = "{0}")
    public static Collection<String> data() {
        return asList(
            StreamsConfig.EXACTLY_ONCE,
            StreamsConfig.EXACTLY_ONCE_V2
        );
    }

    @Parameterized.Parameter
    public String eosConfig;

    private static Properties producerConfig;

    private static final long COMMIT_INTERVAL_MS = 100L;

    @BeforeClass
    public static void startCluster() throws IOException {
        CLUSTER.start();

        STREAMS_CONFIG.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        STREAMS_CONFIG.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        STREAMS_CONFIG.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        STREAMS_CONFIG.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        STREAMS_CONFIG.put(StreamsConfig.STATE_DIR_CONFIG, TEST_FOLDER.getRoot().getPath());
        STREAMS_CONFIG.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL_MS);
        STREAMS_CONFIG.put(StreamsConfig.DEFAULT_DSL_STORE_CONFIG, StreamsConfig.TXN_ROCKS_DB);

        producerConfig = TestUtils.producerConfig(
            CLUSTER.bootstrapServers(),
            StringSerializer.class,
            StringSerializer.class
        );
    }

    private Properties consumerConfig(final String groupId) {
        return TestUtils.consumerConfig(
            CLUSTER.bootstrapServers(),
            groupId,
            StringDeserializer.class,
            StringDeserializer.class,
            Utils.mkProperties(Collections.singletonMap(
                ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                IsolationLevel.READ_COMMITTED.name().toLowerCase(Locale.ROOT))
            )
        );
    }

    @AfterClass
    public static void closeCluster() throws IOException {
        CLUSTER.stop();
        purgeLocalStreamsState(STREAMS_CONFIG);
    }

    @ClassRule
    public static final TemporaryFolder TEST_FOLDER = new TemporaryFolder(TestUtils.tempDirectory());

    private static final Properties STREAMS_CONFIG = new Properties();

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(3);

    @Test
    public void testDiscardsUncommittedDataKVStore() throws Exception {
        final String uniqueTestName = safeUniqueTestName(getClass(), testName);
        final String input = uniqueTestName + "-input";
        final String storeName = uniqueTestName + "-store";
        final String output = uniqueTestName + "-output";
        final String appId = uniqueTestName + "-appId";
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        STREAMS_CONFIG.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, eosConfig);
        producerConfig.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, appId + "-producer");

        cleanStateBeforeTest(CLUSTER, input, output);

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> inputStream = builder.stream(input);
        final StoreBuilder<KeyValueStore<String, String>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(storeName, true),
                Serdes.String(),
                Serdes.String()
            )
            .withCachingDisabled();
        builder.addStateStore(storeBuilder);

        final AtomicBoolean crashRequested = new AtomicBoolean(false);
        final CountDownLatch crashLatch = new CountDownLatch(1);
        final AtomicBoolean skipRecords = new AtomicBoolean(false);

        inputStream.process(new ProcessorSupplier<String, String, String, String>() {

                @Override
                public Processor<String, String, String, String> get() {
                    return new Processor<String, String, String, String>() {

                        private KeyValueStore<String, String> store;
                        private ProcessorContext<String, String> context;

                        @Override
                        public void init(final ProcessorContext<String, String> context) {
                            this.store = Objects.requireNonNull(context.getStateStore(storeName));
                            this.context = context;
                        }

                        @Override
                        public void process(final Record<String, String> record) {
                            if (skipRecords.get()) {
                                return;
                            }

                            store.put(record.key(), record.value());

                            // crash after updating local state, but before forwarding the record
                            if (crashRequested.get()) {
                                try {
                                    // wait for the main thread to ensure that the uncommitted value is readable
                                    crashLatch.await();
                                } catch (final InterruptedException ie) {
                                    throw new RuntimeException(ie);
                                }
                                throw new RuntimeException("BOOOM!");
                            }
                            context.forward(record);
                        }
                    };
                }
            }, storeName)
            .to(output);

        final KafkaStreams driver = getRunningStreams(STREAMS_CONFIG, builder, true);

        try {
            IntegrationTestUtils.produceKeyValuesSynchronously(
                input,
                singletonList(new KeyValue<>("k1", "v1")),
                TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class, producerConfig),
                CLUSTER.time,
                true
            );
            IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                consumerConfig(appId + "readCommitted"), output, 1, 20000L);

            final ReadOnlyKeyValueStore<Object, Object> store = IntegrationTestUtils.getStore(
                storeName, driver, keyValueStore());
            assertEquals("v1", store.get("k1"));

            Thread.sleep(2 * COMMIT_INTERVAL_MS);
            crashRequested.set(true);
            IntegrationTestUtils.produceKeyValuesSynchronously(
                input,
                singletonList(new KeyValue<>("k2", "v2")),
                TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class, producerConfig),
                CLUSTER.time,
                true
            );
            TestUtils.waitForCondition(() -> store.get("k2").equals("v2"),
                "Expected to read the second key");
            crashLatch.countDown();

            TestUtils.waitForCondition(() -> driver.state().hasCompletedShutdown(),
                "Streams app didn't shut down");
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        skipRecords.set(true);
        final KafkaStreams driver1 = getRunningStreams(STREAMS_CONFIG, builder, false);
        final ReadOnlyKeyValueStore<String, String> store = IntegrationTestUtils.getStore(storeName, driver1, keyValueStore());
        assertEquals("v1", store.get("k1"));
        assertNull(store.get("k2"));

        driver1.close();
        quietlyCleanStateAfterTest(CLUSTER, driver);
        quietlyCleanStateAfterTest(CLUSTER, driver1);
    }

    @Test
    public void testDiscardsUncommittedDataTimestampedKVStore() throws Exception {
        final String uniqueTestName = safeUniqueTestName(getClass(), testName);
        final String input = uniqueTestName + "-input";
        final String storeName = uniqueTestName + "-store";
        final String output = uniqueTestName + "-output";
        final String appId = uniqueTestName + "-appId";
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        STREAMS_CONFIG.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, eosConfig);
        producerConfig.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, appId + "-producer");

        cleanStateBeforeTest(CLUSTER, input, output);

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> inputStream = builder.stream(input);

        final StoreBuilder<KeyValueStore<String, String>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentTimestampedKeyValueStore(storeName, true),
                Serdes.String(),
                Serdes.String()
            )
            .withCachingDisabled();
        builder.addStateStore(storeBuilder);

        final AtomicBoolean crashRequested = new AtomicBoolean(false);
        final CountDownLatch crashLatch = new CountDownLatch(1);
        final AtomicBoolean skipRecords = new AtomicBoolean(false);

        inputStream.process(new ProcessorSupplier<String, String, String, String>() {

                @Override
                public Processor<String, String, String, String> get() {
                    return new Processor<String, String, String, String>() {

                        private KeyValueStore<String, String> store;
                        private ProcessorContext<String, String> context;

                        @Override
                        public void init(final ProcessorContext<String, String> context) {
                            this.store = Objects.requireNonNull(context.getStateStore(storeName));
                            this.context = context;
                        }

                        @Override
                        public void process(final Record<String, String> record) {
                            if (skipRecords.get()) {
                                return;
                            }

                            store.put(record.key(), record.value());

                            // crash after updating local state, but before forwarding the record
                            if (crashRequested.get()) {
                                try {
                                    // wait for the main thread to ensure that the uncommitted value is readable
                                    crashLatch.await();
                                } catch (final InterruptedException ie) {
                                    throw new RuntimeException(ie);
                                }
                                throw new RuntimeException("BOOOM!");
                            }
                            context.forward(record);
                        }
                    };
                }
            }, storeName)
            .to(output);


        final KafkaStreams driver = getRunningStreams(STREAMS_CONFIG, builder, true);

        try {
            IntegrationTestUtils.produceKeyValuesSynchronously(
                input,
                singletonList(new KeyValue<>("k1", "v1")),
                TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class, producerConfig),
                CLUSTER.time,
                true
            );
            IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                consumerConfig(appId + "readCommitted"), output, 1);

            final ReadOnlyKeyValueStore<Object, Object> store = IntegrationTestUtils.getStore(
                storeName, driver, keyValueStore());
            assertEquals("v1", store.get("k1"));

            Thread.sleep(2 * COMMIT_INTERVAL_MS);
            crashRequested.set(true);
            IntegrationTestUtils.produceKeyValuesSynchronously(
                input,
                singletonList(new KeyValue<>("k2", "v2")),
                TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class, producerConfig),
                CLUSTER.time,
                true
            );
            TestUtils.waitForCondition(() -> store.get("k2").equals("v2"),
                "Expected to read the second key");
            crashLatch.countDown();

            TestUtils.waitForCondition(() -> driver.state().hasCompletedShutdown(),
                "Streams app didn't shut down");
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        skipRecords.set(true);
        final KafkaStreams driver1 = getRunningStreams(STREAMS_CONFIG, builder, false);

        final ReadOnlyKeyValueStore<String, String> store = IntegrationTestUtils.getStore(storeName, driver1, keyValueStore());
        assertEquals("v1", store.get("k1"));
        assertNull(store.get("k2"));

        driver1.close();
        quietlyCleanStateAfterTest(CLUSTER, driver);
        quietlyCleanStateAfterTest(CLUSTER, driver1);
    }

    @Test
    public void testDiscardsUncommittedDataWindowStore() throws Exception {
        final String uniqueTestName = safeUniqueTestName(getClass(), testName);
        final String input = uniqueTestName + "-input";
        final String storeName = uniqueTestName + "-store";
        final String output = uniqueTestName + "-output";
        final String appId = uniqueTestName + "-appId";
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        STREAMS_CONFIG.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, eosConfig);
        producerConfig.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, appId + "-producer");

        cleanStateBeforeTest(CLUSTER, input, output);

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> inputStream = builder.stream(input);

        final StoreBuilder<WindowStore<String, String>> storeBuilder = Stores.windowStoreBuilder(
                Stores.persistentWindowStore(
                    storeName,
                    Duration.of(1, ChronoUnit.DAYS),
                    Duration.of(1, ChronoUnit.DAYS),
                    false,
                    true),
                Serdes.String(),
                Serdes.String()
            )
            .withCachingDisabled();
        builder.addStateStore(storeBuilder);

        final AtomicBoolean crashRequested = new AtomicBoolean(false);
        final CountDownLatch crashLatch = new CountDownLatch(1);
        final AtomicBoolean skipRecords = new AtomicBoolean(false);

        inputStream.process(new ProcessorSupplier<String, String, String, String>() {

                @Override
                public Processor<String, String, String, String> get() {
                    return new Processor<String, String, String, String>() {

                        private WindowStore<String, String> store;
                        private ProcessorContext<String, String> context;

                        @Override
                        public void init(final ProcessorContext<String, String> context) {
                            this.store = Objects.requireNonNull(context.getStateStore(storeName));
                            this.context = context;
                        }

                        @Override
                        public void process(final Record<String, String> record) {
                            if (skipRecords.get()) {
                                return;
                            }

                            store.put(record.key(), record.value(), record.timestamp());

                            // crash after updating local state, but before forwarding the record
                            if (crashRequested.get()) {
                                try {
                                    // wait for the main thread to ensure that the uncommitted value is readable
                                    crashLatch.await();
                                } catch (final InterruptedException ie) {
                                    throw new RuntimeException(ie);
                                }
                                throw new RuntimeException("BOOOM!");
                            }
                            context.forward(record);
                        }
                    };
                }
            }, storeName)
            .to(output);


        final KafkaStreams driver = getRunningStreams(STREAMS_CONFIG, builder, true);

        final long ts1 = CLUSTER.time.milliseconds();
        final long ts2;
        try {
            IntegrationTestUtils.produceKeyValuesSynchronously(
                input,
                singletonList(new KeyValue<>("k1", "v1")),
                TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class, producerConfig),
                CLUSTER.time,
                true
            );
            IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                consumerConfig(appId + "readCommitted"), output, 1);

            final ReadOnlyWindowStore<Object, Object> store = IntegrationTestUtils.getStore(
                storeName, driver, windowStore());
            assertEquals("v1", store.fetch("k1", ts1));

            Thread.sleep(2 * COMMIT_INTERVAL_MS);
            crashRequested.set(true);
            ts2 = CLUSTER.time.milliseconds();
            IntegrationTestUtils.produceKeyValuesSynchronously(
                input,
                singletonList(new KeyValue<>("k2", "v2")),
                TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class, producerConfig),
                CLUSTER.time,
                true
            );
            TestUtils.waitForCondition(() -> store.fetch("k2", ts2).equals("v2"),
                "Expected to read the second key");
            crashLatch.countDown();

            TestUtils.waitForCondition(() -> driver.state().hasCompletedShutdown(),
                "Streams app didn't shut down");
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        skipRecords.set(true);
        final KafkaStreams driver1 = getRunningStreams(STREAMS_CONFIG, builder, false);

        final ReadOnlyWindowStore<String, String> store = IntegrationTestUtils.getStore(storeName, driver1, windowStore());
        assertEquals("v1", store.fetch("k1", ts1));
        assertNull(store.fetch("k2", ts2));

        driver1.close();
        quietlyCleanStateAfterTest(CLUSTER, driver);
        quietlyCleanStateAfterTest(CLUSTER, driver1);
    }

    @Test
    public void testDiscardsUncommittedDataTSWindowStore() throws Exception {
        final String uniqueTestName = safeUniqueTestName(getClass(), testName);
        final String input = uniqueTestName + "-input";
        final String storeName = uniqueTestName + "-store";
        final String output = uniqueTestName + "-output";
        final String appId = uniqueTestName + "-appId";
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        STREAMS_CONFIG.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, eosConfig);
        producerConfig.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, appId + "-producer");

        cleanStateBeforeTest(CLUSTER, input, output);

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> inputStream = builder.stream(input);

        final StoreBuilder<WindowStore<String, String>> storeBuilder = Stores.windowStoreBuilder(
                Stores.persistentTimestampedWindowStore(
                    storeName,
                    Duration.of(1L, ChronoUnit.DAYS),
                    Duration.of(1L, ChronoUnit.DAYS),
                    false,
                    true),
                Serdes.String(),
                Serdes.String()
            )
            .withCachingDisabled();
        builder.addStateStore(storeBuilder);

        final AtomicBoolean crashRequested = new AtomicBoolean(false);
        final CountDownLatch crashLatch = new CountDownLatch(1);
        final AtomicBoolean skipRecords = new AtomicBoolean(false);

        inputStream.process(new ProcessorSupplier<String, String, String, String>() {

                @Override
                public Processor<String, String, String, String> get() {
                    return new Processor<String, String, String, String>() {

                        private WindowStore<String, String> store;
                        private ProcessorContext<String, String> context;

                        @Override
                        public void init(final ProcessorContext<String, String> context) {
                            this.store = Objects.requireNonNull(context.getStateStore(storeName));
                            this.context = context;
                        }

                        @Override
                        public void process(final Record<String, String> record) {
                            if (skipRecords.get()) {
                                return;
                            }

                            store.put(record.key(), record.value(), record.timestamp());

                            // crash after updating local state, but before forwarding the record
                            if (crashRequested.get()) {
                                try {
                                    // wait for the main thread to ensure that the uncommitted value is readable
                                    crashLatch.await();
                                } catch (final InterruptedException ie) {
                                    throw new RuntimeException(ie);
                                }
                                throw new RuntimeException("BOOOM!");
                            }
                            context.forward(record);
                        }
                    };
                }
            }, storeName)
            .to(output);

        final KafkaStreams driver = getRunningStreams(STREAMS_CONFIG, builder, true);

        final long ts1 = CLUSTER.time.milliseconds();
        final long ts2;
        try {
            IntegrationTestUtils.produceKeyValuesSynchronously(
                input,
                singletonList(new KeyValue<>("k1", "v1")),
                TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class, producerConfig),
                CLUSTER.time,
                true
            );
            IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                consumerConfig(appId + "readCommitted"), output, 1);

            final ReadOnlyWindowStore<String, String> store = IntegrationTestUtils.getStore(
                storeName, driver, windowStore());
            assertEquals("v1", store.fetch("k1", ts1));

            Thread.sleep(2 * COMMIT_INTERVAL_MS);
            crashRequested.set(true);
            ts2 = CLUSTER.time.milliseconds();
            IntegrationTestUtils.produceKeyValuesSynchronously(
                input,
                singletonList(new KeyValue<>("k2", "v2")),
                TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class, producerConfig),
                CLUSTER.time,
                true
            );
            TestUtils.waitForCondition(() -> store.fetch("k2", ts2).equals("v2"),
                "Expected to read the second key");
            crashLatch.countDown();

            TestUtils.waitForCondition(() -> driver.state().hasCompletedShutdown(),
                "Streams app didn't shut down");
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        skipRecords.set(true);
        final KafkaStreams driver1 = getRunningStreams(STREAMS_CONFIG, builder, false);

        final ReadOnlyWindowStore<String, String> store = IntegrationTestUtils.getStore(storeName, driver1, windowStore());
        // RecordConverters puts the timestamp inside the value after reading raw records from the changelog
        final String val = store.fetch("k1", ts1);
        assertTrue(
            String.format("Incorrect value in the state store. Expected: %s, got: %s", "v1", val),
            val.endsWith("v1")
        );
        assertNull(store.fetch("k2", ts2));

        driver1.close();
        quietlyCleanStateAfterTest(CLUSTER, driver);
        quietlyCleanStateAfterTest(CLUSTER, driver1);
    }

    @Test
    public void testDiscardsUncommittedDataSessionStore() throws Exception {
        final String uniqueTestName = safeUniqueTestName(getClass(), testName);
        final String input = uniqueTestName + "-input";
        final String storeName = uniqueTestName + "-store";
        final String output = uniqueTestName + "-output";
        final String appId = uniqueTestName + "-appId";
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        STREAMS_CONFIG.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, eosConfig);
        producerConfig.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, appId + "-producer");

        cleanStateBeforeTest(CLUSTER, input, output);

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> inputStream = builder.stream(input);

        final StoreBuilder<SessionStore<String, String>> storeBuilder = Stores.sessionStoreBuilder(
                Stores.persistentSessionStore(
                    storeName,
                    Duration.of(1, ChronoUnit.DAYS),
                    true),
                Serdes.String(),
                Serdes.String()
            )
            .withCachingDisabled();
        builder.addStateStore(storeBuilder);

        final AtomicBoolean crashRequested = new AtomicBoolean(false);
        final CountDownLatch crashLatch = new CountDownLatch(1);
        final AtomicBoolean skipRecords = new AtomicBoolean(false);

        final long sessionStart = 0L;
        final long sessionEnd = 10L;

        inputStream.process(new ProcessorSupplier<String, String, String, String>() {

                @Override
                public Processor<String, String, String, String> get() {
                    return new Processor<String, String, String, String>() {

                        private SessionStore<String, String> store;
                        private ProcessorContext<String, String> context;

                        @Override
                        public void init(final ProcessorContext<String, String> context) {
                            this.store = Objects.requireNonNull(context.getStateStore(storeName));
                            this.context = context;
                        }

                        @Override
                        public void process(final Record<String, String> record) {
                            if (skipRecords.get()) {
                                return;
                            }

                            store.put(new Windowed<>(record.key(), new SessionWindow(sessionStart, sessionEnd)), record.value());

                            // crash after updating local state, but before forwarding the record
                            if (crashRequested.get()) {
                                try {
                                    // wait for the main thread to ensure that the uncommitted value is readable
                                    crashLatch.await();
                                } catch (final InterruptedException ie) {
                                    throw new RuntimeException(ie);
                                }
                                throw new RuntimeException("BOOOM!");
                            }
                            context.forward(record);
                        }
                    };
                }
            }, storeName)
            .to(output);


        final KafkaStreams driver = getRunningStreams(STREAMS_CONFIG, builder, true);

        try {
            IntegrationTestUtils.produceKeyValuesSynchronously(
                input,
                singletonList(new KeyValue<>("k1", "v1")),
                TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class, producerConfig),
                CLUSTER.time,
                true
            );
            IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                consumerConfig(appId + "readCommitted"), output, 1);

            final ReadOnlySessionStore<Object, Object> store = IntegrationTestUtils.getStore(
                storeName, driver, sessionStore());
            assertEquals("v1", store.fetchSession("k1", sessionStart, sessionEnd));

            Thread.sleep(2 * COMMIT_INTERVAL_MS);
            crashRequested.set(true);
            IntegrationTestUtils.produceKeyValuesSynchronously(
                input,
                singletonList(new KeyValue<>("k2", "v2")),
                TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class, producerConfig),
                CLUSTER.time,
                true
            );
            TestUtils.waitForCondition(() -> store.fetchSession("k2", sessionStart, sessionEnd).equals("v2"),
                "Expected to read the second key");
            crashLatch.countDown();

            TestUtils.waitForCondition(() -> driver.state().hasCompletedShutdown(),
                "Streams app didn't shut down");
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        skipRecords.set(true);
        final KafkaStreams driver1 = getRunningStreams(STREAMS_CONFIG, builder, false);
        final ReadOnlySessionStore<Object, Object> store = IntegrationTestUtils.getStore(
            storeName, driver1, sessionStore());
        assertEquals("v1",  store.fetchSession("k1", sessionStart, sessionEnd));
        assertNull(store.fetchSession("k2", sessionStart, sessionEnd));

        driver1.close();
        quietlyCleanStateAfterTest(CLUSTER, driver);
        quietlyCleanStateAfterTest(CLUSTER, driver1);
    }
}
