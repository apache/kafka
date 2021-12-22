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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.internals.RocksDBStore;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.getStore;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.apache.kafka.streams.state.QueryableStoreTypes.keyValueStore;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@Category({IntegrationTest.class})
public class ConsistencyVectorIntegrationTest {

    private static final int NUM_BROKERS = 1;
    private static int port = 0;
    private static final String INPUT_TOPIC_NAME = "input-topic";
    private static final String TABLE_NAME = "source-table";

    public final EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster(NUM_BROKERS);

    @Rule
    public TestName testName = new TestName();

    private final List<KafkaStreams> streamsToCleanup = new ArrayList<>();
    private final MockTime mockTime = cluster.time;

    @Before
    public void before() throws InterruptedException, IOException {
        cluster.start();
        cluster.createTopic(INPUT_TOPIC_NAME, 1, 1);
    }

    @After
    public void after() {
        for (final KafkaStreams kafkaStreams : streamsToCleanup) {
            kafkaStreams.close();
        }
        cluster.stop();
    }

    @Test
    public void shouldHaveSamePositionBoundActiveAndStandBy() throws Exception {
        final int batch1NumMessages = 100;
        final int key = 1;
        final Semaphore semaphore = new Semaphore(0);

        final StreamsBuilder builder = new StreamsBuilder();
        Objects.requireNonNull(TABLE_NAME, "name cannot be null");
        final TestingRocksDbKeyValueBytesStoreSupplier supplier =
                new TestingRocksDbKeyValueBytesStoreSupplier(TABLE_NAME);

        builder.table(INPUT_TOPIC_NAME, Consumed.with(Serdes.Integer(), Serdes.Integer()),
                        Materialized.<Integer, Integer>as(supplier).withCachingDisabled())
                .toStream()
                .peek((k, v) -> semaphore.release());

        final KafkaStreams kafkaStreams1 = createKafkaStreams(builder, streamsConfiguration());
        final KafkaStreams kafkaStreams2 = createKafkaStreams(builder, streamsConfiguration());
        final List<KafkaStreams> kafkaStreamsList = Arrays.asList(kafkaStreams1, kafkaStreams2);

        startApplicationAndWaitUntilRunning(kafkaStreamsList, Duration.ofSeconds(60));

        produceValueRange(key, 0, batch1NumMessages);

        // Assert that all messages in the first batch were processed in a timely manner
        assertThat(semaphore.tryAcquire(batch1NumMessages, 60, TimeUnit.SECONDS), is(equalTo(true)));

        final QueryableStoreType<ReadOnlyKeyValueStore<Integer, Integer>> queryableStoreType = keyValueStore();

        // Assert that both active and standby have the same position bound
        TestUtils.waitForCondition(() -> {
            final ReadOnlyKeyValueStore<Integer, Integer> store1 = getStore(TABLE_NAME, kafkaStreams1, true, queryableStoreType);
            return store1.get(key) == batch1NumMessages - 1;
        }, "store1 cannot find results for key");
        TestUtils.waitForCondition(() -> {
            final ReadOnlyKeyValueStore<Integer, Integer> store2 = getStore(TABLE_NAME, kafkaStreams2, true, queryableStoreType);
            return store2.get(key) == batch1NumMessages - 1;
        }, "store2 cannot find results for key");

        final AtomicInteger count = new AtomicInteger();
        for (final TestingRocksDBStore store : supplier.stores) {
            if (store.getDbDir() != null) {
                assertThat(store.getDbDir().toString().contains("/0_0/"), is(true));
                assertThat(store.getPosition().getPartitionPositions(INPUT_TOPIC_NAME), notNullValue());
                assertThat(store.getPosition().getPartitionPositions(INPUT_TOPIC_NAME), hasEntry(0, 99L));
                count.incrementAndGet();
            }
        }
        assertThat(count.get(), is(2));
    }

    public class TestingRocksDBStore extends RocksDBStore {
        public TestingRocksDBStore(final String name, final String metricsScope) {
            super(name, metricsScope);
        }

        public Position getPosition() {
            return position;
        }

        public File getDbDir() {
            return dbDir;
        }
    }

    public class TestingRocksDbKeyValueBytesStoreSupplier implements KeyValueBytesStoreSupplier {

        public List<TestingRocksDBStore> stores = new LinkedList<>();

        private final String name;

        public TestingRocksDbKeyValueBytesStoreSupplier(final String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public KeyValueStore<Bytes, byte[]> get() {
            final TestingRocksDBStore testingRocksDBStore = new TestingRocksDBStore(name, metricsScope());
            stores.add(testingRocksDBStore);
            return testingRocksDBStore;
        }

        @Override
        public String metricsScope() {
            return "rocksdb";
        }
    }


    private KafkaStreams createKafkaStreams(final StreamsBuilder builder, final Properties config) {
        final KafkaStreams streams = new KafkaStreams(builder.build(config), config);
        streamsToCleanup.add(streams);
        return streams;
    }

    private void produceValueRange(final int key, final int start, final int endExclusive) {
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);

        IntegrationTestUtils.produceKeyValuesSynchronously(
            INPUT_TOPIC_NAME,
            IntStream.range(start, endExclusive)
                     .mapToObj(i -> KeyValue.pair(key, i))
                     .collect(Collectors.toList()),
            producerProps,
            mockTime);
    }

    private Properties streamsConfiguration() {
        final String safeTestName = safeUniqueTestName(getClass(), testName);
        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-" + safeTestName);
        config.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:" + (++port));
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        config.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        config.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 200);
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 1000);
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        config.put(InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED, true);
        return config;
    }
}
