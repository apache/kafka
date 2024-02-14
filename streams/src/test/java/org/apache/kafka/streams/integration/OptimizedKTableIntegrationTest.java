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

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.test.NoRetryException;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Timeout(600)
@Tag("integration")
public class OptimizedKTableIntegrationTest {
    private static final Logger LOG = LoggerFactory.getLogger(OptimizedKTableIntegrationTest.class);
    private static final int NUM_BROKERS = 1;
    private static int port = 0;
    private static final String INPUT_TOPIC_NAME = "input-topic";
    private static final String TABLE_NAME = "source-table";

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    private final List<KafkaStreams> streamsToCleanup = new ArrayList<>();
    private final MockTime mockTime = CLUSTER.time;

    @BeforeEach
    public void before() throws InterruptedException {
        CLUSTER.createTopic(INPUT_TOPIC_NAME, 2, 1);
    }

    @AfterEach
    public void after() {
        for (final KafkaStreams kafkaStreams : streamsToCleanup) {
            kafkaStreams.close();
        }
    }

    @Test
    public void shouldApplyUpdatesToStandbyStore(final TestInfo testInfo) throws Exception {
        final int batch1NumMessages = 100;
        final int batch2NumMessages = 100;
        final int key = 1;
        final Semaphore semaphore = new Semaphore(0);

        final StreamsBuilder builder = new StreamsBuilder();
        builder
            .table(INPUT_TOPIC_NAME, Consumed.with(Serdes.Integer(), Serdes.Integer()),
                Materialized.<Integer, Integer, KeyValueStore<Bytes, byte[]>>as(TABLE_NAME)
                    .withCachingDisabled())
            .toStream()
            .peek((k, v) -> semaphore.release());

        final String safeTestName = safeUniqueTestName(testInfo);
        final KafkaStreams kafkaStreams1 = createKafkaStreams(builder, streamsConfiguration(safeTestName));
        final KafkaStreams kafkaStreams2 = createKafkaStreams(builder, streamsConfiguration(safeTestName));
        final List<KafkaStreams> kafkaStreamsList = Arrays.asList(kafkaStreams1, kafkaStreams2);

        try {
            startApplicationAndWaitUntilRunning(kafkaStreamsList, Duration.ofSeconds(60));

            produceValueRange(key, 0, batch1NumMessages);

            // Assert that all messages in the first batch were processed in a timely manner
            assertThat(semaphore.tryAcquire(batch1NumMessages, 60, TimeUnit.SECONDS), is(equalTo(true)));

            final AtomicReference<ReadOnlyKeyValueStore<Integer, Integer>> newActiveStore = new AtomicReference<>(null);
            TestUtils.retryOnExceptionWithTimeout(() -> {
                final ReadOnlyKeyValueStore<Integer, Integer> store1 = IntegrationTestUtils.getStore(TABLE_NAME, kafkaStreams1, QueryableStoreTypes.keyValueStore());
                final ReadOnlyKeyValueStore<Integer, Integer> store2 = IntegrationTestUtils.getStore(TABLE_NAME, kafkaStreams2, QueryableStoreTypes.keyValueStore());

                final KeyQueryMetadata keyQueryMetadata = kafkaStreams1.queryMetadataForKey(TABLE_NAME, key, (topic, somekey, value, numPartitions) -> 0);

                try {
                    // Assert that the current value in store reflects all messages being processed
                    if ((keyQueryMetadata.activeHost().port() % 2) == 1) {
                        assertThat(store1.get(key), is(equalTo(batch1NumMessages - 1)));
                        kafkaStreams1.close();
                        newActiveStore.set(store2);
                    } else {
                        assertThat(store2.get(key), is(equalTo(batch1NumMessages - 1)));
                        kafkaStreams2.close();
                        newActiveStore.set(store1);
                    }
                } catch (final InvalidStateStoreException e) {
                    LOG.warn("Detected an unexpected rebalance during test. Retrying if possible.", e);
                    throw e;
                } catch (final Throwable t) {
                    LOG.error("Caught non-retriable exception in test. Exiting.", t);
                    throw new NoRetryException(t);
                }
            });

            // Wait for failover
            TestUtils.retryOnExceptionWithTimeout(60 * 1000, 100, () -> {
                // Assert that after failover we have recovered to the last store write
                assertThat(newActiveStore.get().get(key), is(equalTo(batch1NumMessages - 1)));
            });

            final int totalNumMessages = batch1NumMessages + batch2NumMessages;

            produceValueRange(key, batch1NumMessages, totalNumMessages);

            // Assert that all messages in the second batch were processed in a timely manner
            assertThat(semaphore.tryAcquire(batch2NumMessages, 60, TimeUnit.SECONDS), is(equalTo(true)));

            TestUtils.retryOnExceptionWithTimeout(60 * 1000, 100, () -> {
                // Assert that the current value in store reflects all messages being processed
                assertThat(newActiveStore.get().get(key), is(equalTo(totalNumMessages - 1)));
            });
        } finally {
            kafkaStreams1.close();
            kafkaStreams2.close();
        }
    }

    private void produceValueRange(final int key, final int start, final int endExclusive) {
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
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

    private KafkaStreams createKafkaStreams(final StreamsBuilder builder, final Properties config) {
        final KafkaStreams streams = new KafkaStreams(builder.build(config), config);
        streamsToCleanup.add(streams);
        return streams;
    }

    private Properties streamsConfiguration(final String safeTestName) {
        final Properties config = new Properties();
        config.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-" + safeTestName);
        config.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:" + (++port));
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        config.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        config.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        config.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 200);
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 1000);
        return config;
    }
}
