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

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class OptimizedKTableIntegrationTest {
    private static final int NUM_BROKERS = 1;
    private static int port = 0;
    private static final String INPUT_TOPIC_NAME = "input-topic";
    private static final String TABLE_NAME = "source-table";

    @Rule
    public final EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster(NUM_BROKERS);

    private final List<KafkaStreams> streamsToCleanup = new ArrayList<>();
    private final MockTime mockTime = cluster.time;

    @Before
    public void before() throws InterruptedException {
        cluster.createTopic(INPUT_TOPIC_NAME, 2, 1);
    }

    @After
    public void after() {
        for (final KafkaStreams kafkaStreams : streamsToCleanup) {
            kafkaStreams.close();
        }
    }

    @Test
    public void standbyShouldNotPerformRestoreAtStartup() throws Exception {
        final int numMessages = 10;
        final int key = 1;
        final Semaphore semaphore = new Semaphore(0);

        final StreamsBuilder builder = new StreamsBuilder();
        builder
            .table(INPUT_TOPIC_NAME, Consumed.with(Serdes.Integer(), Serdes.Integer()),
                Materialized.<Integer, Integer, KeyValueStore<Bytes, byte[]>>as(TABLE_NAME)
                    .withCachingDisabled())
            .toStream()
            .peek((k, v) -> semaphore.release());

        final KafkaStreams kafkaStreams1 = createKafkaStreams(builder, streamsConfiguration());
        final KafkaStreams kafkaStreams2 = createKafkaStreams(builder, streamsConfiguration());
        final List<KafkaStreams> kafkaStreamsList = Arrays.asList(kafkaStreams1, kafkaStreams2);

        produceValueRange(key, 0, 10);

        final AtomicLong restoreStartOffset = new AtomicLong(-1);
        kafkaStreamsList.forEach(kafkaStreams -> {
            kafkaStreams.setGlobalStateRestoreListener(createTrackingRestoreListener(restoreStartOffset, new AtomicLong()));
        });
        startApplicationAndWaitUntilRunning(kafkaStreamsList, Duration.ofSeconds(60));

        // Assert that all messages in the first batch were processed in a timely manner
        assertThat(semaphore.tryAcquire(numMessages, 60, TimeUnit.SECONDS), is(equalTo(true)));

        // Assert that no restore occurred
        assertThat(restoreStartOffset.get(), is(equalTo(-1L)));
    }

    @Test
    public void shouldApplyUpdatesToStandbyStore() throws Exception {
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

        final KafkaStreams kafkaStreams1 = createKafkaStreams(builder, streamsConfiguration());
        final KafkaStreams kafkaStreams2 = createKafkaStreams(builder, streamsConfiguration());
        final List<KafkaStreams> kafkaStreamsList = Arrays.asList(kafkaStreams1, kafkaStreams2);

        final AtomicLong restoreStartOffset = new AtomicLong(-1L);
        final AtomicLong restoreEndOffset = new AtomicLong(-1L);
        kafkaStreamsList.forEach(kafkaStreams -> {
            kafkaStreams.setGlobalStateRestoreListener(createTrackingRestoreListener(restoreStartOffset, restoreEndOffset));
        });
        startApplicationAndWaitUntilRunning(kafkaStreamsList, Duration.ofSeconds(60));

        produceValueRange(key, 0, batch1NumMessages);

        // Assert that all messages in the first batch were processed in a timely manner
        assertThat(semaphore.tryAcquire(batch1NumMessages, 60, TimeUnit.SECONDS), is(equalTo(true)));

        final ReadOnlyKeyValueStore<Integer, Integer> store1 = kafkaStreams1
            .store(TABLE_NAME, QueryableStoreTypes.keyValueStore());

        final ReadOnlyKeyValueStore<Integer, Integer> store2 = kafkaStreams2
            .store(TABLE_NAME, QueryableStoreTypes.keyValueStore());

        final boolean kafkaStreams1WasFirstActive;
        final KeyQueryMetadata keyQueryMetadata = kafkaStreams1.queryMetadataForKey(TABLE_NAME, key, (topic, somekey, value, numPartitions) -> 0);

        if ((keyQueryMetadata.getActiveHost().port() % 2) == 1) {
            kafkaStreams1WasFirstActive = true;
        } else {
            // Assert that data from the job was sent to the store
            assertThat(store2.get(key), is(notNullValue()));
            kafkaStreams1WasFirstActive = false;
        }

        // Assert that no restore has occurred, ensures that when we check later that the restore
        // notification actually came from after the rebalance.
        assertThat(restoreStartOffset.get(), is(equalTo(-1L)));

        // Assert that the current value in store reflects all messages being processed
        assertThat(kafkaStreams1WasFirstActive ? store1.get(key) : store2.get(key), is(equalTo(batch1NumMessages - 1)));

        if (kafkaStreams1WasFirstActive) {
            kafkaStreams1.close();
        } else {
            kafkaStreams2.close();
        }

        final ReadOnlyKeyValueStore<Integer, Integer> newActiveStore =
            kafkaStreams1WasFirstActive ? store2 : store1;
        TestUtils.retryOnExceptionWithTimeout(100, 60 * 1000, () -> {
            // Assert that after failover we have recovered to the last store write
            assertThat(newActiveStore.get(key), is(equalTo(batch1NumMessages - 1)));
        });

        final int totalNumMessages = batch1NumMessages + batch2NumMessages;

        produceValueRange(key, batch1NumMessages, totalNumMessages);

        // Assert that all messages in the second batch were processed in a timely manner
        assertThat(semaphore.tryAcquire(batch2NumMessages, 60, TimeUnit.SECONDS), is(equalTo(true)));

        // Assert that either restore was unnecessary or we restored from an offset later than 0
        assertThat(restoreStartOffset.get(), is(anyOf(greaterThan(0L), equalTo(-1L))));

        // Assert that either restore was unnecessary or we restored to the last offset before we closed the kafkaStreams
        assertThat(restoreEndOffset.get(), is(anyOf(equalTo(batch1NumMessages - 1L), equalTo(-1L))));

        // Assert that the current value in store reflects all messages being processed
        assertThat(newActiveStore.get(key), is(equalTo(totalNumMessages - 1)));
    }

    private void produceValueRange(final int key, final int start, final int endExclusive) throws Exception {
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

    private KafkaStreams createKafkaStreams(final StreamsBuilder builder, final Properties config) {
        final KafkaStreams streams = new KafkaStreams(builder.build(config), config);
        streamsToCleanup.add(streams);
        return streams;
    }

    private StateRestoreListener createTrackingRestoreListener(final AtomicLong restoreStartOffset,
                                                               final AtomicLong restoreEndOffset) {
        return new StateRestoreListener() {
            @Override
            public void onRestoreStart(final TopicPartition topicPartition,
                                       final String storeName,
                                       final long startingOffset,
                                       final long endingOffset) {
                restoreStartOffset.set(startingOffset);
                restoreEndOffset.set(endingOffset);
            }

            @Override
            public void onBatchRestored(final TopicPartition topicPartition, final String storeName,
                final long batchEndOffset, final long numRestored) {

            }

            @Override
            public void onRestoreEnd(final TopicPartition topicPartition, final String storeName,
                final long totalRestored) {

            }
        };
    }

    private Properties streamsConfiguration() {
        final String applicationId = "streamsApp";
        final Properties config = new Properties();
        config.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        config.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:" + String.valueOf(++port));
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        config.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(applicationId).getPath());
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        config.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 200);
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 1000);
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        return config;
    }
}
