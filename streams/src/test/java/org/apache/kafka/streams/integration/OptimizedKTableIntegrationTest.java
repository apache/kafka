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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
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
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
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

    private static final String INPUT_TOPIC_NAME = "input-topic";
    private static final String TABLE_NAME = "source-table";

    @Rule
    public final EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster(NUM_BROKERS);

    private final Map<KafkaStreams, State> clientStates = new HashMap<>();
    private final Lock clientStatesLock = new ReentrantLock();
    private final Condition clientStateUpdate = clientStatesLock.newCondition();
    private final MockTime mockTime = cluster.time;

    @Before
    public void before() throws InterruptedException {
        cluster.createTopic(INPUT_TOPIC_NAME, 2, 1);
    }

    @After
    public void after() {
        for (final KafkaStreams client : clientStates.keySet()) {
            client.close();
        }
    }

    @Test
    public void standbyShouldNotPerformRestoreAtStartup() throws InterruptedException, ExecutionException {
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

        final KafkaStreams client1 = createClient(builder, streamsConfiguration());
        final KafkaStreams client2 = createClient(builder, streamsConfiguration());
        final List<KafkaStreams> clients = Arrays.asList(client1, client2);

        final AtomicLong restoreStartOffset = new AtomicLong(-1);
        clients.forEach(client -> {
            client.setGlobalStateRestoreListener(createTrackingRestoreListener(restoreStartOffset));
            client.start();
        });
        waitForClientsToEnterRunningState(clients, 60, TimeUnit.SECONDS);

        produceValueRange(key, 0, 10);

        assertThat("all messages in the first batch were processed in a timely manner",
            semaphore.tryAcquire(numMessages, 60, TimeUnit.SECONDS));

        assertThat("no restore has occurred", restoreStartOffset.get(), is(equalTo(-1L)));
    }

    @Test
    public void shouldApplyUpdatesToStandbyStore() throws InterruptedException, ExecutionException {
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

        final KafkaStreams client1 = createClient(builder, streamsConfiguration());
        final KafkaStreams client2 = createClient(builder, streamsConfiguration());
        final List<KafkaStreams> clients = Arrays.asList(client1, client2);

        final AtomicLong restoreStartOffset = new AtomicLong(-1L);
        clients.forEach(client -> {
            client.setGlobalStateRestoreListener(createTrackingRestoreListener(restoreStartOffset));
            client.start();
        });
        waitForClientsToEnterRunningState(clients, 60, TimeUnit.SECONDS);

        produceValueRange(key, 0, batch1NumMessages);

        assertThat("all messages in the first batch were processed in a timely manner",
            semaphore.tryAcquire(batch1NumMessages, 60, TimeUnit.SECONDS));

        final ReadOnlyKeyValueStore<Integer, Integer> store1 = client1
            .store(TABLE_NAME, QueryableStoreTypes.keyValueStore());

        final ReadOnlyKeyValueStore<Integer, Integer> store2 = client2
            .store(TABLE_NAME, QueryableStoreTypes.keyValueStore());

        final boolean client1WasFirstActive;
        if (store1.get(key) != null) {
            client1WasFirstActive = true;
        } else {
            assertThat("data from the job was sent to the store", store2.get(key) != null);
            client1WasFirstActive = false;
        }

        assertThat("no restore has occurred", restoreStartOffset.get(), is(equalTo(-1L)));
        assertThat("current value in store should reflect all messages being processed",
            client1WasFirstActive ? store1.get(key) : store2.get(key), is(equalTo(batch1NumMessages - 1)));

        if (client1WasFirstActive) {
            client1.close();
        } else {
            client2.close();
        }

        final int totalNumMessages = batch1NumMessages + batch2NumMessages;

        produceValueRange(key, batch1NumMessages, totalNumMessages);

        assertThat("all messages in the second batch were processed in a timely manner",
            semaphore.tryAcquire(batch2NumMessages, 60, TimeUnit.SECONDS));

        assertThat("either restore was unnecessary or we restored from an offset later than 0",
            restoreStartOffset.get(), is(anyOf(greaterThan(0L), equalTo(-1L))));

        assertThat("current value in store should reflect all messages being processed",
            client1WasFirstActive ? store2.get(key) : store1.get(key), is(equalTo(totalNumMessages - 1)));
    }

    private void produceValueRange(final int key, final int start, final int endExclusive) throws ExecutionException, InterruptedException {
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

    private void waitForClientsToEnterRunningState(final Collection<KafkaStreams> clients,
                                                   final long time,
                                                   final TimeUnit timeUnit) throws InterruptedException {

        final long expectedEnd = System.currentTimeMillis() + timeUnit.toMillis(time);

        clientStatesLock.lock();
        try {
            while (!clients.stream().allMatch(client -> clientStates.get(client) == State.RUNNING)) {
                if (expectedEnd <= System.currentTimeMillis()) {
                    assertThat("requested clients entered " + State.RUNNING + " in a timely manner", false);
                }
                final long millisRemaining = Math.max(1, expectedEnd - System.currentTimeMillis());
                clientStateUpdate.await(millisRemaining, TimeUnit.MILLISECONDS);
            }
        } finally {
            clientStatesLock.unlock();
        }
    }

    private KafkaStreams createClient(final StreamsBuilder builder, final Properties config) {
        final KafkaStreams client = new KafkaStreams(builder.build(config), config);
        clientStatesLock.lock();
        try {
            clientStates.put(client, client.state());
        } finally {
            clientStatesLock.unlock();
        }

        client.setStateListener((newState, oldState) -> {
            clientStatesLock.lock();
            try {
                clientStates.put(client, newState);
                if (newState == State.RUNNING) {
                    if (clientStates.values().stream().allMatch(state -> state == State.RUNNING)) {
                        clientStateUpdate.signalAll();
                    }
                }
            } finally {
                clientStatesLock.unlock();
            }
        });
        return client;
    }

    private StateRestoreListener createTrackingRestoreListener(final AtomicLong restoreStartOffset) {
        return new StateRestoreListener() {
            @Override
            public void onRestoreStart(final TopicPartition topicPartition, final String storeName,
                final long startingOffset, final long endingOffset) {
                restoreStartOffset.set(startingOffset);
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
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        config.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(applicationId).getPath());
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        config.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        return config;
    }
}
