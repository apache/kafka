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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsNotStartedException;
import org.apache.kafka.streams.errors.StreamsStoppedException;
import org.apache.kafka.streams.errors.UnknownStateStoreException;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.processor.internals.StreamThread.State;
import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.StoreQueryUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Collections.singleton;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.streams.query.StateQueryRequest.inStore;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesPattern;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Timeout(600)
@Tag("integration")
public class IQv2IntegrationTest {
    private static final int NUM_BROKERS = 1;
    public static final Duration WINDOW_SIZE = Duration.ofMinutes(5);
    private static int port = 0;
    private static final String INPUT_TOPIC_NAME = "input-topic";
    private static final Position INPUT_POSITION = Position.emptyPosition();
    private static final String STORE_NAME = "kv-store";

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    private KafkaStreams kafkaStreams;

    @BeforeAll
    public static void before()
        throws InterruptedException, IOException, ExecutionException, TimeoutException {
        CLUSTER.start();
        final int partitions = 2;
        CLUSTER.createTopic(INPUT_TOPIC_NAME, partitions, 1);

        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);

        final List<Future<RecordMetadata>> futures = new LinkedList<>();
        try (final Producer<Integer, Integer> producer = new KafkaProducer<>(producerProps)) {
            for (int i = 0; i < 3; i++) {
                final Future<RecordMetadata> send = producer.send(
                    new ProducerRecord<>(
                        INPUT_TOPIC_NAME,
                        i % partitions,
                        Time.SYSTEM.milliseconds(),
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
                .withComponent(INPUT_TOPIC_NAME, 1, 0L)
        ));
    }

    @BeforeEach
    public void beforeTest(final TestInfo testInfo) {
        final StreamsBuilder builder = new StreamsBuilder();

        builder.table(
            INPUT_TOPIC_NAME,
            Consumed.with(Serdes.Integer(), Serdes.Integer()),
            Materialized.as(STORE_NAME)
        );


        final String safeTestName = IntegrationTestUtils.safeUniqueTestName(testInfo);
        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration(safeTestName));
        kafkaStreams.cleanUp();
    }

    @AfterEach
    public void afterTest() {
        kafkaStreams.close();
        kafkaStreams.cleanUp();
    }

    @AfterAll
    public static void after() {
        CLUSTER.stop();
    }

    @Test
    public void shouldFailUnknownStore() {
        final KeyQuery<Integer, ValueAndTimestamp<Integer>> query = KeyQuery.withKey(1);
        final StateQueryRequest<ValueAndTimestamp<Integer>> request =
            inStore("unknown-store").withQuery(query);

        assertThrows(UnknownStateStoreException.class, () -> kafkaStreams.query(request));
    }

    @Test
    public void shouldFailNotStarted() {
        final KeyQuery<Integer, ValueAndTimestamp<Integer>> query = KeyQuery.withKey(1);
        final StateQueryRequest<ValueAndTimestamp<Integer>> request =
            inStore(STORE_NAME).withQuery(query);

        assertThrows(StreamsNotStartedException.class, () -> kafkaStreams.query(request));
    }

    @Test
    public void shouldFailStopped() {
        final KeyQuery<Integer, ValueAndTimestamp<Integer>> query = KeyQuery.withKey(1);
        final StateQueryRequest<ValueAndTimestamp<Integer>> request =
            inStore(STORE_NAME).withQuery(query);

        kafkaStreams.start();
        kafkaStreams.close(Duration.ZERO);
        assertThrows(StreamsStoppedException.class, () -> kafkaStreams.query(request));
    }

    @Test
    public void shouldRejectNonRunningActive()
        throws NoSuchFieldException, IllegalAccessException {
        final KeyQuery<Integer, ValueAndTimestamp<Integer>> query = KeyQuery.withKey(1);
        final StateQueryRequest<ValueAndTimestamp<Integer>> request =
            inStore(STORE_NAME).withQuery(query).requireActive();
        final Set<Integer> partitions = mkSet(0, 1);

        kafkaStreams.start();

        final Field threadsField = KafkaStreams.class.getDeclaredField("threads");
        threadsField.setAccessible(true);
        @SuppressWarnings("unchecked") final List<StreamThread> threads =
            (List<StreamThread>) threadsField.get(kafkaStreams);
        final StreamThread streamThread = threads.get(0);

        final Field stateLock = StreamThread.class.getDeclaredField("stateLock");
        stateLock.setAccessible(true);
        final Object lock = stateLock.get(streamThread);

        // wait for the desired partitions to be assigned
        IntegrationTestUtils.iqv2WaitForPartitions(
            kafkaStreams,
            inStore(STORE_NAME).withQuery(query),
            partitions
        );

        // then lock the thread state, change it, and make our assertions.
        synchronized (lock) {
            final Field stateField = StreamThread.class.getDeclaredField("state");
            stateField.setAccessible(true);
            stateField.set(streamThread, State.PARTITIONS_ASSIGNED);

            final StateQueryResult<ValueAndTimestamp<Integer>> result =
                IntegrationTestUtils.iqv2WaitForPartitions(
                    kafkaStreams,
                    request,
                    partitions
                );

            assertThat(result.getPartitionResults().keySet(), is(partitions));
            for (final Integer partition : partitions) {
                assertThat(result.getPartitionResults().get(partition).isFailure(), is(true));
                assertThat(
                    result.getPartitionResults().get(partition).getFailureReason(),
                    is(FailureReason.NOT_ACTIVE)
                );
                assertThat(
                    result.getPartitionResults().get(partition).getFailureMessage(),
                    is("Query requires a running active task,"
                        + " but partition was in state PARTITIONS_ASSIGNED and was active.")
                );
            }
        }
    }

    @Test
    public void shouldFetchFromPartition() {
        final KeyQuery<Integer, ValueAndTimestamp<Integer>> query = KeyQuery.withKey(1);
        final int partition = 1;
        final Set<Integer> partitions = singleton(partition);
        final StateQueryRequest<ValueAndTimestamp<Integer>> request =
            inStore(STORE_NAME).withQuery(query).withPartitions(partitions);

        kafkaStreams.start();
        final StateQueryResult<ValueAndTimestamp<Integer>> result =
            IntegrationTestUtils.iqv2WaitForResult(kafkaStreams, request);

        assertThat(result.getPartitionResults().keySet(), equalTo(partitions));
    }

    @Test
    public void shouldFetchExplicitlyFromAllPartitions() {
        final KeyQuery<Integer, ValueAndTimestamp<Integer>> query = KeyQuery.withKey(1);
        final Set<Integer> partitions = mkSet(0, 1);
        final StateQueryRequest<ValueAndTimestamp<Integer>> request =
            inStore(STORE_NAME).withQuery(query).withAllPartitions();

        kafkaStreams.start();
        final StateQueryResult<ValueAndTimestamp<Integer>> result =
            IntegrationTestUtils.iqv2WaitForPartitions(kafkaStreams, request, partitions);

        assertThat(result.getPartitionResults().keySet(), equalTo(partitions));
    }

    @Test
    public void shouldNotRequireQueryHandler(final TestInfo testInfo) {
        final KeyQuery<Integer, ValueAndTimestamp<Integer>> query = KeyQuery.withKey(1);
        final int partition = 1;
        final Set<Integer> partitions = singleton(partition);
        final StateQueryRequest<ValueAndTimestamp<Integer>> request =
            inStore(STORE_NAME).withQuery(query).withPartitions(partitions);

        final StreamsBuilder builder = new StreamsBuilder();

        builder.table(
            INPUT_TOPIC_NAME,
            Consumed.with(Serdes.Integer(), Serdes.Integer()),
            Materialized.as(new KeyValueBytesStoreSupplier() {
                @Override
                public String name() {
                    return STORE_NAME;
                }

                @Override
                public KeyValueStore<Bytes, byte[]> get() {
                    return new KeyValueStore<Bytes, byte[]>() {
                        private boolean open = false;
                        private Map<Bytes, byte[]> map = new HashMap<>();
                        private Position position;
                        private StateStoreContext context;

                        @Override
                        public void put(final Bytes key, final byte[] value) {
                            synchronized (position) {
                                map.put(key, value);
                                StoreQueryUtils.updatePosition(position, context);
                            }
                        }

                        @Override
                        public byte[] putIfAbsent(final Bytes key, final byte[] value) {
                            synchronized (position) {
                                StoreQueryUtils.updatePosition(position, context);
                                return map.putIfAbsent(key, value);
                            }
                        }

                        @Override
                        public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
                            synchronized (position) {
                                StoreQueryUtils.updatePosition(position, context);
                                for (final KeyValue<Bytes, byte[]> entry : entries) {
                                    map.put(entry.key, entry.value);
                                }
                            }
                        }

                        @Override
                        public byte[] delete(final Bytes key) {
                            synchronized (position) {
                                StoreQueryUtils.updatePosition(position, context);
                                return map.remove(key);
                            }
                        }

                        @Override
                        public String name() {
                            return STORE_NAME;
                        }

                        @Deprecated
                        @Override
                        public void init(final ProcessorContext context, final StateStore root) {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public void init(final StateStoreContext context, final StateStore root) {
                            context.register(root, (key, value) -> put(Bytes.wrap(key), value));
                            this.open = true;
                            this.position = Position.emptyPosition();
                            this.context = context;
                        }

                        @Override
                        public void flush() {

                        }

                        @Override
                        public void close() {
                            this.open = false;
                            map.clear();
                        }

                        @Override
                        public boolean persistent() {
                            return false;
                        }

                        @Override
                        public boolean isOpen() {
                            return open;
                        }

                        @Override
                        public Position getPosition() {
                            return position;
                        }

                        @Override
                        public byte[] get(final Bytes key) {
                            return map.get(key);
                        }

                        @Override
                        public KeyValueIterator<Bytes, byte[]> range(
                            final Bytes from,
                            final Bytes to
                        ) {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public KeyValueIterator<Bytes, byte[]> all() {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public long approximateNumEntries() {
                            return map.size();
                        }
                    };
                }

                @Override
                public String metricsScope() {
                    return "nonquery";
                }
            })
        );

        // Discard the basic streams and replace with test-specific topology
        kafkaStreams.close();
        final String safeTestName = IntegrationTestUtils.safeUniqueTestName(testInfo);
        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration(safeTestName));
        kafkaStreams.cleanUp();

        kafkaStreams.start();
        final StateQueryResult<ValueAndTimestamp<Integer>> result =
            IntegrationTestUtils.iqv2WaitForResult(kafkaStreams, request);

        final QueryResult<ValueAndTimestamp<Integer>> queryResult =
            result.getPartitionResults().get(partition);
        assertThat(queryResult.isFailure(), is(true));
        assertThat(queryResult.getFailureReason(), is(FailureReason.UNKNOWN_QUERY_TYPE));
        assertThat(queryResult.getFailureMessage(), matchesPattern(
            "This store (.*) doesn't know how to execute the given query (.*)."
                + " Contact the store maintainer if you need support for a new query type."
        ));
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
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 200);
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 1000);
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        return config;
    }
}
