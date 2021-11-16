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
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.InteractiveQueryRequest;
import org.apache.kafka.streams.query.InteractiveQueryResult;
import org.apache.kafka.streams.query.Iterators;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.RawKeyQuery;
import org.apache.kafka.streams.query.RawScanQuery;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.query.InteractiveQueryRequest.inStore;
import static org.apache.kafka.streams.query.PositionBound.at;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

@Category({IntegrationTest.class})
public class IQv2IntegrationTest {

    private static final int NUM_BROKERS = 1;
    private static int port = 0;
    private static final String INPUT_TOPIC_NAME = "input-topic";
    private static final String INPUT2_TOPIC_NAME = "input2-topic";
    private static final String UNCACHED_TABLE = "uncached-table";
    private static final String UNCACHED_COUNTS_TABLE = "uncached-counts-table";
    private static final String CACHED_TABLE = "cached-table";

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    @Rule
    public TestName testName = new TestName();
    private static KafkaStreams kafkaStreams;

    @BeforeClass
    public static void before() throws InterruptedException, IOException {
        CLUSTER.start();
        CLUSTER.createTopic(INPUT_TOPIC_NAME, 2, 1);
        CLUSTER.createTopic(INPUT2_TOPIC_NAME, 2, 1);

        final Semaphore semaphore = new Semaphore(0);

        final StreamsBuilder builder = new StreamsBuilder();
        builder
            .table(
                INPUT_TOPIC_NAME,
                Consumed.with(Serdes.Integer(), Serdes.Integer()),
                Materialized.<Integer, Integer, KeyValueStore<Bytes, byte[]>>as(UNCACHED_TABLE)
                    .withCachingDisabled()
            )
            .filter(
                (a, b) -> true,
                Materialized.as(CACHED_TABLE)
            )
            .toStream()
            .peek((k, v) -> semaphore.release());

        builder
            .stream(
                INPUT2_TOPIC_NAME,
                Consumed.with(Serdes.Integer(), Serdes.Integer())
            )
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeAndGrace(
                Duration.ofMillis(100),
                Duration.ZERO
            ))
            .count(
                Materialized
                    .<Integer, Long, WindowStore<Bytes, byte[]>>as(UNCACHED_COUNTS_TABLE)
                    .withCachingDisabled()
            )
            .toStream()
            .peek((k, v) -> semaphore.release());

        kafkaStreams =
            IntegrationTestUtils.getRunningStreams(streamsConfiguration(), builder, true);

        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);

        IntegrationTestUtils.produceKeyValuesSynchronously(
            INPUT_TOPIC_NAME,
            Arrays.asList(new KeyValue<>(1, 1), new KeyValue<>(2, 2), new KeyValue<>(3, 3)),
            producerProps,
            Time.SYSTEM
        );
        // Assert that all messages in the first batch were processed in a timely manner
        assertThat(semaphore.tryAcquire(3, 60, TimeUnit.SECONDS), is(equalTo(true)));

        IntegrationTestUtils.produceSynchronously(
            producerProps,
            false,
            INPUT2_TOPIC_NAME,
            Optional.empty(),
            Arrays.asList(
                new KeyValueTimestamp<>(1, 1, 0),
                new KeyValueTimestamp<>(1, 1, 10)
            )
        );

        // Assert that we processed the second batch (should see both updates, since caching is disabled)
        assertThat(semaphore.tryAcquire(2, 60, TimeUnit.SECONDS), is(equalTo(true)));


    }

    @AfterClass
    public static void after() {
        kafkaStreams.close(Duration.of(1, ChronoUnit.MINUTES));
        kafkaStreams.cleanUp();
        CLUSTER.stop();
    }

    @Test
    public void shouldQueryKeyFromCachedTable() {

        final StateSerdes<Integer, ValueAndTimestamp<Integer>> serdes =
            kafkaStreams.serdesForStore(CACHED_TABLE);

        final byte[] rawKey = serdes.rawKey(1);
        final InteractiveQueryResult<byte[]> result = kafkaStreams.query(
            inStore(CACHED_TABLE).withQuery(RawKeyQuery.withKey(rawKey)));

        System.out.println("|||" + result);
        final QueryResult<byte[]> rawValueResult = result.getPartitionResults().get(0);
        final ValueAndTimestamp<Integer> value =
            serdes.valueFrom(rawValueResult.getResult());
        System.out.println("|||" + value);

        assertThat(value.value(), is(1));
        assertThat(result.getPosition(),
            is(Position.fromMap(
                mkMap(mkEntry("input-topic", mkMap(mkEntry(0, 0L), mkEntry(1, 1L)))))));
    }

    @Test
    public void shouldQueryKeyFromUncachedTable() {

        final StateSerdes<Integer, ValueAndTimestamp<Integer>> serdes =
            kafkaStreams.serdesForStore(UNCACHED_TABLE);

        final byte[] rawKey = serdes.rawKey(1);
        final InteractiveQueryResult<byte[]> result = kafkaStreams.query(
            inStore(UNCACHED_TABLE).withQuery(RawKeyQuery.withKey(rawKey)));

        System.out.println("|||" + result);
        final QueryResult<byte[]> rawValueResult = result.getPartitionResults().get(0);
        final ValueAndTimestamp<Integer> value =
            serdes.valueFrom(rawValueResult.getResult());
        System.out.println("|||" + value);

        assertThat(value.value(), is(1));
        assertThat(result.getPosition(),
            is(Position.fromMap(
                mkMap(mkEntry("input-topic", mkMap(mkEntry(0, 0L), mkEntry(1, 1L)))))));
    }

    @Test
    public void shouldQueryTypedKeyFromUncachedTable() {
        final Integer key = 1;

        final InteractiveQueryRequest<ValueAndTimestamp<Integer>> query =
            inStore(UNCACHED_TABLE).withQuery(KeyQuery.withKey(key));

        final InteractiveQueryResult<ValueAndTimestamp<Integer>> result = kafkaStreams.query(query);

        final ValueAndTimestamp<Integer> value = result.getOnlyPartitionResult().getResult();

        assertThat(value.value(), is(1));
        assertThat(result.getPosition(),
            is(Position.fromMap(
                mkMap(mkEntry("input-topic", mkMap(mkEntry(0, 0L), mkEntry(1, 1L)))))));
    }

    @Test
    public void exampleKeyQueryIntoWindowStore() {
        final Windowed<Integer> key = new Windowed<>(1, new TimeWindow(0L, 99L));

        final InteractiveQueryRequest<ValueAndTimestamp<Long>> query =
            inStore(UNCACHED_COUNTS_TABLE).withQuery(KeyQuery.withKey(key));

        final InteractiveQueryResult<ValueAndTimestamp<Long>> result = kafkaStreams.query(query);

        final ValueAndTimestamp<Long> value = result.getOnlyPartitionResult().getResult();

        assertThat(value.value(), is(2L));
    }

    @Test
    public void shouldScanUncachedTablePartitions() {

        final StateSerdes<Integer, ValueAndTimestamp<Integer>> serdes =
            kafkaStreams.serdesForStore(UNCACHED_TABLE);

        final InteractiveQueryResult<KeyValueIterator<Bytes, byte[]>> scanResult =
            kafkaStreams.query(inStore(UNCACHED_TABLE).withQuery(RawScanQuery.scan()));

        System.out.println("|||" + scanResult);
        final Map<Integer, QueryResult<KeyValueIterator<Bytes, byte[]>>> partitionResults =
            scanResult.getPartitionResults();
        for (final Entry<Integer, QueryResult<KeyValueIterator<Bytes, byte[]>>> entry : partitionResults.entrySet()) {
            try (final KeyValueIterator<Bytes, byte[]> keyValueIterator =
                entry.getValue().getResult()) {
                while (keyValueIterator.hasNext()) {
                    final KeyValue<Bytes, byte[]> next = keyValueIterator.next();
                    System.out.println(
                        "|||" + entry.getKey() +
                            " " + serdes.keyFrom(next.key.get()) +
                            " " + serdes.valueFrom(next.value)
                    );
                }
            }
        }

        assertThat(scanResult.getPosition(),
            is(Position.fromMap(
                mkMap(mkEntry("input-topic", mkMap(mkEntry(0, 0L), mkEntry(1, 1L)))))));
    }

    @Test
    public void shouldScanUncachedTableCollated() {

        final StateSerdes<Integer, ValueAndTimestamp<Integer>> serdes =
            kafkaStreams.serdesForStore(UNCACHED_TABLE);

        final InteractiveQueryResult<KeyValueIterator<Bytes, byte[]>> scanResult =
            kafkaStreams.query(inStore(UNCACHED_TABLE).withQuery(RawScanQuery.scan()));

        System.out.println("|||" + scanResult);
        final Map<Integer, QueryResult<KeyValueIterator<Bytes, byte[]>>> partitionResults = scanResult.getPartitionResults();

        final List<KeyValueIterator<Bytes, byte[]>> collect =
            partitionResults
                .values()
                .stream()
                .map(QueryResult::getResult)
                .collect(Collectors.toList());
        try (final CloseableIterator<KeyValue<Bytes, byte[]>> collate = Iterators.collate(
            collect)) {
            while (collate.hasNext()) {
                final KeyValue<Bytes, byte[]> next = collate.next();
                System.out.println(
                    "|||" +
                        " " + serdes.keyFrom(next.key.get()) +
                        " " + serdes.valueFrom(next.value)
                );
            }
        }

        assertThat(scanResult.getPosition(),
            is(Position.fromMap(
                mkMap(mkEntry("input-topic", mkMap(mkEntry(0, 0L), mkEntry(1, 1L)))))));
    }

    @Test
    public void shouldQueryWithinBound() {

        final StateSerdes<Integer, ValueAndTimestamp<Integer>> serdes =
            kafkaStreams.serdesForStore(UNCACHED_TABLE);

        final byte[] rawKey = serdes.rawKey(1);
        final InteractiveQueryResult<byte[]> result = kafkaStreams.query(
            inStore(UNCACHED_TABLE)
                .withQuery(RawKeyQuery.withKey(rawKey))
                .withPositionBound(
                    at(
                        Position
                            .emptyPosition()
                            .withComponent(INPUT_TOPIC_NAME, 0, 0L)
                            .withComponent(INPUT_TOPIC_NAME, 1, 1L)
                    )
                )
        );

        System.out.println("|||" + result);
        final QueryResult<byte[]> rawValueResult = result.getPartitionResults().get(0);
        final ValueAndTimestamp<Integer> value =
            serdes.valueFrom(rawValueResult.getResult());
        System.out.println("|||" + value);
        assertThat(result.getPosition(),
            is(Position.fromMap(
                mkMap(mkEntry("input-topic", mkMap(mkEntry(0, 0L), mkEntry(1, 1L)))))));
    }

    @Test
    public void shouldFailQueryOutsideOfBound() {

        final StateSerdes<Integer, ValueAndTimestamp<Integer>> serdes =
            kafkaStreams.serdesForStore(UNCACHED_TABLE);

        final byte[] rawKey = serdes.rawKey(1);
        // intentionally setting the bound higher than the current position.
        final InteractiveQueryResult<byte[]> result = kafkaStreams.query(
            inStore(UNCACHED_TABLE)
                .withQuery(RawKeyQuery.withKey(rawKey))
                .withPositionBound(
                    at(
                        Position
                            .emptyPosition()
                            .withComponent(INPUT_TOPIC_NAME, 0, 1L)
                            .withComponent(INPUT_TOPIC_NAME, 1, 2L)
                    )
                )
        );

        System.out.println("|||" + result);
        final QueryResult<byte[]> rawValueResult = result.getPartitionResults().get(0);

        final RuntimeException runtimeException = assertThrows(
            RuntimeException.class,
            rawValueResult::getResult
        );
        assertThat(
            runtimeException.getMessage(),
            is("NOT_UP_TO_BOUND: For store partition 0, the current position Position{position={input-topic={0=0}}} is not yet up to the bound PositionBound{position=Position{position={input-topic={0=1, 1=2}}}}"));

        assertThat(rawValueResult.isFailure(), is(true));
        assertThat(rawValueResult.getFailureReason(), is(FailureReason.NOT_UP_TO_BOUND));
        assertThat(rawValueResult.getFailure(),
            is("For store partition 0, the current position Position{position={input-topic={0=0}}} is not yet up to the bound PositionBound{position=Position{position={input-topic={0=1, 1=2}}}}"));
        assertThat(result.getPosition(), is(Position.emptyPosition()));
    }


    @Test
    public void shouldPartiallySucceedOnPartialBound() {

        final StateSerdes<Integer, ValueAndTimestamp<Integer>> serdes =
            kafkaStreams.serdesForStore(UNCACHED_TABLE);

        final InteractiveQueryResult<KeyValueIterator<Bytes, byte[]>> scanResult =
            kafkaStreams.query(
                inStore(UNCACHED_TABLE)
                    .withQuery(RawScanQuery.scan())
                    .withPositionBound(
                        at(
                            Position
                                .emptyPosition()
                                .withComponent(INPUT_TOPIC_NAME, 0, 0L)
                                .withComponent(INPUT_TOPIC_NAME, 1, 2L)
                        )
                    )
            );

        System.out.println("|||" + scanResult);
        final Map<Integer, QueryResult<KeyValueIterator<Bytes, byte[]>>> partitionResults = scanResult.getPartitionResults();
        for (final Entry<Integer, QueryResult<KeyValueIterator<Bytes, byte[]>>> entry : partitionResults.entrySet()) {
            final QueryResult<KeyValueIterator<Bytes, byte[]>> value = entry.getValue();
            if (value.isSuccess()) {
                try (final KeyValueIterator<Bytes, byte[]> keyValueIterator =
                    value.getResult()) {
                    while (keyValueIterator.hasNext()) {
                        final KeyValue<Bytes, byte[]> next = keyValueIterator.next();
                        System.out.println(
                            "|||" + entry.getKey() +
                                " " + serdes.keyFrom(next.key.get()) +
                                " " + serdes.valueFrom(next.value)
                        );
                    }
                }
            }
        }

        assertThat(scanResult.getPartitionResults().get(0).isSuccess(), is(true));
        assertThat(scanResult.getPartitionResults().get(1).isFailure(), is(true));
        assertThat(scanResult.getPartitionResults().get(1).getFailureReason(),
            is(FailureReason.NOT_UP_TO_BOUND));
        assertThat(scanResult.getPosition(),
            is(Position.fromMap(mkMap(mkEntry("input-topic", mkMap(mkEntry(0, 0L)))))));
    }

    private static Properties streamsConfiguration() {
        final String safeTestName = IQv2IntegrationTest.class.getName();
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
        return config;
    }
}
