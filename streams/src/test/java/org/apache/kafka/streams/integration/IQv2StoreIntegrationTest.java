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
import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.RangeQuery;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
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

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;

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

    private static final int NUM_BROKERS = 1;
    public static final Duration WINDOW_SIZE = Duration.ofMinutes(5);
    private static int port = 0;
    private static final String INPUT_TOPIC_NAME = "input-topic";
    private static final Position INPUT_POSITION = Position.emptyPosition();
    private static final String STORE_NAME = "kv-store";

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final StoresToTest storeToTest;

    public static class UnknownQuery implements Query<Void> {

    }

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
                    false);
            }
        },
        ROCKS_WINDOW {
            @Override
            public StoreSupplier<?> supplier() {
                return Stores.persistentWindowStore(STORE_NAME, Duration.ofDays(1), WINDOW_SIZE,
                    false);
            }
        },
        TIME_ROCKS_WINDOW {
            @Override
            public StoreSupplier<?> supplier() {
                return Stores.persistentTimestampedWindowStore(STORE_NAME, Duration.ofDays(1),
                    WINDOW_SIZE, false);
            }
        },
        IN_MEMORY_SESSION {
            @Override
            public StoreSupplier<?> supplier() {
                return Stores.inMemorySessionStore(STORE_NAME, Duration.ofDays(1));
            }
        },
        ROCKS_SESSION {
            @Override
            public StoreSupplier<?> supplier() {
                return Stores.persistentSessionStore(STORE_NAME, Duration.ofDays(1));
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
    }

    @Parameterized.Parameters(name = "cache={0}, log={1}, supplier={2}")
    public static Collection<Object[]> data() {
        final List<Object[]> values = new ArrayList<>();
        for (final boolean cacheEnabled : Arrays.asList(true, false)) {
            for (final boolean logEnabled : Arrays.asList(true, false)) {
                for (final StoresToTest toTest : StoresToTest.values()) {
                    values.add(new Object[]{cacheEnabled, logEnabled, toTest.name()});
                }
            }
        }
        return values;
    }

    public IQv2StoreIntegrationTest(
        final boolean cache,
        final boolean log,
        final String storeToTest) {
        this.cache = cache;
        this.log = log;
        this.storeToTest = StoresToTest.valueOf(storeToTest);
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
                .withComponent(INPUT_TOPIC_NAME, 1, 1L)
        ));
    }

    @Before
    public void beforeTest() {
        final StoreSupplier<?> supplier = storeToTest.supplier();
        final Properties streamsConfig = streamsConfiguration(
            cache,
            log,
            storeToTest.name()
        );

        final StreamsBuilder builder = new StreamsBuilder();
        if (supplier instanceof KeyValueBytesStoreSupplier) {
            final Materialized<Integer, Integer, KeyValueStore<Bytes, byte[]>> materialized =
                Materialized.as((KeyValueBytesStoreSupplier) supplier);

            if (cache) {
                materialized.withCachingEnabled();
            } else {
                materialized.withCachingDisabled();
            }

            if (log) {
                materialized.withLoggingEnabled(Collections.emptyMap());
            } else {
                materialized.withCachingDisabled();
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
        } else if (supplier instanceof WindowBytesStoreSupplier) {
            final Materialized<Integer, Integer, WindowStore<Bytes, byte[]>> materialized =
                Materialized.as((WindowBytesStoreSupplier) supplier);

            if (cache) {
                materialized.withCachingEnabled();
            } else {
                materialized.withCachingDisabled();
            }

            if (log) {
                materialized.withLoggingEnabled(Collections.emptyMap());
            } else {
                materialized.withCachingDisabled();
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
        } else if (supplier instanceof SessionBytesStoreSupplier) {
            final Materialized<Integer, Integer, SessionStore<Bytes, byte[]>> materialized =
                Materialized.as((SessionBytesStoreSupplier) supplier);

            if (cache) {
                materialized.withCachingEnabled();
            } else {
                materialized.withCachingDisabled();
            }

            if (log) {
                materialized.withLoggingEnabled(Collections.emptyMap());
            } else {
                materialized.withCachingDisabled();
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

    @After
    public void afterTest() {
        kafkaStreams.close();
        kafkaStreams.cleanUp();
    }

    @AfterClass
    public static void after() {
        CLUSTER.stop();
    }

    @Test
    public void verifyStore() {
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

    private void globalShouldRejectAllQueries() {
        // See KAFKA-13523

        final KeyQuery<Integer, ValueAndTimestamp<Integer>> query = KeyQuery.withKey(1);
        final StateQueryRequest<ValueAndTimestamp<Integer>> request =
            inStore(STORE_NAME).withQuery(query);

        final StateQueryResult<ValueAndTimestamp<Integer>> result = kafkaStreams.query(request);

        assertThat(result.getGlobalResult().isFailure(), is(true));
        assertThat(result.getGlobalResult().getFailureReason(),
            is(FailureReason.UNKNOWN_QUERY_TYPE));
        assertThat(result.getGlobalResult().getFailureMessage(),
            is("Global stores do not yet support the KafkaStreams#query API."
                + " Use KafkaStreams#store instead."));
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
                assertThat(queryResult.getFailureReason(),
                    is(FailureReason.UNKNOWN_QUERY_TYPE));
                assertThat(queryResult.getFailureMessage(),
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

                final KeyValueIterator<Integer, V> iterator = queryResult.get(partition)
                                                                         .getResult();
                while (iterator.hasNext()) {
                    actualValue.add(valueExtactor.apply(iterator.next().value));
                }
                assertThat(queryResult.get(partition).getExecutionInfo(), is(empty()));
            }
            assertThat(actualValue, is(expectedValue));
        }
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
        final String supplier) {
        final String safeTestName =
            IQv2StoreIntegrationTest.class.getName() + "-" + cache + "-" + log + "-" + supplier;
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