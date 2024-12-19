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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.query.MultiVersionedKeyQuery;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.ResultOrder;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.query.VersionedKeyQuery;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.VersionedRecord;
import org.apache.kafka.streams.state.VersionedRecordIterator;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoField;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("integration")
public class IQv2VersionedStoreIntegrationTest {
    private static final int NUM_BROKERS = 1;
    private static final String INPUT_TOPIC_NAME = "input-topic";
    private static final String STORE_NAME = "versioned-store";
    private static final Duration HISTORY_RETENTION = Duration.ofDays(1);
    private static final Duration SEGMENT_INTERVAL = Duration.ofHours(1);

    private static final int RECORD_KEY = 2;
    private static final int NON_EXISTING_KEY = 3;

    private static final Instant BASE_TIMESTAMP = Instant.parse("2023-01-01T10:00:00.00Z");
    private static final Long BASE_TIMESTAMP_LONG = BASE_TIMESTAMP.getLong(ChronoField.INSTANT_SECONDS);
    private static final Integer[] RECORD_VALUES = {2, 20, 200, 2000};
    private static final Long[] RECORD_TIMESTAMPS = {BASE_TIMESTAMP_LONG, BASE_TIMESTAMP_LONG + 10, BASE_TIMESTAMP_LONG + 20, BASE_TIMESTAMP_LONG + 30};
    private static final int RECORD_NUMBER = RECORD_VALUES.length;
    private static final int LAST_INDEX = RECORD_NUMBER - 1;
    private static final Position INPUT_POSITION = Position.emptyPosition();

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS, Utils.mkProperties(Map.of("auto.create.topics.enable", "true",
             GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, "classic,consumer,streams",
            ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG, "true")));

    private KafkaStreams kafkaStreams;

    @BeforeAll
    public static void before() throws Exception {
        CLUSTER.start();

        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        try (final KafkaProducer<Integer, Integer> producer = new KafkaProducer<>(producerProps)) {
            producer.send(new ProducerRecord<>(INPUT_TOPIC_NAME, 0,  RECORD_TIMESTAMPS[0], RECORD_KEY, RECORD_VALUES[0])).get();
            producer.send(new ProducerRecord<>(INPUT_TOPIC_NAME, 0,  RECORD_TIMESTAMPS[1], RECORD_KEY, RECORD_VALUES[1])).get();
            producer.send(new ProducerRecord<>(INPUT_TOPIC_NAME, 0,  RECORD_TIMESTAMPS[2], RECORD_KEY, RECORD_VALUES[2])).get();
            producer.send(new ProducerRecord<>(INPUT_TOPIC_NAME, 0,  RECORD_TIMESTAMPS[3], RECORD_KEY, RECORD_VALUES[3])).get();
        }
        INPUT_POSITION.withComponent(INPUT_TOPIC_NAME, 0, 3);
    }

    @BeforeEach
    public void beforeTest() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.table(INPUT_TOPIC_NAME,
            Materialized.as(Stores.persistentVersionedKeyValueStore(STORE_NAME, HISTORY_RETENTION, SEGMENT_INTERVAL)));
        final Properties configs = new Properties();
        configs.put(StreamsConfig.APPLICATION_ID_CONFIG, "app");
        configs.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:7070");
        configs.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        configs.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class.getName());
        configs.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class.getName());
        kafkaStreams = IntegrationTestUtils.getStartedStreams(configs, builder, true);
    }

    @AfterEach
    public void afterTest() {
        if (kafkaStreams != null) {
            kafkaStreams.close(Duration.ofSeconds(60));
            kafkaStreams.cleanUp();
        }
    }

    @AfterAll
    public static void after() {
        CLUSTER.stop();
    }

    @Test
    public void verifyStore() {
        /* Test Versioned Key Queries */
        // retrieve the latest value
        shouldHandleVersionedKeyQuery(Optional.empty(), RECORD_VALUES[3], RECORD_TIMESTAMPS[3], Optional.empty());
        shouldHandleVersionedKeyQuery(Optional.of(Instant.now()), RECORD_VALUES[3], RECORD_TIMESTAMPS[3], Optional.empty());
        shouldHandleVersionedKeyQuery(Optional.of(Instant.ofEpochMilli(RECORD_TIMESTAMPS[3])), RECORD_VALUES[3], RECORD_TIMESTAMPS[3], Optional.empty());
        // retrieve the old value
        shouldHandleVersionedKeyQuery(Optional.of(Instant.ofEpochMilli(RECORD_TIMESTAMPS[0])), RECORD_VALUES[0], RECORD_TIMESTAMPS[0], Optional.of(RECORD_TIMESTAMPS[1]));
        // there is no record for the provided timestamp
        shouldVerifyGetNullForVersionedKeyQuery(RECORD_KEY, Instant.ofEpochMilli(RECORD_TIMESTAMPS[0] - 50));
        // there is no record with this key
        shouldVerifyGetNullForVersionedKeyQuery(NON_EXISTING_KEY, Instant.now());

        /* Test Multi Versioned Key Queries */
        // retrieve all existing values
        shouldHandleMultiVersionedKeyQuery(Optional.empty(), Optional.empty(), ResultOrder.ANY, 0, LAST_INDEX);
        // retrieve all existing values in ascending order
        shouldHandleMultiVersionedKeyQuery(Optional.empty(), Optional.empty(), ResultOrder.ASCENDING, 0, LAST_INDEX);
        // retrieve existing values in query defined time range
        shouldHandleMultiVersionedKeyQuery(Optional.of(Instant.ofEpochMilli(RECORD_TIMESTAMPS[1] + 5)), Optional.of(Instant.now()),
                                           ResultOrder.ANY, 1, LAST_INDEX);
        // there is no record in the query specified time range
        shouldVerifyGetNullForMultiVersionedKeyQuery(RECORD_KEY,
                                                     Optional.of(Instant.ofEpochMilli(RECORD_TIMESTAMPS[0] - 100)), Optional.of(Instant.ofEpochMilli(RECORD_TIMESTAMPS[0] - 50)),
                                                     ResultOrder.ANY);
        // there is no record in the query specified time range even retrieving results in ascending order
        shouldVerifyGetNullForMultiVersionedKeyQuery(RECORD_KEY,
                                                     Optional.of(Instant.ofEpochMilli(RECORD_TIMESTAMPS[0] - 100)), Optional.of(Instant.ofEpochMilli(RECORD_TIMESTAMPS[0] - 50)),
                                                     ResultOrder.ASCENDING);
        // there is no record with this key
        shouldVerifyGetNullForMultiVersionedKeyQuery(NON_EXISTING_KEY, Optional.empty(), Optional.empty(), ResultOrder.ANY);
        // there is no record with this key even retrieving results in ascending order
        shouldVerifyGetNullForMultiVersionedKeyQuery(NON_EXISTING_KEY, Optional.empty(), Optional.empty(), ResultOrder.ASCENDING);
        // test concurrent write while retrieving records
        shouldHandleRaceCondition();
    }

    private void shouldHandleVersionedKeyQuery(final Optional<Instant> queryTimestamp,
                                               final Integer expectedValue,
                                               final Long expectedTimestamp,
                                               final Optional<Long> expectedValidToTime) {

        final VersionedKeyQuery<Integer, Integer> query = defineQuery(RECORD_KEY, queryTimestamp);

        final QueryResult<VersionedRecord<Integer>> queryResult = sendRequestAndReceiveResults(query, kafkaStreams);

        // verify results
        if (queryResult == null) {
            throw new AssertionError("The query returned null.");
        }
        if (queryResult.isFailure()) {
            throw new AssertionError(queryResult.toString());
        }
        if (queryResult.getResult() == null) {
            throw new AssertionError("The query returned null.");
        }

        assertThat(queryResult.isSuccess(), is(true));
        final VersionedRecord<Integer> result1 = queryResult.getResult();
        assertThat(result1.value(), is(expectedValue));
        assertThat(result1.timestamp(), is(expectedTimestamp));
        assertThat(result1.validTo(), is(expectedValidToTime));
        assertThat(queryResult.getExecutionInfo(), is(empty()));
    }

    private void shouldVerifyGetNullForVersionedKeyQuery(final Integer key, final Instant queryTimestamp) {
        final VersionedKeyQuery<Integer, Integer> query = defineQuery(key, Optional.of(queryTimestamp));
        assertThat(sendRequestAndReceiveResults(query, kafkaStreams), nullValue());
    }

    private void shouldHandleMultiVersionedKeyQuery(final Optional<Instant> fromTime, final Optional<Instant> toTime,
                                                    final ResultOrder order, final int expectedArrayLowerBound, final int expectedArrayUpperBound) {

        final MultiVersionedKeyQuery<Integer, Integer> query = defineQuery(RECORD_KEY, fromTime, toTime, order);

        final Map<Integer, QueryResult<VersionedRecordIterator<Integer>>> partitionResults = sendRequestAndReceiveResults(query, kafkaStreams);

        // verify results
        for (final Entry<Integer, QueryResult<VersionedRecordIterator<Integer>>> partitionResultsEntry : partitionResults.entrySet()) {
            verifyPartitionResult(partitionResultsEntry.getValue());
            try (final VersionedRecordIterator<Integer> iterator = partitionResultsEntry.getValue().getResult()) {
                int i = order.equals(ResultOrder.ASCENDING) ? 0 : expectedArrayUpperBound;
                int iteratorSize = 0;
                while (iterator.hasNext()) {
                    final VersionedRecord<Integer> record = iterator.next();
                    final Long timestamp = record.timestamp();
                    final Optional<Long> validTo = record.validTo();
                    final Integer value = record.value();

                    final Optional<Long> expectedValidTo = i < expectedArrayUpperBound ? Optional.of(RECORD_TIMESTAMPS[i + 1]) : Optional.empty();
                    assertThat(value, is(RECORD_VALUES[i]));
                    assertThat(timestamp, is(RECORD_TIMESTAMPS[i]));
                    assertThat(validTo, is(expectedValidTo));
                    i = order.equals(ResultOrder.ASCENDING) ? i + 1 : i - 1;
                    iteratorSize++;
                }
                // The number of returned records by query is equal to expected number of records
                assertThat(iteratorSize, equalTo(expectedArrayUpperBound - expectedArrayLowerBound + 1));
            }
        }
    }

    private void shouldVerifyGetNullForMultiVersionedKeyQuery(final Integer key, final Optional<Instant> fromTime, final Optional<Instant> toTime, final ResultOrder order) {
        final MultiVersionedKeyQuery<Integer, Integer> query = defineQuery(key, fromTime, toTime, order);

        final Map<Integer, QueryResult<VersionedRecordIterator<Integer>>> partitionResults = sendRequestAndReceiveResults(query, kafkaStreams);

        // verify results
        for (final Entry<Integer, QueryResult<VersionedRecordIterator<Integer>>> partitionResultsEntry : partitionResults.entrySet()) {
            try (final VersionedRecordIterator<Integer> iterator = partitionResultsEntry.getValue().getResult()) {
                assertFalse(iterator.hasNext());
            }
        }
    }

    /**
     * This method updates a record value in an existing timestamp, while it is retrieving records.
     * Since IQv2 guarantees snapshot semantics, we expect that the old value is retrieved.
     */
    private void shouldHandleRaceCondition() {
        final MultiVersionedKeyQuery<Integer, Integer> query = defineQuery(RECORD_KEY, Optional.empty(), Optional.empty(), ResultOrder.ANY);

        final Map<Integer, QueryResult<VersionedRecordIterator<Integer>>> partitionResults = sendRequestAndReceiveResults(query, kafkaStreams);

        // verify results in two steps
        for (final Entry<Integer, QueryResult<VersionedRecordIterator<Integer>>> partitionResultsEntry : partitionResults.entrySet()) {
            try (final VersionedRecordIterator<Integer> iterator = partitionResultsEntry.getValue().getResult()) {
                int i = LAST_INDEX;
                int iteratorSize = 0;

                // step 1:
                while (iterator.hasNext()) {
                    final VersionedRecord<Integer> record = iterator.next();
                    final Long timestamp = record.timestamp();
                    final Optional<Long> validTo = record.validTo();
                    final Integer value = record.value();

                    final Optional<Long> expectedValidTo = i < LAST_INDEX ? Optional.of(RECORD_TIMESTAMPS[i + 1]) : Optional.empty();
                    assertThat(value, is(RECORD_VALUES[i]));
                    assertThat(timestamp, is(RECORD_TIMESTAMPS[i]));
                    assertThat(validTo, is(expectedValidTo));
                    i--;
                    iteratorSize++;
                    if (i == 2) {
                        break;
                    }
                }

                // update the value of the oldest record
                updateRecordValue();

                // step 2: continue reading records from through the already opened iterator
                while (iterator.hasNext()) {
                    final VersionedRecord<Integer> record = iterator.next();
                    final Long timestamp = record.timestamp();
                    final Optional<Long> validTo = record.validTo();
                    final Integer value = record.value();

                    final Optional<Long> expectedValidTo = Optional.of(RECORD_TIMESTAMPS[i + 1]);
                    assertThat(value, is(RECORD_VALUES[i]));
                    assertThat(timestamp, is(RECORD_TIMESTAMPS[i]));
                    assertThat(validTo, is(expectedValidTo));
                    i--;
                    iteratorSize++;
                }

                // The number of returned records by query is equal to expected number of records
                assertThat(iteratorSize, equalTo(RECORD_NUMBER));
            }
        }
    }

    private static VersionedKeyQuery<Integer, Integer> defineQuery(final Integer key, final Optional<Instant> queryTimestamp) {
        VersionedKeyQuery<Integer, Integer> query = VersionedKeyQuery.withKey(key);
        if (queryTimestamp.isPresent()) {
            query = query.asOf(queryTimestamp.get());
        }
        return query;
    }

    private static MultiVersionedKeyQuery<Integer, Integer> defineQuery(final Integer key, final Optional<Instant> fromTime, final Optional<Instant> toTime, final ResultOrder order) {
        MultiVersionedKeyQuery<Integer, Integer> query = MultiVersionedKeyQuery.withKey(key);
        if (fromTime.isPresent()) {
            query = query.fromTime(fromTime.get());
        }
        if (toTime.isPresent()) {
            query = query.toTime(toTime.get());
        }
        if (order.equals(ResultOrder.ASCENDING)) {
            query = query.withAscendingTimestamps();
        }
        return query;
    }

    private static Map<Integer, QueryResult<VersionedRecordIterator<Integer>>> sendRequestAndReceiveResults(final MultiVersionedKeyQuery<Integer, Integer> query, final KafkaStreams kafkaStreams) {
        final StateQueryRequest<VersionedRecordIterator<Integer>> request = StateQueryRequest.inStore(STORE_NAME).withQuery(query).withPositionBound(PositionBound.at(INPUT_POSITION));
        final StateQueryResult<VersionedRecordIterator<Integer>> result = IntegrationTestUtils.iqv2WaitForResult(kafkaStreams, request);
        return result.getPartitionResults();
    }

    private static QueryResult<VersionedRecord<Integer>> sendRequestAndReceiveResults(final VersionedKeyQuery<Integer, Integer> query, final KafkaStreams kafkaStreams) {
        final StateQueryRequest<VersionedRecord<Integer>> request = StateQueryRequest.inStore(STORE_NAME).withQuery(query).withPositionBound(PositionBound.at(INPUT_POSITION));
        final StateQueryResult<VersionedRecord<Integer>> result = IntegrationTestUtils.iqv2WaitForResult(kafkaStreams, request);
        return result.getOnlyPartitionResult();
    }

    private static void verifyPartitionResult(final QueryResult<VersionedRecordIterator<Integer>> result) {
        assertThat(result.getExecutionInfo(), is(empty()));
        if (result.isFailure()) {
            throw new AssertionError(result.toString());
        }
        assertThat(result.isSuccess(), is(true));
        assertThrows(IllegalArgumentException.class, result::getFailureReason);
        assertThrows(IllegalArgumentException.class, result::getFailureMessage);
    }

    /**
     * This method inserts a new value (999999) for the key in the oldest timestamp (RECORD_TIMESTAMPS[0]).
     */
    private static void updateRecordValue() {
        // update the record value at RECORD_TIMESTAMPS[0]
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        try (final KafkaProducer<Integer, Integer> producer = new KafkaProducer<>(producerProps)) {
            producer.send(new ProducerRecord<>(INPUT_TOPIC_NAME, 0, RECORD_TIMESTAMPS[0], RECORD_KEY, 999999));
        }
        INPUT_POSITION.withComponent(INPUT_TOPIC_NAME, 0, 4);
        assertThat(INPUT_POSITION, equalTo(Position.emptyPosition().withComponent(INPUT_TOPIC_NAME, 0, 4)));

        // make sure that the new value is picked up by the store
        final Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "foo");
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try {
            IntegrationTestUtils.waitUntilMinRecordsReceived(consumerProps, INPUT_TOPIC_NAME, RECORD_NUMBER + 1);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }
}