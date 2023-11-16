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

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoField;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
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
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.query.VersionedKeyQuery;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.VersionedRecordIterator;
import org.apache.kafka.streams.state.VersionedRecord;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({IntegrationTest.class})
public class IQv2VersionedStoreIntegrationTest {
    private static final int NUM_BROKERS = 1;
    private static final String INPUT_TOPIC_NAME = "input-topic";
    private static final String STORE_NAME = "versioned-store";
    private static final Duration HISTORY_RETENTION = Duration.ofDays(1);
    private static final Instant BASE_TIMESTAMP = Instant.parse("2023-01-01T10:00:00.00Z");
//    private static final Long RECORD_TIMESTAMP_OLD = BASE_TIMESTAMP.toEpochMilli();
//    private static final Long RECORD_TIMESTAMP_NEW = RECORD_TIMESTAMP_OLD + 100;
    private static final int RECORD_KEY = 2;
//    private static final int RECORD_VALUE_OLD = 2;
//    private static final int RECORD_VALUE_NEW = 3;

    private static final Instant RECORD_TIMESTAMP = Instant.parse("2023-01-01T10:00:00.00Z");
    private static final Long RECORD_TIMESTAMP_LONG = RECORD_TIMESTAMP.getLong(ChronoField.INSTANT_SECONDS);
    private static final Integer[] RECORD_VALUES = {2, 20, 200, 2000};
    private static final Long[] RECORD_TIMESTAMPS = {RECORD_TIMESTAMP_LONG, RECORD_TIMESTAMP_LONG + 10, RECORD_TIMESTAMP_LONG + 20, RECORD_TIMESTAMP_LONG + 30};
    private static final int RECORD_NUMBER = RECORD_VALUES.length;





  private static final Position INPUT_POSITION = Position.emptyPosition();

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS,
        Utils.mkProperties(Collections.singletonMap("auto.create.topics.enable", "true")));
    private KafkaStreams kafkaStreams;

    @BeforeClass
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

    @Before
    public void beforeTest() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.table(INPUT_TOPIC_NAME,
            Materialized.as(Stores.persistentVersionedKeyValueStore(STORE_NAME, HISTORY_RETENTION)));
        final Properties configs = new Properties();
        configs.put(StreamsConfig.APPLICATION_ID_CONFIG, "app");
        configs.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        configs.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class.getName());
        configs.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class.getName());
        kafkaStreams = IntegrationTestUtils.getStartedStreams(configs, builder, true);
    }

    @After
    public void afterTest() {
        if (kafkaStreams != null) {
            kafkaStreams.close();
            kafkaStreams.cleanUp();
        }
    }

    @AfterClass
    public static void after() {
        CLUSTER.stop();
    }

    @Test
    public void verifyStore() {
        /* Test Versioned Key Queries */

        // retrieve the latest value
        shouldHandleVersionedKeyQuery(RECORD_KEY, Optional.empty(), RECORD_VALUES[3], RECORD_TIMESTAMPS[3]);
        shouldHandleVersionedKeyQuery(RECORD_KEY, Optional.of(Instant.now()), RECORD_VALUES[3], RECORD_TIMESTAMPS[3]);
        shouldHandleVersionedKeyQuery(RECORD_KEY, Optional.of(Instant.ofEpochMilli(RECORD_TIMESTAMPS[3])), RECORD_VALUES[3], RECORD_TIMESTAMPS[3]);
        // retrieve the old value
        shouldHandleVersionedKeyQuery(RECORD_KEY, Optional.of(Instant.ofEpochMilli(RECORD_TIMESTAMPS[0])), RECORD_VALUES[0], RECORD_TIMESTAMPS[0]);
        // there is no record for the provided timestamp
        shouldVerifyGetNullforVersionedKeyQuery(RECORD_KEY, Instant.ofEpochMilli(RECORD_TIMESTAMPS[0] - 50));
        // there is no record with this key
        shouldVerifyGetNullforVersionedKeyQuery(3, Instant.now());

        /* Test Multi Versioned Key Queries */
        shouldHandleMultiVersionedKeyQuery(2);
        shouldHandleMultiVersionedKeyQueryOrderedDescendingly(2);
        shouldHandleMultiVersionedKeyQueryWithTS(2,  Instant.ofEpochMilli(RECORD_TIMESTAMPS[1] + 5), Instant.now());
    }

    private void shouldHandleVersionedKeyQuery(final Integer key,
                                               final Optional<Instant> queryTimestamp,
                                               final Integer expectedValue,
                                               final Long expectedTimestamp) {

        VersionedKeyQuery<Integer, Integer> query = VersionedKeyQuery.withKey(key);
        if (queryTimestamp.isPresent()) {
            query = query.asOf(queryTimestamp.get());
        }
        final StateQueryRequest<VersionedRecord<Integer>> request = StateQueryRequest.inStore(STORE_NAME).withQuery(query).withPositionBound(PositionBound.at(INPUT_POSITION));
        final StateQueryResult<VersionedRecord<Integer>> result = IntegrationTestUtils.iqv2WaitForResult(kafkaStreams, request);

        if (result.getOnlyPartitionResult() == null) {
            throw new AssertionError("The query returned null.");
        }
        final QueryResult<VersionedRecord<Integer>> queryResult = result.getOnlyPartitionResult();
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
        assertThat(queryResult.getExecutionInfo(), is(empty()));
    }

    private void shouldVerifyGetNullforVersionedKeyQuery(final Integer key, final Instant queryTimestamp) {
        VersionedKeyQuery<Integer, Integer> query = VersionedKeyQuery.withKey(key);
        query = query.asOf(queryTimestamp);
        final StateQueryRequest<VersionedRecord<Integer>> request = StateQueryRequest.inStore(STORE_NAME).withQuery(query).withPositionBound(PositionBound.at(INPUT_POSITION));
        final StateQueryResult<VersionedRecord<Integer>> result = IntegrationTestUtils.iqv2WaitForResult(kafkaStreams, request);
        assertThat(result.getOnlyPartitionResult(), nullValue());
    }

    public void shouldHandleMultiVersionedKeyQuery(final Integer key) {
      final MultiVersionedKeyQuery<Integer, Integer> query = MultiVersionedKeyQuery.withKey(key);
      handleMultiVersionedKeyQuery(query, 0, RECORD_NUMBER - 1, true);
    }

    public void shouldHandleMultiVersionedKeyQueryOrderedDescendingly(final Integer key) {
      MultiVersionedKeyQuery<Integer, Integer> query = MultiVersionedKeyQuery.withKey(key);
      query = query.withDescendingTimestamps();
      handleMultiVersionedKeyQuery(query, 0, RECORD_NUMBER - 1, false);
    }

    public void shouldHandleMultiVersionedKeyQueryWithTS(final Integer key, final Instant fromTime, final Instant toTime) {
      MultiVersionedKeyQuery<Integer, Integer> query = MultiVersionedKeyQuery.withKey(key);
      query = query.fromTime(fromTime).toTime(toTime);
      handleMultiVersionedKeyQuery(query, 1, RECORD_NUMBER - 1, true);
    }

    private void handleMultiVersionedKeyQuery(final MultiVersionedKeyQuery<Integer, Integer> query,
        final int arrayLowerBound,
        final int arrayUpperBound,
        final boolean ascending) {

      final StateQueryRequest<VersionedRecordIterator<Integer>> request = StateQueryRequest.inStore(STORE_NAME).withQuery(query);
      final StateQueryResult<VersionedRecordIterator<Integer>> result = kafkaStreams.query(request);
      final QueryResult<VersionedRecordIterator<Integer>> queryResult = result.getOnlyPartitionResult();
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


      final Map<Integer, QueryResult<VersionedRecordIterator<Integer>>> partitionResults = result.getPartitionResults();
      for (final Entry<Integer, QueryResult<VersionedRecordIterator<Integer>>> entry : partitionResults.entrySet()) {
        try (final VersionedRecordIterator<Integer> iterator = entry.getValue().getResult()) {
          int i = ascending ? arrayUpperBound : 0;
          int iteratorSize = 0;
          while (iterator.hasNext()) {
            final VersionedRecord<Integer> record = iterator.next();
            final Long timestamp = record.timestamp();
            final Long validTo = record.validTo();
            final Integer value = record.value();

            final Long expectedValidTo = i < arrayUpperBound ? RECORD_TIMESTAMPS[i + 1] : Long.MAX_VALUE;
            assertThat(value, is(RECORD_VALUES[i]));
            assertThat(timestamp, is(RECORD_TIMESTAMPS[i]));
            assertThat(validTo, is(expectedValidTo));
            assertThat(queryResult.getExecutionInfo(), is(empty()));
            i = ascending ? i - 1 : i + 1;
            iteratorSize++;
          }
          // The number of returned records by query is equal to expected number of records
          assertThat(iteratorSize, equalTo(arrayUpperBound - arrayLowerBound + 1));
        }
      }
    }
}