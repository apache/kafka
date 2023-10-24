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
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoField;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
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
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.query.VersionedKeyQuery;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.VersionedRecord;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({IntegrationTest.class})
public class IQv2VersionedStoreIntegrationTest {
    private static final int NUM_BROKERS = 1;
    private static final String INPUT_TOPIC_NAME = "input-topic";
    private static final String STORE_NAME = "versioned-store";
    private static final Duration HISTORY_RETENTION = Duration.ofDays(1);
    private static final Instant RECORD_TIMESTAMP = Instant.now();
    private static final Long RECORD_TIMESTAMP_LONG = RECORD_TIMESTAMP.getLong(ChronoField.INSTANT_SECONDS);


    private static final Logger LOG = LoggerFactory.getLogger(IQv2VersionedStoreIntegrationTest.class);

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS,
        Utils.mkProperties(Collections.singletonMap("auto.create.topics.enable", "true")));
    private StreamsBuilder builder;
    private KafkaStreams kafkaStreams;

    public IQv2VersionedStoreIntegrationTest(){}

    @BeforeClass
    public static void before()
        throws InterruptedException, IOException, ExecutionException, TimeoutException {
        CLUSTER.start();
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        Thread.sleep(10000);
        final KafkaProducer<Integer, Integer> producer = new KafkaProducer<>(producerProps);
        producer.send(new ProducerRecord<>(INPUT_TOPIC_NAME, 0, RECORD_TIMESTAMP_LONG, 2, 2)).get();
    }

    @Before
    public void beforeTest() throws InterruptedException {
        builder = new StreamsBuilder();
        builder.table(INPUT_TOPIC_NAME,
            Materialized.as(Stores.persistentVersionedKeyValueStore(STORE_NAME, HISTORY_RETENTION)));
        final Properties configs = new Properties();
        configs.put(StreamsConfig.APPLICATION_ID_CONFIG, "app");
        configs.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        configs.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class.getName());
        configs.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class.getName());
        kafkaStreams = new KafkaStreams(builder.build(), configs);
        kafkaStreams.start();
        Thread.sleep(10000);
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
        try {
            final Function<VersionedRecord<Integer>, Integer> valueExtractor =
                VersionedRecord :: value;
            final Function<VersionedRecord<Integer>, Long> timestampExtractor =
                VersionedRecord :: timestamp;
            shouldHandleVersionedKeyQuery(2, valueExtractor, 2);
            shouldHandleVersionedKeyQueryWithTS(2, valueExtractor, 2, timestampExtractor, RECORD_TIMESTAMP_LONG);
            shouldThrowNPEWithNullKey();

        } catch (final AssertionError e) {
            LOG.error("Failed assertion", e);
            throw e;
        }
    }

    public void shouldHandleVersionedKeyQuery(
        final Integer key,
        final Function<VersionedRecord<Integer>, Integer> valueExtactor,
        final Integer expectedValue) {
        final VersionedKeyQuery<Integer, Integer> query = VersionedKeyQuery.withKey(key);
        final StateQueryRequest<VersionedRecord<Integer>> request = StateQueryRequest.inStore(
            STORE_NAME).withQuery(query);
        final StateQueryResult<VersionedRecord<Integer>> result = kafkaStreams.query(request);
        final QueryResult<VersionedRecord<Integer>> queryResult = result.getOnlyPartitionResult();
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

        final VersionedRecord<Integer> result1 = queryResult.getResult();
        final Integer integer = valueExtactor.apply(result1);
        assertThat(integer, is(expectedValue));
        assertThat(queryResult.getExecutionInfo(), is(empty()));
    }

    public void shouldHandleVersionedKeyQueryWithTS(
        final Integer key,
        final Function<VersionedRecord<Integer>, Integer> valueExtactor,
        final Integer expectedValue,
        final Function<VersionedRecord<Integer>, Long> timestampExtactor,
        final Long expectedTimestamp) {
        VersionedKeyQuery<Integer, Integer> query = VersionedKeyQuery.withKey(key);
        query = query.asOf(Instant.now());
        final StateQueryRequest<VersionedRecord<Integer>> request = StateQueryRequest.inStore(
            STORE_NAME).withQuery(query);
        final StateQueryResult<VersionedRecord<Integer>> result = kafkaStreams.query(request);
        final QueryResult<VersionedRecord<Integer>> queryResult = result.getOnlyPartitionResult();
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

        final VersionedRecord<Integer> result1 = queryResult.getResult();
        final Integer value = valueExtactor.apply(result1);
        final Long timestamp = timestampExtactor.apply(result1);
        assertThat(value, is(expectedValue));
        assertThat(timestamp, is(expectedTimestamp));
        assertThat(queryResult.getExecutionInfo(), is(empty()));
    }

    public void shouldThrowNPEWithNullKey() {
        assertThrows(NullPointerException.class, () -> VersionedKeyQuery.withKey(null));
    }
}