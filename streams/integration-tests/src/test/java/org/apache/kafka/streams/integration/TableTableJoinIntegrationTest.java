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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.test.TestRecord;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Tests all available joins of Kafka Streams DSL.
 */
@Tag("integration")
@Timeout(600)
public class TableTableJoinIntegrationTest extends AbstractJoinIntegrationTest {
    private static final String APP_ID = "table-table-join-integration-test";
    private static final String STORE_NAME = APP_ID + "-store";
    private final TestRecord<Long, String> expectedFinalJoinResultUnversioned =
            new TestRecord<>(ANY_UNIQUE_KEY, "F-f", null, 4L);
    private final TestRecord<Long, String> expectedFinalJoinResultLeftVersionedOnly =
            new TestRecord<>(ANY_UNIQUE_KEY, "E-f", null, 15L);
    private final TestRecord<Long, String> expectedFinalJoinResultRightVersionedOnly =
            new TestRecord<>(ANY_UNIQUE_KEY, "F-e", null, 14L);
    private final TestRecord<Long, String> expectedFinalMultiJoinResult =
            new TestRecord<>(ANY_UNIQUE_KEY, "F-f-f", null, 4L);

    private final Materialized<Long, String, KeyValueStore<Bytes, byte[]>> materialized = Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as(STORE_NAME)
            .withKeySerde(Serdes.Long())
            .withValueSerde(Serdes.String())
            .withCachingDisabled()
            .withLoggingDisabled();

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testInner(final boolean cacheEnabled) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Long, String> leftTable = builder.table(INPUT_TOPIC_LEFT,
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("left").withLoggingDisabled());
        final KTable<Long, String> rightTable = builder.table(INPUT_TOPIC_RIGHT,
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("right").withLoggingDisabled());
        final Properties streamsConfig = setupConfigsAndUtils(cacheEnabled);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-inner");
        leftTable.join(rightTable, valueJoiner, materialized).toStream().to(OUTPUT_TOPIC);

        if (cacheEnabled) {
            runTestWithDriver(input, expectedFinalJoinResultUnversioned, STORE_NAME, streamsConfig, builder.build(streamsConfig));
        } else {
            final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
                null,
                null,
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-a", null, 4L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a", null, 5L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-b", null, 6L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 7L)),
                null,
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "C-c", null, 10L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 11L)),
                null,
                null,
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "D-d", null, 7L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 7L)),
                null,
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "E-e", null, 15L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 14L)),
                null,
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "F-f", null, 4L))
            );

            runTestWithDriver(input, expectedResult, STORE_NAME, streamsConfig, builder.build(streamsConfig));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testLeft(final boolean cacheEnabled) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Long, String> leftTable = builder.table(INPUT_TOPIC_LEFT,
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("left").withLoggingDisabled());
        final KTable<Long, String> rightTable = builder.table(INPUT_TOPIC_RIGHT,
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("right").withLoggingDisabled());
        final Properties streamsConfig = setupConfigsAndUtils(cacheEnabled);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-left");
        leftTable.leftJoin(rightTable, valueJoiner, materialized).toStream().to(OUTPUT_TOPIC);

        if (cacheEnabled) {
            runTestWithDriver(input, expectedFinalJoinResultUnversioned, STORE_NAME, streamsConfig, builder.build(streamsConfig));
        } else {
            final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
                null,
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-null", null, 3L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-a", null, 4L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a", null, 5L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-b", null, 6L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 7L)),
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "C-null", null, 9L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "C-c", null, 10L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "C-null", null, 11L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 12L)),
                null,
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "D-d", null, 7L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 7L)),
                null,
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "E-e", null, 15L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 14L)),
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "F-null", null, 4L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "F-f", null, 4L))
            );

            runTestWithDriver(input, expectedResult, STORE_NAME, streamsConfig, builder.build(streamsConfig));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testOuter(final boolean cacheEnabled) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Long, String> leftTable = builder.table(INPUT_TOPIC_LEFT,
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("left").withLoggingDisabled());
        final KTable<Long, String> rightTable = builder.table(INPUT_TOPIC_RIGHT,
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("right").withLoggingDisabled());
        final Properties streamsConfig = setupConfigsAndUtils(cacheEnabled);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-outer");
        leftTable.outerJoin(rightTable, valueJoiner, materialized).toStream().to(OUTPUT_TOPIC);

        if (cacheEnabled) {
            runTestWithDriver(input, expectedFinalJoinResultUnversioned, STORE_NAME, streamsConfig, builder.build(streamsConfig));
        } else {
            final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
                null,
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-null", null, 3L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-a", null, 4L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a", null, 5L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-b", null, 6L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "null-b", null, 7L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 8L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "C-null", null, 9L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "C-c", null, 10L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "C-null", null, 11L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 12L)),
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "null-d", null, 7L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "D-d", null, 7L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "null-d", null, 7L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 3L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "null-e", null, 14L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "E-e", null, 15L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "null-e", null, 14L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 9L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "F-null", null, 4L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "F-f", null, 4L))
            );

            runTestWithDriver(input, expectedResult, STORE_NAME, streamsConfig, builder.build(streamsConfig));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testInnerWithVersionedStores(final boolean cacheEnabled) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Long, String> leftTable = builder.table(INPUT_TOPIC_LEFT,
                Materialized.<Long, String>as(Stores.persistentVersionedKeyValueStore("left", Duration.ofMinutes(5))).withLoggingDisabled());
        final KTable<Long, String> rightTable = builder.table(INPUT_TOPIC_RIGHT,
                Materialized.<Long, String>as(Stores.persistentVersionedKeyValueStore("right", Duration.ofMinutes(5))).withLoggingDisabled());
        final Properties streamsConfig = setupConfigsAndUtils(cacheEnabled);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-inner");
        leftTable.join(rightTable, valueJoiner, materialized).toStream().to(OUTPUT_TOPIC);

        // versioned stores do not support caching, so we expect the same result regardless of whether caching is enabled or not
        final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-a", null, 4L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a", null, 5L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-b", null, 6L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 7L)),
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "C-c", null, 10L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 11L)),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "E-e", null, 15L)),
            null,
            null,
            null,
            null
        );

        runTestWithDriver(input, expectedResult, STORE_NAME, streamsConfig, builder.build(streamsConfig));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testLeftWithVersionedStores(final boolean cacheEnabled) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Long, String> leftTable = builder.table(INPUT_TOPIC_LEFT,
                Materialized.<Long, String>as(Stores.persistentVersionedKeyValueStore("left", Duration.ofMinutes(5))).withLoggingDisabled());
        final KTable<Long, String> rightTable = builder.table(INPUT_TOPIC_RIGHT,
                Materialized.<Long, String>as(Stores.persistentVersionedKeyValueStore("right", Duration.ofMinutes(5))).withLoggingDisabled());
        final Properties streamsConfig = setupConfigsAndUtils(cacheEnabled);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-left");
        leftTable.leftJoin(rightTable, valueJoiner, materialized).toStream().to(OUTPUT_TOPIC);

        // versioned stores do not support caching, so we expect the same result regardless of whether caching is enabled or not
        final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-null", null, 3L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-a", null, 4L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a", null, 5L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-b", null, 6L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 7L)),
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "C-null", null, 9L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "C-c", null, 10L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "C-null", null, 11L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 12L)),
            null,
            null,
            null,
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "E-e", null, 15L)),
            null,
            null,
            null,
            null
        );

        runTestWithDriver(input, expectedResult, STORE_NAME, streamsConfig, builder.build(streamsConfig));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testOuterWithVersionedStores(final boolean cacheEnabled) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Long, String> leftTable = builder.table(INPUT_TOPIC_LEFT,
                Materialized.<Long, String>as(Stores.persistentVersionedKeyValueStore("left", Duration.ofMinutes(5))).withLoggingDisabled());
        final KTable<Long, String> rightTable = builder.table(INPUT_TOPIC_RIGHT,
                Materialized.<Long, String>as(Stores.persistentVersionedKeyValueStore("right", Duration.ofMinutes(5))).withLoggingDisabled());
        final Properties streamsConfig = setupConfigsAndUtils(cacheEnabled);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-outer");
        leftTable.outerJoin(rightTable, valueJoiner, materialized).toStream().to(OUTPUT_TOPIC);

        // versioned stores do not support caching, so we expect the same result regardless of whether caching is enabled or not
        final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-null", null, 3L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-a", null, 4L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a", null, 5L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-b", null, 6L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "null-b", null, 7L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 8L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "C-null", null, 9L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "C-c", null, 10L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "C-null", null, 11L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 12L)),
            null,
            null,
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "null-e", null, 14L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "E-e", null, 15L)),
            null,
            null,
            null,
            null
        );

        runTestWithDriver(input, expectedResult, STORE_NAME, streamsConfig, builder.build(streamsConfig));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testInnerWithLeftVersionedOnly(final boolean cacheEnabled) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Long, String> leftTable = builder.table(INPUT_TOPIC_LEFT,
                Materialized.<Long, String>as(Stores.persistentVersionedKeyValueStore("left", Duration.ofMinutes(5))).withLoggingDisabled());
        final KTable<Long, String> rightTable = builder.table(INPUT_TOPIC_RIGHT,
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("right").withLoggingDisabled());
        final Properties streamsConfig = setupConfigsAndUtils(cacheEnabled);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-inner");
        leftTable.join(rightTable, valueJoiner, materialized).toStream().to(OUTPUT_TOPIC);

        if (cacheEnabled) {
            runTestWithDriver(input, expectedFinalJoinResultLeftVersionedOnly, STORE_NAME, streamsConfig, builder.build(streamsConfig));
        } else {
            final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
                null,
                null,
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-a", null, 4L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a", null, 5L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-b", null, 6L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 7L)),
                null,
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "C-c", null, 10L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 11L)),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "E-e", null, 15L)),
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 15L)),
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "E-f", null, 15L))
            );

            runTestWithDriver(input, expectedResult, STORE_NAME, streamsConfig, builder.build(streamsConfig));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testLeftWithLeftVersionedOnly(final boolean cacheEnabled) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Long, String> leftTable = builder.table(INPUT_TOPIC_LEFT,
                Materialized.<Long, String>as(Stores.persistentVersionedKeyValueStore("left", Duration.ofMinutes(5))).withLoggingDisabled());
        final KTable<Long, String> rightTable = builder.table(INPUT_TOPIC_RIGHT,
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("right").withLoggingDisabled());
        final Properties streamsConfig = setupConfigsAndUtils(cacheEnabled);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-left");
        leftTable.leftJoin(rightTable, valueJoiner, materialized).toStream().to(OUTPUT_TOPIC);

        if (cacheEnabled) {
            runTestWithDriver(input, expectedFinalJoinResultLeftVersionedOnly, STORE_NAME, streamsConfig, builder.build(streamsConfig));
        } else {
            final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
                null,
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-null", null, 3L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-a", null, 4L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a", null, 5L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-b", null, 6L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 7L)),
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "C-null", null, 9L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "C-c", null, 10L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "C-null", null, 11L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 12L)),
                null,
                null,
                null,
                null,
                null,
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "E-e", null, 15L)),
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "E-null", null, 15L)),
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "E-f", null, 15L))
            );

            runTestWithDriver(input, expectedResult, STORE_NAME, streamsConfig, builder.build(streamsConfig));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testOuterWithLeftVersionedOnly(final boolean cacheEnabled) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Long, String> leftTable = builder.table(INPUT_TOPIC_LEFT,
                Materialized.<Long, String>as(Stores.persistentVersionedKeyValueStore("left", Duration.ofMinutes(5))).withLoggingDisabled());
        final KTable<Long, String> rightTable = builder.table(INPUT_TOPIC_RIGHT,
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("right").withLoggingDisabled());
        final Properties streamsConfig = setupConfigsAndUtils(cacheEnabled);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-outer");
        leftTable.outerJoin(rightTable, valueJoiner, materialized).toStream().to(OUTPUT_TOPIC);

        if (cacheEnabled) {
            runTestWithDriver(input, expectedFinalJoinResultLeftVersionedOnly, STORE_NAME, streamsConfig, builder.build(streamsConfig));
        } else {
            final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
                null,
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-null", null, 3L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-a", null, 4L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a", null, 5L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-b", null, 6L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "null-b", null, 7L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 8L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "C-null", null, 9L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "C-c", null, 10L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "C-null", null, 11L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 12L)),
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "null-d", null, 7L)),
                null,
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 3L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "null-e", null, 14L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "E-e", null, 15L)),
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "E-null", null, 15L)),
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "E-f", null, 15L))
            );

            runTestWithDriver(input, expectedResult, STORE_NAME, streamsConfig, builder.build(streamsConfig));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testInnerWithRightVersionedOnly(final boolean cacheEnabled) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Long, String> leftTable = builder.table(INPUT_TOPIC_LEFT,
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("left").withLoggingDisabled());
        final KTable<Long, String> rightTable = builder.table(INPUT_TOPIC_RIGHT,
                Materialized.<Long, String>as(Stores.persistentVersionedKeyValueStore("right", Duration.ofMinutes(5))).withLoggingDisabled());
        final Properties streamsConfig = setupConfigsAndUtils(cacheEnabled);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-inner");
        leftTable.join(rightTable, valueJoiner, materialized).toStream().to(OUTPUT_TOPIC);

        if (cacheEnabled) {
            runTestWithDriver(input, expectedFinalJoinResultRightVersionedOnly, STORE_NAME, streamsConfig, builder.build(streamsConfig));
        } else {
            final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
                null,
                null,
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-a", null, 4L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a", null, 5L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-b", null, 6L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 7L)),
                null,
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "C-c", null, 10L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 11L)),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "E-e", null, 15L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 14L)),
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "F-e", null, 14L)),
                null
            );

            runTestWithDriver(input, expectedResult, STORE_NAME, streamsConfig, builder.build(streamsConfig));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testLeftWithRightVersionedOnly(final boolean cacheEnabled) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Long, String> leftTable = builder.table(INPUT_TOPIC_LEFT,
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("left").withLoggingDisabled());
        final KTable<Long, String> rightTable = builder.table(INPUT_TOPIC_RIGHT,
                Materialized.<Long, String>as(Stores.persistentVersionedKeyValueStore("right", Duration.ofMinutes(5))).withLoggingDisabled());
        final Properties streamsConfig = setupConfigsAndUtils(cacheEnabled);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-left");
        leftTable.leftJoin(rightTable, valueJoiner, materialized).toStream().to(OUTPUT_TOPIC);

        if (cacheEnabled) {
            runTestWithDriver(input, expectedFinalJoinResultRightVersionedOnly, STORE_NAME, streamsConfig, builder.build(streamsConfig));
        } else {
            final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
                null,
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-null", null, 3L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-a", null, 4L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a", null, 5L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-b", null, 6L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 7L)),
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "C-null", null, 9L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "C-c", null, 10L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "C-null", null, 11L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 12L)),
                null,
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "D-null", null, 6L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 2L)),
                null,
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "E-e", null, 15L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 14L)),
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "F-e", null, 14L)),
                null
            );

            runTestWithDriver(input, expectedResult, STORE_NAME, streamsConfig, builder.build(streamsConfig));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testOuterWithRightVersionedOnly(final boolean cacheEnabled) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Long, String> leftTable = builder.table(INPUT_TOPIC_LEFT,
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("left").withLoggingDisabled());
        final KTable<Long, String> rightTable = builder.table(INPUT_TOPIC_RIGHT,
                Materialized.<Long, String>as(Stores.persistentVersionedKeyValueStore("right", Duration.ofMinutes(5))).withLoggingDisabled());
        final Properties streamsConfig = setupConfigsAndUtils(cacheEnabled);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-outer");
        leftTable.outerJoin(rightTable, valueJoiner, materialized).toStream().to(OUTPUT_TOPIC);

        if (cacheEnabled) {
            runTestWithDriver(input, expectedFinalJoinResultRightVersionedOnly, STORE_NAME, streamsConfig, builder.build(streamsConfig));
        } else {
            final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
                null,
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-null", null, 3L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-a", null, 4L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a", null, 5L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-b", null, 6L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "null-b", null, 7L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 8L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "C-null", null, 9L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "C-c", null, 10L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "C-null", null, 11L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 12L)),
                null,
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "D-null", null, 6L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 2L)),
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "null-e", null, 14L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "E-e", null, 15L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "null-e", null, 14L)),
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "F-e", null, 14L)),
                null
            );

            runTestWithDriver(input, expectedResult, STORE_NAME, streamsConfig, builder.build(streamsConfig));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testInnerInner(final boolean cacheEnabled) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Long, String> leftTable = builder.table(INPUT_TOPIC_LEFT,
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("left").withLoggingDisabled());
        final KTable<Long, String> rightTable = builder.table(INPUT_TOPIC_RIGHT,
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("right").withLoggingDisabled());
        final Properties streamsConfig = setupConfigsAndUtils(cacheEnabled);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-inner-inner");
        leftTable.join(rightTable, valueJoiner)
                 .join(rightTable, valueJoiner, materialized)
                 .toStream()
                 .to(OUTPUT_TOPIC);

        if (cacheEnabled) {
            runTestWithDriver(input, expectedFinalMultiJoinResult, STORE_NAME, streamsConfig, builder.build(streamsConfig));
        } else {
            // TODO K6443: the duplicate below for all the multi-joins are due to
            //             KAFKA-6443, should be updated once it is fixed.
            final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
                null,
                null,
                null,
                Arrays.asList(
                    new TestRecord<>(ANY_UNIQUE_KEY, "A-a-a", null, 4L),
                    new TestRecord<>(ANY_UNIQUE_KEY, "A-a-a", null, 4L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a-a", null, 5L)),
                Arrays.asList(
                    new TestRecord<>(ANY_UNIQUE_KEY, "B-b-b", null, 6L),
                    new TestRecord<>(ANY_UNIQUE_KEY, "B-b-b", null, 6L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 7L)),
                null,
                null,
                Arrays.asList(
                    new TestRecord<>(ANY_UNIQUE_KEY, "C-c-c", null, 10L),
                    new TestRecord<>(ANY_UNIQUE_KEY, "C-c-c", null, 10L)),
                null, // correct would be -> new TestRecord<>(ANY_UNIQUE_KEY, null, null, 11L)
                      // we don't get correct value, because of self-join of `rightTable`
                null,
                null,
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "D-d-d", null, 15L)),
                null,
                null
            );

            runTestWithDriver(inputWithoutOutOfOrderData, expectedResult, STORE_NAME, streamsConfig, builder.build(streamsConfig));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testInnerLeft(final boolean cacheEnabled) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Long, String> leftTable = builder.table(INPUT_TOPIC_LEFT,
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("left").withLoggingDisabled());
        final KTable<Long, String> rightTable = builder.table(INPUT_TOPIC_RIGHT,
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("right").withLoggingDisabled());
        final Properties streamsConfig = setupConfigsAndUtils(cacheEnabled);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-inner-left");
        leftTable.join(rightTable, valueJoiner)
                 .leftJoin(rightTable, valueJoiner, materialized)
                 .toStream()
                 .to(OUTPUT_TOPIC);

        if (cacheEnabled) {
            runTestWithDriver(input, expectedFinalMultiJoinResult, STORE_NAME, streamsConfig, builder.build(streamsConfig));
        } else {
            final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
                null,
                null,
                null,
                Arrays.asList(
                    new TestRecord<>(ANY_UNIQUE_KEY, "A-a-a", null, 4L),
                    new TestRecord<>(ANY_UNIQUE_KEY, "A-a-a", null, 4L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a-a", null, 5L)),
                Arrays.asList(
                    new TestRecord<>(ANY_UNIQUE_KEY, "B-b-b", null, 6L),
                    new TestRecord<>(ANY_UNIQUE_KEY, "B-b-b", null, 6L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 7L)),
                null,
                null,
                Arrays.asList(
                    new TestRecord<>(ANY_UNIQUE_KEY, "C-c-c", null, 10L),
                    new TestRecord<>(ANY_UNIQUE_KEY, "C-c-c", null, 10L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 11L)),
                null,
                null,
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "D-d-d", null, 15L)),
                null,
                null
            );

            runTestWithDriver(inputWithoutOutOfOrderData, expectedResult, STORE_NAME, streamsConfig, builder.build(streamsConfig));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testInnerOuter(final boolean cacheEnabled) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Long, String> leftTable = builder.table(INPUT_TOPIC_LEFT,
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("left").withLoggingDisabled());
        final KTable<Long, String> rightTable = builder.table(INPUT_TOPIC_RIGHT,
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("right").withLoggingDisabled());
        final Properties streamsConfig = setupConfigsAndUtils(cacheEnabled);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-inner-outer");
        leftTable.join(rightTable, valueJoiner)
                 .outerJoin(rightTable, valueJoiner, materialized)
                 .toStream()
                 .to(OUTPUT_TOPIC);

        if (cacheEnabled) {
            runTestWithDriver(input, expectedFinalMultiJoinResult, STORE_NAME, streamsConfig, builder.build(streamsConfig));
        } else {
            final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
                null,
                null,
                null,
                Arrays.asList(
                    new TestRecord<>(ANY_UNIQUE_KEY, "A-a-a", null, 4L),
                    new TestRecord<>(ANY_UNIQUE_KEY, "A-a-a", null, 4L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a-a", null, 5L)),
                Arrays.asList(
                    new TestRecord<>(ANY_UNIQUE_KEY, "B-b-b", null, 6L),
                    new TestRecord<>(ANY_UNIQUE_KEY, "B-b-b", null, 6L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "null-b", null, 7L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 8L)),
                null,
                Arrays.asList(
                    new TestRecord<>(ANY_UNIQUE_KEY, "C-c-c", null, 10L),
                    new TestRecord<>(ANY_UNIQUE_KEY, "C-c-c", null, 10L)),
                Arrays.asList(
                    new TestRecord<>(ANY_UNIQUE_KEY, null, null, 11L),
                    new TestRecord<>(ANY_UNIQUE_KEY, null, null, 11L)),
                null,
                null,
                // incorrect result `null-d` is caused by self-join of `rightTable`
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "null-d", null, 14L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "D-d-d", null, 15L)),
                null,
                null
            );

            runTestWithDriver(inputWithoutOutOfOrderData, expectedResult, STORE_NAME, streamsConfig, builder.build(streamsConfig));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testLeftInner(final boolean cacheEnabled) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Long, String> leftTable = builder.table(INPUT_TOPIC_LEFT,
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("left").withLoggingDisabled());
        final KTable<Long, String> rightTable = builder.table(INPUT_TOPIC_RIGHT,
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("right").withLoggingDisabled());
        final Properties streamsConfig = setupConfigsAndUtils(cacheEnabled);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-inner-inner");
        leftTable.leftJoin(rightTable, valueJoiner)
                 .join(rightTable, valueJoiner, materialized)
                 .toStream()
                 .to(OUTPUT_TOPIC);

        if (cacheEnabled) {
            runTestWithDriver(input, expectedFinalMultiJoinResult, STORE_NAME, streamsConfig, builder.build(streamsConfig));
        } else {
            final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
                null,
                null,
                null,
                Arrays.asList(
                    new TestRecord<>(ANY_UNIQUE_KEY, "A-a-a", null, 4L),
                    new TestRecord<>(ANY_UNIQUE_KEY, "A-a-a", null, 4L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a-a", null, 5L)),
                Arrays.asList(
                    new TestRecord<>(ANY_UNIQUE_KEY, "B-b-b", null, 6L),
                    new TestRecord<>(ANY_UNIQUE_KEY, "B-b-b", null, 6L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 7L)),
                null,
                null,
                Arrays.asList(
                    new TestRecord<>(ANY_UNIQUE_KEY, "C-c-c", null, 10L),
                    new TestRecord<>(ANY_UNIQUE_KEY, "C-c-c", null, 10L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 11L)),
                null,
                null,
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "D-d-d", null, 15L)),
                null,
                null
            );

            runTestWithDriver(inputWithoutOutOfOrderData, expectedResult, STORE_NAME, streamsConfig, builder.build(streamsConfig));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testLeftLeft(final boolean cacheEnabled) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Long, String> leftTable = builder.table(INPUT_TOPIC_LEFT,
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("left").withLoggingDisabled());
        final KTable<Long, String> rightTable = builder.table(INPUT_TOPIC_RIGHT,
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("right").withLoggingDisabled());
        final Properties streamsConfig = setupConfigsAndUtils(cacheEnabled);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-inner-left");
        leftTable.leftJoin(rightTable, valueJoiner)
                 .leftJoin(rightTable, valueJoiner, materialized)
                 .toStream()
                 .to(OUTPUT_TOPIC);

        if (cacheEnabled) {
            runTestWithDriver(input, expectedFinalMultiJoinResult, STORE_NAME, streamsConfig, builder.build(streamsConfig));
        } else {
            final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
                null,
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-null-null", null, 3L)),
                Arrays.asList(
                    new TestRecord<>(ANY_UNIQUE_KEY, "A-a-a", null, 4L),
                    new TestRecord<>(ANY_UNIQUE_KEY, "A-a-a", null, 4L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a-a", null, 5L)),
                Arrays.asList(
                    new TestRecord<>(ANY_UNIQUE_KEY, "B-b-b", null, 6L),
                    new TestRecord<>(ANY_UNIQUE_KEY, "B-b-b", null, 6L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 7L)),
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "C-null-null", null, 9L)),
                Arrays.asList(
                    new TestRecord<>(ANY_UNIQUE_KEY, "C-c-c", null, 10L),
                    new TestRecord<>(ANY_UNIQUE_KEY, "C-c-c", null, 10L)),
                Arrays.asList(
                    new TestRecord<>(ANY_UNIQUE_KEY, "C-null-null", null, 11L),
                    new TestRecord<>(ANY_UNIQUE_KEY, "C-null-null", null, 11L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 12L)),
                null,
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "D-d-d", null, 15L)),
                null,
                null
            );

            runTestWithDriver(inputWithoutOutOfOrderData, expectedResult, STORE_NAME, streamsConfig, builder.build(streamsConfig));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testLeftOuter(final boolean cacheEnabled) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Long, String> leftTable = builder.table(INPUT_TOPIC_LEFT,
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("left").withLoggingDisabled());
        final KTable<Long, String> rightTable = builder.table(INPUT_TOPIC_RIGHT,
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("right").withLoggingDisabled());
        final Properties streamsConfig = setupConfigsAndUtils(cacheEnabled);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-inner-outer");
        leftTable.leftJoin(rightTable, valueJoiner)
                 .outerJoin(rightTable, valueJoiner, materialized)
                 .toStream()
                 .to(OUTPUT_TOPIC);

        if (cacheEnabled) {
            runTestWithDriver(input, expectedFinalMultiJoinResult, STORE_NAME, streamsConfig, builder.build(streamsConfig));
        } else {
            final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
                null,
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-null-null", null, 3L)),
                Arrays.asList(
                    new TestRecord<>(ANY_UNIQUE_KEY, "A-a-a", null, 4L),
                    new TestRecord<>(ANY_UNIQUE_KEY, "A-a-a", null, 4L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a-a", null, 5L)),
                Arrays.asList(
                    new TestRecord<>(ANY_UNIQUE_KEY, "B-b-b", null, 6L),
                    new TestRecord<>(ANY_UNIQUE_KEY, "B-b-b", null, 6L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "null-b", null, 7L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 8L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "C-null-null", null, 9L)),
                Arrays.asList(
                    new TestRecord<>(ANY_UNIQUE_KEY, "C-c-c", null, 10L),
                    new TestRecord<>(ANY_UNIQUE_KEY, "C-c-c", null, 10L)),
                Arrays.asList(
                    new TestRecord<>(ANY_UNIQUE_KEY, "C-null-null", null, 11L),
                    new TestRecord<>(ANY_UNIQUE_KEY, "C-null-null", null, 11L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 12L)),
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "null-d", null, 14L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "D-d-d", null, 15L)),
                null,
                null
            );

            runTestWithDriver(inputWithoutOutOfOrderData, expectedResult, STORE_NAME, streamsConfig, builder.build(streamsConfig));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testOuterInner(final boolean cacheEnabled) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Long, String> leftTable = builder.table(INPUT_TOPIC_LEFT,
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("left").withLoggingDisabled());
        final KTable<Long, String> rightTable = builder.table(INPUT_TOPIC_RIGHT,
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("right").withLoggingDisabled());
        final Properties streamsConfig = setupConfigsAndUtils(cacheEnabled);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-inner-inner");
        leftTable.outerJoin(rightTable, valueJoiner)
                 .join(rightTable, valueJoiner, materialized)
                 .toStream()
                 .to(OUTPUT_TOPIC);

        if (cacheEnabled) {
            runTestWithDriver(input, expectedFinalMultiJoinResult, STORE_NAME, streamsConfig, builder.build(streamsConfig));
        } else {
            final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
                null,
                null,
                null,
                Arrays.asList(
                    new TestRecord<>(ANY_UNIQUE_KEY, "A-a-a", null, 4L),
                    new TestRecord<>(ANY_UNIQUE_KEY, "A-a-a", null, 4L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a-a", null, 5L)),
                Arrays.asList(
                    new TestRecord<>(ANY_UNIQUE_KEY, "B-b-b", null, 6L),
                    new TestRecord<>(ANY_UNIQUE_KEY, "B-b-b", null, 6L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "null-b-b", null, 7L)),
                null,
                null,
                Arrays.asList(
                    new TestRecord<>(ANY_UNIQUE_KEY, "C-c-c", null, 10L),
                    new TestRecord<>(ANY_UNIQUE_KEY, "C-c-c", null, 10L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 11L)),
                null,
                null,
                Arrays.asList(
                    new TestRecord<>(ANY_UNIQUE_KEY, "null-d-d", null, 14L),
                    new TestRecord<>(ANY_UNIQUE_KEY, "null-d-d", null, 14L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "D-d-d", null, 15L)),
                null,
                null
            );

            runTestWithDriver(inputWithoutOutOfOrderData, expectedResult, STORE_NAME, streamsConfig, builder.build(streamsConfig));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testOuterLeft(final boolean cacheEnabled) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Long, String> leftTable = builder.table(INPUT_TOPIC_LEFT,
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("left").withLoggingDisabled());
        final KTable<Long, String> rightTable = builder.table(INPUT_TOPIC_RIGHT,
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("right").withLoggingDisabled());
        final Properties streamsConfig = setupConfigsAndUtils(cacheEnabled);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-inner-left");
        leftTable.outerJoin(rightTable, valueJoiner)
                 .leftJoin(rightTable, valueJoiner, materialized)
                 .toStream()
                 .to(OUTPUT_TOPIC);

        if (cacheEnabled) {
            runTestWithDriver(input, expectedFinalMultiJoinResult, STORE_NAME, streamsConfig, builder.build(streamsConfig));
        } else {
            final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
                null,
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-null-null", null, 3L)),
                Arrays.asList(
                    new TestRecord<>(ANY_UNIQUE_KEY, "A-a-a", null, 4L),
                    new TestRecord<>(ANY_UNIQUE_KEY, "A-a-a", null, 4L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a-a", null, 5L)),
                Arrays.asList(
                    new TestRecord<>(ANY_UNIQUE_KEY, "B-b-b", null, 6L),
                    new TestRecord<>(ANY_UNIQUE_KEY, "B-b-b", null, 6L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "null-b-b", null, 7L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 8L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "C-null-null", null, 9L)),
                Arrays.asList(
                    new TestRecord<>(ANY_UNIQUE_KEY, "C-c-c", null, 10L),
                    new TestRecord<>(ANY_UNIQUE_KEY, "C-c-c", null, 10L)),
                Arrays.asList(
                    new TestRecord<>(ANY_UNIQUE_KEY, "C-null-null", null, 11L),
                    new TestRecord<>(ANY_UNIQUE_KEY, "C-null-null", null, 11L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 12L)),
                null,
                Arrays.asList(
                    new TestRecord<>(ANY_UNIQUE_KEY, "null-d-d", null, 14L),
                    new TestRecord<>(ANY_UNIQUE_KEY, "null-d-d", null, 14L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "D-d-d", null, 15L)),
                null,
                null
            );

            runTestWithDriver(inputWithoutOutOfOrderData, expectedResult, STORE_NAME, streamsConfig, builder.build(streamsConfig));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testOuterOuter(final boolean cacheEnabled) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Long, String> leftTable = builder.table(INPUT_TOPIC_LEFT,
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("left").withLoggingDisabled());
        final KTable<Long, String> rightTable = builder.table(INPUT_TOPIC_RIGHT,
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("right").withLoggingDisabled());
        final Properties streamsConfig = setupConfigsAndUtils(cacheEnabled);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-inner-outer");
        leftTable.outerJoin(rightTable, valueJoiner)
                 .outerJoin(rightTable, valueJoiner, materialized)
                 .toStream()
                 .to(OUTPUT_TOPIC);

        if (cacheEnabled) {
            runTestWithDriver(input, expectedFinalMultiJoinResult, STORE_NAME, streamsConfig, builder.build(streamsConfig));
        } else {
            final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
                null,
                null,
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-null-null", null, 3L)),
                Arrays.asList(
                    new TestRecord<>(ANY_UNIQUE_KEY, "A-a-a", null, 4L),
                    new TestRecord<>(ANY_UNIQUE_KEY, "A-a-a", null, 4L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a-a", null, 5L)),
                Arrays.asList(
                    new TestRecord<>(ANY_UNIQUE_KEY, "B-b-b", null, 6L),
                    new TestRecord<>(ANY_UNIQUE_KEY, "B-b-b", null, 6L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "null-b-b", null, 7L)),
                Arrays.asList(
                    new TestRecord<>(ANY_UNIQUE_KEY, null, null, 8L),
                    new TestRecord<>(ANY_UNIQUE_KEY, null, null, 8L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "C-null-null", null, 9L)),
                Arrays.asList(
                    new TestRecord<>(ANY_UNIQUE_KEY, "C-c-c", null, 10L),
                    new TestRecord<>(ANY_UNIQUE_KEY, "C-c-c", null, 10L)),
                Arrays.asList(
                    new TestRecord<>(ANY_UNIQUE_KEY, "C-null-null", null, 11L),
                    new TestRecord<>(ANY_UNIQUE_KEY, "C-null-null", null, 11L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, null, null, 12L)),
                null,
                Arrays.asList(
                    new TestRecord<>(ANY_UNIQUE_KEY, "null-d-d", null, 14L),
                    new TestRecord<>(ANY_UNIQUE_KEY, "null-d-d", null, 14L)),
                Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "D-d-d", null, 15L)),
                null,
                null
            );
            runTestWithDriver(inputWithoutOutOfOrderData, expectedResult, STORE_NAME, streamsConfig, builder.build(streamsConfig));
        }
    }
}
