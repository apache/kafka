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

import java.time.Duration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.test.IntegrationTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tests all available joins of Kafka Streams DSL.
 */
@Category({IntegrationTest.class})
@RunWith(value = Parameterized.class)
public class TableTableJoinIntegrationTest extends AbstractJoinIntegrationTest {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(600);
    private KTable<Long, String> leftTable;
    private KTable<Long, String> rightTable;

    public TableTableJoinIntegrationTest(final boolean cacheEnabled) {
        super(cacheEnabled);
    }

    @Before
    public void prepareTopology() throws InterruptedException {
        super.prepareEnvironment();

        appID = "table-table-join-integration-test";

        builder = new StreamsBuilder();
    }

    private final TestRecord<Long, String> expectedFinalJoinResultUnversioned = new TestRecord<>(ANY_UNIQUE_KEY, "F-f", null, 4L);
    private final TestRecord<Long, String> expectedFinalJoinResultLeftVersionedOnly = new TestRecord<>(ANY_UNIQUE_KEY, "E-f", null, 15L);
    private final TestRecord<Long, String> expectedFinalJoinResultRightVersionedOnly = new TestRecord<>(ANY_UNIQUE_KEY, "F-e", null, 14L);
    private final TestRecord<Long, String> expectedFinalMultiJoinResult = new TestRecord<>(ANY_UNIQUE_KEY, "F-f-f", null, 4L);
    private final String storeName = appID + "-store";

    private final Materialized<Long, String, KeyValueStore<Bytes, byte[]>> materialized = Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as(storeName)
            .withKeySerde(Serdes.Long())
            .withValueSerde(Serdes.String())
            .withCachingDisabled()
            .withLoggingDisabled();

    @Test
    public void testInner() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner");

        leftTable = builder.table(INPUT_TOPIC_LEFT, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("left").withLoggingDisabled());
        rightTable = builder.table(INPUT_TOPIC_RIGHT, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("right").withLoggingDisabled());
        leftTable.join(rightTable, valueJoiner, materialized).toStream().to(OUTPUT_TOPIC);

        if (cacheEnabled) {
            runTestWithDriver(input, expectedFinalJoinResultUnversioned, storeName);
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

            runTestWithDriver(input, expectedResult, storeName);
        }
    }

    @Test
    public void testLeft() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-left");

        leftTable = builder.table(INPUT_TOPIC_LEFT, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("left").withLoggingDisabled());
        rightTable = builder.table(INPUT_TOPIC_RIGHT, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("right").withLoggingDisabled());
        leftTable.leftJoin(rightTable, valueJoiner, materialized).toStream().to(OUTPUT_TOPIC);

        if (cacheEnabled) {
            runTestWithDriver(input, expectedFinalJoinResultUnversioned, storeName);
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

            runTestWithDriver(input, expectedResult, storeName);
        }
    }

    @Test
    public void testOuter() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-outer");

        leftTable = builder.table(INPUT_TOPIC_LEFT, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("left").withLoggingDisabled());
        rightTable = builder.table(INPUT_TOPIC_RIGHT, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("right").withLoggingDisabled());
        leftTable.outerJoin(rightTable, valueJoiner, materialized).toStream().to(OUTPUT_TOPIC);

        if (cacheEnabled) {
            runTestWithDriver(input, expectedFinalJoinResultUnversioned, storeName);
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

            runTestWithDriver(input, expectedResult, storeName);
        }
    }

    @Test
    public void testInnerWithVersionedStores() {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner");

        leftTable = builder.table(INPUT_TOPIC_LEFT, Materialized.<Long, String>as(
            Stores.persistentVersionedKeyValueStore("left", Duration.ofMinutes(5))
        ).withLoggingDisabled());
        rightTable = builder.table(INPUT_TOPIC_RIGHT, Materialized.<Long, String>as(
            Stores.persistentVersionedKeyValueStore("right", Duration.ofMinutes(5))
        ).withLoggingDisabled());
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

        runTestWithDriver(input, expectedResult, storeName);
    }

    @Test
    public void testLeftWithVersionedStores() {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-left");

        leftTable = builder.table(INPUT_TOPIC_LEFT, Materialized.<Long, String>as(
            Stores.persistentVersionedKeyValueStore("left", Duration.ofMinutes(5))
        ).withLoggingDisabled());
        rightTable = builder.table(INPUT_TOPIC_RIGHT, Materialized.<Long, String>as(
            Stores.persistentVersionedKeyValueStore("right", Duration.ofMinutes(5))
        ).withLoggingDisabled());
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

        runTestWithDriver(input, expectedResult, storeName);
    }

    @Test
    public void testOuterWithVersionedStores() {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-outer");

        leftTable = builder.table(INPUT_TOPIC_LEFT, Materialized.<Long, String>as(
            Stores.persistentVersionedKeyValueStore("left", Duration.ofMinutes(5))
        ).withLoggingDisabled());
        rightTable = builder.table(INPUT_TOPIC_RIGHT, Materialized.<Long, String>as(
            Stores.persistentVersionedKeyValueStore("right", Duration.ofMinutes(5))
        ).withLoggingDisabled());
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

        runTestWithDriver(input, expectedResult, storeName);
    }

    @Test
    public void testInnerWithLeftVersionedOnly() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner");

        leftTable = builder.table(INPUT_TOPIC_LEFT, Materialized.<Long, String>as(
            Stores.persistentVersionedKeyValueStore("left", Duration.ofMinutes(5))
        ).withLoggingDisabled());
        rightTable = builder.table(INPUT_TOPIC_RIGHT, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("right").withLoggingDisabled());
        leftTable.join(rightTable, valueJoiner, materialized).toStream().to(OUTPUT_TOPIC);

        if (cacheEnabled) {
            runTestWithDriver(input, expectedFinalJoinResultLeftVersionedOnly, storeName);
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

            runTestWithDriver(input, expectedResult, storeName);
        }
    }

    @Test
    public void testLeftWithLeftVersionedOnly() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-left");

        leftTable = builder.table(INPUT_TOPIC_LEFT, Materialized.<Long, String>as(
            Stores.persistentVersionedKeyValueStore("left", Duration.ofMinutes(5))
        ).withLoggingDisabled());
        rightTable = builder.table(INPUT_TOPIC_RIGHT, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("right").withLoggingDisabled());
        leftTable.leftJoin(rightTable, valueJoiner, materialized).toStream().to(OUTPUT_TOPIC);

        if (cacheEnabled) {
            runTestWithDriver(input, expectedFinalJoinResultLeftVersionedOnly, storeName);
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

            runTestWithDriver(input, expectedResult, storeName);
        }
    }

    @Test
    public void testOuterWithLeftVersionedOnly() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-outer");

        leftTable = builder.table(INPUT_TOPIC_LEFT, Materialized.<Long, String>as(
            Stores.persistentVersionedKeyValueStore("left", Duration.ofMinutes(5))
        ).withLoggingDisabled());
        rightTable = builder.table(INPUT_TOPIC_RIGHT, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("right").withLoggingDisabled());
        leftTable.outerJoin(rightTable, valueJoiner, materialized).toStream().to(OUTPUT_TOPIC);

        if (cacheEnabled) {
            runTestWithDriver(input, expectedFinalJoinResultLeftVersionedOnly, storeName);
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

            runTestWithDriver(input, expectedResult, storeName);
        }
    }

    @Test
    public void testInnerWithRightVersionedOnly() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner");

        leftTable = builder.table(INPUT_TOPIC_LEFT, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("left").withLoggingDisabled());
        rightTable = builder.table(INPUT_TOPIC_RIGHT, Materialized.<Long, String>as(
            Stores.persistentVersionedKeyValueStore("right", Duration.ofMinutes(5))
        ).withLoggingDisabled());
        leftTable.join(rightTable, valueJoiner, materialized).toStream().to(OUTPUT_TOPIC);

        if (cacheEnabled) {
            runTestWithDriver(input, expectedFinalJoinResultRightVersionedOnly, storeName);
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

            runTestWithDriver(input, expectedResult, storeName);
        }
    }

    @Test
    public void testLeftWithRightVersionedOnly() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-left");

        leftTable = builder.table(INPUT_TOPIC_LEFT, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("left").withLoggingDisabled());
        rightTable = builder.table(INPUT_TOPIC_RIGHT, Materialized.<Long, String>as(
            Stores.persistentVersionedKeyValueStore("right", Duration.ofMinutes(5))
        ).withLoggingDisabled());
        leftTable.leftJoin(rightTable, valueJoiner, materialized).toStream().to(OUTPUT_TOPIC);

        if (cacheEnabled) {
            runTestWithDriver(input, expectedFinalJoinResultRightVersionedOnly, storeName);
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

            runTestWithDriver(input, expectedResult, storeName);
        }
    }

    @Test
    public void testOuterWithRightVersionedOnly() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-outer");

        leftTable = builder.table(INPUT_TOPIC_LEFT, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("left").withLoggingDisabled());
        rightTable = builder.table(INPUT_TOPIC_RIGHT, Materialized.<Long, String>as(
            Stores.persistentVersionedKeyValueStore("right", Duration.ofMinutes(5))
        ).withLoggingDisabled());
        leftTable.outerJoin(rightTable, valueJoiner, materialized).toStream().to(OUTPUT_TOPIC);

        if (cacheEnabled) {
            runTestWithDriver(input, expectedFinalJoinResultRightVersionedOnly, storeName);
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

            runTestWithDriver(input, expectedResult, storeName);
        }
    }

    @Test
    public void testInnerInner() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner-inner");

        leftTable = builder.table(INPUT_TOPIC_LEFT, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("left").withLoggingDisabled());
        rightTable = builder.table(INPUT_TOPIC_RIGHT, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("right").withLoggingDisabled());
        leftTable.join(rightTable, valueJoiner)
                 .join(rightTable, valueJoiner, materialized)
                 .toStream()
                 .to(OUTPUT_TOPIC);

        if (cacheEnabled) {
            runTestWithDriver(input, expectedFinalMultiJoinResult, storeName);
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

            runTestWithDriver(inputWithoutOutOfOrderData, expectedResult, storeName);
        }
    }

    @Test
    public void testInnerLeft() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner-left");

        leftTable = builder.table(INPUT_TOPIC_LEFT, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("left").withLoggingDisabled());
        rightTable = builder.table(INPUT_TOPIC_RIGHT, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("right").withLoggingDisabled());
        leftTable.join(rightTable, valueJoiner)
                 .leftJoin(rightTable, valueJoiner, materialized)
                 .toStream()
                 .to(OUTPUT_TOPIC);

        if (cacheEnabled) {
            runTestWithDriver(input, expectedFinalMultiJoinResult, storeName);
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

            runTestWithDriver(inputWithoutOutOfOrderData, expectedResult, storeName);
        }
    }

    @Test
    public void testInnerOuter() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner-outer");

        leftTable = builder.table(INPUT_TOPIC_LEFT, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("left").withLoggingDisabled());
        rightTable = builder.table(INPUT_TOPIC_RIGHT, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("right").withLoggingDisabled());
        leftTable.join(rightTable, valueJoiner)
                 .outerJoin(rightTable, valueJoiner, materialized)
                 .toStream()
                 .to(OUTPUT_TOPIC);

        if (cacheEnabled) {
            runTestWithDriver(input, expectedFinalMultiJoinResult, storeName);
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

            runTestWithDriver(inputWithoutOutOfOrderData, expectedResult, storeName);
        }
    }

    @Test
    public void testLeftInner() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner-inner");

        leftTable = builder.table(INPUT_TOPIC_LEFT, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("left").withLoggingDisabled());
        rightTable = builder.table(INPUT_TOPIC_RIGHT, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("right").withLoggingDisabled());
        leftTable.leftJoin(rightTable, valueJoiner)
                 .join(rightTable, valueJoiner, materialized)
                 .toStream()
                 .to(OUTPUT_TOPIC);

        if (cacheEnabled) {
            runTestWithDriver(input, expectedFinalMultiJoinResult, storeName);
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

            runTestWithDriver(inputWithoutOutOfOrderData, expectedResult, storeName);
        }
    }

    @Test
    public void testLeftLeft() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner-left");

        leftTable = builder.table(INPUT_TOPIC_LEFT, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("left").withLoggingDisabled());
        rightTable = builder.table(INPUT_TOPIC_RIGHT, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("right").withLoggingDisabled());
        leftTable.leftJoin(rightTable, valueJoiner)
                 .leftJoin(rightTable, valueJoiner, materialized)
                 .toStream()
                 .to(OUTPUT_TOPIC);

        if (cacheEnabled) {
            runTestWithDriver(input, expectedFinalMultiJoinResult, storeName);
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

            runTestWithDriver(inputWithoutOutOfOrderData, expectedResult, storeName);
        }
    }

    @Test
    public void testLeftOuter() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner-outer");

        leftTable = builder.table(INPUT_TOPIC_LEFT, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("left").withLoggingDisabled());
        rightTable = builder.table(INPUT_TOPIC_RIGHT, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("right").withLoggingDisabled());
        leftTable.leftJoin(rightTable, valueJoiner)
                 .outerJoin(rightTable, valueJoiner, materialized)
                 .toStream()
                 .to(OUTPUT_TOPIC);

        if (cacheEnabled) {
            runTestWithDriver(input, expectedFinalMultiJoinResult, storeName);
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

            runTestWithDriver(inputWithoutOutOfOrderData, expectedResult, storeName);
        }
    }

    @Test
    public void testOuterInner() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner-inner");

        leftTable = builder.table(INPUT_TOPIC_LEFT, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("left").withLoggingDisabled());
        rightTable = builder.table(INPUT_TOPIC_RIGHT, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("right").withLoggingDisabled());
        leftTable.outerJoin(rightTable, valueJoiner)
                 .join(rightTable, valueJoiner, materialized)
                 .toStream()
                 .to(OUTPUT_TOPIC);

        if (cacheEnabled) {
            runTestWithDriver(input, expectedFinalMultiJoinResult, storeName);
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

            runTestWithDriver(inputWithoutOutOfOrderData, expectedResult, storeName);
        }
    }

    @Test
    public void testOuterLeft() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner-left");

        leftTable = builder.table(INPUT_TOPIC_LEFT, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("left").withLoggingDisabled());
        rightTable = builder.table(INPUT_TOPIC_RIGHT, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("right").withLoggingDisabled());
        leftTable.outerJoin(rightTable, valueJoiner)
                 .leftJoin(rightTable, valueJoiner, materialized)
                 .toStream()
                 .to(OUTPUT_TOPIC);

        if (cacheEnabled) {
            runTestWithDriver(input, expectedFinalMultiJoinResult, storeName);
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

            runTestWithDriver(inputWithoutOutOfOrderData, expectedResult, storeName);
        }
    }

    @Test
    public void testOuterOuter() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner-outer");

        leftTable = builder.table(INPUT_TOPIC_LEFT, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("left").withLoggingDisabled());
        rightTable = builder.table(INPUT_TOPIC_RIGHT, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("right").withLoggingDisabled());
        leftTable.outerJoin(rightTable, valueJoiner)
                 .outerJoin(rightTable, valueJoiner, materialized)
                 .toStream()
                 .to(OUTPUT_TOPIC);

        if (cacheEnabled) {
            runTestWithDriver(input, expectedFinalMultiJoinResult, storeName);
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
            runTestWithDriver(inputWithoutOutOfOrderData, expectedResult, storeName);
        }
    }
}
