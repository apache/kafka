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
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.IntegrationTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
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
        leftTable = builder.table(INPUT_TOPIC_LEFT, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("left").withLoggingDisabled());
        rightTable = builder.table(INPUT_TOPIC_RIGHT, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("right").withLoggingDisabled());
    }

    final private KeyValueTimestamp<Long, String> expectedFinalJoinResult = new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-d", 15L);
    final private KeyValueTimestamp<Long, String> expectedFinalMultiJoinResult = new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-d-d", 15L);
    final private String storeName = appID + "-store";

    private Materialized<Long, String, KeyValueStore<Bytes, byte[]>> materialized = Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as(storeName)
            .withKeySerde(Serdes.Long())
            .withValueSerde(Serdes.String())
            .withCachingDisabled()
            .withLoggingDisabled();

    final private class CountingPeek implements ForeachAction<Long, String> {
        final private KeyValueTimestamp<Long, String> expected;

        CountingPeek(final boolean multiJoin) {
            this.expected = multiJoin ? expectedFinalMultiJoinResult : expectedFinalJoinResult;
        }

        @Override
        public void apply(final Long key, final String value) {
            numRecordsExpected++;
            if (expected.value().equals(value)) {
                final boolean ret = finalResultReached.compareAndSet(false, true);

                if (!ret) {
                    // do nothing; it is possible that we will see multiple duplicates of final results due to KAFKA-4309
                    // TODO: should be removed when KAFKA-4309 is fixed
                }
            }
        }
    }

    @Test
    public void testInner() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner");

        if (cacheEnabled) {
            leftTable.join(rightTable, valueJoiner, materialized).toStream().peek(new CountingPeek(false)).to(OUTPUT_TOPIC);
            runTest(expectedFinalJoinResult, storeName);
        } else {
            final List<List<KeyValueTimestamp<Long, String>>> expectedResult = Arrays.asList(
                null,
                null,
                null,
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a", 4L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-a", 5L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b", 6L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, null, 7L)),
                null,
                null,
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c", 10L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, null, 11L)),
                null,
                null,
                null,
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-d", 15L))
            );

            leftTable.join(rightTable, valueJoiner, materialized).toStream().to(OUTPUT_TOPIC);
            runTest(expectedResult, storeName);
        }
    }

    @Test
    public void testLeft() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-left");

        if (cacheEnabled) {
            leftTable.leftJoin(rightTable, valueJoiner, materialized).toStream().peek(new CountingPeek(false)).to(OUTPUT_TOPIC);
            runTest(expectedFinalJoinResult, storeName);
        } else {
            final List<List<KeyValueTimestamp<Long, String>>> expectedResult = Arrays.asList(
                null,
                null,
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-null", 3L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a", 4L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-a", 5L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b", 6L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, null, 7L)),
                null,
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-null", 9L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c", 10L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-null", 11L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, null, 12L)),
                null,
                null,
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-d", 15L))
            );

            leftTable.leftJoin(rightTable, valueJoiner, materialized).toStream().to(OUTPUT_TOPIC);
            runTest(expectedResult, storeName);
        }
    }

    @Test
    public void testOuter() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-outer");

        if (cacheEnabled) {
            leftTable.outerJoin(rightTable, valueJoiner, materialized).toStream().peek(new CountingPeek(false)).to(OUTPUT_TOPIC);
            runTest(expectedFinalJoinResult, storeName);
        } else {
            final List<List<KeyValueTimestamp<Long, String>>> expectedResult = Arrays.asList(
                null,
                null,
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-null", 3L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a", 4L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-a", 5L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b", 6L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "null-b", 7L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, null, 8L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-null", 9L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c", 10L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-null", 11L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, null, 12L)),
                null,
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "null-d", 14L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-d", 15L))
            );

            leftTable.outerJoin(rightTable, valueJoiner, materialized).toStream().to(OUTPUT_TOPIC);
            runTest(expectedResult, storeName);
        }
    }

    @Test
    public void testInnerInner() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner-inner");

        if (cacheEnabled) {
            leftTable.join(rightTable, valueJoiner)
                    .join(rightTable, valueJoiner, materialized)
                    .toStream()
                    .peek(new CountingPeek(true))
                    .to(OUTPUT_TOPIC);
            runTest(expectedFinalMultiJoinResult, storeName);
        } else {
            // FIXME: the duplicate below for all the multi-joins
            //        are due to KAFKA-6443, should be updated once it is fixed.
            final List<List<KeyValueTimestamp<Long, String>>> expectedResult = Arrays.asList(
                null,
                null,
                null,
                Arrays.asList(
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a-a", 4L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a-a", 4L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-a-a", 5L)),
                Arrays.asList(
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b-b", 6L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b-b", 6L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, null, 7L)),
                null,
                null,
                Arrays.asList(
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c-c", 10L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c-c", 10L)),
                null, // correct would be -> new KeyValueTimestamp<>(ANY_UNIQUE_KEY, null, 11L)
                      // we don't get correct value, because of self-join of `rightTable`
                null,
                null,
                null,
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-d-d", 15L))
            );

            leftTable.join(rightTable, valueJoiner)
                    .join(rightTable, valueJoiner, materialized)
                    .toStream().to(OUTPUT_TOPIC);

            runTest(expectedResult, storeName);
        }
    }

    @Test
    public void testInnerLeft() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner-left");

        if (cacheEnabled) {
            leftTable.join(rightTable, valueJoiner)
                    .leftJoin(rightTable, valueJoiner, materialized)
                    .toStream()
                    .peek(new CountingPeek(true))
                    .to(OUTPUT_TOPIC);
            runTest(expectedFinalMultiJoinResult, storeName);
        } else {
            final List<List<KeyValueTimestamp<Long, String>>> expectedResult = Arrays.asList(
                null,
                null,
                null,
                Arrays.asList(
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a-a", 4L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a-a", 4L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-a-a", 5L)),
                Arrays.asList(
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b-b", 6L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b-b", 6L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, null, 7L)),
                null,
                null,
                Arrays.asList(
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c-c", 10L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c-c", 10L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, null, 11L)),
                null,
                null,
                null,
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-d-d", 15L))
            );

            leftTable.join(rightTable, valueJoiner)
                    .leftJoin(rightTable, valueJoiner, materialized)
                    .toStream()
                    .to(OUTPUT_TOPIC);

            runTest(expectedResult, storeName);
        }
    }

    @Test
    public void testInnerOuter() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner-outer");

        if (cacheEnabled) {
            leftTable.join(rightTable, valueJoiner)
                    .outerJoin(rightTable, valueJoiner, materialized)
                    .toStream()
                    .peek(new CountingPeek(true))
                    .to(OUTPUT_TOPIC);
            runTest(expectedFinalMultiJoinResult, storeName);
        } else {
            final List<List<KeyValueTimestamp<Long, String>>> expectedResult = Arrays.asList(
                null,
                null,
                null,
                Arrays.asList(
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a-a", 4L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a-a", 4L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-a-a", 5L)),
                Arrays.asList(
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b-b", 6L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b-b", 6L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "null-b", 7L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, null, 8L)),
                null,
                Arrays.asList(
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c-c", 10L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c-c", 10L)),
                Arrays.asList(
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, null, 11L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, null, 11L)),
                null,
                null,
                null,
                Arrays.asList(
                    // incorrect result `null-d` is caused by self-join of `rightTable`
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "null-d", 14L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-d-d", 15L))
            );

            leftTable.join(rightTable, valueJoiner)
                    .outerJoin(rightTable, valueJoiner, materialized)
                    .toStream().to(OUTPUT_TOPIC);

            runTest(expectedResult, storeName);
        }
    }

    @Test
    public void testLeftInner() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner-inner");

        if (cacheEnabled) {
            leftTable.leftJoin(rightTable, valueJoiner)
                    .join(rightTable, valueJoiner, materialized)
                    .toStream()
                    .peek(new CountingPeek(true))
                    .to(OUTPUT_TOPIC);
            runTest(expectedFinalMultiJoinResult, storeName);
        } else {
            final List<List<KeyValueTimestamp<Long, String>>> expectedResult = Arrays.asList(
                null,
                null,
                null,
                Arrays.asList(
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a-a", 4L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a-a", 4L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-a-a", 5L)),
                Arrays.asList(
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b-b", 6L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b-b", 6L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, null, 7L)),
                null,
                null,
                Arrays.asList(
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c-c", 10L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c-c", 10L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, null, 11L)),
                null,
                null,
                null,
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-d-d", 15L))
            );

            leftTable.leftJoin(rightTable, valueJoiner)
                    .join(rightTable, valueJoiner, materialized)
                    .toStream()
                    .to(OUTPUT_TOPIC);

            runTest(expectedResult, storeName);
        }
    }

    @Test
    public void testLeftLeft() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner-left");

        if (cacheEnabled) {
            leftTable.leftJoin(rightTable, valueJoiner)
                    .leftJoin(rightTable, valueJoiner, materialized)
                    .toStream()
                    .peek(new CountingPeek(true))
                    .to(OUTPUT_TOPIC);
            runTest(expectedFinalMultiJoinResult, storeName);
        } else {
            final List<List<KeyValueTimestamp<Long, String>>> expectedResult = Arrays.asList(
                null,
                null,
                null,
                Arrays.asList(
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-null-null", 3L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a-a", 4L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a-a", 4L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-a-a", 5L)),
                Arrays.asList(
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b-b", 6L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b-b", 6L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, null, 7L)),
                null,
                null,
                Arrays.asList(
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-null-null", 9L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c-c", 10L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c-c", 10L)),
                Arrays.asList(
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-null-null", 11L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-null-null", 11L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, null, 12L)),
                null,
                null,
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-d-d", 15L))
            );

            leftTable.leftJoin(rightTable, valueJoiner)
                    .leftJoin(rightTable, valueJoiner, materialized)
                    .toStream()
                    .to(OUTPUT_TOPIC);

            runTest(expectedResult, storeName);
        }
    }

    @Test
    public void testLeftOuter() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner-outer");

        if (cacheEnabled) {
            leftTable.leftJoin(rightTable, valueJoiner)
                    .outerJoin(rightTable, valueJoiner, materialized)
                    .toStream()
                    .peek(new CountingPeek(true))
                    .to(OUTPUT_TOPIC);
            runTest(expectedFinalMultiJoinResult, storeName);
        } else {
            final List<List<KeyValueTimestamp<Long, String>>> expectedResult = Arrays.asList(
                null,
                null,
                null,
                Arrays.asList(
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-null-null", 3L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a-a", 4L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a-a", 4L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-a-a", 5L)),
                Arrays.asList(
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b-b", 6L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b-b", 6L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "null-b", 7L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, null, 8L)),
                null,
                Arrays.asList(
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-null-null", 9L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c-c", 10L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c-c", 10L)),
                Arrays.asList(
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-null-null", 11L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-null-null", 11L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, null, 12L)),
                null,
                null,
                Arrays.asList(
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "null-d", 14L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-d-d", 15L))
            );

            leftTable.leftJoin(rightTable, valueJoiner)
                    .outerJoin(rightTable, valueJoiner, materialized)
                    .toStream().to(OUTPUT_TOPIC);

            runTest(expectedResult, storeName);
        }
    }

    @Test
    public void testOuterInner() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner-inner");

        if (cacheEnabled) {
            leftTable.outerJoin(rightTable, valueJoiner)
                    .join(rightTable, valueJoiner, materialized)
                    .toStream()
                    .peek(new CountingPeek(true))
                    .to(OUTPUT_TOPIC);
            runTest(expectedFinalMultiJoinResult, storeName);
        } else {
            final List<List<KeyValueTimestamp<Long, String>>> expectedResult = Arrays.asList(
                null,
                null,
                null,
                Arrays.asList(
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a-a", 4L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a-a", 4L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-a-a", 5L)),
                Arrays.asList(
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b-b", 6L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b-b", 6L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "null-b-b", 7L)),
                null,
                null,
                Arrays.asList(
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c-c", 10L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c-c", 10L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, null, 11L)),
                null,
                null,
                Arrays.asList(
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "null-d-d", 14L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "null-d-d", 14L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-d-d", 15L))
            );

            leftTable.outerJoin(rightTable, valueJoiner)
                    .join(rightTable, valueJoiner, materialized)
                    .toStream()
                    .to(OUTPUT_TOPIC);

            runTest(expectedResult, storeName);
        }
    }

    @Test
    public void testOuterLeft() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner-left");

        if (cacheEnabled) {
            leftTable.outerJoin(rightTable, valueJoiner)
                    .leftJoin(rightTable, valueJoiner, materialized)
                    .toStream()
                    .peek(new CountingPeek(true))
                    .to(OUTPUT_TOPIC);
            runTest(expectedFinalMultiJoinResult, storeName);
        } else {
            final List<List<KeyValueTimestamp<Long, String>>> expectedResult = Arrays.asList(
                null,
                null,
                null,
                Arrays.asList(
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-null-null", 3L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a-a", 4L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a-a", 4L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-a-a", 5L)),
                Arrays.asList(
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b-b", 6L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b-b", 6L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "null-b-b", 7L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, null, 8L)),
                null,
                Arrays.asList(
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-null-null", 9L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c-c", 10L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c-c", 10L)),
                Arrays.asList(
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-null-null", 11L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-null-null", 11L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, null, 12L)),
                null,
                Arrays.asList(
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "null-d-d", 14L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "null-d-d", 14L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-d-d", 15L))
            );

            leftTable.outerJoin(rightTable, valueJoiner)
                    .leftJoin(rightTable, valueJoiner, materialized)
                    .toStream()
                    .to(OUTPUT_TOPIC);

            runTest(expectedResult, storeName);
        }
    }

    @Test
    public void testOuterOuter() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner-outer");

        if (cacheEnabled) {
            leftTable.outerJoin(rightTable, valueJoiner)
                    .outerJoin(rightTable, valueJoiner, materialized)
                    .toStream()
                    .peek(new CountingPeek(true))
                    .to(OUTPUT_TOPIC);
            runTest(expectedFinalMultiJoinResult, storeName);
        } else {
            final List<List<KeyValueTimestamp<Long, String>>> expectedResult = Arrays.asList(
                null,
                null,
                null,
                Arrays.asList(
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-null-null", 3L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a-a", 4L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a-a", 4L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-a-a", 5L)),
                Arrays.asList(
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b-b", 6L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b-b", 6L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "null-b-b", 7L)),
                Arrays.asList(
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, null, 8L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, null, 8L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-null-null", 9L)),
                Arrays.asList(
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c-c", 10L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c-c", 10L)),
                Arrays.asList(
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-null-null", 11L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-null-null", 11L)),
                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, null, 12L)),
                null,
                null,
                Arrays.asList(
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "null-d-d", 14L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "null-d-d", 14L),
                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-d-d", 15L))
            );

            leftTable.outerJoin(rightTable, valueJoiner)
                    .outerJoin(rightTable, valueJoiner, materialized)
                    .toStream()
                    .to(OUTPUT_TOPIC);

            runTest(expectedResult, storeName);
        }
    }
}
