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

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KTable;
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

    public TableTableJoinIntegrationTest(boolean cacheEnabled) {
        super(cacheEnabled);
    }

    @Before
    public void prepareTopology() throws InterruptedException {
        super.prepareEnvironment();

        APP_ID = "table-table-join-integration-test";

        builder = new StreamsBuilder();
        leftTable = builder.table(INPUT_TOPIC_LEFT);
        rightTable = builder.table(INPUT_TOPIC_RIGHT);
    }

    final private String expectedFinalJoinResult = "D-d";
    final private String expectedFinalMultiJoinResult = "D-d-d";

    final private class CountingPeek implements ForeachAction<Long, String> {
        final private String expected;

        CountingPeek(final boolean multiJoin) {
            this.expected = multiJoin ? expectedFinalMultiJoinResult : expectedFinalJoinResult;
        }

        @Override
        public void apply(final Long key, final String value) {
            numRecordsExpected++;
            if (value.equals(expected)) {
                boolean ret = finalResultReached.compareAndSet(false, true);

                if (!ret) {
                    // do nothing; it is possible that we will see multiple duplicates of final results due to KAFKA-4309
                    // TODO: should be removed when KAFKA-4309 is fixed
                }
            }
        }
    }

    @Test
    public void testInner() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-inner");

        if (cacheEnabled) {
            leftTable.join(rightTable, valueJoiner).toStream().peek(new CountingPeek(false)).to(OUTPUT_TOPIC);
            runTest(expectedFinalJoinResult);
        } else {
            List<List<String>> expectedResult = Arrays.asList(
                    null,
                    null,
                    null,
                    Collections.singletonList("A-a"),
                    Collections.singletonList("B-a"),
                    Collections.singletonList("B-b"),
                    Collections.singletonList((String) null),
                    null,
                    null,
                    Collections.singletonList("C-c"),
                    Collections.singletonList((String) null),
                    null,
                    null,
                    null,
                    Collections.singletonList("D-d")
            );

            leftTable.join(rightTable, valueJoiner).toStream().to(OUTPUT_TOPIC);
            runTest(expectedResult);
        }
    }

    @Test
    public void testLeft() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-left");

        if (cacheEnabled) {
            leftTable.leftJoin(rightTable, valueJoiner).toStream().peek(new CountingPeek(false)).to(OUTPUT_TOPIC);
            runTest(expectedFinalJoinResult);
        } else {
            List<List<String>> expectedResult = Arrays.asList(
                    null,
                    null,
                    Collections.singletonList("A-null"),
                    Collections.singletonList("A-a"),
                    Collections.singletonList("B-a"),
                    Collections.singletonList("B-b"),
                    Collections.singletonList((String) null),
                    null,
                    Collections.singletonList("C-null"),
                    Collections.singletonList("C-c"),
                    Collections.singletonList("C-null"),
                    Collections.singletonList((String) null),
                    null,
                    null,
                    Collections.singletonList("D-d")
            );

            leftTable.leftJoin(rightTable, valueJoiner).toStream().to(OUTPUT_TOPIC);
            runTest(expectedResult);
        }
    }

    @Test
    public void testOuter() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-outer");

        if (cacheEnabled) {
            leftTable.outerJoin(rightTable, valueJoiner).toStream().peek(new CountingPeek(false)).to(OUTPUT_TOPIC);
            runTest(expectedFinalJoinResult);
        } else {
            List<List<String>> expectedResult = Arrays.asList(
                    null,
                    null,
                    Collections.singletonList("A-null"),
                    Collections.singletonList("A-a"),
                    Collections.singletonList("B-a"),
                    Collections.singletonList("B-b"),
                    Collections.singletonList("null-b"),
                    Collections.singletonList((String) null),
                    Collections.singletonList("C-null"),
                    Collections.singletonList("C-c"),
                    Collections.singletonList("C-null"),
                    Collections.singletonList((String) null),
                    null,
                    Collections.singletonList("null-d"),
                    Collections.singletonList("D-d")
            );

            leftTable.outerJoin(rightTable, valueJoiner).toStream().to(OUTPUT_TOPIC);
            runTest(expectedResult);
        }
    }

    @Test
    public void testInnerInner() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-inner-inner");

        if (cacheEnabled) {
            leftTable.join(rightTable, valueJoiner)
                    .join(rightTable, valueJoiner)
                    .toStream()
                    .peek(new CountingPeek(true))
                    .to(OUTPUT_TOPIC);
            runTest(expectedFinalMultiJoinResult);
        } else {
            List<List<String>> expectedResult = Arrays.asList(
                    null,
                    null,
                    null,
                    Arrays.asList("A-a-a", "A-a-a"),
                    Collections.singletonList("B-a-a"),
                    Arrays.asList("B-b-b", "B-b-b"),
                    Collections.singletonList((String) null),
                    null,
                    null,
                    Arrays.asList("C-c-c", "C-c-c"),
                    null,
                    null,
                    null,
                    null,
                    Collections.singletonList("D-d-d")
            );

            leftTable.join(rightTable, valueJoiner)
                    .join(rightTable, valueJoiner)
                    .toStream().to(OUTPUT_TOPIC);

            runTest(expectedResult);
        }
    }

    @Test
    public void testInnerLeft() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-inner-left");

        if (cacheEnabled) {
            leftTable.join(rightTable, valueJoiner)
                    .leftJoin(rightTable, valueJoiner)
                    .toStream()
                    .peek(new CountingPeek(true))
                    .to(OUTPUT_TOPIC);
            runTest(expectedFinalMultiJoinResult);
        } else {
            List<List<String>> expectedResult = Arrays.asList(
                    null,
                    null,
                    null,
                    Arrays.asList("A-a-a", "A-a-a"),
                    Collections.singletonList("B-a-a"),
                    Arrays.asList("B-b-b", "B-b-b"),
                    Collections.singletonList((String) null),
                    null,
                    null,
                    Arrays.asList("C-c-c", "C-c-c"),
                    Arrays.asList((String) null, null),
                    null,
                    null,
                    null,
                    Collections.singletonList("D-d-d")
            );

            leftTable.join(rightTable, valueJoiner)
                    .leftJoin(rightTable, valueJoiner)
                    .toStream()
                    .peek(new ForeachAction<Long, String>() {
                        @Override
                        public void apply(Long key, String value) {
                            System.out.println("OUTPUT: " + key + "->" + value);
                        }
                    })
                    .to(OUTPUT_TOPIC);

            runTest(expectedResult);
        }
    }

    @Test
    public void testInnerOuter() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-inner-outer");

        if (cacheEnabled) {
            leftTable.join(rightTable, valueJoiner)
                    .outerJoin(rightTable, valueJoiner)
                    .toStream()
                    .peek(new CountingPeek(true))
                    .to(OUTPUT_TOPIC);
            runTest(expectedFinalMultiJoinResult);
        } else {
            List<List<String>> expectedResult = Arrays.asList(
                    null,
                    null,
                    null,
                    Arrays.asList("A-a-a", "A-a-a"),
                    Collections.singletonList("B-a-a"),
                    Arrays.asList("B-b-b", "B-b-b"),
                    Collections.singletonList("null-b"),
                    Collections.singletonList((String) null),
                    null,
                    Arrays.asList("C-c-c", "C-c-c"),
                    Arrays.asList((String) null, null),
                    null,
                    null,
                    null,
                    Arrays.asList("null-d", "D-d-d")
            );

            leftTable.join(rightTable, valueJoiner)
                    .outerJoin(rightTable, valueJoiner)
                    .toStream().to(OUTPUT_TOPIC);

            runTest(expectedResult);
        }
    }
}
