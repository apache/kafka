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
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.MockMapper;
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
public class JoinIntegrationTest extends AbstractJoinIntegrationTest {
    private KStream<Long, String> leftStream;
    private KStream<Long, String> rightStream;
    private KTable<Long, String> leftTable;
    private KTable<Long, String> rightTable;

    public JoinIntegrationTest(boolean cacheEnabled) {
        super(cacheEnabled);
    }

    @Before
    public void prepareTopology() throws InterruptedException {
        super.prepareEnvironment();

        APP_ID = "stream-stream-join-integration-test";

        builder = new StreamsBuilder();
        leftTable = builder.table(INPUT_TOPIC_LEFT);
        rightTable = builder.table(INPUT_TOPIC_RIGHT);
        leftStream = leftTable.toStream();
        rightStream = rightTable.toStream();
    }

    @Test
    public void testInnerKStreamKStream() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-inner-KStream-KStream");

        final List<List<String>> expectedResult = Arrays.asList(
            null,
            null,
            null,
            Collections.singletonList("A-a"),
            Collections.singletonList("B-a"),
            Arrays.asList("A-b", "B-b"),
            null,
            null,
            Arrays.asList("C-a", "C-b"),
            Arrays.asList("A-c", "B-c", "C-c"),
            null,
            null,
            null,
            Arrays.asList("A-d", "B-d", "C-d"),
            Arrays.asList("D-a", "D-b", "D-c", "D-d")
        );

        leftStream.join(rightStream, valueJoiner, JoinWindows.of(10000)).to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }

    @Test
    public void testInnerRepartitionedKStreamKStream() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-inner-repartitioned-KStream-KStream");

        final List<List<String>> expectedResult = Arrays.asList(
                null,
                null,
                null,
                Collections.singletonList("A-a"),
                Collections.singletonList("B-a"),
                Arrays.asList("A-b", "B-b"),
                null,
                null,
                Arrays.asList("C-a", "C-b"),
                Arrays.asList("A-c", "B-c", "C-c"),
                null,
                null,
                null,
                Arrays.asList("A-d", "B-d", "C-d"),
                Arrays.asList("D-a", "D-b", "D-c", "D-d")
        );

        leftStream.map(MockMapper.<Long, String>noOpKeyValueMapper())
                .join(rightStream.flatMap(MockMapper.<Long, String>noOpFlatKeyValueMapper())
                                 .selectKey(MockMapper.<Long, String>selectKeyKeyValueMapper()),
                       valueJoiner, JoinWindows.of(10000)).to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }

    @Test
    public void testLeftKStreamKStream() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-left-KStream-KStream");

        final List<List<String>> expectedResult = Arrays.asList(
            null,
            null,
            Collections.singletonList("A-null"),
            Collections.singletonList("A-a"),
            Collections.singletonList("B-a"),
            Arrays.asList("A-b", "B-b"),
            null,
            null,
            Arrays.asList("C-a", "C-b"),
            Arrays.asList("A-c", "B-c", "C-c"),
            null,
            null,
            null,
            Arrays.asList("A-d", "B-d", "C-d"),
            Arrays.asList("D-a", "D-b", "D-c", "D-d")
        );

        leftStream.leftJoin(rightStream, valueJoiner, JoinWindows.of(10000)).to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }

    @Test
    public void testLeftRepartitionedKStreamKStream() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-left-repartitioned-KStream-KStream");

        final List<List<String>> expectedResult = Arrays.asList(
                null,
                null,
                Collections.singletonList("A-null"),
                Collections.singletonList("A-a"),
                Collections.singletonList("B-a"),
                Arrays.asList("A-b", "B-b"),
                null,
                null,
                Arrays.asList("C-a", "C-b"),
                Arrays.asList("A-c", "B-c", "C-c"),
                null,
                null,
                null,
                Arrays.asList("A-d", "B-d", "C-d"),
                Arrays.asList("D-a", "D-b", "D-c", "D-d")
        );

        leftStream.map(MockMapper.<Long, String>noOpKeyValueMapper())
                .leftJoin(rightStream.flatMap(MockMapper.<Long, String>noOpFlatKeyValueMapper())
                                     .selectKey(MockMapper.<Long, String>selectKeyKeyValueMapper()),
                        valueJoiner, JoinWindows.of(10000)).to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }

    @Test
    public void testOuterKStreamKStream() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-outer-KStream-KStream");

        final List<List<String>> expectedResult = Arrays.asList(
            null,
            null,
            Collections.singletonList("A-null"),
            Collections.singletonList("A-a"),
            Collections.singletonList("B-a"),
            Arrays.asList("A-b", "B-b"),
            null,
            null,
            Arrays.asList("C-a", "C-b"),
            Arrays.asList("A-c", "B-c", "C-c"),
            null,
            null,
            null,
            Arrays.asList("A-d", "B-d", "C-d"),
            Arrays.asList("D-a", "D-b", "D-c", "D-d")
        );

        leftStream.outerJoin(rightStream, valueJoiner, JoinWindows.of(10000)).to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }

    @Test
    public void testOuterRepartitionedKStreamKStream() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-outer-repartitioned-KStream-KStream");

        final List<List<String>> expectedResult = Arrays.asList(
                null,
                null,
                Collections.singletonList("A-null"),
                Collections.singletonList("A-a"),
                Collections.singletonList("B-a"),
                Arrays.asList("A-b", "B-b"),
                null,
                null,
                Arrays.asList("C-a", "C-b"),
                Arrays.asList("A-c", "B-c", "C-c"),
                null,
                null,
                null,
                Arrays.asList("A-d", "B-d", "C-d"),
                Arrays.asList("D-a", "D-b", "D-c", "D-d")
        );

        leftStream.map(MockMapper.<Long, String>noOpKeyValueMapper())
                .outerJoin(rightStream.flatMap(MockMapper.<Long, String>noOpFlatKeyValueMapper())
                                .selectKey(MockMapper.<Long, String>selectKeyKeyValueMapper()),
                        valueJoiner, JoinWindows.of(10000)).to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }

    @Test
    public void testMultiInnerKStreamKStream() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-multi-inner-KStream-KStream");

        final List<List<String>> expectedResult = Arrays.asList(
                null,
                null,
                null,
                Collections.singletonList("A-a-a"),
                Collections.singletonList("B-a-a"),
                Arrays.asList("A-b-a", "B-b-a", "A-a-b", "B-a-b", "A-b-b", "B-b-b"),
                null,
                null,
                Arrays.asList("C-a-a", "C-a-b", "C-b-a", "C-b-b"),
                Arrays.asList("A-c-a", "A-c-b", "B-c-a", "B-c-b", "C-c-a", "C-c-b", "A-a-c", "B-a-c",
                        "A-b-c", "B-b-c", "C-a-c", "C-b-c", "A-c-c", "B-c-c", "C-c-c"),
                null,
                null,
                null,
                Arrays.asList("A-d-a", "A-d-b", "A-d-c", "B-d-a", "B-d-b", "B-d-c", "C-d-a", "C-d-b", "C-d-c",
                        "A-a-d", "B-a-d", "A-b-d", "B-b-d", "C-a-d", "C-b-d", "A-c-d", "B-c-d", "C-c-d",
                        "A-d-d", "B-d-d", "C-d-d"),
                Arrays.asList("D-a-a", "D-a-b", "D-a-c", "D-a-d", "D-b-a", "D-b-b", "D-b-c", "D-b-d", "D-c-a",
                        "D-c-b", "D-c-c", "D-c-d", "D-d-a", "D-d-b", "D-d-c", "D-d-d")
        );

        leftStream.join(rightStream, valueJoiner, JoinWindows.of(10000))
                .join(rightStream, valueJoiner, JoinWindows.of(10000)).to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }

    @Test
    public void testInnerKStreamKTable() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-inner-KStream-KTable");

        final List<List<String>> expectedResult = Arrays.asList(
            null,
            null,
            null,
            null,
            Collections.singletonList("B-a"),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            Collections.singletonList("D-d")
        );

        leftStream.join(rightTable, valueJoiner).to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }

    @Test
    public void testLeftKStreamKTable() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-left-KStream-KTable");

        final List<List<String>> expectedResult = Arrays.asList(
            null,
            null,
            Collections.singletonList("A-null"),
            null,
            Collections.singletonList("B-a"),
            null,
            null,
            null,
            Collections.singletonList("C-null"),
            null,
            null,
            null,
            null,
            null,
            Collections.singletonList("D-d")
        );

        leftStream.leftJoin(rightTable, valueJoiner).to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }

    @Test
    public void testInnerKTableKTable() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-inner-KTable-KTable");

        // TODO: the duplicate is due to KAFKA-4309, should be removed when it is fixed
        List<List<String>> expectedResult;
        if (cacheEnabled) {
            expectedResult = Arrays.asList(
                    null,
                    null,
                    null,
                    Arrays.asList("A-a", "A-a"),    // dup
                    Collections.singletonList("B-a"),
                    Collections.singletonList("B-b"),
                    Collections.singletonList((String) null),
                    null,
                    Collections.singletonList((String) null),   // dup
                    Collections.singletonList("C-c"),
                    Collections.singletonList((String) null),
                    null,
                    null,
                    Collections.singletonList((String) null),   // dup
                    Collections.singletonList("D-d")
            );
        } else {
            expectedResult = Arrays.asList(
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
        }

        leftTable.join(rightTable, valueJoiner).toStream().to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }

    @Test
    public void testLeftKTableKTable() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-left-KTable-KTable");

        // TODO: the duplicate is due to KAFKA-4309, should be removed when it is fixed
        List<List<String>> expectedResult;
        if (cacheEnabled) {
            expectedResult = Arrays.asList(
                    null,
                    null,
                    Arrays.asList("A-null", "A-null"),  // dup
                    Collections.singletonList("A-a"),
                    Collections.singletonList("B-a"),
                    Collections.singletonList("B-b"),
                    Collections.singletonList((String) null),
                    null,
                    Arrays.asList("C-null", "C-null"),      // dup
                    Collections.singletonList("C-c"),
                    Collections.singletonList("C-null"),
                    Collections.singletonList((String) null),
                    null,
                    null,
                    Arrays.asList("D-d", "D-d")
            );
        } else {
            expectedResult = Arrays.asList(
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
        }

        leftTable.leftJoin(rightTable, valueJoiner).toStream().to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }

    @Test
    public void testOuterKTableKTable() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-outer-KTable-KTable");

        // TODO: the duplicate is due to KAFKA-4309, should be removed when it is fixed
        List<List<String>> expectedResult;
        if (cacheEnabled) {
            expectedResult = Arrays.asList(
                    null,
                    null,
                    Arrays.asList("A-null", "A-null"),
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
        } else {
            expectedResult = Arrays.asList(
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
        }

        leftTable.outerJoin(rightTable, valueJoiner).toStream().to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }
}
