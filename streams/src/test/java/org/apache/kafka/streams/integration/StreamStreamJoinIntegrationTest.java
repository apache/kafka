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
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.MockMapper;

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

import static java.time.Duration.ofHours;
import static java.time.Duration.ofSeconds;

/**
 * Tests all available joins of Kafka Streams DSL.
 */
@Category({IntegrationTest.class})
@RunWith(value = Parameterized.class)
public class StreamStreamJoinIntegrationTest extends AbstractJoinIntegrationTest {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(600);
    private KStream<Long, String> leftStream;
    private KStream<Long, String> rightStream;

    public StreamStreamJoinIntegrationTest(final boolean cacheEnabled) {
        super(cacheEnabled);
    }

    @Before
    public void prepareTopology() throws InterruptedException {
        super.prepareEnvironment();

        appID = "stream-stream-join-integration-test";

        builder = new StreamsBuilder();
        leftStream = builder.stream(INPUT_TOPIC_LEFT);
        rightStream = builder.stream(INPUT_TOPIC_RIGHT);
    }

    @Test
    public void testSelfJoin() {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-selfJoin");
        STREAMS_CONFIG.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);

        final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-A", null, 2L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "B-A", null, 3L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-B", null, 3L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-B", null, 3L)),
            null,
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "C-A", null, 5L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-B", null, 5L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-C", null, 5L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-C", null, 5L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-C", null, 5L)),
            null,
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "D-A", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-B", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-C", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-D", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-D", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-D", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-D", null, 7L))
        );

        leftStream.join(
            leftStream,
            valueJoiner,
            JoinWindows.ofTimeDifferenceAndGrace(ofSeconds(10), ofHours(24))
        ).to(OUTPUT_TOPIC);

        runSelfJoinTestWithDriver(expectedResult);
    }

    @Test
    public void testInner() {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner");

        final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-a", null, 4L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a", null, 5L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-b", null, 6L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-b", null, 6L)),
            null,
            null,
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "C-a", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-b", null, 9L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-c", null, 10L)),
            null,
            null,
            null,
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-d", null, 14L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "D-a", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-b", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-c", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-d", null, 15L)),
            null,
            null
        );

        leftStream.join(
            rightStream,
            valueJoiner,
            JoinWindows.ofTimeDifferenceAndGrace(ofSeconds(10), ofHours(24))
        ).to(OUTPUT_TOPIC);

        runTestWithDriver(inputWithoutOutOfOrderData, expectedResult);
    }

    @Test
    public void testInnerRepartitioned() {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner-repartitioned");

        final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-a", null, 4L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a", null, 5L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-b", null, 6L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-b", null, 6L)),
            null,
            null,
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "C-a", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-b", null, 9L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-c", null, 10L)),
            null,
            null,
            null,
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-d", null, 14L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "D-a", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-b", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-c", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-d", null, 15L)),
            null,
            null
        );

        leftStream.map(MockMapper.noOpKeyValueMapper())
            .join(
                rightStream.flatMap(MockMapper.noOpFlatKeyValueMapper())
                    .selectKey(MockMapper.selectKeyKeyValueMapper()),
                valueJoiner,
                JoinWindows.ofTimeDifferenceAndGrace(ofSeconds(10), ofHours(24))
            ).to(OUTPUT_TOPIC);

        runTestWithDriver(inputWithoutOutOfOrderData, expectedResult);
    }

    @Test
    public void testLeft() {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-left");

        final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-a", null, 4L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a", null, 5L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-b", null, 6L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-b", null, 6L)),
            null,
            null,
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "C-a", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-b", null, 9L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-c", null, 10L)),
            null,
            null,
            null,
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-d", null, 14L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "D-a", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-b", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-c", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-d", null, 15L)),
            Arrays.asList(
                new TestRecord<>(null, "E-null", null, 16L)),
            null
        );

        leftStream.leftJoin(
            rightStream,
            valueJoiner,
            JoinWindows.ofTimeDifferenceAndGrace(ofSeconds(10), ofHours(24))
        ).to(OUTPUT_TOPIC);

        runTestWithDriver(inputWithoutOutOfOrderData, expectedResult);
    }

    @Test
    public void testLeftRepartitioned() {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-left-repartitioned");

        final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-a", null, 4L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a", null, 5L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-b", null, 6L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-b", null, 6L)),
            null,
            null,
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "C-a", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-b", null, 9L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-c", null, 10L)),
            null,
            null,
            null,
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-d", null, 14L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "D-a", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-b", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-c", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-d", null, 15L)),
            Arrays.asList(
                new TestRecord<>(null, "E-null", null, 16L)),
            null
        );

        leftStream.map(MockMapper.noOpKeyValueMapper())
            .leftJoin(
                rightStream.flatMap(MockMapper.noOpFlatKeyValueMapper())
                     .selectKey(MockMapper.selectKeyKeyValueMapper()),
                valueJoiner,
                JoinWindows.ofTimeDifferenceAndGrace(ofSeconds(10), ofHours(24))
            ).to(OUTPUT_TOPIC);

        runTestWithDriver(inputWithoutOutOfOrderData, expectedResult);
    }

    @Test
    public void testOuter() {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-outer");

        final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-a", null, 4L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a", null, 5L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-b", null, 6L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-b", null, 6L)),
            null,
            null,
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "C-a", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-b", null, 9L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-c", null, 10L)),
            null,
            null,
            null,
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-d", null, 14L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "D-a", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-b", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-c", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-d", null, 15L)),
            Arrays.asList(
                new TestRecord<>(null, "E-null", null, 16L)),
            Arrays.asList(
                new TestRecord<>(null, "null-e", null, 17L))
        );

        leftStream.outerJoin(
            rightStream,
            valueJoiner,
            JoinWindows.ofTimeDifferenceAndGrace(ofSeconds(10), ofHours(24))
        ).to(OUTPUT_TOPIC);

        runTestWithDriver(inputWithoutOutOfOrderData, expectedResult);
    }

    @Test
    public void testOuterRepartitioned() {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-outer");

        final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-a", null, 4L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a", null, 5L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-b", null, 6L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-b", null, 6L)),
            null,
            null,
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "C-a", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-b", null, 9L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-c", null, 10L)),
            null,
            null,
            null,
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-d", null, 14L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "D-a", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-b", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-c", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-d", null, 15L)),
            Arrays.asList(
                new TestRecord<>(null, "E-null", null, 16L)),
            Arrays.asList(
                new TestRecord<>(null, "null-e", null, 17L))
        );

        leftStream.map(MockMapper.noOpKeyValueMapper())
            .outerJoin(
                rightStream.flatMap(MockMapper.noOpFlatKeyValueMapper())
                    .selectKey(MockMapper.selectKeyKeyValueMapper()),
                valueJoiner,
                JoinWindows.ofTimeDifferenceAndGrace(ofSeconds(10), ofHours(24))
            ).to(OUTPUT_TOPIC);

        runTestWithDriver(inputWithoutOutOfOrderData, expectedResult);
    }

    @Test
    public void testMultiInner() {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-multi-inner");

        final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-a-a", null, 4L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a-a", null, 5L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-b-a", null, 6L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-b-a", null, 6L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-a-b", null, 6L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-a-b", null, 6L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-b-b", null, 6L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-b-b", null, 6L)),
            null,
            null,
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "C-a-a", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-a-b", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-b-a", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-b-b", null, 9L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-c-a", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-c-b", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-c-a", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-c-b", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-c-a", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-c-b", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-a-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-a-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-b-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-b-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-a-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-b-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-c-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-c-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-c-c", null, 10L)),
            null,
            null,
            null,
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-d-a", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-d-b", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-d-c", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-d-a", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-d-b", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-d-c", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-d-a", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-d-b", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-d-c", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-a-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-a-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-b-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-b-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-a-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-b-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-c-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-c-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-c-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-d-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-d-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-d-d", null, 14L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "D-a-a", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-a-b", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-a-c", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-a-d", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-b-a", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-b-b", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-b-c", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-b-d", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-c-a", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-c-b", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-c-c", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-c-d", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-d-a", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-d-b", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-d-c", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-d-d", null, 15L)),
            null,
            null
        );

        leftStream.join(
            rightStream,
            valueJoiner,
            JoinWindows.ofTimeDifferenceAndGrace(ofSeconds(10), ofHours(24))
        ).join(
            rightStream,
            valueJoiner,
            JoinWindows.ofTimeDifferenceAndGrace(ofSeconds(10), ofHours(24))
        ).to(OUTPUT_TOPIC);

        runTestWithDriver(inputWithoutOutOfOrderData, expectedResult);
    }
}
