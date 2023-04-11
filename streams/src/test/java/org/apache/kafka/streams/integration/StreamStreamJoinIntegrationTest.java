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
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "E-a", null, 4L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-b", null, 6L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-d", null, 14L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-e", null, 3L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-e", null, 4L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-e", null, 5L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-e", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-e", null, 15L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-f", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-f", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-f", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-f", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-f", null, 15L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "F-e", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-a", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-b", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-f", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-d", null, 14L))
        );

        leftStream.join(
            rightStream,
            valueJoiner,
            JoinWindows.ofTimeDifferenceAndGrace(ofSeconds(10), ofHours(24))
        ).to(OUTPUT_TOPIC);

        runTestWithDriver(expectedResult);
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
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "E-a", null, 4L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-b", null, 6L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-d", null, 14L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-e", null, 3L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-e", null, 4L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-e", null, 5L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-e", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-e", null, 15L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-f", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-f", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-f", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-f", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-f", null, 15L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "F-e", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-a", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-b", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-f", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-d", null, 14L))
        );

        leftStream.map(MockMapper.noOpKeyValueMapper())
            .join(
                rightStream.flatMap(MockMapper.noOpFlatKeyValueMapper())
                    .selectKey(MockMapper.selectKeyKeyValueMapper()),
                valueJoiner,
                JoinWindows.ofTimeDifferenceAndGrace(ofSeconds(10), ofHours(24))
            ).to(OUTPUT_TOPIC);

        runTestWithDriver(expectedResult);
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
                new TestRecord<>(ANY_UNIQUE_KEY, "E-a", null, 4L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-b", null, 6L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-d", null, 14L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-e", null, 3L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-e", null, 4L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-e", null, 5L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-e", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-e", null, 15L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-f", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-f", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-f", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-f", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-f", null, 15L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "F-e", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-a", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-b", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-f", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-d", null, 14L))
        );

        leftStream.leftJoin(
            rightStream,
            valueJoiner,
            JoinWindows.ofTimeDifferenceAndGrace(ofSeconds(10), ofHours(24))
        ).to(OUTPUT_TOPIC);

        runTestWithDriver(expectedResult);
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
                new TestRecord<>(ANY_UNIQUE_KEY, "E-a", null, 4L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-b", null, 6L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-d", null, 14L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-e", null, 3L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-e", null, 4L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-e", null, 5L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-e", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-e", null, 15L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-f", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-f", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-f", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-f", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-f", null, 15L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "F-e", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-a", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-b", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-f", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-d", null, 14L))
        );

        leftStream.map(MockMapper.noOpKeyValueMapper())
            .leftJoin(
                rightStream.flatMap(MockMapper.noOpFlatKeyValueMapper())
                     .selectKey(MockMapper.selectKeyKeyValueMapper()),
                valueJoiner,
                JoinWindows.ofTimeDifferenceAndGrace(ofSeconds(10), ofHours(24))
            ).to(OUTPUT_TOPIC);

        runTestWithDriver(expectedResult);
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
                new TestRecord<>(ANY_UNIQUE_KEY, "E-a", null, 4L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-b", null, 6L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-d", null, 14L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-e", null, 3L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-e", null, 4L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-e", null, 5L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-e", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-e", null, 15L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-f", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-f", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-f", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-f", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-f", null, 15L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "F-e", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-a", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-b", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-f", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-d", null, 14L))
        );

        leftStream.outerJoin(
            rightStream,
            valueJoiner,
            JoinWindows.ofTimeDifferenceAndGrace(ofSeconds(10), ofHours(24))
        ).to(OUTPUT_TOPIC);

        runTestWithDriver(expectedResult);
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
                new TestRecord<>(ANY_UNIQUE_KEY, "E-a", null, 4L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-b", null, 6L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-d", null, 14L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-e", null, 3L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-e", null, 4L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-e", null, 5L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-e", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-e", null, 15L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-f", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-f", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-f", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-f", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-f", null, 15L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "F-e", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-a", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-b", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-f", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-d", null, 14L))
        );

        leftStream.map(MockMapper.noOpKeyValueMapper())
            .outerJoin(
                rightStream.flatMap(MockMapper.noOpFlatKeyValueMapper())
                    .selectKey(MockMapper.selectKeyKeyValueMapper()),
                valueJoiner,
                JoinWindows.ofTimeDifferenceAndGrace(ofSeconds(10), ofHours(24))
            ).to(OUTPUT_TOPIC);

        runTestWithDriver(expectedResult);
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
            Arrays.<TestRecord<Long, String>>asList(
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
            Arrays.<TestRecord<Long, String>>asList(
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
            Arrays.<TestRecord<Long, String>>asList(
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
            Arrays.<TestRecord<Long, String>>asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "E-a-a", null, 4L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-a-b", null, 6L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-a-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-a-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-b-a", null, 6L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-b-b", null, 6L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-b-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-b-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-c-a", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-c-b", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-c-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-c-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-d-a", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-d-b", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-d-c", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-d-d", null, 14L)),
            Arrays.<TestRecord<Long, String>>asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-e-a", null, 4L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-e-b", null, 6L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-e-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-e-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-e-a", null, 4L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-e-b", null, 6L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-e-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-e-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-e-a", null, 5L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-e-b", null, 6L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-e-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-e-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-e-a", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-e-b", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-e-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-e-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-e-a", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-e-b", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-e-c", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-e-d", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-e-e", null, 3L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-a-e", null, 4L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-a-e", null, 4L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-e-e", null, 4L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-a-e", null, 5L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-e-e", null, 5L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-b-e", null, 6L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-b-e", null, 6L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-b-e", null, 6L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-a-e", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-b-e", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-e-e", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-c-e", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-c-e", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-c-e", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-c-e", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-d-e", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-d-e", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-d-e", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-d-e", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-a-e", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-b-e", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-c-e", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-d-e", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-e-e", null, 15L)),
            Arrays.<TestRecord<Long, String>>asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-f-e", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-f-a", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-f-b", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-f-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-f-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-f-e", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-f-a", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-f-b", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-f-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-f-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-f-e", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-f-a", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-f-b", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-f-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-f-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-f-e", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-f-a", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-f-b", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-f-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-f-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-f-e", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-f-a", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-f-b", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-f-c", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-f-d", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-e-f", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-a-f", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-a-f", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-e-f", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-a-f", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-e-f", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-b-f", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-b-f", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-b-f", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-f-f", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-f-f", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-f-f", null, 7L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-a-f", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-b-f", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-e-f", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-f-f", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-c-f", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-c-f", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-c-f", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-c-f", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-d-f", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-d-f", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-d-f", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "E-d-f", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-a-f", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-b-f", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-c-f", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-d-f", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-e-f", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-f-f", null, 15L)),
            Arrays.<TestRecord<Long, String>>asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "F-e-e", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-e-a", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-e-b", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-e-f", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-e-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-e-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-a-e", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-a-a", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-a-b", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-a-f", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-a-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-a-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-b-e", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-b-a", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-b-b", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-b-f", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-b-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-b-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-f-e", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-f-a", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-f-b", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-f-f", null, 8L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-f-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-f-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-c-e", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-c-a", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-c-b", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-c-f", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-c-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-c-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-d-e", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-d-a", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-d-b", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-d-f", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-d-c", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "F-d-d", null, 14L))
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

        runTestWithDriver(expectedResult);
    }
}
