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

import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
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

import static java.time.Duration.ofSeconds;

/**
 * Tests all available joins of Kafka Streams DSL.
 */
@Category({IntegrationTest.class})
@RunWith(value = Parameterized.class)
public class StreamStreamJoinIntegrationTest extends AbstractJoinIntegrationTest {
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
    public void testInner() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner");

        final List<List<KeyValueTimestamp<Long, String>>> expectedResult = Arrays.asList(
            null,
            null,
            null,
            Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a", 4L)),
            Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-a", 5L)),
            Arrays.asList(
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-b", 6L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b", 6L)),
            null,
            null,
            Arrays.asList(
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-a", 9L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-b", 9L)),
            Arrays.asList(
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-c", 10L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-c", 10L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c", 10L)),
            null,
            null,
            null,
            Arrays.asList(
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-d", 14L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-d", 14L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-d", 14L)),
            Arrays.asList(
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-a", 15L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-b", 15L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-c", 15L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-d", 15L))
        );

        leftStream.join(rightStream, valueJoiner, JoinWindows.of(ofSeconds(10))).to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }

    @Test
    public void testInnerRepartitioned() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner-repartitioned");

        final List<List<KeyValueTimestamp<Long, String>>> expectedResult = Arrays.asList(
            null,
            null,
            null,
            Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a", 4L)),
            Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-a", 5L)),
            Arrays.asList(
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-b", 6L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b", 6L)),
            null,
            null,
            Arrays.asList(
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-a", 9L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-b", 9L)),
            Arrays.asList(
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-c", 10L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-c", 10L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c", 10L)),
            null,
            null,
            null,
            Arrays.asList(
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-d", 14L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-d", 14L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-d", 14L)),
            Arrays.asList(
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-a", 15L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-b", 15L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-c", 15L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-d", 15L))
        );

        leftStream.map(MockMapper.noOpKeyValueMapper())
                .join(rightStream.flatMap(MockMapper.noOpFlatKeyValueMapper())
                                 .selectKey(MockMapper.selectKeyKeyValueMapper()),
                       valueJoiner, JoinWindows.of(ofSeconds(10))).to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }

    @Test
    public void testLeft() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-left");

        final List<List<KeyValueTimestamp<Long, String>>> expectedResult = Arrays.asList(
            null,
            null,
            Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-null", 3L)),
            Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a", 4L)),
            Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-a", 5L)),
            Arrays.asList(
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-b", 6L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b", 6L)),
            null,
            null,
            Arrays.asList(
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-a", 9L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-b", 9L)),
            Arrays.asList(
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-c", 10L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-c", 10L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c", 10L)),
            null,
            null,
            null,
            Arrays.asList(
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-d", 14L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-d", 14L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-d", 14L)),
            Arrays.asList(
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-a", 15L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-b", 15L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-c", 15L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-d", 15L))
        );

        leftStream.leftJoin(rightStream, valueJoiner, JoinWindows.of(ofSeconds(10))).to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }

    @Test
    public void testLeftRepartitioned() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-left-repartitioned");

        final List<List<KeyValueTimestamp<Long, String>>> expectedResult = Arrays.asList(
            null,
            null,
            Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-null", 3L)),
            Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a", 4L)),
            Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-a", 5L)),
            Arrays.asList(
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-b", 6L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b", 6L)),
            null,
            null,
            Arrays.asList(
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-a", 9L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-b", 9L)),
            Arrays.asList(
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-c", 10L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-c", 10L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c", 10L)),
            null,
            null,
            null,
            Arrays.asList(
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-d", 14L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-d", 14L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-d", 14L)),
            Arrays.asList(
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-a", 15L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-b", 15L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-c", 15L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-d", 15L))
        );

        leftStream.map(MockMapper.noOpKeyValueMapper())
                .leftJoin(rightStream.flatMap(MockMapper.noOpFlatKeyValueMapper())
                                     .selectKey(MockMapper.selectKeyKeyValueMapper()),
                        valueJoiner, JoinWindows.of(ofSeconds(10))).to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }

    @Test
    public void testOuter() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-outer");

        final List<List<KeyValueTimestamp<Long, String>>> expectedResult = Arrays.asList(
            null,
            null,
            Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-null", 3L)),
            Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a", 4L)),
            Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-a", 5L)),
            Arrays.asList(
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-b", 6L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b", 6L)),
            null,
            null,
            Arrays.asList(
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-a", 9L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-b", 9L)),
            Arrays.asList(
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-c", 10L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-c", 10L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c", 10L)),
            null,
            null,
            null,
            Arrays.asList(
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-d", 14L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-d", 14L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-d", 14L)),
            Arrays.asList(
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-a", 15L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-b", 15L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-c", 15L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-d", 15L))
        );

        leftStream.outerJoin(rightStream, valueJoiner, JoinWindows.of(ofSeconds(10))).to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }

    @Test
    public void testOuterRepartitioned() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-outer");

        final List<List<KeyValueTimestamp<Long, String>>> expectedResult = Arrays.asList(
            null,
            null,
            Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-null", 3L)),
            Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a", 4L)),
            Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-a", 5L)),
            Arrays.asList(
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-b", 6L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b", 6L)),
            null,
            null,
            Arrays.asList(
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-a", 9L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-b", 9L)),
            Arrays.asList(
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-c", 10L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-c", 10L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c", 10L)),
            null,
            null,
            null,
            Arrays.asList(
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-d", 14L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-d", 14L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-d", 14L)),
            Arrays.asList(
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-a", 15L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-b", 15L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-c", 15L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-d", 15L))
        );

        leftStream.map(MockMapper.noOpKeyValueMapper())
                .outerJoin(rightStream.flatMap(MockMapper.noOpFlatKeyValueMapper())
                                .selectKey(MockMapper.selectKeyKeyValueMapper()),
                        valueJoiner, JoinWindows.of(ofSeconds(10))).to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }

    @Test
    public void testMultiInner() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-multi-inner");

        final List<List<KeyValueTimestamp<Long, String>>> expectedResult = Arrays.asList(
            null,
            null,
            null,
            Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a-a", 4L)),
            Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-a-a", 5L)),
            Arrays.asList(
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-b-a", 6L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b-a", 6L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a-b", 6L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-a-b", 6L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-b-b", 6L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b-b", 6L)),
            null,
            null,
            Arrays.asList(
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-a-a", 9L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-a-b", 9L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-b-a", 9L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-b-b", 9L)),
            Arrays.asList(
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-c-a", 10L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-c-b", 10L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-c-a", 10L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-c-b", 10L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c-a", 10L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c-b", 10L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a-c", 10L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-a-c", 10L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-b-c", 10L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b-c", 10L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-a-c", 10L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-b-c", 10L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-c-c", 10L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-c-c", 10L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c-c", 10L)),
            null,
            null,
            null,
            Arrays.asList(
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-d-a", 14L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-d-b", 14L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-d-c", 14L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-d-a", 14L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-d-b", 14L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-d-c", 14L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-d-a", 14L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-d-b", 14L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-d-c", 14L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a-d", 14L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-a-d", 14L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-b-d", 14L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b-d", 14L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-a-d", 14L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-b-d", 14L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-c-d", 14L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-c-d", 14L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c-d", 14L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-d-d", 14L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-d-d", 14L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-d-d", 14L)),
            Arrays.asList(
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-a-a", 15L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-a-b", 15L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-a-c", 15L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-a-d", 15L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-b-a", 15L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-b-b", 15L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-b-c", 15L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-b-d", 15L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-c-a", 15L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-c-b", 15L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-c-c", 15L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-c-d", 15L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-d-a", 15L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-d-b", 15L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-d-c", 15L),
                new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-d-d", 15L))
        );

        leftStream.join(rightStream, valueJoiner, JoinWindows.of(ofSeconds(10)))
                .join(rightStream, valueJoiner, JoinWindows.of(ofSeconds(10))).to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }
}
