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
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
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
public class StreamTableJoinIntegrationTest extends AbstractJoinIntegrationTest {

    private static final String STORE_NAME = "table-store";

    @Rule
    public Timeout globalTimeout = Timeout.seconds(600);
    private KStream<Long, String> leftStream;
    private KTable<Long, String> rightTable;

    public StreamTableJoinIntegrationTest(final boolean cacheEnabled) {
        super(cacheEnabled);
    }

    @Before
    public void prepareTopology() throws InterruptedException {
        super.prepareEnvironment();

        appID = "stream-table-join-integration-test";

        builder = new StreamsBuilder();
    }

    @Test
    public void testInner() {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner");

        leftStream = builder.stream(INPUT_TOPIC_LEFT);
        rightTable = builder.table(INPUT_TOPIC_RIGHT);
        leftStream.join(rightTable, valueJoiner).to(OUTPUT_TOPIC);

        final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
            null,
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a", null, 5L)),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "D-d", null,  6L)),
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "E-e", null,  15L)),
            null,
            null,
            null,
            null
        );

        runTestWithDriver(input, expectedResult);
    }

    @Test
    public void testLeft() {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-left");

        leftStream = builder.stream(INPUT_TOPIC_LEFT);
        rightTable = builder.table(INPUT_TOPIC_RIGHT);
        leftStream.leftJoin(rightTable, valueJoiner).to(OUTPUT_TOPIC);

        final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-null", null, 3L)),
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a", null, 5L)),
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "C-null", null, 9L)),
            null,
            null,
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "D-d", null, 6L)),
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "E-e", null,  15L)),
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "F-null", null,  4L)),
            null
        );

        runTestWithDriver(input, expectedResult);
    }

    @Test
    public void testInnerWithVersionedStore() {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner");

        leftStream = builder.stream(INPUT_TOPIC_LEFT);
        rightTable = builder.table(INPUT_TOPIC_RIGHT, Materialized.as(
            Stores.persistentVersionedKeyValueStore(STORE_NAME, Duration.ofMinutes(5))));
        leftStream.join(rightTable, valueJoiner).to(OUTPUT_TOPIC);

        final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
            null,
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a", null, 5L)),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "D-b", null,  6L)),
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "E-e", null,  15L)),
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "F-a", null,  4L)),
            null
        );

        runTestWithDriver(input, expectedResult);
    }

    @Test
    public void testLeftWithVersionedStore() {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-left");

        leftStream = builder.stream(INPUT_TOPIC_LEFT);
        rightTable = builder.table(INPUT_TOPIC_RIGHT, Materialized.as(
            Stores.persistentVersionedKeyValueStore(STORE_NAME, Duration.ofMinutes(5))));
        leftStream.leftJoin(rightTable, valueJoiner).to(OUTPUT_TOPIC);

        final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-null", null, 3L)),
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a", null, 5L)),
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "C-null", null, 9L)),
            null,
            null,
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "D-b", null, 6L)),
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "E-e", null,  15L)),
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "F-a", null,  4L)),
            null
        );

        runTestWithDriver(input, expectedResult);
    }
}
