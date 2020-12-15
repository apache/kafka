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
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.test.TestRecord;
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
public class StreamTableJoinIntegrationTest extends AbstractJoinIntegrationTest {
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
        rightTable = builder.table(INPUT_TOPIC_RIGHT);
        leftStream = builder.stream(INPUT_TOPIC_LEFT);
    }

    @Test
    public void testInner() {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner");

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
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "D-d", null,  15L))
        );

        leftStream.join(rightTable, valueJoiner).to(OUTPUT_TOPIC);
        runTestWithDriver(expectedResult);
    }

    @Test
    public void testLeft() {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-left");

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
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "D-d", null, 15L))
        );

        leftStream.leftJoin(rightTable, valueJoiner).to(OUTPUT_TOPIC);

        runTestWithDriver(expectedResult);
    }
}
