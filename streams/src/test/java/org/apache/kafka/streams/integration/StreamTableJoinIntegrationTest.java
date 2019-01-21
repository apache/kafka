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

import org.apache.kafka.streams.KafkaStreamsWrapper;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;


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
    public void testShouldAutoShutdownOnIncompleteMetadata() throws InterruptedException {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-incomplete");

        final KStream<Long, String> notExistStream = builder.stream(INPUT_TOPIC_LEFT + "-not-existed");

        final KTable<Long, String> aggregatedTable = notExistStream.leftJoin(rightTable, valueJoiner)
                .groupBy((key, value) -> key)
                .reduce((value1, value2) -> value1 + value2);

        // Write the (continuously updating) results to the output topic.
        aggregatedTable.toStream().to(OUTPUT_TOPIC);

        final KafkaStreamsWrapper streams = new KafkaStreamsWrapper(builder.build(), STREAMS_CONFIG);
        final IntegrationTestUtils.StateListenerStub listener = new IntegrationTestUtils.StateListenerStub();
        streams.setStreamThreadStateListener(listener);
        streams.start();

        TestUtils.waitForCondition(listener::revokedToPendingShutdownSeen, "Did not seen thread state transited to PENDING_SHUTDOWN");

        streams.close();
        assertEquals(listener.createdToRevokedSeen(), true);
        assertEquals(listener.revokedToPendingShutdownSeen(), true);
    }

    @Test
    public void testInner() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner");

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
    public void testLeft() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-left");

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
}
