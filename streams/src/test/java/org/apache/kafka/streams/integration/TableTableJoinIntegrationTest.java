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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.MockKeyValueMapper;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

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

    private List<List<String>> dedupExpectedJoinResult = Arrays.asList(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            Arrays.asList("D-d", "D-d")
    );

    private List<List<String>> dedupExpectedMultiJoinResult = Arrays.asList(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            Arrays.asList("D-d-d", "D-d-d")
    );

    @Test
    public void testInner() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-inner");

        // TODO: the duplicate is due to KAFKA-4309, should be removed when it is fixed
        List<List<String>> expectedResult;
        if (cacheEnabled) {
            expectedResult = dedupExpectedJoinResult;
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
    public void testLeft() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-left");

        // TODO: the duplicate is due to KAFKA-4309, should be removed when it is fixed
        List<List<String>> expectedResult;
        if (cacheEnabled) {
            expectedResult = dedupExpectedJoinResult;
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
    public void testOuter() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-outer");

        // TODO: the duplicate is due to KAFKA-4309, should be removed when it is fixed
        List<List<String>> expectedResult;
        if (cacheEnabled) {
            expectedResult = dedupExpectedJoinResult;
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

    @Test
    public void testInnerInner() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-inner-inner");

        // TODO: the duplicate is due to KAFKA-4309, should be removed when it is fixed
        List<List<String>> expectedResult;
        if (cacheEnabled) {
            expectedResult = dedupExpectedMultiJoinResult;
        } else {
            expectedResult = Arrays.asList(
                    null,
                    null,
                    null,
                    Collections.singletonList("A-a-a"),
                    Collections.singletonList("B-a-a"),
                    Collections.singletonList("B-b-b"),
                    Collections.singletonList((String) null),
                    null,
                    null,
                    Collections.singletonList("C-c-c"),
                    Collections.singletonList((String) null),
                    null,
                    null,
                    null,
                    Collections.singletonList("D-d-d")
            );
        }

        leftTable.join(rightTable, valueJoiner)
                 .groupBy(MockKeyValueMapper.<Long, String>NoOpKeyValueMapper())
                 .reduce(reducer, reducer)
                 .join(rightTable, valueJoiner)
                 .toStream().to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }

    @Test
    public void testInnerLeft() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-inner-left");

        // TODO: the duplicate is due to KAFKA-4309, should be removed when it is fixed
        List<List<String>> expectedResult;
        if (cacheEnabled) {
            expectedResult = dedupExpectedMultiJoinResult;
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

        leftTable.join(rightTable, valueJoiner)
                .groupBy(MockKeyValueMapper.<Long, String>NoOpKeyValueMapper())
                .reduce(reducer, reducer)
                .leftJoin(rightTable, valueJoiner)
                .toStream().to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }

    @Test
    public void testInnerOuter() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-inner-outer");

        // TODO: the duplicate is due to KAFKA-4309, should be removed when it is fixed
        List<List<String>> expectedResult;
        if (cacheEnabled) {
            expectedResult = dedupExpectedMultiJoinResult;
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

        leftTable.join(rightTable, valueJoiner)
                .groupBy(MockKeyValueMapper.<Long, String>NoOpKeyValueMapper())
                .reduce(reducer, reducer)
                .leftJoin(rightTable, valueJoiner)
                .toStream().to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }
}
