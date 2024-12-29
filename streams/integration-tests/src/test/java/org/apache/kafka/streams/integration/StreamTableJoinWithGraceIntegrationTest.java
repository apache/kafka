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
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.test.TestRecord;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Tests all available joins of Kafka Streams DSL.
 */
@Tag("integration")
@Timeout(600)
public class StreamTableJoinWithGraceIntegrationTest extends AbstractJoinIntegrationTest {
    private static final String STORE_NAME = "table-store";
    private static final String APP_ID = "stream-table-join-integration-test";
    private static final Joined<Long, String, String> JOINED =
            Joined.with(Serdes.Long(), Serdes.String(), Serdes.String(), "Grace", Duration.ofMillis(2));

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testInnerWithVersionedStore(final boolean cacheEnabled) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Long, String> leftStream = builder.stream(INPUT_TOPIC_LEFT);
        final KTable<Long, String> rightTable = builder.table(INPUT_TOPIC_RIGHT, Materialized.as(
                Stores.persistentVersionedKeyValueStore(STORE_NAME, Duration.ofMinutes(5))));
        final Properties streamsConfig = setupConfigsAndUtils(cacheEnabled);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-inner");

        leftStream.join(rightTable, valueJoiner, JOINED).to(OUTPUT_TOPIC);

        final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
            null,
            null,
            null,
            null,
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
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "D-b", null,  6L)),
            null,
            null,
            null,
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "F-a", null,  4L)),
            null
        );

        runTestWithDriver(input, expectedResult, streamsConfig, builder.build(streamsConfig));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testLeftWithVersionedStore(final boolean cacheEnabled) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Long, String> leftStream = builder.stream(INPUT_TOPIC_LEFT);
        final KTable<Long, String> rightTable = builder.table(INPUT_TOPIC_RIGHT, Materialized.as(
                Stores.persistentVersionedKeyValueStore(STORE_NAME, Duration.ofMinutes(5))));
        final Properties streamsConfig = setupConfigsAndUtils(cacheEnabled);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-left");
        leftStream.leftJoin(rightTable, valueJoiner, JOINED).to(OUTPUT_TOPIC);

        final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
            null,
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-null", null, 3L)),
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a", null, 5L)),
            null,
            null,
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "D-b", null, 6L)),
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "C-null", null, 9L)),
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "F-a", null,  4L)),
            null
        );

        runTestWithDriver(input, expectedResult, streamsConfig, builder.build(streamsConfig));
    }
}