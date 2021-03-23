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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.ValueJoinerWithKey;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static java.time.Duration.ofMillis;
import static org.junit.Assert.assertEquals;

public class KStreamImplValueJoinerWithKeyTest {

    private KStream<String, Integer> leftStream;
    private KStream<String, Integer> rightStream;
    private KTable<String, Integer> ktable;
    private GlobalKTable<String, Integer> globalKTable;
    private StreamsBuilder builder;

    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());
    private final String leftTopic = "left";
    private final String rightTopic = "right";
    private final String ktableTopic = "ktableTopic";
    private final String globalTopic = "globalTopic";
    private final String outputTopic = "joined-result";

    private final ValueJoinerWithKey<String, Integer, Integer, String> valueJoinerWithKey =
        (key, lv, rv) -> key + ":" + (lv + (rv == null ? 0 : rv));
    private final JoinWindows joinWindows = JoinWindows.of(ofMillis(100));
    private final StreamJoined<String, Integer, Integer> streamJoined =
            StreamJoined.with(Serdes.String(), Serdes.Integer(), Serdes.Integer());
    private final Joined<String, Integer, Integer> joined =
            Joined.with(Serdes.String(), Serdes.Integer(), Serdes.Integer());
    private final KeyValueMapper<String, Integer, String> keyValueMapper =
        (k, v) -> k;

    @Before
    public void setup() {
        builder = new StreamsBuilder();
        leftStream = builder.stream(leftTopic, Consumed.with(Serdes.String(), Serdes.Integer()));
        rightStream = builder.stream(rightTopic, Consumed.with(Serdes.String(), Serdes.Integer()));
        ktable = builder.table(ktableTopic, Consumed.with(Serdes.String(), Serdes.Integer()));
        globalKTable = builder.globalTable(globalTopic, Consumed.with(Serdes.String(), Serdes.Integer()));
    }

    @Test
    public void shouldIncludeKeyInStreamSteamJoinResults() {
        leftStream.join(
                rightStream,
                valueJoinerWithKey,
                joinWindows,
                streamJoined
        ).to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
        // Left KV A, 3, Right KV A, 5
        runJoinTopology(builder,
                Collections.singletonList(KeyValue.pair("A", "A:5")),
                false,
                rightTopic
        );
    }

    @Test
    public void shouldIncludeKeyInStreamLeftJoinResults() {
        leftStream.leftJoin(
                rightStream,
                valueJoinerWithKey,
                joinWindows,
                streamJoined
        ).to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
        // Left KV A, 3, Right KV A, 5
        // TTD pipes records to left stream first, then right
        // with TTD there's no caching, so join emits immediately with "A, 3, null" then "A, 3, 5"
        final List<KeyValue<String, String>> expectedResults = Arrays.asList(KeyValue.pair("A", "A:3"), KeyValue.pair("A", "A:5"));
        runJoinTopology(builder,
                expectedResults,
                false,
                rightTopic
        );
    }

    @Test
    public void shouldIncludeKeyInStreamOuterJoinResults() {
        leftStream.outerJoin(
                rightStream,
                valueJoinerWithKey,
                joinWindows,
                streamJoined
        ).to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        // Left KV A, 3, Right KV A, 5
        // TTD pipes records to left stream first, then right
        // with TTD there's no caching, so join emits immediately with "A, 3, null" then "A, 3, 5"
        final List<KeyValue<String, String>> expectedResults = Arrays.asList(KeyValue.pair("A", "A:3"), KeyValue.pair("A", "A:5"));
        runJoinTopology(builder,
                expectedResults,
                false,
                rightTopic
        );
    }

    @Test
    public void shouldIncludeKeyInStreamTableJoinResults() {
        leftStream.join(
            ktable,
            valueJoinerWithKey,
            joined
        ).to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
        // Left KV A, 3, Table KV A, 5
        runJoinTopology(builder,
                Collections.singletonList(KeyValue.pair("A", "A:5")),
                true,
                ktableTopic
        );
    }

    @Test
    public void shouldIncludeKeyInStreamTableLeftJoinResults() {
        leftStream.leftJoin(
            ktable,
            valueJoinerWithKey,
            joined
        ).to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
        // Left KV A, 3, Table KV A, 5
        runJoinTopology(builder,
                Collections.singletonList(KeyValue.pair("A", "A:5")),
                true,
                ktableTopic
        );
    }

    @Test
    public void shouldIncludeKeyInStreamGlobalTableJoinResults() {
        leftStream.join(
            globalKTable,
            keyValueMapper,
            valueJoinerWithKey
        ).to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
        // Left KV A, 3, GlobalTable KV A, 5
        runJoinTopology(builder,
                Collections.singletonList(KeyValue.pair("A", "A:5")),
                true,
                globalTopic
        );
    }

    @Test
    public void shouldIncludeKeyInStreamGlobalTableLeftJoinResults() {
        leftStream.leftJoin(
            globalKTable,
            keyValueMapper,
           valueJoinerWithKey
        ).to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
        // Left KV A, 3, GlobalTable KV A, 5
        runJoinTopology(builder,
                Collections.singletonList(KeyValue.pair("A", "A:5")),
                true,
                globalTopic
        );
    }


    private void runJoinTopology(final StreamsBuilder builder,
                                 final List<KeyValue<String, String>> expectedResults,
                                 final boolean isTableJoin,
                                 final String rightTopic) {
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {

            final TestInputTopic<String, Integer> rightInputTopic =
                    driver.createInputTopic(rightTopic, new StringSerializer(), new IntegerSerializer());

            final TestInputTopic<String, Integer> leftInputTopic =
                    driver.createInputTopic(leftTopic, new StringSerializer(), new IntegerSerializer());

            final TestOutputTopic<String, String> joinResultTopic =
                    driver.createOutputTopic(outputTopic, new StringDeserializer(), new StringDeserializer());

            // with stream table joins need to make sure records hit
            // the table first, joins only triggered from streams side
            if (isTableJoin) {
                rightInputTopic.pipeInput("A", 2);
                leftInputTopic.pipeInput("A", 3);
            } else {
                leftInputTopic.pipeInput("A", 3);
                rightInputTopic.pipeInput("A", 2);
            }

            final List<KeyValue<String, String>> actualResult = joinResultTopic.readKeyValuesToList();
            assertEquals(expectedResults, actualResult);
        }
    }
}
