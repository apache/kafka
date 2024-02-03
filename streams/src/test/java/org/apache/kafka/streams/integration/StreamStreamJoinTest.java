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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import static org.apache.kafka.streams.kstream.JoinWindows.ofTimeDifferenceAndGrace;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class StreamStreamJoinTest {

    private static final ValueJoiner<String, String, String> JOINER = (lv, rv) -> lv + "|" + rv;
    private static final String LEFT = "left";
    private static final String RIGHT = "right";
    private static final String OUT = "out";
    private TopologyTestDriver testDriver;
    private StreamsBuilder builder;
    private KStream<String, String> leftStream;
    private KStream<String, String> rightStream;
    private TestInputTopic<String, String> left;
    private TestInputTopic<String, String> right;
    private TestOutputTopic<String, String> out;
    private static final String KEY = "k";

    @BeforeEach
    void beforeEach() {
        builder = new StreamsBuilder();
        leftStream = builder.stream(LEFT);
        rightStream = builder.stream(RIGHT);
    }

    @Test
    void test() throws Exception {
        leftStream
            .leftJoin(rightStream, JOINER, ofTimeDifferenceAndGrace(Duration.ofMillis(2), Duration.ofMillis(0)))
            .to(OUT);
        initTopology();
        left.pipeInput(KEY, "A", 100);
        right.pipeInput(KEY, "a",  101);
        left.pipeInput(KEY, "X", 0);
        assertEquals(
            Arrays.asList(
                new KeyValue<>(KEY, "A|a")
                //This test currently fails, the X record is also emitted, why?
            ),
            out.readKeyValuesToList()
        );
    }

    private void initTopology() {
        testDriver = new TopologyTestDriver(builder.build(), props());
        left = testDriver.createInputTopic(
            LEFT,
            new StringSerializer(),
            new StringSerializer()
        );
        right = testDriver.createInputTopic(
            RIGHT,
            new StringSerializer(),
            new StringSerializer()
        );
        out = testDriver.createOutputTopic(
            OUT,
            new StringDeserializer(),
            new StringDeserializer()
        );
    }

    private static Properties props() {
        final Properties props = new Properties();
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return props;
    }
}
