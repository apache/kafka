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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;


public class GlobalKTableJoinsTest {

    private final StreamsBuilder builder = new StreamsBuilder();
    private final Map<String, String> results = new HashMap<>();
    private final String streamTopic = "stream";
    private final String globalTopic = "global";
    private GlobalKTable<String, String> global;
    private KStream<String, String> stream;
    private KeyValueMapper<String, String, String> keyValueMapper;
    private ForeachAction<String, String> action;


    @Before
    public void setUp() {
        final Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());
        global = builder.globalTable(globalTopic, consumed);
        stream = builder.stream(streamTopic, consumed);
        keyValueMapper = new KeyValueMapper<String, String, String>() {
            @Override
            public String apply(final String key, final String value) {
                return value;
            }
        };
        action = new ForeachAction<String, String>() {
            @Override
            public void apply(final String key, final String value) {
                results.put(key, value);
            }
        };
    }

    @Test
    public void shouldLeftJoinWithStream() {
        stream
            .leftJoin(global, keyValueMapper, MockValueJoiner.TOSTRING_JOINER)
            .foreach(action);

        final Map<String, String> expected = new HashMap<>();
        expected.put("1", "a+A");
        expected.put("2", "b+B");
        expected.put("3", "c+null");

        verifyJoin(expected);

    }

    @Test
    public void shouldInnerJoinWithStream() {
        stream
            .join(global, keyValueMapper, MockValueJoiner.TOSTRING_JOINER)
            .foreach(action);

        final Map<String, String> expected = new HashMap<>();
        expected.put("1", "a+A");
        expected.put("2", "b+B");

        verifyJoin(expected);
    }

    private void verifyJoin(final Map<String, String> expected) {
        final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());
        final Properties props = StreamsTestUtils.topologyTestConfig(Serdes.String(), Serdes.String());

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            // write some data to the global table
            driver.pipeInput(recordFactory.create(globalTopic, "a", "A"));
            driver.pipeInput(recordFactory.create(globalTopic, "b", "B"));
            //write some data to the stream
            driver.pipeInput(recordFactory.create(streamTopic, "1", "a"));
            driver.pipeInput(recordFactory.create(streamTopic, "2", "b"));
            driver.pipeInput(recordFactory.create(streamTopic, "3", "c"));
        }

        assertEquals(expected, results);
    }
}
