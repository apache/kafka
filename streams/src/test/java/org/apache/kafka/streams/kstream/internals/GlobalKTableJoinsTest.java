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
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.MockProcessorSupplier;
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
    private final String streamTopic = "stream";
    private final String globalTopic = "global";
    private GlobalKTable<String, String> global;
    private KStream<String, String> stream;
    private KeyValueMapper<String, String, String> keyValueMapper;

    @Before
    public void setUp() {
        final Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());
        global = builder.globalTable(globalTopic, consumed);
        stream = builder.stream(streamTopic, consumed);
        keyValueMapper = (key, value) -> value;
    }

    @Test
    public void shouldLeftJoinWithStream() {
        final MockProcessorSupplier<String, String> supplier = new MockProcessorSupplier<>();
        stream
            .leftJoin(global, keyValueMapper, MockValueJoiner.TOSTRING_JOINER)
            .process(supplier);

        final Map<String, ValueAndTimestamp<String>> expected = new HashMap<>();
        expected.put("1", ValueAndTimestamp.make("a+A", 2L));
        expected.put("2", ValueAndTimestamp.make("b+B", 10L));
        expected.put("3", ValueAndTimestamp.make("c+null", 3L));

        verifyJoin(expected, supplier);
    }

    @Test
    public void shouldInnerJoinWithStream() {
        final MockProcessorSupplier<String, String> supplier = new MockProcessorSupplier<>();
        stream
            .join(global, keyValueMapper, MockValueJoiner.TOSTRING_JOINER)
            .process(supplier);

        final Map<String, ValueAndTimestamp<String>> expected = new HashMap<>();
        expected.put("1", ValueAndTimestamp.make("a+A", 2L));
        expected.put("2", ValueAndTimestamp.make("b+B", 10L));

        verifyJoin(expected, supplier);
    }

    private void verifyJoin(final Map<String, ValueAndTimestamp<String>> expected,
                            final MockProcessorSupplier<String, String> supplier) {
        final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());
        final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            // write some data to the global table
            driver.pipeInput(recordFactory.create(globalTopic, "a", "A", 1L));
            driver.pipeInput(recordFactory.create(globalTopic, "b", "B", 5L));
            //write some data to the stream
            driver.pipeInput(recordFactory.create(streamTopic, "1", "a", 2L));
            driver.pipeInput(recordFactory.create(streamTopic, "2", "b", 10L));
            driver.pipeInput(recordFactory.create(streamTopic, "3", "c", 3L));
        }

        assertEquals(expected, supplier.theCapturedProcessor().lastValueAndTimestampPerKey);
    }
}
