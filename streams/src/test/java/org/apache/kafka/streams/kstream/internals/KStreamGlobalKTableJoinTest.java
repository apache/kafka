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
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TopologyWrapper;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.MockProcessor;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class KStreamGlobalKTableJoinTest {
    private final static String[] EMPTY = new String[0];

    private final String streamTopic = "streamTopic";
    private final String globalTableTopic = "globalTableTopic";
    private final int[] expectedKeys = {0, 1, 2, 3};

    private TopologyTestDriver driver;
    private MockProcessor<Integer, String> processor;
    private StreamsBuilder builder;

    @Before
    public void setUp() {

        builder = new StreamsBuilder();
        final KStream<Integer, String> stream;
        final GlobalKTable<String, String> table; // value of stream optionally contains key of table
        final KeyValueMapper<Integer, String, String> keyMapper;

        final MockProcessorSupplier<Integer, String> supplier = new MockProcessorSupplier<>();
        final Consumed<Integer, String> streamConsumed = Consumed.with(Serdes.Integer(), Serdes.String());
        final Consumed<String, String> tableConsumed = Consumed.with(Serdes.String(), Serdes.String());
        stream = builder.stream(streamTopic, streamConsumed);
        table = builder.globalTable(globalTableTopic, tableConsumed);
        keyMapper = (key, value) -> {
            final String[] tokens = value.split(",");
            // Value is comma delimited. If second token is present, it's the key to the global ktable.
            // If not present, use null to indicate no match
            return tokens.length > 1 ? tokens[1] : null;
        };
        stream.join(table, keyMapper, MockValueJoiner.TOSTRING_JOINER).process(supplier);

        final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.Integer(), Serdes.String());
        driver = new TopologyTestDriver(builder.build(), props);

        processor = supplier.theCapturedProcessor();
    }

    @After
    public void cleanup() {
        driver.close();
    }

    private void pushToStream(final int messageCount, final String valuePrefix, final boolean includeForeignKey) {
        final ConsumerRecordFactory<Integer, String> recordFactory =
            new ConsumerRecordFactory<>(new IntegerSerializer(), new StringSerializer(), 0L, 1L);
        for (int i = 0; i < messageCount; i++) {
            String value = valuePrefix + expectedKeys[i];
            if (includeForeignKey) {
                value = value + ",FKey" + expectedKeys[i];
            }
            driver.pipeInput(recordFactory.create(streamTopic, expectedKeys[i], value));
        }
    }

    private void pushToGlobalTable(final int messageCount, final String valuePrefix) {
        final ConsumerRecordFactory<String, String> recordFactory =
            new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());
        for (int i = 0; i < messageCount; i++) {
            driver.pipeInput(recordFactory.create(globalTableTopic, "FKey" + expectedKeys[i], valuePrefix + expectedKeys[i]));
        }
    }

    private void pushNullValueToGlobalTable(final int messageCount) {
        final ConsumerRecordFactory<String, String> recordFactory =
            new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());
        for (int i = 0; i < messageCount; i++) {
            driver.pipeInput(recordFactory.create(globalTableTopic, "FKey" + expectedKeys[i], (String) null));
        }
    }

    @Test
    public void shouldNotRequireCopartitioning() {
        final Collection<Set<String>> copartitionGroups =
            TopologyWrapper.getInternalTopologyBuilder(builder.build()).copartitionGroups();

        assertEquals("KStream-GlobalKTable joins do not need to be co-partitioned", 0, copartitionGroups.size());
    }

    @Test
    public void shouldNotJoinWithEmptyGlobalTableOnStreamUpdates() {

        // push two items to the primary stream. the globalTable is empty

        pushToStream(2, "X", true);
        processor.checkAndClearProcessResult(EMPTY);
    }

    @Test
    public void shouldNotJoinOnGlobalTableUpdates() {

        // push two items to the primary stream. the globalTable is empty

        pushToStream(2, "X", true);
        processor.checkAndClearProcessResult(EMPTY);

        // push two items to the globalTable. this should not produce any item.

        pushToGlobalTable(2, "Y");
        processor.checkAndClearProcessResult(EMPTY);

        // push all four items to the primary stream. this should produce two items.

        pushToStream(4, "X", true);
        processor.checkAndClearProcessResult("0:X0,FKey0+Y0 (ts: 0)", "1:X1,FKey1+Y1 (ts: 1)");

        // push all items to the globalTable. this should not produce any item

        pushToGlobalTable(4, "YY");
        processor.checkAndClearProcessResult(EMPTY);

        // push all four items to the primary stream. this should produce four items.

        pushToStream(4, "X", true);
        processor.checkAndClearProcessResult("0:X0,FKey0+YY0 (ts: 0)", "1:X1,FKey1+YY1 (ts: 1)", "2:X2,FKey2+YY2 (ts: 2)", "3:X3,FKey3+YY3 (ts: 3)");

        // push all items to the globalTable. this should not produce any item

        pushToGlobalTable(4, "YYY");
        processor.checkAndClearProcessResult(EMPTY);
    }

    @Test
    public void shouldJoinOnlyIfMatchFoundOnStreamUpdates() {

        // push two items to the globalTable. this should not produce any item.

        pushToGlobalTable(2, "Y");
        processor.checkAndClearProcessResult(EMPTY);

        // push all four items to the primary stream. this should produce two items.

        pushToStream(4, "X", true);
        processor.checkAndClearProcessResult("0:X0,FKey0+Y0 (ts: 0)", "1:X1,FKey1+Y1 (ts: 1)");

    }

    @Test
    public void shouldClearGlobalTableEntryOnNullValueUpdates() {

        // push all four items to the globalTable. this should not produce any item.

        pushToGlobalTable(4, "Y");
        processor.checkAndClearProcessResult(EMPTY);

        // push all four items to the primary stream. this should produce four items.

        pushToStream(4, "X", true);
        processor.checkAndClearProcessResult("0:X0,FKey0+Y0 (ts: 0)", "1:X1,FKey1+Y1 (ts: 1)", "2:X2,FKey2+Y2 (ts: 2)", "3:X3,FKey3+Y3 (ts: 3)");

        // push two items with null to the globalTable as deletes. this should not produce any item.

        pushNullValueToGlobalTable(2);
        processor.checkAndClearProcessResult(EMPTY);

        // push all four items to the primary stream. this should produce two items.

        pushToStream(4, "XX", true);
        processor.checkAndClearProcessResult("2:XX2,FKey2+Y2 (ts: 2)", "3:XX3,FKey3+Y3 (ts: 3)");
    }

    @Test
    public void shouldNotJoinOnNullKeyMapperValues() {

        // push all items to the globalTable. this should not produce any item

        pushToGlobalTable(4, "Y");
        processor.checkAndClearProcessResult(EMPTY);

        // push all four items to the primary stream with no foreign key, resulting in null keyMapper values.
        // this should not produce any item.

        pushToStream(4, "XXX", false);
        processor.checkAndClearProcessResult(EMPTY);
    }

}
