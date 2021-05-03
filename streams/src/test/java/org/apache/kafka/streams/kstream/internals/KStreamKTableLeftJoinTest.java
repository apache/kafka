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
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TopologyWrapper;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.test.MockProcessor;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class KStreamKTableLeftJoinTest {
    private final static KeyValueTimestamp[] EMPTY = new KeyValueTimestamp[0];

    private final String streamTopic = "streamTopic";
    private final String tableTopic = "tableTopic";
    private TestInputTopic<Integer, String> inputStreamTopic;
    private TestInputTopic<Integer, String> inputTableTopic;
    private final int[] expectedKeys = {0, 1, 2, 3};

    private TopologyTestDriver driver;
    private MockProcessor<Integer, String> processor;
    private StreamsBuilder builder;

    @Before
    public void setUp() {
        builder = new StreamsBuilder();

        final KStream<Integer, String> stream;
        final KTable<Integer, String> table;

        final MockProcessorSupplier<Integer, String> supplier = new MockProcessorSupplier<>();
        final Consumed<Integer, String> consumed = Consumed.with(Serdes.Integer(), Serdes.String());
        stream = builder.stream(streamTopic, consumed);
        table = builder.table(tableTopic, consumed);
        stream.leftJoin(table, MockValueJoiner.TOSTRING_JOINER).process(supplier);

        final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.Integer(), Serdes.String());
        driver = new TopologyTestDriver(builder.build(), props);
        inputStreamTopic = driver.createInputTopic(streamTopic, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
        inputTableTopic = driver.createInputTopic(tableTopic, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);

        processor = supplier.theCapturedProcessor();
    }

    @After
    public void cleanup() {
        driver.close();
    }

    private void pushToStream(final int messageCount,
                              final String valuePrefix,
                              final long startTimestampMs) {
        for (int i = 0; i < messageCount; i++) {
            inputStreamTopic.pipeInput(
                expectedKeys[i],
                valuePrefix + expectedKeys[i],
                startTimestampMs + i
            );
        }
    }

    private void pushToTable(final int messageCount,
                             final String valuePrefix,
                             final long startTimestampMs) {
        final Random r = new Random(System.currentTimeMillis());
        for (int i = 0; i < messageCount; i++) {
            inputTableTopic.pipeInput(
                expectedKeys[i],
                valuePrefix + expectedKeys[i],
                startTimestampMs + i
            );
        }
    }

    private void pushNullValueToTable(final int messageCount,
                                      final long startTimestampMs) {
        for (int i = 0; i < messageCount; i++) {
            inputTableTopic.pipeInput(expectedKeys[i], null, startTimestampMs + i);
        }
    }

    @Test
    public void shouldRequireCopartitionedStreams() {
        final Collection<Set<String>> copartitionGroups =
            TopologyWrapper.getInternalTopologyBuilder(builder.build()).copartitionGroups();

        assertEquals(1, copartitionGroups.size());
        assertEquals(new HashSet<>(Arrays.asList(streamTopic, tableTopic)), copartitionGroups.iterator().next());
    }

    @Test
    public void shouldJoinWithEmptyTableOnStreamUpdates() {
        // push two items to the primary stream. the table is empty
        pushToStream(2, "X", 0L);
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(0, "X0+null", 0L),
            new KeyValueTimestamp<>(1, "X1+null", 1L)
        );
    }

    @Test
    public void shouldNotJoinOnTableUpdates() {
        // push two items to the primary stream. the table is empty
        pushToStream(2, "X", 0L);
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(0, "X0+null", 0L),
            new KeyValueTimestamp<>(1, "X1+null", 1L)
        );

        // push two items to the table. this should not produce any item.
        pushToTable(2, "Y", 10L);
        processor.checkAndClearProcessResult(EMPTY);

        // push all four items to the primary stream. this should produce four items.
        pushToStream(4, "X", 20L);
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(0, "X0+Y0", 20L),
            new KeyValueTimestamp<>(1, "X1+Y1", 21L),
            new KeyValueTimestamp<>(2, "X2+null", 22L),
            new KeyValueTimestamp<>(3, "X3+null", 23L)
        );

        // push all items to the table. this should not produce any item
        pushToTable(4, "YY", 30L);
        processor.checkAndClearProcessResult(EMPTY);

        // push all four items to the primary stream. this should produce four items.
        pushToStream(4, "X", 40L);
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(0, "X0+YY0", 40L),
            new KeyValueTimestamp<>(1, "X1+YY1", 41L),
            new KeyValueTimestamp<>(2, "X2+YY2", 42L),
            new KeyValueTimestamp<>(3, "X3+YY3", 43L)
        );

        // push all items to the table. this should not produce any item
        pushToTable(4, "YYY", 50L);
        processor.checkAndClearProcessResult(EMPTY);
    }

    @Test
    public void shouldJoinRegardlessIfMatchFoundOnStreamUpdates() {
        // push two items to the table. this should not produce any item.
        pushToTable(2, "Y", 0L);
        processor.checkAndClearProcessResult(EMPTY);

        // push all four items to the primary stream. this should produce four items.
        pushToStream(4, "X", 10L);
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(0, "X0+Y0", 10L),
            new KeyValueTimestamp<>(1, "X1+Y1", 11L),
            new KeyValueTimestamp<>(2, "X2+null", 12L),
            new KeyValueTimestamp<>(3, "X3+null", 13L)
        );

    }

    @Test
    public void shouldClearTableEntryOnNullValueUpdates() {
        // push all four items to the table. this should not produce any item.
        pushToTable(4, "Y", 0L);
        processor.checkAndClearProcessResult(EMPTY);

        // push all four items to the primary stream. this should produce four items.
        pushToStream(4, "X", 10L);
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(0, "X0+Y0", 10L),
            new KeyValueTimestamp<>(1, "X1+Y1", 11L),
            new KeyValueTimestamp<>(2, "X2+Y2", 12L),
            new KeyValueTimestamp<>(3, "X3+Y3", 13L)
        );

        // push two items with null to the table as deletes. this should not produce any item.
        pushNullValueToTable(2, 20L);
        processor.checkAndClearProcessResult(EMPTY);

        // push all four items to the primary stream. this should produce four items.
        pushToStream(4, "XX", 30L);
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(0, "XX0+null", 30L),
            new KeyValueTimestamp<>(1, "XX1+null", 31L),
            new KeyValueTimestamp<>(2, "XX2+Y2", 32L),
            new KeyValueTimestamp<>(3, "XX3+Y3", 33L)
        );
    }

}
