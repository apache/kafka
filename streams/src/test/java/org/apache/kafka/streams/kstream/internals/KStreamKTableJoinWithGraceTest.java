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
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TopologyWrapper;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.MockApiProcessor;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class KStreamKTableJoinWithGraceTest {
    private final static KeyValueTimestamp<?, ?>[] EMPTY = new KeyValueTimestamp[0];

    private final String streamTopic = "streamTopic";
    private final String tableTopic = "tableTopic";
    private TestInputTopic<Integer, String> inputStreamTopic;
    private TestInputTopic<Integer, String> inputTableTopic;
    private final int[] expectedKeys = {0, 1, 2, 3};

    private MockApiProcessor<Integer, String, Void, Void> processor;
    private TopologyTestDriver driver;
    private StreamsBuilder builder;
    private final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();

    @BeforeEach
    public void setUp() {
        builder = new StreamsBuilder();
        final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.Integer(), Serdes.String());
        driver = new TopologyTestDriver(builder.build(), props);

    }

    private void makeJoin(final Duration grace) {
        final KStream<Integer, String> stream;
        final KTable<Integer, String> table;

        final Consumed<Integer, String> consumed = Consumed.with(Serdes.Integer(), Serdes.String());
        stream = builder.stream(streamTopic, consumed);
        table = builder.table(tableTopic, consumed, Materialized.as(
            Stores.persistentVersionedKeyValueStore("V-grace", Duration.ofMinutes(5))));
        stream.join(table,
            MockValueJoiner.TOSTRING_JOINER,
            Joined.with(Serdes.Integer(), Serdes.String(), Serdes.String(), "Grace", grace)
        ).process(supplier);
        final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.Integer(), Serdes.String());
        driver = new TopologyTestDriver(builder.build(), props);
        inputStreamTopic = driver.createInputTopic(streamTopic, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
        inputTableTopic = driver.createInputTopic(tableTopic, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);

        processor = supplier.theCapturedProcessor();
    }

    @AfterEach
    public void cleanup() {
        driver.close();
    }

    private void pushToStream(final int messageCount, final String valuePrefix) {
        for (int i = 0; i < messageCount; i++) {
            inputStreamTopic.pipeInput(expectedKeys[i], valuePrefix + expectedKeys[i], i);
        }
    }

    private void pushToTable(final int messageCount, final String valuePrefix) {
        for (int i = 0; i < messageCount; i++) {
            inputTableTopic.pipeInput(
                expectedKeys[i],
                valuePrefix + expectedKeys[i],
                0);
        }
    }

    private void pushNullValueToTable() {
        for (int i = 0; i < 2; i++) {
            inputTableTopic.pipeInput(expectedKeys[i], null);
        }
    }

    @Test
    public void shouldFailIfTableIsNotVersioned() {
        final StreamsBuilder builder = new StreamsBuilder();
        final Properties props = new Properties();
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.NO_OPTIMIZATION);
        final KStream<String, String> streamA = builder.stream("topic", Consumed.with(Serdes.String(), Serdes.String()));
        final KTable<String, String> tableB = builder.table("topic2", Consumed.with(Serdes.String(), Serdes.String()));

        assertThrows(IllegalArgumentException.class,
            () -> streamA.join(tableB, (value1, value2) -> value1 + value2, Joined.with(Serdes.String(), Serdes.String(), Serdes.String(), "first-join", Duration.ofMillis(6))).to("out-one"));
    }

    @Test
    public void shouldDelayJoinByGracePeriod() {
        makeJoin(Duration.ofMillis(2));

        // push four items to the table. this should not produce any item.
        pushToTable(4, "Y");
        processor.checkAndClearProcessResult(EMPTY);

        // push all four items to the primary stream. this should produce two items.
        pushToStream(4, "X");
        processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "X0+Y0", 0),
            new KeyValueTimestamp<>(1, "X1+Y1", 1));

        // push all items to the table. this should not produce any item
        pushToTable(4, "YY");
        processor.checkAndClearProcessResult(EMPTY);

        // push all four items to the primary stream. this should produce two items.
        pushToStream(4, "X");
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(0, "X0+YY0", 0),
            new KeyValueTimestamp<>(1, "X1+YY1", 1));

        // push all items to the table. this should not produce any item
        pushToTable(4, "YYY");
        processor.checkAndClearProcessResult(EMPTY);
    }

    @Test
    public void shouldCreateRepartitionTopicsWithUserProvidedName() {
        final StreamsBuilder builder = new StreamsBuilder();
        final Properties props = new Properties();
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.NO_OPTIMIZATION);
        final KStream<String, String> streamA = builder.stream("topic", Consumed.with(Serdes.String(), Serdes.String()));
        final KTable<String, String> tableB = builder.table("topic2", Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.as(Stores.persistentVersionedKeyValueStore("tableB", Duration.ofMinutes(5))));
        final KTable<String, String> tableC = builder.table("topic3", Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.as(Stores.persistentVersionedKeyValueStore("tableC", Duration.ofMinutes(5))));
        final KStream<String, String> rekeyedStream = streamA.map((k, v) -> new KeyValue<>(v, k));

        rekeyedStream.join(tableB, (value1, value2) -> value1 + value2, Joined.with(Serdes.String(), Serdes.String(), Serdes.String(), "first-join", Duration.ZERO)).to("out-one");
        rekeyedStream.join(tableC, (value1, value2) -> value1 + value2, Joined.with(Serdes.String(), Serdes.String(), Serdes.String(), "second-join", Duration.ZERO)).to("out-two");
        final Topology topology = builder.build(props);
        System.out.println(topology.describe().toString());
        assertEquals(expectedTopologyWithUserProvidedRepartitionTopicNames, topology.describe().toString());
    }

    @Test
    public void shouldRequireCopartitionedStreams() {
        makeJoin(Duration.ofMillis(9));

        final Collection<Set<String>> copartitionGroups =
            TopologyWrapper.getInternalTopologyBuilder(builder.build()).copartitionGroups();

        assertEquals(1, copartitionGroups.size());
        assertEquals(new HashSet<>(Arrays.asList(streamTopic, tableTopic)), copartitionGroups.iterator().next());
    }

    @Test
    public void shouldNotJoinWithEmptyTableOnStreamUpdates() {
        makeJoin(Duration.ofMillis(1));
        // push four items to the primary stream. the table is empty
        pushToStream(4, "X");
        processor.checkAndClearProcessResult(EMPTY);
    }

    @Test
    public void shouldNotJoinOnTableUpdates() {
        makeJoin(Duration.ofMillis(1));
        // push two items to the primary stream. the table is empty, but one stays in the buffer
        pushToStream(2, "X");
        processor.checkAndClearProcessResult(EMPTY);

        // push two items to the table. this should not produce any item.
        pushToTable(2, "Y");
        processor.checkAndClearProcessResult(EMPTY);

        // push all four items to the primary stream. this should produce three items.
        pushToStream(4, "X");
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(0, "X0+Y0", 0),
            new KeyValueTimestamp<>(1, "X1+Y1", 1),
            new KeyValueTimestamp<>(1, "X1+Y1", 1));

        // push all items to the table. this should not produce any item
        pushToTable(4, "YY");
        processor.checkAndClearProcessResult(EMPTY);

        // push all four items to the primary stream. this should produce three items. One is still in the stream buffer
        pushToStream(4, "X");
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(0, "X0+YY0", 0),
            new KeyValueTimestamp<>(1, "X1+YY1", 1),
            new KeyValueTimestamp<>(2, "X2+YY2", 2));

        // push all items to the table. this should not produce any item
        pushToTable(4, "YYY");
        processor.checkAndClearProcessResult(EMPTY);
    }

    @Test
    public void shouldJoinOnlyIfMatchFoundOnStreamUpdates() {
        makeJoin(Duration.ofMillis(3));
        // push two items to the table. this should not produce any item.
        pushToTable(2, "Y");
        processor.checkAndClearProcessResult(EMPTY);

        // push all four items to the primary stream. this should produce one item the rest are in the stream buffer.
        pushToStream(4, "X");
        processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "X0+Y0", 0));
    }

    @Test
    public void shouldClearTableEntryOnNullValueUpdates() {
        makeJoin(Duration.ofMillis(2));
        // push all four items to the table. this should not produce any item.
        pushToTable(4, "Y");
        processor.checkAndClearProcessResult(EMPTY);

        // push all four items to the primary stream. this should produce four items.
        pushToStream(4, "X");
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(0, "X0+Y0", 0),
            new KeyValueTimestamp<>(1, "X1+Y1", 1));

        // push two items with null to the table as deletes. this should not produce any item.
        pushNullValueToTable();
        processor.checkAndClearProcessResult(EMPTY);

        // push all four items to the primary stream. this should produce no items.
        pushToStream(4, "XX");
        processor.checkAndClearProcessResult(EMPTY);
    }

    @Test
    public void shouldLogAndMeterWhenSkippingNullLeftKey() {
        makeJoin(Duration.ofMillis(1));
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(KStreamKTableJoin.class)) {
            final TestInputTopic<Integer, String> inputTopic =
                driver.createInputTopic(streamTopic, new IntegerSerializer(), new StringSerializer());
            inputTopic.pipeInput(null, "A");

            assertThat(
                appender.getMessages(),
                hasItem("Skipping record due to null join key or value. topic=[streamTopic] partition=[0] "
                    + "offset=[0]"));
        }
    }

    @Test
    public void shouldLogAndMeterWhenSkippingNullLeftValue() {
        makeJoin(Duration.ofMillis(3));
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(KStreamKTableJoin.class)) {
            final TestInputTopic<Integer, String> inputTopic =
                driver.createInputTopic(streamTopic, new IntegerSerializer(), new StringSerializer());
            inputTopic.pipeInput(1, null);

            assertThat(
                appender.getMessages(),
                hasItem("Skipping record due to null join key or value. topic=[streamTopic] partition=[0] "
                    + "offset=[0]")
            );
        }
    }

    private final String expectedTopologyWithUserProvidedRepartitionTopicNames =
        "Topologies:\n" +
            "   Sub-topology: 0\n" +
            "    Source: KSTREAM-SOURCE-0000000000 (topics: [topic])\n" +
            "      --> KSTREAM-MAP-0000000005\n" +
            "    Processor: KSTREAM-MAP-0000000005 (stores: [])\n" +
            "      --> first-join-repartition-filter, second-join-repartition-filter\n" +
            "      <-- KSTREAM-SOURCE-0000000000\n" +
            "    Processor: first-join-repartition-filter (stores: [])\n" +
            "      --> first-join-repartition-sink\n" +
            "      <-- KSTREAM-MAP-0000000005\n" +
            "    Processor: second-join-repartition-filter (stores: [])\n" +
            "      --> second-join-repartition-sink\n" +
            "      <-- KSTREAM-MAP-0000000005\n" +
            "    Sink: first-join-repartition-sink (topic: first-join-repartition)\n" +
            "      <-- first-join-repartition-filter\n" +
            "    Sink: second-join-repartition-sink (topic: second-join-repartition)\n" +
            "      <-- second-join-repartition-filter\n" +
            "\n" +
            "  Sub-topology: 1\n" +
            "    Source: first-join-repartition-source (topics: [first-join-repartition])\n" +
            "      --> first-join\n" +
            "    Source: KSTREAM-SOURCE-0000000001 (topics: [topic2])\n" +
            "      --> KTABLE-SOURCE-0000000002\n" +
            "    Processor: first-join (stores: [tableB])\n" +
            "      --> KSTREAM-SINK-0000000010\n" +
            "      <-- first-join-repartition-source\n" +
            "    Sink: KSTREAM-SINK-0000000010 (topic: out-one)\n" +
            "      <-- first-join\n" +
            "    Processor: KTABLE-SOURCE-0000000002 (stores: [tableB])\n" +
            "      --> none\n" +
            "      <-- KSTREAM-SOURCE-0000000001\n" +
            "\n" +
            "  Sub-topology: 2\n" +
            "    Source: second-join-repartition-source (topics: [second-join-repartition])\n" +
            "      --> second-join\n" +
            "    Source: KSTREAM-SOURCE-0000000003 (topics: [topic3])\n" +
            "      --> KTABLE-SOURCE-0000000004\n" +
            "    Processor: second-join (stores: [tableC])\n" +
            "      --> KSTREAM-SINK-0000000015\n" +
            "      <-- second-join-repartition-source\n" +
            "    Sink: KSTREAM-SINK-0000000015 (topic: out-two)\n" +
            "      <-- second-join\n" +
            "    Processor: KTABLE-SOURCE-0000000004 (stores: [tableC])\n" +
            "      --> none\n" +
            "      <-- KSTREAM-SOURCE-0000000003\n\n";
}