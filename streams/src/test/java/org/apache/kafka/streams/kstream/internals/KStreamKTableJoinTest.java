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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TopologyTestDriverBuilder;
import org.apache.kafka.streams.TopologyWrapper;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.test.MockApiProcessor;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.StreamsTestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KStreamKTableJoinTest {
    private static final KeyValueTimestamp<?, ?>[] EMPTY = new KeyValueTimestamp[0];

    private final String streamTopic = "streamTopic";
    private final String tableTopic = "tableTopic";
    private TestInputTopic<Integer, String> inputStreamTopic;
    private TestInputTopic<Integer, String> inputTableTopic;
    private final int[] expectedKeys = {0, 1, 2, 3};

    private MockApiProcessor<Integer, String, Void, Void> processor;
    private TopologyTestDriver driver;
    private StreamsBuilder builder;
    private final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();

    public void setUp(final boolean withHeaders) {
        builder = new StreamsBuilder();

        final KStream<Integer, String> stream;
        final KTable<Integer, String> table;

        final Consumed<Integer, String> consumed = Consumed.with(Serdes.Integer(), Serdes.String());
        stream = builder.stream(streamTopic, consumed);
        table = builder.table(tableTopic, consumed);
        stream.join(table, MockValueJoiner.TOSTRING_JOINER).process(supplier);
        final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.Integer(), Serdes.String());
        StreamsTestUtils.maybeSetDslStoreFormatHeaders(props, withHeaders);
        driver = new TopologyTestDriverBuilder(builder.build()).withConfig(props).build();
        inputStreamTopic = driver.createInputTopic(streamTopic, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
        inputTableTopic = driver.createInputTopic(tableTopic, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);

        processor = supplier.theCapturedProcessor();
    }

    @AfterEach
    public void cleanup() {
        if (driver != null) {
            driver.close();
        }
    }

    private void pushToStream(final int messageCount, final String valuePrefix) {
        for (int i = 0; i < messageCount; i++) {
            inputStreamTopic.pipeInput(expectedKeys[i], valuePrefix + expectedKeys[i], i);
        }
    }

    private void pushToTableNonRandom(final int messageCount, final String valuePrefix) {
        for (int i = 0; i < messageCount; i++) {
            inputTableTopic.pipeInput(
                expectedKeys[i],
                valuePrefix + expectedKeys[i],
                0);
        }
    }

    private void pushToTable(final int messageCount, final String valuePrefix) {
        final Random r = new Random(System.currentTimeMillis());
        for (int i = 0; i < messageCount; i++) {
            inputTableTopic.pipeInput(
                expectedKeys[i],
                valuePrefix + expectedKeys[i],
                r.nextInt(Integer.MAX_VALUE));
        }
    }

    private void pushNullValueToTable() {
        for (int i = 0; i < 2; i++) {
            inputTableTopic.pipeInput(expectedKeys[i], null);
        }
    }


    private void makeJoin(final Duration grace) {
        final KStream<Integer, String> stream;
        final KTable<Integer, String> table;
        final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        builder = new StreamsBuilder();

        final Consumed<Integer, String> consumed = Consumed.with(Serdes.Integer(), Serdes.String());
        stream = builder.stream(streamTopic, consumed);
        table = builder.table("tableTopic2", consumed, Materialized.as(
            Stores.persistentVersionedKeyValueStore("V-grace", Duration.ofMinutes(5))));
        stream.join(table,
            MockValueJoiner.TOSTRING_JOINER,
            Joined.with(Serdes.Integer(), Serdes.String(), Serdes.String(), "Grace", grace)
        ).process(supplier);
        final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.Integer(), Serdes.String());
        driver = new TopologyTestDriverBuilder(builder.build()).withConfig(props).build();
        inputStreamTopic = driver.createInputTopic(streamTopic, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
        inputTableTopic = driver.createInputTopic("tableTopic2", new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);

        processor = supplier.theCapturedProcessor();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldFailIfTableIsNotVersioned(final boolean withHeaders) {
        final StreamsBuilder builder = new StreamsBuilder();
        final Properties props = new Properties();
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.NO_OPTIMIZATION);
        StreamsTestUtils.maybeSetDslStoreFormatHeaders(props, withHeaders);
        final KStream<String, String> streamA = builder.stream("topic", Consumed.with(Serdes.String(), Serdes.String()));
        final KTable<String, String> tableB = builder.table("topic2", Consumed.with(Serdes.String(), Serdes.String()));

        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
            () -> streamA.join(tableB, (value1, value2) -> value1 + value2, Joined.with(Serdes.String(), Serdes.String(), Serdes.String(), "first-join", Duration.ofMillis(6))).to("out-one"));
        assertThat(
            exception.getMessage(),
            is("KTable must be versioned to use a grace period in a stream table join.")
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldFailIfTableIsNotVersionedButMaterializationIsInherited(final boolean withHeaders) {
        final StreamsBuilder builder = new StreamsBuilder();
        final Properties props = new Properties();
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.NO_OPTIMIZATION);
        StreamsTestUtils.maybeSetDslStoreFormatHeaders(props, withHeaders);
        final KStream<String, String> streamA = builder.stream("topic", Consumed.with(Serdes.String(), Serdes.String()));
        final KTable<String, String> source = builder.table("topic2", Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.as(Stores.inMemoryKeyValueStore("tableB")));
        final KTable<String, String> tableB = source.filter((k, v) -> true);
        // the filter operation forces the table materialization to be inherited

        streamA.join(tableB, (value1, value2) -> value1 + value2, Joined.with(Serdes.String(), Serdes.String(), Serdes.String(), "first-join", Duration.ofMillis(6))).to("out-one");

        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, builder::build);
        assertThat(
            exception.getMessage(),
            is("KTable must be versioned to use a grace period in a stream table join.")
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldNotFailIfTableIsVersionedButMaterializationIsInherited(final boolean withHeaders) {
        final StreamsBuilder builder = new StreamsBuilder();
        final Properties props = new Properties();
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.NO_OPTIMIZATION);
        StreamsTestUtils.maybeSetDslStoreFormatHeaders(props, withHeaders);
        final KStream<String, String> streamA = builder.stream("topic", Consumed.with(Serdes.String(), Serdes.String()));
        final KTable<String, String> source = builder.table("topic2", Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.as(Stores.persistentVersionedKeyValueStore("tableB", Duration.ofMinutes(5))));
        final KTable<String, String> tableB = source.filter((k, v) -> true);
        // the filter operation forces the table materialization to be inherited

        streamA.join(tableB, (value1, value2) -> value1 + value2, Joined.with(Serdes.String(), Serdes.String(), Serdes.String(), "first-join", Duration.ofMillis(6))).to("out-one");

        //should not throw an error
        builder.build();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldFailIfGracePeriodIsLongerThanHistoryRetention(final boolean withHeaders) {
        final StreamsBuilder builder = new StreamsBuilder();
        final Properties props = new Properties();
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.NO_OPTIMIZATION);
        StreamsTestUtils.maybeSetDslStoreFormatHeaders(props, withHeaders);
        final KStream<String, String> streamA = builder.stream("topic", Consumed.with(Serdes.String(), Serdes.String()));
        final KTable<String, String> tableB = builder.table("topic2", Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.as(Stores.persistentVersionedKeyValueStore("tableB", Duration.ofMinutes(5))));

        streamA.join(tableB, (value1, value2) -> value1 + value2, Joined.with(Serdes.String(), Serdes.String(), Serdes.String(), "first-join", Duration.ofMinutes(6))).to("out-one");

        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> builder.build(props));
        assertThat(exception.getMessage(), is("History retention must be at least grace period."));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldFailIfGracePeriodIsLongerThanHistoryRetentionAndInheritedStore(final boolean withHeaders) {
        final StreamsBuilder builder = new StreamsBuilder();
        final Properties props = new Properties();
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.NO_OPTIMIZATION);
        StreamsTestUtils.maybeSetDslStoreFormatHeaders(props, withHeaders);
        final KStream<String, String> streamA = builder.stream("topic", Consumed.with(Serdes.String(), Serdes.String()));
        final KTable<String, String> source = builder.table("topic2", Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.as(Stores.persistentVersionedKeyValueStore("V-grace", Duration.ofMinutes(0))));
        final KTable<String, String> tableB = source.filter((k, v) -> true);
        // the filter operation forces the table materialization to be inherited

        streamA.join(tableB, (value1, value2) -> value1 + value2, Joined.with(Serdes.String(), Serdes.String(), Serdes.String(), "first-join", Duration.ofMillis(6))).to("out-one");

        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> builder.build(props));
        assertThat(exception.getMessage(), is("History retention must be at least grace period."));
    }


    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldDelayJoinByGracePeriod(final boolean withHeaders) {
        setUp(withHeaders);
        makeJoin(Duration.ofMillis(2));

        // push four items to the table. this should not produce any item.
        pushToTableNonRandom(4, "Y");
        processor.checkAndClearProcessResult(EMPTY);

        // push all four items to the primary stream. this should produce two items.
        pushToStream(4, "X");
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(0, "X0+Y0", 0),
            new KeyValueTimestamp<>(1, "X1+Y1", 1));

        // push all items to the table. this should not produce any item
        pushToTableNonRandom(4, "YY");
        processor.checkAndClearProcessResult(EMPTY);

        // push all four items to the primary stream. this should produce two items.
        pushToStream(4, "X");
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(0, "X0+YY0", 0),
            new KeyValueTimestamp<>(1, "X1+YY1", 1));

        inputStreamTopic.pipeInput(5, "test", 7);

        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(2, "X2+YY2", 2),
            new KeyValueTimestamp<>(2, "X2+YY2", 2),
            new KeyValueTimestamp<>(3, "X3+YY3", 3),
            new KeyValueTimestamp<>(3, "X3+YY3", 3));


        // push all items to the table. this should not produce any item
        pushToTableNonRandom(4, "YYY");
        processor.checkAndClearProcessResult(EMPTY);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldHandleLateJoinsWithGracePeriod(final boolean withHeaders) {
        setUp(withHeaders);
        makeJoin(Duration.ofMillis(2));

        // push four items to the table. this should not produce any item.
        pushToTableNonRandom(4, "Y");
        processor.checkAndClearProcessResult(EMPTY);

        // push 4 records into the buffer and evict the first two
        pushToStream(4, "X");
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(0, "X0+Y0", 0),
            new KeyValueTimestamp<>(1, "X1+Y1", 1));

        //should be processed immediately and not evict any other records
        pushToStream(1, "X");
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(0, "X0+Y0", 0));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldDeserializeBufferedValueWithPutTimeHeadersDuringEviction(final boolean withHeaders) {
        builder = new StreamsBuilder();
        final Consumed<Integer, String> consumed = Consumed.with(Serdes.Integer(), Serdes.String());
        final KStream<Integer, String> stream = builder.stream(streamTopic, consumed);
        final KTable<Integer, String> table = builder.table("tableTopic2", consumed, Materialized.as(
            Stores.persistentVersionedKeyValueStore("V-grace", Duration.ofMinutes(5))));
        stream.join(table,
            MockValueJoiner.TOSTRING_JOINER,
            // The built-in leaf serdes (String, primitives, …) ignore the `headers` argument, so with any of them the test would pass identically
            // with and without the fix (a false green). The HeaderValueAppendingSerde is the minimal serde whose deserializer reflects the `headers`
            // argument into the value
            Joined.with(Serdes.Integer(), new HeaderValueAppendingSerde(), Serdes.String(), "Grace", Duration.ofMillis(2))
        ).process(supplier);
        final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.Integer(), Serdes.String());
        StreamsTestUtils.maybeSetDslStoreFormatHeaders(props, withHeaders);
        driver = new TopologyTestDriverBuilder(builder.build()).withConfig(props).build();
        inputStreamTopic = driver.createInputTopic(streamTopic, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
        inputTableTopic = driver.createInputTopic("tableTopic2", new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
        processor = supplier.theCapturedProcessor();

        // Table entry so the buffered stream record finds a join match when it is evicted.
        inputTableTopic.pipeInput(0, "Y0", 0L);

        // Record A is buffered (grace not yet elapsed) with header v=first at t=0. Nothing is emitted yet.
        inputStreamTopic.pipeInput(new TestRecord<>(0, "X0", headers("first"), 0L));
        processor.checkAndClearProcessResult(EMPTY);

        // Record B carries header v=second and its t=10 timestamp advances stream time past A's grace window, which
        // triggers A's eviction while the live processor context still holds B's headers (v=second). B itself is
        // buffered, not evicted, so it produces no output here.
        inputStreamTopic.pipeInput(new TestRecord<>(1, "X1", headers("second"), 10L));

        // A must be deserialized with its own put-time header (first), NOT B's live-context header (second). Before
        // the fix this asserted "X0[second]+Y0".
        processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "X0[first]+Y0", 0));
    }

    private static Headers headers(final String value) {
        return new RecordHeaders(new Header[]{new RecordHeader("v", value.getBytes(StandardCharsets.UTF_8))});
    }

    /**
     * A stream-side value {@link Serde} whose <em>deserializer</em> output depends on the headers it is handed: it
     * appends the value of the {@code "v"} header to the deserialized string. Serialization is a plain string encode
     * and ignores headers. This fixture exists so tests can observe <em>which</em> headers reached the buffer's value
     * deserializer during eviction -- a plain serde would ignore the headers argument and hide the difference.
     */
    private static final class HeaderValueAppendingSerde implements Serde<String> {
        @Override
        public Serializer<String> serializer() {
            return new StringSerializer();
        }

        @Override
        public Deserializer<String> deserializer() {
            return new Deserializer<>() {
                @Override
                public String deserialize(final String topic, final byte[] data) {
                    return data == null ? null : new String(data, StandardCharsets.UTF_8);
                }

                @Override
                public String deserialize(final String topic, final Headers headers, final byte[] data) {
                    final String base = deserialize(topic, data);
                    final Header header = headers == null ? null : headers.lastHeader("v");
                    if (base == null || header == null) {
                        return base;
                    }
                    return base + "[" + new String(header.value(), StandardCharsets.UTF_8) + "]";
                }
            };
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldReuseRepartitionTopicWithGeneratedName(final boolean withHeaders) {
        final StreamsBuilder builder = new StreamsBuilder();
        final Properties props = new Properties();
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.NO_OPTIMIZATION);
        StreamsTestUtils.maybeSetDslStoreFormatHeaders(props, withHeaders);
        final KStream<String, String> streamA = builder.stream("topic", Consumed.with(Serdes.String(), Serdes.String()));
        final KTable<String, String> tableB = builder.table("topic2", Consumed.with(Serdes.String(), Serdes.String()));
        final KTable<String, String> tableC = builder.table("topic3", Consumed.with(Serdes.String(), Serdes.String()));
        final KStream<String, String> rekeyedStream = streamA.map((k, v) -> new KeyValue<>(v, k));
        rekeyedStream.join(tableB, (value1, value2) -> value1 + value2).to("out-one");
        rekeyedStream.join(tableC, (value1, value2) -> value1 + value2).to("out-two");
        final Topology topology = builder.build(props);
        assertEquals(expectedTopologyWithGeneratedRepartitionTopicNames, topology.describe().toString());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldCreateRepartitionTopicsWithUserProvidedName(final boolean withHeaders) {
        final StreamsBuilder builder = new StreamsBuilder();
        final Properties props = new Properties();
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.NO_OPTIMIZATION);
        StreamsTestUtils.maybeSetDslStoreFormatHeaders(props, withHeaders);
        final KStream<String, String> streamA = builder.stream("topic", Consumed.with(Serdes.String(), Serdes.String()));
        final KTable<String, String> tableB = builder.table("topic2", Consumed.with(Serdes.String(), Serdes.String()));
        final KTable<String, String> tableC = builder.table("topic3", Consumed.with(Serdes.String(), Serdes.String()));
        final KStream<String, String> rekeyedStream = streamA.map((k, v) -> new KeyValue<>(v, k));

        rekeyedStream.join(tableB, (value1, value2) -> value1 + value2, Joined.with(Serdes.String(), Serdes.String(), Serdes.String(), "first-join")).to("out-one");
        rekeyedStream.join(tableC, (value1, value2) -> value1 + value2, Joined.with(Serdes.String(), Serdes.String(), Serdes.String(), "second-join")).to("out-two");
        final Topology topology = builder.build(props);
        System.out.println(topology.describe().toString());
        assertEquals(expectedTopologyWithUserProvidedRepartitionTopicNames, topology.describe().toString());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldRequireCopartitionedStreams(final boolean withHeaders) {
        setUp(withHeaders);
        final Collection<Set<String>> copartitionGroups =
            TopologyWrapper.getInternalTopologyBuilder(builder.build()).copartitionGroups();

        assertEquals(1, copartitionGroups.size());
        assertEquals(Set.of(streamTopic, tableTopic), copartitionGroups.iterator().next());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldNotJoinWithEmptyTableOnStreamUpdates(final boolean withHeaders) {
        setUp(withHeaders);
        // push two items to the primary stream. the table is empty
        pushToStream(2, "X");
        processor.checkAndClearProcessResult(EMPTY);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldNotJoinOnTableUpdates(final boolean withHeaders) {
        setUp(withHeaders);
        // push two items to the primary stream. the table is empty
        pushToStream(2, "X");
        processor.checkAndClearProcessResult(EMPTY);

        // push two items to the table. this should not produce any item.
        pushToTable(2, "Y");
        processor.checkAndClearProcessResult(EMPTY);

        // push all four items to the primary stream. this should produce two items.
        pushToStream(4, "X");
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(0, "X0+Y0", 0),
            new KeyValueTimestamp<>(1, "X1+Y1", 1)
        );

        // push all items to the table. this should not produce any item
        pushToTable(4, "YY");
        processor.checkAndClearProcessResult(EMPTY);

        // push all four items to the primary stream. this should produce four items.
        pushToStream(4, "X");
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(0, "X0+YY0", 0),
            new KeyValueTimestamp<>(1, "X1+YY1", 1),
            new KeyValueTimestamp<>(2, "X2+YY2", 2),
            new KeyValueTimestamp<>(3, "X3+YY3", 3)
        );

        // push all items to the table. this should not produce any item
        pushToTable(4, "YYY");
        processor.checkAndClearProcessResult(EMPTY);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldJoinOnlyIfMatchFoundOnStreamUpdates(final boolean withHeaders) {
        setUp(withHeaders);
        // push two items to the table. this should not produce any item.
        pushToTable(2, "Y");
        processor.checkAndClearProcessResult(EMPTY);

        // push all four items to the primary stream. this should produce two items.
        pushToStream(4, "X");
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(0, "X0+Y0", 0),
            new KeyValueTimestamp<>(1, "X1+Y1", 1)
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldClearTableEntryOnNullValueUpdates(final boolean withHeaders) {
        setUp(withHeaders);
        // push all four items to the table. this should not produce any item.
        pushToTable(4, "Y");
        processor.checkAndClearProcessResult(EMPTY);

        // push all four items to the primary stream. this should produce four items.
        pushToStream(4, "X");
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(0, "X0+Y0", 0),
            new KeyValueTimestamp<>(1, "X1+Y1", 1),
            new KeyValueTimestamp<>(2, "X2+Y2", 2),
            new KeyValueTimestamp<>(3, "X3+Y3", 3)
        );

        // push two items with null to the table as deletes. this should not produce any item.
        pushNullValueToTable();
        processor.checkAndClearProcessResult(EMPTY);

        // push all four items to the primary stream. this should produce two items.
        pushToStream(4, "XX");
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(2, "XX2+Y2", 2),
            new KeyValueTimestamp<>(3, "XX3+Y3", 3)
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldLogAndMeterWhenSkippingNullLeftKey(final boolean withHeaders) {
        setUp(withHeaders);
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(KStreamKTableJoinProcessor.class)) {
            final TestInputTopic<Integer, String> inputTopic =
                driver.createInputTopic(streamTopic, new IntegerSerializer(), new StringSerializer());
            inputTopic.pipeInput(null, "A");

            assertThat(
                appender.getMessages(),
                hasItem("Skipping record due to null join key or value. topic=[streamTopic] partition=[0] "
                    + "offset=[0]"));
        }

        assertThat(
            driver.metrics().get(
                new MetricName(
                    "dropped-records-total",
                    "stream-task-metrics",
                    "",
                    mkMap(
                        mkEntry("thread-id", Thread.currentThread().getName()),
                        mkEntry("task-id", "0_0")
                    )
                ))
                .metricValue(),
            is(1.0)
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldLogAndMeterWhenSkippingNullLeftValue(final boolean withHeaders) {
        setUp(withHeaders);
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(KStreamKTableJoinProcessor.class)) {
            final TestInputTopic<Integer, String> inputTopic =
                driver.createInputTopic(streamTopic, new IntegerSerializer(), new StringSerializer());
            inputTopic.pipeInput(1, null);

            assertThat(
                appender.getMessages(),
                hasItem("Skipping record due to null join key or value. topic=[streamTopic] partition=[0] "
                    + "offset=[0]")
            );
        }

        assertThat(
            driver.metrics().get(
                    new MetricName(
                        "dropped-records-total",
                        "stream-task-metrics",
                        "",
                        mkMap(
                            mkEntry("thread-id", Thread.currentThread().getName()),
                            mkEntry("task-id", "0_0")
                        )
                    ))
                .metricValue(),
            is(1.0)
        );
    }


    private final String expectedTopologyWithGeneratedRepartitionTopicNames =
        "Topologies:\n"
        + "   Sub-topology: 0\n"
        + "    Source: KSTREAM-SOURCE-0000000000 (topics: [topic])\n"
        + "      --> KSTREAM-MAP-0000000007\n"
        + "    Processor: KSTREAM-MAP-0000000007 (stores: [])\n"
        + "      --> KSTREAM-FILTER-0000000009\n"
        + "      <-- KSTREAM-SOURCE-0000000000\n"
        + "    Processor: KSTREAM-FILTER-0000000009 (stores: [])\n"
        + "      --> KSTREAM-SINK-0000000008\n"
        + "      <-- KSTREAM-MAP-0000000007\n"
        + "    Sink: KSTREAM-SINK-0000000008 (topic: KSTREAM-MAP-0000000007-repartition)\n"
        + "      <-- KSTREAM-FILTER-0000000009\n"
        + "\n"
        + "  Sub-topology: 1\n"
        + "    Source: KSTREAM-SOURCE-0000000010 (topics: [KSTREAM-MAP-0000000007-repartition])\n"
        + "      --> KSTREAM-JOIN-0000000011, KSTREAM-JOIN-0000000016\n"
        + "    Processor: KSTREAM-JOIN-0000000011 (stores: [topic2-STATE-STORE-0000000001])\n"
        + "      --> KSTREAM-SINK-0000000012\n"
        + "      <-- KSTREAM-SOURCE-0000000010\n"
        + "    Processor: KSTREAM-JOIN-0000000016 (stores: [topic3-STATE-STORE-0000000004])\n"
        + "      --> KSTREAM-SINK-0000000017\n"
        + "      <-- KSTREAM-SOURCE-0000000010\n"
        + "    Source: KSTREAM-SOURCE-0000000002 (topics: [topic2])\n"
        + "      --> KTABLE-SOURCE-0000000003\n"
        + "    Source: KSTREAM-SOURCE-0000000005 (topics: [topic3])\n"
        + "      --> KTABLE-SOURCE-0000000006\n"
        + "    Sink: KSTREAM-SINK-0000000012 (topic: out-one)\n"
        + "      <-- KSTREAM-JOIN-0000000011\n"
        + "    Sink: KSTREAM-SINK-0000000017 (topic: out-two)\n"
        + "      <-- KSTREAM-JOIN-0000000016\n"
        + "    Processor: KTABLE-SOURCE-0000000003 (stores: [topic2-STATE-STORE-0000000001])\n"
        + "      --> none\n"
        + "      <-- KSTREAM-SOURCE-0000000002\n"
        + "    Processor: KTABLE-SOURCE-0000000006 (stores: [topic3-STATE-STORE-0000000004])\n"
        + "      --> none\n"
        + "      <-- KSTREAM-SOURCE-0000000005\n\n";


    private final String expectedTopologyWithUserProvidedRepartitionTopicNames =
            "Topologies:\n"
                    + "   Sub-topology: 0\n"
                    + "    Source: KSTREAM-SOURCE-0000000000 (topics: [topic])\n"
                    + "      --> KSTREAM-MAP-0000000007\n"
                    + "    Processor: KSTREAM-MAP-0000000007 (stores: [])\n"
                    + "      --> first-join-repartition-filter, second-join-repartition-filter\n"
                    + "      <-- KSTREAM-SOURCE-0000000000\n"
                    + "    Processor: first-join-repartition-filter (stores: [])\n"
                    + "      --> first-join-repartition-sink\n"
                    + "      <-- KSTREAM-MAP-0000000007\n"
                    + "    Processor: second-join-repartition-filter (stores: [])\n"
                    + "      --> second-join-repartition-sink\n"
                    + "      <-- KSTREAM-MAP-0000000007\n"
                    + "    Sink: first-join-repartition-sink (topic: first-join-repartition)\n"
                    + "      <-- first-join-repartition-filter\n"
                    + "    Sink: second-join-repartition-sink (topic: second-join-repartition)\n"
                    + "      <-- second-join-repartition-filter\n"
                    + "\n"
                    + "  Sub-topology: 1\n"
                    + "    Source: first-join-repartition-source (topics: [first-join-repartition])\n"
                    + "      --> first-join\n"
                    + "    Source: KSTREAM-SOURCE-0000000002 (topics: [topic2])\n"
                    + "      --> KTABLE-SOURCE-0000000003\n"
                    + "    Processor: first-join (stores: [topic2-STATE-STORE-0000000001])\n"
                    + "      --> KSTREAM-SINK-0000000012\n"
                    + "      <-- first-join-repartition-source\n"
                    + "    Sink: KSTREAM-SINK-0000000012 (topic: out-one)\n"
                    + "      <-- first-join\n"
                    + "    Processor: KTABLE-SOURCE-0000000003 (stores: [topic2-STATE-STORE-0000000001])\n"
                    + "      --> none\n"
                    + "      <-- KSTREAM-SOURCE-0000000002\n"
                    + "\n"
                    + "  Sub-topology: 2\n"
                    + "    Source: second-join-repartition-source (topics: [second-join-repartition])\n"
                    + "      --> second-join\n"
                    + "    Source: KSTREAM-SOURCE-0000000005 (topics: [topic3])\n"
                    + "      --> KTABLE-SOURCE-0000000006\n"
                    + "    Processor: second-join (stores: [topic3-STATE-STORE-0000000004])\n"
                    + "      --> KSTREAM-SINK-0000000017\n"
                    + "      <-- second-join-repartition-source\n"
                    + "    Sink: KSTREAM-SINK-0000000017 (topic: out-two)\n"
                    + "      <-- second-join\n"
                    + "    Processor: KTABLE-SOURCE-0000000006 (stores: [topic3-STATE-STORE-0000000004])\n"
                    + "      --> none\n"
                    + "      <-- KSTREAM-SOURCE-0000000005\n\n";

}
