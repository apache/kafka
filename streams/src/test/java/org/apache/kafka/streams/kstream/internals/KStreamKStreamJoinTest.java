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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TopologyWrapper;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.processor.internals.InternalTopicConfig;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.test.MockProcessor;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static java.time.Duration.ofMillis;
import static org.apache.kafka.test.StreamsTestUtils.getMetricByName;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class KStreamKStreamJoinTest {
    private final static KeyValueTimestamp<?, ?>[] EMPTY = new KeyValueTimestamp[0];

    private final String topic1 = "topic1";
    private final String topic2 = "topic2";
    private final Consumed<Integer, String> consumed = Consumed.with(Serdes.Integer(), Serdes.String());
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());

    private final JoinWindows joinWindows = JoinWindows.of(ofMillis(50)).grace(Duration.ofMillis(50));
    private final StreamJoined<String, Integer, Integer> streamJoined = StreamJoined.with(Serdes.String(), Serdes.Integer(), Serdes.Integer());
    private final String errorMessagePrefix = "Window settings mismatch. WindowBytesStoreSupplier settings";

    @Test
    public void shouldLogAndMeterOnSkippedRecordsWithNullValueWithBuiltInMetricsVersion0100To24() {
        shouldLogAndMeterOnSkippedRecordsWithNullValue(StreamsConfig.METRICS_0100_TO_24);
    }

    @Test
    public void shouldLogAndMeterOnSkippedRecordsWithNullValueWithBuiltInMetricsVersionLatest() {
        shouldLogAndMeterOnSkippedRecordsWithNullValue(StreamsConfig.METRICS_LATEST);
    }


    @Test
    public void shouldReuseRepartitionTopicWithGeneratedName() {
        final StreamsBuilder builder = new StreamsBuilder();
        final Properties props = new Properties();
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.NO_OPTIMIZATION);
        final KStream<String, String> stream1 = builder.stream("topic", Consumed.with(Serdes.String(), Serdes.String()));
        final KStream<String, String> stream2 = builder.stream("topic2", Consumed.with(Serdes.String(), Serdes.String()));
        final KStream<String, String> stream3 = builder.stream("topic3", Consumed.with(Serdes.String(), Serdes.String()));
        final KStream<String, String> newStream = stream1.map((k, v) -> new KeyValue<>(v, k));
        newStream.join(stream2, (value1, value2) -> value1 + value2, JoinWindows.of(ofMillis(100))).to("out-one");
        newStream.join(stream3, (value1, value2) -> value1 + value2, JoinWindows.of(ofMillis(100))).to("out-to");
        assertEquals(expectedTopologyWithGeneratedRepartitionTopic, builder.build(props).describe().toString());
    }

    @Test
    public void shouldCreateRepartitionTopicsWithUserProvidedName() {
        final StreamsBuilder builder = new StreamsBuilder();
        final Properties props = new Properties();
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.NO_OPTIMIZATION);
        final KStream<String, String> stream1 = builder.stream("topic", Consumed.with(Serdes.String(), Serdes.String()));
        final KStream<String, String> stream2 = builder.stream("topic2", Consumed.with(Serdes.String(), Serdes.String()));
        final KStream<String, String> stream3 = builder.stream("topic3", Consumed.with(Serdes.String(), Serdes.String()));
        final KStream<String, String> newStream = stream1.map((k, v) -> new KeyValue<>(v, k));
        final StreamJoined<String, String, String> streamJoined = StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String());
        newStream.join(stream2, (value1, value2) -> value1 + value2, JoinWindows.of(ofMillis(100)), streamJoined.withName("first-join")).to("out-one");
        newStream.join(stream3, (value1, value2) -> value1 + value2, JoinWindows.of(ofMillis(100)), streamJoined.withName("second-join")).to("out-two");
        final Topology topology =  builder.build(props);
        System.out.println(topology.describe().toString());
        assertEquals(expectedTopologyWithUserNamedRepartitionTopics, topology.describe().toString());
    }

    private void shouldLogAndMeterOnSkippedRecordsWithNullValue(final String builtInMetricsVersion) {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, Integer> left = builder.stream("left", Consumed.with(Serdes.String(), Serdes.Integer()));
        final KStream<String, Integer> right = builder.stream("right", Consumed.with(Serdes.String(), Serdes.Integer()));

        left.join(
            right,
            Integer::sum,
            JoinWindows.of(ofMillis(100)),
            StreamJoined.with(Serdes.String(), Serdes.Integer(), Serdes.Integer())
        );

        props.setProperty(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, builtInMetricsVersion);

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(KStreamKStreamJoin.class);
             final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {

            final TestInputTopic<String, Integer> inputTopic =
                    driver.createInputTopic("left", new StringSerializer(), new IntegerSerializer());
            inputTopic.pipeInput("A", null);

            assertThat(
                appender.getMessages(),
                hasItem("Skipping record due to null key or value. key=[A] value=[null] topic=[left] partition=[0] "
                    + "offset=[0]")
            );

            if (StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion)) {
                assertEquals(
                    1.0,
                    getMetricByName(
                        driver.metrics(),
                        "skipped-records-total",
                        "stream-metrics"
                    ).metricValue()
                );
            }
        }
    }

    @Test
    public void shouldDisableLoggingOnStreamJoined() {

        final JoinWindows joinWindows = JoinWindows.of(ofMillis(100)).grace(Duration.ofMillis(50));
        final StreamJoined<String, Integer, Integer> streamJoined = StreamJoined
            .with(Serdes.String(), Serdes.Integer(), Serdes.Integer())
            .withStoreName("store")
            .withLoggingDisabled();

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, Integer> left = builder.stream("left", Consumed.with(Serdes.String(), Serdes.Integer()));
        final KStream<String, Integer> right = builder.stream("right", Consumed.with(Serdes.String(), Serdes.Integer()));

        left.join(
            right,
            (value1, value2) -> value1 + value2,
            joinWindows,
            streamJoined
        );

        final Topology topology = builder.build();
        final InternalTopologyBuilder internalTopologyBuilder = TopologyWrapper.getInternalTopologyBuilder(topology);

        assertThat(internalTopologyBuilder.stateStores().get("store-this-join-store").loggingEnabled(), equalTo(false));
        assertThat(internalTopologyBuilder.stateStores().get("store-other-join-store").loggingEnabled(), equalTo(false));
    }

    @Test
    public void shouldEnableLoggingWithCustomConfigOnStreamJoined() {

        final JoinWindows joinWindows = JoinWindows.of(ofMillis(100)).grace(Duration.ofMillis(50));
        final StreamJoined<String, Integer, Integer> streamJoined = StreamJoined
            .with(Serdes.String(), Serdes.Integer(), Serdes.Integer())
            .withStoreName("store")
            .withLoggingEnabled(Collections.singletonMap("test", "property"));

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, Integer> left = builder.stream("left", Consumed.with(Serdes.String(), Serdes.Integer()));
        final KStream<String, Integer> right = builder.stream("right", Consumed.with(Serdes.String(), Serdes.Integer()));

        left.join(
            right,
            (value1, value2) -> value1 + value2,
            joinWindows,
            streamJoined
        );

        final Topology topology = builder.build();
        final InternalTopologyBuilder internalTopologyBuilder = TopologyWrapper.getInternalTopologyBuilder(topology);

        internalTopologyBuilder.buildSubtopology(0);

        assertThat(internalTopologyBuilder.stateStores().get("store-this-join-store").loggingEnabled(), equalTo(true));
        assertThat(internalTopologyBuilder.stateStores().get("store-other-join-store").loggingEnabled(), equalTo(true));
        assertThat(internalTopologyBuilder.topicGroups().get(0).stateChangelogTopics.size(), equalTo(2));
        for (final InternalTopicConfig config : internalTopologyBuilder.topicGroups().get(0).stateChangelogTopics.values()) {
            assertThat(
                config.getProperties(Collections.emptyMap(), 0).get("test"),
                equalTo("property")
            );
        }
    }

    @Test
    public void shouldThrowExceptionThisStoreSupplierRetentionDoNotMatchWindowsSizeAndGrace() {
        // Case where retention of thisJoinStore doesn't match JoinWindows
        final WindowBytesStoreSupplier thisStoreSupplier = buildWindowBytesStoreSupplier("in-memory-join-store", 500, 100, true);
        final WindowBytesStoreSupplier otherStoreSupplier = buildWindowBytesStoreSupplier("in-memory-join-store-other", 150, 100, true);


        buildStreamsJoinThatShouldThrow(streamJoined.withThisStoreSupplier(thisStoreSupplier).withOtherStoreSupplier(otherStoreSupplier),
            joinWindows,
            errorMessagePrefix);
    }

    @Test
    public void shouldThrowExceptionThisStoreSupplierWindowSizeDoesNotMatchJoinWindowsWindowSize() {
        //Case where window size of thisJoinStore doesn't match JoinWindows
        final WindowBytesStoreSupplier thisStoreSupplier = buildWindowBytesStoreSupplier("in-memory-join-store", 150, 150, true);
        final WindowBytesStoreSupplier otherStoreSupplier = buildWindowBytesStoreSupplier("in-memory-join-store-other", 150, 100, true);

        buildStreamsJoinThatShouldThrow(streamJoined.withThisStoreSupplier(thisStoreSupplier).withOtherStoreSupplier(otherStoreSupplier),
            joinWindows,
            errorMessagePrefix);
    }

    @Test
    public void shouldThrowExceptionWhenThisJoinStoreSetsRetainDuplicatesFalse() {
        //Case where thisJoinStore retain duplicates false
        final WindowBytesStoreSupplier thisStoreSupplier = buildWindowBytesStoreSupplier("in-memory-join-store", 150, 100, false);
        final WindowBytesStoreSupplier otherStoreSupplier = buildWindowBytesStoreSupplier("in-memory-join-store-other", 150, 100, true);

        buildStreamsJoinThatShouldThrow(streamJoined.withThisStoreSupplier(thisStoreSupplier).withOtherStoreSupplier(otherStoreSupplier),
            joinWindows,
            "The StoreSupplier must set retainDuplicates=true, found retainDuplicates=false");
    }

    @Test
    public void shouldThrowExceptionOtherStoreSupplierRetentionDoNotMatchWindowsSizeAndGrace() {
        //Case where retention size of otherJoinStore doesn't match JoinWindows
        final WindowBytesStoreSupplier thisStoreSupplier = buildWindowBytesStoreSupplier("in-memory-join-store", 150, 100, true);
        final WindowBytesStoreSupplier otherStoreSupplier = buildWindowBytesStoreSupplier("in-memory-join-store-other", 500, 100, true);

        buildStreamsJoinThatShouldThrow(streamJoined.withThisStoreSupplier(thisStoreSupplier).withOtherStoreSupplier(otherStoreSupplier),
            joinWindows,
            errorMessagePrefix);
    }

    @Test
    public void shouldThrowExceptionOtherStoreSupplierWindowSizeDoesNotMatchJoinWindowsWindowSize() {
        //Case where window size of otherJoinStore doesn't match JoinWindows
        final WindowBytesStoreSupplier thisStoreSupplier = buildWindowBytesStoreSupplier("in-memory-join-store", 150, 100, true);
        final WindowBytesStoreSupplier otherStoreSupplier = buildWindowBytesStoreSupplier("in-memory-join-store-other", 150, 150, true);

        buildStreamsJoinThatShouldThrow(streamJoined.withThisStoreSupplier(thisStoreSupplier).withOtherStoreSupplier(otherStoreSupplier),
            joinWindows,
            errorMessagePrefix);
    }

    @Test
    public void shouldThrowExceptionWhenOtherJoinStoreSetsRetainDuplicatesFalse() {
        //Case where otherJoinStore retain duplicates false
        final WindowBytesStoreSupplier thisStoreSupplier = buildWindowBytesStoreSupplier("in-memory-join-store", 150, 100, true);
        final WindowBytesStoreSupplier otherStoreSupplier = buildWindowBytesStoreSupplier("in-memory-join-store-other", 150, 100, false);

        buildStreamsJoinThatShouldThrow(streamJoined.withThisStoreSupplier(thisStoreSupplier).withOtherStoreSupplier(otherStoreSupplier),
            joinWindows,
            "The StoreSupplier must set retainDuplicates=true, found retainDuplicates=false");
    }

    @Test
    public void shouldBuildJoinWithCustomStoresAndCorrectWindowSettings() {
        //Case where everything matches up
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, Integer> left = builder.stream("left", Consumed.with(Serdes.String(), Serdes.Integer()));
        final KStream<String, Integer> right = builder.stream("right", Consumed.with(Serdes.String(), Serdes.Integer()));

        left.join(right,
            Integer::sum,
            joinWindows,
            streamJoined);

        builder.build();
    }

    @Test
    public void shouldExceptionWhenJoinStoresDontHaveUniqueNames() {
        final JoinWindows joinWindows = JoinWindows.of(ofMillis(100)).grace(Duration.ofMillis(50));
        final StreamJoined<String, Integer, Integer> streamJoined = StreamJoined.with(Serdes.String(), Serdes.Integer(), Serdes.Integer());
        final WindowBytesStoreSupplier thisStoreSupplier = buildWindowBytesStoreSupplier("in-memory-join-store", 150, 100, true);
        final WindowBytesStoreSupplier otherStoreSupplier = buildWindowBytesStoreSupplier("in-memory-join-store", 150, 100, true);

        buildStreamsJoinThatShouldThrow(streamJoined.withThisStoreSupplier(thisStoreSupplier).withOtherStoreSupplier(otherStoreSupplier),
            joinWindows,
            "Both StoreSuppliers have the same name.  StoreSuppliers must provide unique names");
    }

    @Test
    public void shouldJoinWithCustomStoreSuppliers() {

        final JoinWindows joinWindows = JoinWindows.of(ofMillis(100));

        final WindowBytesStoreSupplier thisStoreSupplier = Stores.inMemoryWindowStore("in-memory-join-store",
            Duration.ofMillis(joinWindows.size() + joinWindows.gracePeriodMs()),
            Duration.ofMillis(joinWindows.size()), true);

        final WindowBytesStoreSupplier otherStoreSupplier = Stores.inMemoryWindowStore("in-memory-join-store-other",
            Duration.ofMillis(joinWindows.size() + joinWindows.gracePeriodMs()),
            Duration.ofMillis(joinWindows.size()), true);

        final StreamJoined<String, Integer, Integer> streamJoined = StreamJoined.with(Serdes.String(), Serdes.Integer(), Serdes.Integer());

        //Case with 2 custom store suppliers
        runJoin(streamJoined.withThisStoreSupplier(thisStoreSupplier).withOtherStoreSupplier(otherStoreSupplier), joinWindows);

        //Case with this stream store supplier
        runJoin(streamJoined.withThisStoreSupplier(thisStoreSupplier), joinWindows);

        //Case with other stream store supplier
        runJoin(streamJoined.withOtherStoreSupplier(otherStoreSupplier), joinWindows);


    }

    private void runJoin(final StreamJoined<String, Integer, Integer> streamJoined,
                         final JoinWindows joinWindows) {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, Integer> left = builder.stream("left", Consumed.with(Serdes.String(), Serdes.Integer()));
        final KStream<String, Integer> right = builder.stream("right", Consumed.with(Serdes.String(), Serdes.Integer()));
        final MockProcessorSupplier<String, Integer> supplier = new MockProcessorSupplier<>();
        final KStream<String, Integer> joinedStream;

        joinedStream = left.join(
            right,
            Integer::sum,
            joinWindows,
            streamJoined);

        joinedStream.process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, Integer> inputTopicLeft =
                    driver.createInputTopic("left", new StringSerializer(), new IntegerSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<String, Integer> inputTopicRight =
                    driver.createInputTopic("right", new StringSerializer(), new IntegerSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockProcessor<String, Integer> processor = supplier.theCapturedProcessor();

            inputTopicLeft.pipeInput("A", 1, 1L);
            inputTopicLeft.pipeInput("B", 1, 2L);

            inputTopicRight.pipeInput("A", 1, 1L);
            inputTopicRight.pipeInput("B", 2, 2L);

            processor.checkAndClearProcessResult(new KeyValueTimestamp<>("A", 2, 1L),
                new KeyValueTimestamp<>("B", 3, 2L));
        }
    }

    @Test
    public void testJoin() {
        final StreamsBuilder builder = new StreamsBuilder();

        final int[] expectedKeys = new int[] {0, 1, 2, 3};

        final KStream<Integer, String> stream1;
        final KStream<Integer, String> stream2;
        final KStream<Integer, String> joined;
        final MockProcessorSupplier<Integer, String> supplier = new MockProcessorSupplier<>();
        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed);
        joined = stream1.join(
            stream2,
            MockValueJoiner.TOSTRING_JOINER,
            JoinWindows.of(ofMillis(100)),
            StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.String()));
        joined.process(supplier);

        final Collection<Set<String>> copartitionGroups =
            TopologyWrapper.getInternalTopologyBuilder(builder.build()).copartitionGroups();

        assertEquals(1, copartitionGroups.size());
        assertEquals(new HashSet<>(Arrays.asList(topic1, topic2)), copartitionGroups.iterator().next());

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<Integer, String> inputTopic1 =
                    driver.createInputTopic(topic1, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<Integer, String> inputTopic2 =
                    driver.createInputTopic(topic2, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockProcessor<Integer, String> processor = supplier.theCapturedProcessor();

            // push two items to the primary stream; the other window is empty
            // w1 = {}
            // w2 = {}
            // --> w1 = { 0:A0, 1:A1 }
            //     w2 = {}
            for (int i = 0; i < 2; i++) {
                inputTopic1.pipeInput(expectedKeys[i], "A" + expectedKeys[i]);
            }
            processor.checkAndClearProcessResult(EMPTY);

            // push two items to the other stream; this should produce two items
            // w1 = { 0:A0, 1:A1 }
            // w2 = {}
            // --> w1 = { 0:A0, 1:A1 }
            //     w2 = { 0:a0, 1:a1 }
            for (int i = 0; i < 2; i++) {
                inputTopic2.pipeInput(expectedKeys[i], "a" + expectedKeys[i]);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+a0", 0),
                new KeyValueTimestamp<>(1, "A1+a1", 0));

            // push all four items to the primary stream; this should produce two items
            // w1 = { 0:A0, 1:A1 }
            // w2 = { 0:a0, 1:a1 }
            // --> w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 3:B3 }
            //     w2 = { 0:a0, 1:a1 }
            for (final int expectedKey : expectedKeys) {
                inputTopic1.pipeInput(expectedKey, "B" + expectedKey);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "B0+a0", 0),
                new KeyValueTimestamp<>(1, "B1+a1", 0));

            // push all items to the other stream; this should produce six items
            // w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 3:B3 }
            // w2 = { 0:a0, 1:a1 }
            // --> w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 3:B3 }
            //     w2 = { 0:a0, 1:a1, 0:b0, 1:b1, 2:b2, 3:b3 }
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, "b" + expectedKey);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+b0", 0),
                new KeyValueTimestamp<>(0, "B0+b0", 0),
                new KeyValueTimestamp<>(1, "A1+b1", 0),
                new KeyValueTimestamp<>(1, "B1+b1", 0),
                new KeyValueTimestamp<>(2, "B2+b2", 0),
                new KeyValueTimestamp<>(3, "B3+b3", 0));

            // push all four items to the primary stream; this should produce six items
            // w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 3:B3 }
            // w2 = { 0:a0, 1:a1, 0:b0, 1:b1, 2:b2, 3:b3 }
            // --> w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 3:B3, 0:C0, 1:C1, 2:C2, 3:C3 }
            //     w2 = { 0:a0, 1:a1, 0:b0, 1:b1, 2:b2, 3:b3 }
            for (final int expectedKey : expectedKeys) {
                inputTopic1.pipeInput(expectedKey, "C" + expectedKey);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "C0+a0", 0),
                new KeyValueTimestamp<>(0, "C0+b0", 0),
                new KeyValueTimestamp<>(1, "C1+a1", 0),
                new KeyValueTimestamp<>(1, "C1+b1", 0),
                new KeyValueTimestamp<>(2, "C2+b2", 0),
                new KeyValueTimestamp<>(3, "C3+b3", 0));

            // push two items to the other stream; this should produce six items
            // w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 3:B3, 0:C0, 1:C1, 2:C2, 3:C3 }
            // w2 = { 0:a0, 1:a1, 0:b0, 1:b1, 2:b2, 3:b3 }
            // --> w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 3:B3, 0:C0, 1:C1, 2:C2, 3:C3 }
            //     w2 = { 0:a0, 1:a1, 0:b0, 1:b1, 2:b2, 3:b3, 0:c0, 1:c1 }
            for (int i = 0; i < 2; i++) {
                inputTopic2.pipeInput(expectedKeys[i], "c" + expectedKeys[i]);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+c0", 0),
                new KeyValueTimestamp<>(0, "B0+c0", 0),
                new KeyValueTimestamp<>(0, "C0+c0", 0),
                new KeyValueTimestamp<>(1, "A1+c1", 0),
                new KeyValueTimestamp<>(1, "B1+c1", 0),
                new KeyValueTimestamp<>(1, "C1+c1", 0));
        }
    }

    @Test
    public void testOuterJoin() {
        final StreamsBuilder builder = new StreamsBuilder();

        final int[] expectedKeys = new int[] {0, 1, 2, 3};

        final KStream<Integer, String> stream1;
        final KStream<Integer, String> stream2;
        final KStream<Integer, String> joined;
        final MockProcessorSupplier<Integer, String> supplier = new MockProcessorSupplier<>();

        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed);
        joined = stream1.outerJoin(
            stream2,
            MockValueJoiner.TOSTRING_JOINER,
            JoinWindows.of(ofMillis(100)),
            StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.String()));
        joined.process(supplier);
        final Collection<Set<String>> copartitionGroups =
            TopologyWrapper.getInternalTopologyBuilder(builder.build()).copartitionGroups();

        assertEquals(1, copartitionGroups.size());
        assertEquals(new HashSet<>(Arrays.asList(topic1, topic2)), copartitionGroups.iterator().next());

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<Integer, String> inputTopic1 =
                    driver.createInputTopic(topic1, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<Integer, String> inputTopic2 =
                    driver.createInputTopic(topic2, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockProcessor<Integer, String> processor = supplier.theCapturedProcessor();

            // push two items to the primary stream; the other window is empty; this should produce two items
            // w1 = {}
            // w2 = {}
            // --> w1 = { 0:A0, 1:A1 }
            //     w2 = {}
            for (int i = 0; i < 2; i++) {
                inputTopic1.pipeInput(expectedKeys[i], "A" + expectedKeys[i]);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+null", 0),
                new KeyValueTimestamp<>(1, "A1+null", 0));

            // push two items to the other stream; this should produce two items
            // w1 = { 0:A0, 1:A1 }
            // w2 = {}
            // --> w1 = { 0:A0, 1:A1 }
            //     w2 = { 0:a0, 1:a1 }
            for (int i = 0; i < 2; i++) {
                inputTopic2.pipeInput(expectedKeys[i], "a" + expectedKeys[i]);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+a0", 0),
                new KeyValueTimestamp<>(1, "A1+a1", 0));

            // push all four items to the primary stream; this should produce four items
            // w1 = { 0:A0, 1:A1 }
            // w2 = { 0:a0, 1:a1 }
            // --> w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 3:B3 }
            //     w2 = { 0:a0, 1:a1 }
            for (final int expectedKey : expectedKeys) {
                inputTopic1.pipeInput(expectedKey, "B" + expectedKey);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "B0+a0", 0),
                new KeyValueTimestamp<>(1, "B1+a1", 0),
                new KeyValueTimestamp<>(2, "B2+null", 0),
                new KeyValueTimestamp<>(3, "B3+null", 0));

            // push all items to the other stream; this should produce six items
            // w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 3:B3 }
            // w2 = { 0:a0, 1:a1 }
            // --> w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 3:B3 }
            //     w2 = { 0:a0, 1:a1, 0:b0, 0:b0, 1:b1, 2:b2, 3:b3 }
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, "b" + expectedKey);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+b0", 0),
                new KeyValueTimestamp<>(0, "B0+b0", 0),
                new KeyValueTimestamp<>(1, "A1+b1", 0),
                new KeyValueTimestamp<>(1, "B1+b1", 0),
                new KeyValueTimestamp<>(2, "B2+b2", 0),
                new KeyValueTimestamp<>(3, "B3+b3", 0));

            // push all four items to the primary stream; this should produce six items
            // w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 3:B3 }
            // w2 = { 0:a0, 1:a1, 0:b0, 0:b0, 1:b1, 2:b2, 3:b3 }
            // --> w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 3:B3, 0:C0, 1:C1, 2:C2, 3:C3 }
            //     w2 = { 0:a0, 1:a1, 0:b0, 0:b0, 1:b1, 2:b2, 3:b3 }
            for (final int expectedKey : expectedKeys) {
                inputTopic1.pipeInput(expectedKey, "C" + expectedKey);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "C0+a0", 0),
                new KeyValueTimestamp<>(0, "C0+b0", 0),
                new KeyValueTimestamp<>(1, "C1+a1", 0),
                new KeyValueTimestamp<>(1, "C1+b1", 0),
                new KeyValueTimestamp<>(2, "C2+b2", 0),
                new KeyValueTimestamp<>(3, "C3+b3", 0));

            // push two items to the other stream; this should produce six items
            // w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 3:B3, 0:C0, 1:C1, 2:C2, 3:C3 }
            // w2 = { 0:a0, 1:a1, 0:b0, 0:b0, 1:b1, 2:b2, 3:b3 }
            // --> w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 3:B3, 0:C0, 1:C1, 2:C2, 3:C3 }
            //     w2 = { 0:a0, 1:a1, 0:b0, 0:b0, 1:b1, 2:b2, 3:b3, 0:c0, 1:c1 }
            for (int i = 0; i < 2; i++) {
                inputTopic2.pipeInput(expectedKeys[i], "c" + expectedKeys[i]);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+c0", 0),
                new KeyValueTimestamp<>(0, "B0+c0", 0),
                new KeyValueTimestamp<>(0, "C0+c0", 0),
                new KeyValueTimestamp<>(1, "A1+c1", 0),
                new KeyValueTimestamp<>(1, "B1+c1", 0),
                new KeyValueTimestamp<>(1, "C1+c1", 0));
        }
    }

    @Test
    public void testWindowing() {
        final StreamsBuilder builder = new StreamsBuilder();

        final int[] expectedKeys = new int[] {0, 1, 2, 3};

        final KStream<Integer, String> stream1;
        final KStream<Integer, String> stream2;
        final KStream<Integer, String> joined;
        final MockProcessorSupplier<Integer, String> supplier = new MockProcessorSupplier<>();
        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed);

        joined = stream1.join(
            stream2,
            MockValueJoiner.TOSTRING_JOINER,
            JoinWindows.of(ofMillis(100)),
            StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.String()));
        joined.process(supplier);

        final Collection<Set<String>> copartitionGroups =
            TopologyWrapper.getInternalTopologyBuilder(builder.build()).copartitionGroups();

        assertEquals(1, copartitionGroups.size());
        assertEquals(new HashSet<>(Arrays.asList(topic1, topic2)), copartitionGroups.iterator().next());

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<Integer, String> inputTopic1 =
                    driver.createInputTopic(topic1, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<Integer, String> inputTopic2 =
                    driver.createInputTopic(topic2, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockProcessor<Integer, String> processor = supplier.theCapturedProcessor();
            long time = 0L;

            // push two items to the primary stream; the other window is empty; this should produce no items
            // w1 = {}
            // w2 = {}
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
            //     w2 = {}
            for (int i = 0; i < 2; i++) {
                inputTopic1.pipeInput(expectedKeys[i], "A" + expectedKeys[i], time);
            }
            processor.checkAndClearProcessResult(EMPTY);

            // push two items to the other stream; this should produce two items
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
            // w2 = {}
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
            //     w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0) }
            for (int i = 0; i < 2; i++) {
                inputTopic2.pipeInput(expectedKeys[i], "a" + expectedKeys[i], time);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+a0", 0),
                new KeyValueTimestamp<>(1, "A1+a1", 0));

            // push four items to the primary stream with larger and increasing timestamp; this should produce no items
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
            // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0) }
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //            0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003) }
            //     w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0) }
            time = 1000L;
            for (int i = 0; i < expectedKeys.length; i++) {
                inputTopic1.pipeInput(expectedKeys[i], "B" + expectedKeys[i], time + i);
            }
            processor.checkAndClearProcessResult(EMPTY);

            // push four items to the other stream with fixed larger timestamp; this should produce four items
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //        0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003)  }
            // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0) }
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //            0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003) }
            //     w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
            //            0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100) }
            time += 100L;
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, "b" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "B0+b0", 1100),
                new KeyValueTimestamp<>(1, "B1+b1", 1100),
                new KeyValueTimestamp<>(2, "B2+b2", 1100),
                new KeyValueTimestamp<>(3, "B3+b3", 1100));

            // push four items to the other stream with incremented timestamp; this should produce three items
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //        0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003)  }
            // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
            //        0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100) }
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //            0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003) }
            //     w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
            //            0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100),
            //            0:c0 (ts: 1101), 1:c1 (ts: 1101), 2:c2 (ts: 1101), 3:c3 (ts: 1101) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, "c" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(1, "B1+c1", 1101),
                new KeyValueTimestamp<>(2, "B2+c2", 1101),
                new KeyValueTimestamp<>(3, "B3+c3", 1101));

            // push four items to the other stream with incremented timestamp; this should produce two items
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //        0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003)  }
            // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
            //        0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100),
            //        0:c0 (ts: 1101), 1:c1 (ts: 1101), 2:c2 (ts: 1101), 3:c3 (ts: 1101) }
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //            0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003) }
            //     w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
            //            0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100),
            //            0:c0 (ts: 1101), 1:c1 (ts: 1101), 2:c2 (ts: 1101), 3:c3 (ts: 1101),
            //            0:d0 (ts: 1102), 1:d1 (ts: 1102), 2:d2 (ts: 1102), 3:d3 (ts: 1102) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, "d" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(2, "B2+d2", 1102),
                new KeyValueTimestamp<>(3, "B3+d3", 1102));

            // push four items to the other stream with incremented timestamp; this should produce one item
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //        0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003)  }
            // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
            //        0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100),
            //        0:c0 (ts: 1101), 1:c1 (ts: 1101), 2:c2 (ts: 1101), 3:c3 (ts: 1101),
            //        0:d0 (ts: 1102), 1:d1 (ts: 1102), 2:d2 (ts: 1102), 3:d3 (ts: 1102) }
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //            0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003) }
            //     w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
            //            0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100),
            //            0:c0 (ts: 1101), 1:c1 (ts: 1101), 2:c2 (ts: 1101), 3:c3 (ts: 1101),
            //            0:d0 (ts: 1102), 1:d1 (ts: 1102), 2:d2 (ts: 1102), 3:d3 (ts: 1102),
            //            0:e0 (ts: 1103), 1:e1 (ts: 1103), 2:e2 (ts: 1103), 3:e3 (ts: 1103) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, "e" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(3, "B3+e3", 1103));

            // push four items to the other stream with incremented timestamp; this should produce no items
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //        0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003)  }
            // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
            //        0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100),
            //        0:c0 (ts: 1101), 1:c1 (ts: 1101), 2:c2 (ts: 1101), 3:c3 (ts: 1101),
            //        0:d0 (ts: 1102), 1:d1 (ts: 1102), 2:d2 (ts: 1102), 3:d3 (ts: 1102),
            //        0:e0 (ts: 1103), 1:e1 (ts: 1103), 2:e2 (ts: 1103), 3:e3 (ts: 1103) }
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //            0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003) }
            //     w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
            //            0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100),
            //            0:c0 (ts: 1101), 1:c1 (ts: 1101), 2:c2 (ts: 1101), 3:c3 (ts: 1101),
            //            0:d0 (ts: 1102), 1:d1 (ts: 1102), 2:d2 (ts: 1102), 3:d3 (ts: 1102),
            //            0:e0 (ts: 1103), 1:e1 (ts: 1103), 2:e2 (ts: 1103), 3:e3 (ts: 1103),
            //            0:f0 (ts: 1104), 1:f1 (ts: 1104), 2:f2 (ts: 1104), 3:f3 (ts: 1104) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, "f" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(EMPTY);

            // push four items to the other stream with timestamp before the window bound; this should produce no items
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //        0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003)  }
            // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
            //        0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100),
            //        0:c0 (ts: 1101), 1:c1 (ts: 1101), 2:c2 (ts: 1101), 3:c3 (ts: 1101),
            //        0:d0 (ts: 1102), 1:d1 (ts: 1102), 2:d2 (ts: 1102), 3:d3 (ts: 1102),
            //        0:e0 (ts: 1103), 1:e1 (ts: 1103), 2:e2 (ts: 1103), 3:e3 (ts: 1103),
            //        0:f0 (ts: 1104), 1:f1 (ts: 1104), 2:f2 (ts: 1104), 3:f3 (ts: 1104) }
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //            0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003) }
            //     w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
            //            0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100),
            //            0:c0 (ts: 1101), 1:c1 (ts: 1101), 2:c2 (ts: 1101), 3:c3 (ts: 1101),
            //            0:d0 (ts: 1102), 1:d1 (ts: 1102), 2:d2 (ts: 1102), 3:d3 (ts: 1102),
            //            0:e0 (ts: 1103), 1:e1 (ts: 1103), 2:e2 (ts: 1103), 3:e3 (ts: 1103),
            //            0:f0 (ts: 1104), 1:f1 (ts: 1104), 2:f2 (ts: 1104), 3:f3 (ts: 1104),
            //            0:g0 (ts: 899), 1:g1 (ts: 899), 2:g2 (ts: 899), 3:g3 (ts: 899) }
            time = 1000L - 100L - 1L;
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, "g" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(EMPTY);

            // push four items to the other stream with with incremented timestamp; this should produce one item
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //        0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003)  }
            // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
            //        0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100),
            //        0:c0 (ts: 1101), 1:c1 (ts: 1101), 2:c2 (ts: 1101), 3:c3 (ts: 1101),
            //        0:d0 (ts: 1102), 1:d1 (ts: 1102), 2:d2 (ts: 1102), 3:d3 (ts: 1102),
            //        0:e0 (ts: 1103), 1:e1 (ts: 1103), 2:e2 (ts: 1103), 3:e3 (ts: 1103),
            //        0:f0 (ts: 1104), 1:f1 (ts: 1104), 2:f2 (ts: 1104), 3:f3 (ts: 1104),
            //        0:g0 (ts: 899), 1:g1 (ts: 899), 2:g2 (ts: 899), 3:g3 (ts: 899) }
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //            0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003) }
            //     w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
            //            0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100),
            //            0:c0 (ts: 1101), 1:c1 (ts: 1101), 2:c2 (ts: 1101), 3:c3 (ts: 1101),
            //            0:d0 (ts: 1102), 1:d1 (ts: 1102), 2:d2 (ts: 1102), 3:d3 (ts: 1102),
            //            0:e0 (ts: 1103), 1:e1 (ts: 1103), 2:e2 (ts: 1103), 3:e3 (ts: 1103),
            //            0:f0 (ts: 1104), 1:f1 (ts: 1104), 2:f2 (ts: 1104), 3:f3 (ts: 1104),
            //            0:g0 (ts: 899), 1:g1 (ts: 899), 2:g2 (ts: 899), 3:g3 (ts: 899),
            //            0:h0 (ts: 900), 1:h1 (ts: 900), 2:h2 (ts: 900), 3:h3 (ts: 900) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, "h" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "B0+h0", 1000));

            // push four items to the other stream with with incremented timestamp; this should produce two items
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //        0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003)  }
            // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
            //        0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100),
            //        0:c0 (ts: 1101), 1:c1 (ts: 1101), 2:c2 (ts: 1101), 3:c3 (ts: 1101),
            //        0:d0 (ts: 1102), 1:d1 (ts: 1102), 2:d2 (ts: 1102), 3:d3 (ts: 1102),
            //        0:e0 (ts: 1103), 1:e1 (ts: 1103), 2:e2 (ts: 1103), 3:e3 (ts: 1103),
            //        0:f0 (ts: 1104), 1:f1 (ts: 1104), 2:f2 (ts: 1104), 3:f3 (ts: 1104),
            //        0:g0 (ts: 899), 1:g1 (ts: 899), 2:g2 (ts: 899), 3:g3 (ts: 899),
            //        0:h0 (ts: 900), 1:h1 (ts: 900), 2:h2 (ts: 900), 3:h3 (ts: 900) }
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //            0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003) }
            //     w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
            //            0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100),
            //            0:c0 (ts: 1101), 1:c1 (ts: 1101), 2:c2 (ts: 1101), 3:c3 (ts: 1101),
            //            0:d0 (ts: 1102), 1:d1 (ts: 1102), 2:d2 (ts: 1102), 3:d3 (ts: 1102),
            //            0:e0 (ts: 1103), 1:e1 (ts: 1103), 2:e2 (ts: 1103), 3:e3 (ts: 1103),
            //            0:f0 (ts: 1104), 1:f1 (ts: 1104), 2:f2 (ts: 1104), 3:f3 (ts: 1104),
            //            0:g0 (ts: 899), 1:g1 (ts: 899), 2:g2 (ts: 899), 3:g3 (ts: 899),
            //            0:h0 (ts: 900), 1:h1 (ts: 900), 2:h2 (ts: 900), 3:h3 (ts: 900),
            //            0:i0 (ts: 901), 1:i1 (ts: 901), 2:i2 (ts: 901), 3:i3 (ts: 901) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, "i" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "B0+i0", 1000),
                new KeyValueTimestamp<>(1, "B1+i1", 1001));

            // push four items to the other stream with with incremented timestamp; this should produce three items
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //        0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003)  }
            // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
            //        0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100),
            //        0:c0 (ts: 1101), 1:c1 (ts: 1101), 2:c2 (ts: 1101), 3:c3 (ts: 1101),
            //        0:d0 (ts: 1102), 1:d1 (ts: 1102), 2:d2 (ts: 1102), 3:d3 (ts: 1102),
            //        0:e0 (ts: 1103), 1:e1 (ts: 1103), 2:e2 (ts: 1103), 3:e3 (ts: 1103),
            //        0:f0 (ts: 1104), 1:f1 (ts: 1104), 2:f2 (ts: 1104), 3:f3 (ts: 1104),
            //        0:g0 (ts: 899), 1:g1 (ts: 899), 2:g2 (ts: 899), 3:g3 (ts: 899),
            //        0:h0 (ts: 900), 1:h1 (ts: 900), 2:h2 (ts: 900), 3:h3 (ts: 900),
            //        0:i0 (ts: 901), 1:i1 (ts: 901), 2:i2 (ts: 901), 3:i3 (ts: 901) }
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //            0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003) }
            //     w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
            //            0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100),
            //            0:c0 (ts: 1101), 1:c1 (ts: 1101), 2:c2 (ts: 1101), 3:c3 (ts: 1101),
            //            0:d0 (ts: 1102), 1:d1 (ts: 1102), 2:d2 (ts: 1102), 3:d3 (ts: 1102),
            //            0:e0 (ts: 1103), 1:e1 (ts: 1103), 2:e2 (ts: 1103), 3:e3 (ts: 1103),
            //            0:f0 (ts: 1104), 1:f1 (ts: 1104), 2:f2 (ts: 1104), 3:f3 (ts: 1104),
            //            0:g0 (ts: 899), 1:g1 (ts: 899), 2:g2 (ts: 899), 3:g3 (ts: 899),
            //            0:h0 (ts: 900), 1:h1 (ts: 900), 2:h2 (ts: 900), 3:h3 (ts: 900),
            //            0:i0 (ts: 901), 1:i1 (ts: 901), 2:i2 (ts: 901), 3:i3 (ts: 901),
            //            0:j0 (ts: 902), 1:j1 (ts: 902), 2:j2 (ts: 902), 3:j3 (ts: 902) }
            time += 1;
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, "j" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "B0+j0", 1000),
                new KeyValueTimestamp<>(1, "B1+j1", 1001),
                new KeyValueTimestamp<>(2, "B2+j2", 1002));

            // push four items to the other stream with with incremented timestamp; this should produce four items
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //        0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003)  }
            // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
            //        0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100),
            //        0:c0 (ts: 1101), 1:c1 (ts: 1101), 2:c2 (ts: 1101), 3:c3 (ts: 1101),
            //        0:d0 (ts: 1102), 1:d1 (ts: 1102), 2:d2 (ts: 1102), 3:d3 (ts: 1102),
            //        0:e0 (ts: 1103), 1:e1 (ts: 1103), 2:e2 (ts: 1103), 3:e3 (ts: 1103),
            //        0:f0 (ts: 1104), 1:f1 (ts: 1104), 2:f2 (ts: 1104), 3:f3 (ts: 1104),
            //        0:g0 (ts: 899), 1:g1 (ts: 899), 2:g2 (ts: 899), 3:g3 (ts: 899),
            //        0:h0 (ts: 900), 1:h1 (ts: 900), 2:h2 (ts: 900), 3:h3 (ts: 900),
            //        0:i0 (ts: 901), 1:i1 (ts: 901), 2:i2 (ts: 901), 3:i3 (ts: 901),
            //        0:j0 (ts: 902), 1:j1 (ts: 902), 2:j2 (ts: 902), 3:j3 (ts: 902) }
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //            0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003) }
            //     w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
            //            0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100),
            //            0:c0 (ts: 1101), 1:c1 (ts: 1101), 2:c2 (ts: 1101), 3:c3 (ts: 1101),
            //            0:d0 (ts: 1102), 1:d1 (ts: 1102), 2:d2 (ts: 1102), 3:d3 (ts: 1102),
            //            0:e0 (ts: 1103), 1:e1 (ts: 1103), 2:e2 (ts: 1103), 3:e3 (ts: 1103),
            //            0:f0 (ts: 1104), 1:f1 (ts: 1104), 2:f2 (ts: 1104), 3:f3 (ts: 1104),
            //            0:g0 (ts: 899), 1:g1 (ts: 899), 2:g2 (ts: 899), 3:g3 (ts: 899),
            //            0:h0 (ts: 900), 1:h1 (ts: 900), 2:h2 (ts: 900), 3:h3 (ts: 900),
            //            0:i0 (ts: 901), 1:i1 (ts: 901), 2:i2 (ts: 901), 3:i3 (ts: 901),
            //            0:j0 (ts: 902), 1:j1 (ts: 902), 2:j2 (ts: 902), 3:j3 (ts: 902) }
            //            0:k0 (ts: 903), 1:k1 (ts: 903), 2:k2 (ts: 903), 3:k3 (ts: 903) }
            time += 1;
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, "k" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "B0+k0", 1000),
                new KeyValueTimestamp<>(1, "B1+k1", 1001),
                new KeyValueTimestamp<>(2, "B2+k2", 1002),
                new KeyValueTimestamp<>(3, "B3+k3", 1003));

            // advance time to not join with existing data
            // we omit above exiting data, even if it's still in the window
            //
            // push four items with increasing timestamps to the other stream. the primary window is empty; this should produce no items
            // w1 = {}
            // w2 = {}
            // --> w1 = {}
            //     w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
            time = 2000L;
            for (int i = 0; i < expectedKeys.length; i++) {
                inputTopic2.pipeInput(expectedKeys[i], "l" + expectedKeys[i], time + i);
            }
            processor.checkAndClearProcessResult(EMPTY);

            // push four items with larger timestamps to the primary stream; this should produce four items
            // w1 = {}
            // w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
            // --> w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100) }
            //     w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
            time = 2000L + 100L;
            for (final int expectedKey : expectedKeys) {
                inputTopic1.pipeInput(expectedKey, "C" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "C0+l0", 2100),
                new KeyValueTimestamp<>(1, "C1+l1", 2100),
                new KeyValueTimestamp<>(2, "C2+l2", 2100),
                new KeyValueTimestamp<>(3, "C3+l3", 2100));

            // push four items with increase timestamps to the primary stream; this should produce three items
            // w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100) }
            // w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
            // --> w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100),
            //            0:D0 (ts: 2101), 1:D1 (ts: 2101), 2:D2 (ts: 2101), 3:D3 (ts: 2101) }
            //     w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                inputTopic1.pipeInput(expectedKey, "D" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(1, "D1+l1", 2101),
                new KeyValueTimestamp<>(2, "D2+l2", 2101),
                new KeyValueTimestamp<>(3, "D3+l3", 2101));

            // push four items with increase timestamps to the primary stream; this should produce two items
            // w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100),
            //        0:D0 (ts: 2101), 1:D1 (ts: 2101), 2:D2 (ts: 2101), 3:D3 (ts: 2101) }
            // w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
            // --> w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100),
            //            0:D0 (ts: 2101), 1:D1 (ts: 2101), 2:D2 (ts: 2101), 3:D3 (ts: 2101),
            //            0:E0 (ts: 2102), 1:E1 (ts: 2102), 2:E2 (ts: 2102), 3:E3 (ts: 2102) }
            //     w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                inputTopic1.pipeInput(expectedKey, "E" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(2, "E2+l2", 2102),
                new KeyValueTimestamp<>(3, "E3+l3", 2102));

            // push four items with increase timestamps to the primary stream; this should produce one item
            // w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100),
            //        0:D0 (ts: 2101), 1:D1 (ts: 2101), 2:D2 (ts: 2101), 3:D3 (ts: 2101),
            //        0:E0 (ts: 2102), 1:E1 (ts: 2102), 2:E2 (ts: 2102), 3:E3 (ts: 2102) }
            // w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
            // --> w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100),
            //            0:D0 (ts: 2101), 1:D1 (ts: 2101), 2:D2 (ts: 2101), 3:D3 (ts: 2101),
            //            0:E0 (ts: 2102), 1:E1 (ts: 2102), 2:E2 (ts: 2102), 3:E3 (ts: 2102),
            //            0:F0 (ts: 2103), 1:F1 (ts: 2103), 2:F2 (ts: 2103), 3:F3 (ts: 2103) }
            //     w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                inputTopic1.pipeInput(expectedKey, "F" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(3, "F3+l3", 2103));

            // push four items with increase timestamps (now out of window) to the primary stream; this should produce no items
            // w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100),
            //        0:D0 (ts: 2101), 1:D1 (ts: 2101), 2:D2 (ts: 2101), 3:D3 (ts: 2101),
            //        0:E0 (ts: 2102), 1:E1 (ts: 2102), 2:E2 (ts: 2102), 3:E3 (ts: 2102),
            //        0:F0 (ts: 2103), 1:F1 (ts: 2103), 2:F2 (ts: 2103), 3:F3 (ts: 2103) }
            // w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
            // --> w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100),
            //            0:D0 (ts: 2101), 1:D1 (ts: 2101), 2:D2 (ts: 2101), 3:D3 (ts: 2101),
            //            0:E0 (ts: 2102), 1:E1 (ts: 2102), 2:E2 (ts: 2102), 3:E3 (ts: 2102),
            //            0:F0 (ts: 2103), 1:F1 (ts: 2103), 2:F2 (ts: 2103), 3:F3 (ts: 2103),
            //            0:G0 (ts: 2104), 1:G1 (ts: 2104), 2:G2 (ts: 2104), 3:G3 (ts: 2104) }
            //     w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                inputTopic1.pipeInput(expectedKey, "G" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(EMPTY);

            // push four items with smaller timestamps (before window) to the primary stream; this should produce no items
            // w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100),
            //        0:D0 (ts: 2101), 1:D1 (ts: 2101), 2:D2 (ts: 2101), 3:D3 (ts: 2101),
            //        0:E0 (ts: 2102), 1:E1 (ts: 2102), 2:E2 (ts: 2102), 3:E3 (ts: 2102),
            //        0:F0 (ts: 2103), 1:F1 (ts: 2103), 2:F2 (ts: 2103), 3:F3 (ts: 2103),
            //        0:G0 (ts: 2104), 1:G1 (ts: 2104), 2:G2 (ts: 2104), 3:G3 (ts: 2104) }
            // w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
            // --> w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100),
            //            0:D0 (ts: 2101), 1:D1 (ts: 2101), 2:D2 (ts: 2101), 3:D3 (ts: 2101),
            //            0:E0 (ts: 2102), 1:E1 (ts: 2102), 2:E2 (ts: 2102), 3:E3 (ts: 2102),
            //            0:F0 (ts: 2103), 1:F1 (ts: 2103), 2:F2 (ts: 2103), 3:F3 (ts: 2103),
            //            0:G0 (ts: 2104), 1:G1 (ts: 2104), 2:G2 (ts: 2104), 3:G3 (ts: 2104),
            //            0:H0 (ts: 1899), 1:H1 (ts: 1899), 2:H2 (ts: 1899), 3:H3 (ts: 1899) }
            //     w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
            time = 2000L - 100L - 1L;
            for (final int expectedKey : expectedKeys) {
                inputTopic1.pipeInput(expectedKey, "H" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(EMPTY);

            // push four items with increased timestamps to the primary stream; this should produce one item
            // w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100),
            //        0:D0 (ts: 2101), 1:D1 (ts: 2101), 2:D2 (ts: 2101), 3:D3 (ts: 2101),
            //        0:E0 (ts: 2102), 1:E1 (ts: 2102), 2:E2 (ts: 2102), 3:E3 (ts: 2102),
            //        0:F0 (ts: 2103), 1:F1 (ts: 2103), 2:F2 (ts: 2103), 3:F3 (ts: 2103),
            //        0:G0 (ts: 2104), 1:G1 (ts: 2104), 2:G2 (ts: 2104), 3:G3 (ts: 2104),
            //        0:H0 (ts: 1899), 1:H1 (ts: 1899), 2:H2 (ts: 1899), 3:H3 (ts: 1899) }
            // w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
            // --> w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100),
            //            0:D0 (ts: 2101), 1:D1 (ts: 2101), 2:D2 (ts: 2101), 3:D3 (ts: 2101),
            //            0:E0 (ts: 2102), 1:E1 (ts: 2102), 2:E2 (ts: 2102), 3:E3 (ts: 2102),
            //            0:F0 (ts: 2103), 1:F1 (ts: 2103), 2:F2 (ts: 2103), 3:F3 (ts: 2103),
            //            0:G0 (ts: 2104), 1:G1 (ts: 2104), 2:G2 (ts: 2104), 3:G3 (ts: 2104),
            //            0:H0 (ts: 1899), 1:H1 (ts: 1899), 2:H2 (ts: 1899), 3:H3 (ts: 1899),
            //            0:I0 (ts: 1900), 1:I1 (ts: 1900), 2:I2 (ts: 1900), 3:I3 (ts: 1900) }
            //     w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                inputTopic1.pipeInput(expectedKey, "I" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "I0+l0", 2000));

            // push four items with increased timestamps to the primary stream; this should produce two items
            // w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100),
            //        0:D0 (ts: 2101), 1:D1 (ts: 2101), 2:D2 (ts: 2101), 3:D3 (ts: 2101),
            //        0:E0 (ts: 2102), 1:E1 (ts: 2102), 2:E2 (ts: 2102), 3:E3 (ts: 2102),
            //        0:F0 (ts: 2103), 1:F1 (ts: 2103), 2:F2 (ts: 2103), 3:F3 (ts: 2103),
            //        0:G0 (ts: 2104), 1:G1 (ts: 2104), 2:G2 (ts: 2104), 3:G3 (ts: 2104),
            //        0:H0 (ts: 1899), 1:H1 (ts: 1899), 2:H2 (ts: 1899), 3:H3 (ts: 1899),
            //        0:I0 (ts: 1900), 1:I1 (ts: 1900), 2:I2 (ts: 1900), 3:I3 (ts: 1900) }
            // w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
            // --> w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100),
            //            0:D0 (ts: 2101), 1:D1 (ts: 2101), 2:D2 (ts: 2101), 3:D3 (ts: 2101),
            //            0:E0 (ts: 2102), 1:E1 (ts: 2102), 2:E2 (ts: 2102), 3:E3 (ts: 2102),
            //            0:F0 (ts: 2103), 1:F1 (ts: 2103), 2:F2 (ts: 2103), 3:F3 (ts: 2103),
            //            0:G0 (ts: 2104), 1:G1 (ts: 2104), 2:G2 (ts: 2104), 3:G3 (ts: 2104),
            //            0:H0 (ts: 1899), 1:H1 (ts: 1899), 2:H2 (ts: 1899), 3:H3 (ts: 1899),
            //            0:I0 (ts: 1900), 1:I1 (ts: 1900), 2:I2 (ts: 1900), 3:I3 (ts: 1900),
            //            0:J0 (ts: 1901), 1:J1 (ts: 1901), 2:J2 (ts: 1901), 3:J3 (ts: 1901) }
            //     w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                inputTopic1.pipeInput(expectedKey, "J" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "J0+l0", 2000),
                new KeyValueTimestamp<>(1, "J1+l1", 2001));

            // push four items with increased timestamps to the primary stream; this should produce three items
            // w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100),
            //        0:D0 (ts: 2101), 1:D1 (ts: 2101), 2:D2 (ts: 2101), 3:D3 (ts: 2101),
            //        0:E0 (ts: 2102), 1:E1 (ts: 2102), 2:E2 (ts: 2102), 3:E3 (ts: 2102),
            //        0:F0 (ts: 2103), 1:F1 (ts: 2103), 2:F2 (ts: 2103), 3:F3 (ts: 2103),
            //        0:G0 (ts: 2104), 1:G1 (ts: 2104), 2:G2 (ts: 2104), 3:G3 (ts: 2104),
            //        0:H0 (ts: 1899), 1:H1 (ts: 1899), 2:H2 (ts: 1899), 3:H3 (ts: 1899),
            //        0:I0 (ts: 1900), 1:I1 (ts: 1900), 2:I2 (ts: 1900), 3:I3 (ts: 1900),
            //        0:J0 (ts: 1901), 1:J1 (ts: 1901), 2:J2 (ts: 1901), 3:J3 (ts: 1901) }
            // w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
            // --> w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100),
            //            0:D0 (ts: 2101), 1:D1 (ts: 2101), 2:D2 (ts: 2101), 3:D3 (ts: 2101),
            //            0:E0 (ts: 2102), 1:E1 (ts: 2102), 2:E2 (ts: 2102), 3:E3 (ts: 2102),
            //            0:F0 (ts: 2103), 1:F1 (ts: 2103), 2:F2 (ts: 2103), 3:F3 (ts: 2103),
            //            0:G0 (ts: 2104), 1:G1 (ts: 2104), 2:G2 (ts: 2104), 3:G3 (ts: 2104),
            //            0:H0 (ts: 1899), 1:H1 (ts: 1899), 2:H2 (ts: 1899), 3:H3 (ts: 1899),
            //            0:I0 (ts: 1900), 1:I1 (ts: 1900), 2:I2 (ts: 1900), 3:I3 (ts: 1900),
            //            0:J0 (ts: 1901), 1:J1 (ts: 1901), 2:J2 (ts: 1901), 3:J3 (ts: 1901),
            //            0:K0 (ts: 1902), 1:K1 (ts: 1902), 2:K2 (ts: 1902), 3:K3 (ts: 1902) }
            //     w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                inputTopic1.pipeInput(expectedKey, "K" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "K0+l0", 2000),
                new KeyValueTimestamp<>(1, "K1+l1", 2001),
                new KeyValueTimestamp<>(2, "K2+l2", 2002));

            // push four items with increased timestamps to the primary stream; this should produce four items
            // w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100),
            //        0:D0 (ts: 2101), 1:D1 (ts: 2101), 2:D2 (ts: 2101), 3:D3 (ts: 2101),
            //        0:E0 (ts: 2102), 1:E1 (ts: 2102), 2:E2 (ts: 2102), 3:E3 (ts: 2102),
            //        0:F0 (ts: 2103), 1:F1 (ts: 2103), 2:F2 (ts: 2103), 3:F3 (ts: 2103),
            //        0:G0 (ts: 2104), 1:G1 (ts: 2104), 2:G2 (ts: 2104), 3:G3 (ts: 2104),
            //        0:H0 (ts: 1899), 1:H1 (ts: 1899), 2:H2 (ts: 1899), 3:H3 (ts: 1899),
            //        0:I0 (ts: 1900), 1:I1 (ts: 1900), 2:I2 (ts: 1900), 3:I3 (ts: 1900),
            //        0:J0 (ts: 1901), 1:J1 (ts: 1901), 2:J2 (ts: 1901), 3:J3 (ts: 1901) }
            //        0:K0 (ts: 1902), 1:K1 (ts: 1902), 2:K2 (ts: 1902), 3:K3 (ts: 1902) }
            // w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
            // --> w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100),
            //            0:D0 (ts: 2101), 1:D1 (ts: 2101), 2:D2 (ts: 2101), 3:D3 (ts: 2101),
            //            0:E0 (ts: 2102), 1:E1 (ts: 2102), 2:E2 (ts: 2102), 3:E3 (ts: 2102),
            //            0:F0 (ts: 2103), 1:F1 (ts: 2103), 2:F2 (ts: 2103), 3:F3 (ts: 2103),
            //            0:G0 (ts: 2104), 1:G1 (ts: 2104), 2:G2 (ts: 2104), 3:G3 (ts: 2104),
            //            0:H0 (ts: 1899), 1:H1 (ts: 1899), 2:H2 (ts: 1899), 3:H3 (ts: 1899),
            //            0:I0 (ts: 1900), 1:I1 (ts: 1900), 2:I2 (ts: 1900), 3:I3 (ts: 1900),
            //            0:J0 (ts: 1901), 1:J1 (ts: 1901), 2:J2 (ts: 1901), 3:J3 (ts: 1901),
            //            0:K0 (ts: 1902), 1:K1 (ts: 1902), 2:K2 (ts: 1902), 3:K3 (ts: 1902),
            //            0:L0 (ts: 1903), 1:L1 (ts: 1903), 2:L2 (ts: 1903), 3:L3 (ts: 1903) }
            //     w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                inputTopic1.pipeInput(expectedKey, "L" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "L0+l0", 2000),
                new KeyValueTimestamp<>(1, "L1+l1", 2001),
                new KeyValueTimestamp<>(2, "L2+l2", 2002),
                new KeyValueTimestamp<>(3, "L3+l3", 2003));
        }
    }

    @Test
    public void testAsymmetricWindowingAfter() {
        final StreamsBuilder builder = new StreamsBuilder();

        final int[] expectedKeys = new int[] {0, 1, 2, 3};

        final KStream<Integer, String> stream1;
        final KStream<Integer, String> stream2;
        final KStream<Integer, String> joined;
        final MockProcessorSupplier<Integer, String> supplier = new MockProcessorSupplier<>();
        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed);

        joined = stream1.join(
            stream2,
            MockValueJoiner.TOSTRING_JOINER,
            JoinWindows.of(ofMillis(0)).after(ofMillis(100)),
            StreamJoined.with(Serdes.Integer(),
                Serdes.String(),
                Serdes.String()));
        joined.process(supplier);

        final Collection<Set<String>> copartitionGroups =
            TopologyWrapper.getInternalTopologyBuilder(builder.build()).copartitionGroups();

        assertEquals(1, copartitionGroups.size());
        assertEquals(new HashSet<>(Arrays.asList(topic1, topic2)), copartitionGroups.iterator().next());

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<Integer, String> inputTopic1 =
                    driver.createInputTopic(topic1, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<Integer, String> inputTopic2 =
                    driver.createInputTopic(topic2, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockProcessor<Integer, String> processor = supplier.theCapturedProcessor();
            long time = 1000L;

            // push four items with increasing timestamps to the primary stream; the other window is empty; this should produce no items
            // w1 = {}
            // w2 = {}
            // --> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            //     w2 = {}
            for (int i = 0; i < expectedKeys.length; i++) {
                inputTopic1.pipeInput(expectedKeys[i], "A" + expectedKeys[i], time + i);
            }
            processor.checkAndClearProcessResult(EMPTY);

            // push four items smaller timestamps (out of window) to the secondary stream; this should produce no items
            // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            // w2 = {}
            // --> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            //     w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999) }
            time = 1000L - 1L;
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, "a" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(EMPTY);

            // push four items with increased timestamps to the secondary stream; this should produce one item
            // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            // w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999) }
            // --> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            //     w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999),
            //            0:b0 (ts: 1000), 1:b1 (ts: 1000), 2:b2 (ts: 1000), 3:b3 (ts: 1000) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, "b" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+b0", 1000));

            // push four items with increased timestamps to the secondary stream; this should produce two items
            // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            // w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999),
            //        0:b0 (ts: 1000), 1:b1 (ts: 1000), 2:b2 (ts: 1000), 3:b3 (ts: 1000) }
            // --> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            //     w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999),
            //            0:b0 (ts: 1000), 1:b1 (ts: 1000), 2:b2 (ts: 1000), 3:b3 (ts: 1000),
            //            0:c0 (ts: 1001), 1:c1 (ts: 1001), 2:c2 (ts: 1001), 3:c3 (ts: 1001) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, "c" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+c0", 1001),
                new KeyValueTimestamp<>(1, "A1+c1", 1001));

            // push four items with increased timestamps to the secondary stream; this should produce three items
            // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            // w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999),
            //        0:b0 (ts: 1000), 1:b1 (ts: 1000), 2:b2 (ts: 1000), 3:b3 (ts: 1000),
            //        0:c0 (ts: 1001), 1:c1 (ts: 1001), 2:c2 (ts: 1001), 3:c3 (ts: 1001) }
            // --> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            //     w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999),
            //            0:b0 (ts: 1000), 1:b1 (ts: 1000), 2:b2 (ts: 1000), 3:b3 (ts: 1000),
            //            0:c0 (ts: 1001), 1:c1 (ts: 1001), 2:c2 (ts: 1001), 3:c3 (ts: 1001),
            //            0:d0 (ts: 1002), 1:d1 (ts: 1002), 2:d2 (ts: 1002), 3:d3 (ts: 1002) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, "d" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+d0", 1002),
                new KeyValueTimestamp<>(1, "A1+d1", 1002),
                new KeyValueTimestamp<>(2, "A2+d2", 1002));

            // push four items with increased timestamps to the secondary stream; this should produce four items
            // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            // w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999),
            //        0:b0 (ts: 1000), 1:b1 (ts: 1000), 2:b2 (ts: 1000), 3:b3 (ts: 1000),
            //        0:c0 (ts: 1001), 1:c1 (ts: 1001), 2:c2 (ts: 1001), 3:c3 (ts: 1001),
            //        0:d0 (ts: 1002), 1:d1 (ts: 1002), 2:d2 (ts: 1002), 3:d3 (ts: 1002) }
            // --> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            //     w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999),
            //            0:b0 (ts: 1000), 1:b1 (ts: 1000), 2:b2 (ts: 1000), 3:b3 (ts: 1000),
            //            0:c0 (ts: 1001), 1:c1 (ts: 1001), 2:c2 (ts: 1001), 3:c3 (ts: 1001),
            //            0:d0 (ts: 1002), 1:d1 (ts: 1002), 2:d2 (ts: 1002), 3:d3 (ts: 1002),
            //            0:e0 (ts: 1003), 1:e1 (ts: 1003), 2:e2 (ts: 1003), 3:e3 (ts: 1003) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, "e" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+e0", 1003),
                new KeyValueTimestamp<>(1, "A1+e1", 1003),
                new KeyValueTimestamp<>(2, "A2+e2", 1003),
                new KeyValueTimestamp<>(3, "A3+e3", 1003));

            // push four items with larger timestamps to the secondary stream; this should produce four items
            // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            // w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999),
            //        0:b0 (ts: 1000), 1:b1 (ts: 1000), 2:b2 (ts: 1000), 3:b3 (ts: 1000),
            //        0:c0 (ts: 1001), 1:c1 (ts: 1001), 2:c2 (ts: 1001), 3:c3 (ts: 1001),
            //        0:d0 (ts: 1002), 1:d1 (ts: 1002), 2:d2 (ts: 1002), 3:d3 (ts: 1002),
            //        0:e0 (ts: 1003), 1:e1 (ts: 1003), 2:e2 (ts: 1003), 3:e3 (ts: 1003) }
            // --> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            //     w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999),
            //            0:b0 (ts: 1000), 1:b1 (ts: 1000), 2:b2 (ts: 1000), 3:b3 (ts: 1000),
            //            0:c0 (ts: 1001), 1:c1 (ts: 1001), 2:c2 (ts: 1001), 3:c3 (ts: 1001),
            //            0:d0 (ts: 1002), 1:d1 (ts: 1002), 2:d2 (ts: 1002), 3:d3 (ts: 1002),
            //            0:e0 (ts: 1003), 1:e1 (ts: 1003), 2:e2 (ts: 1003), 3:e3 (ts: 1003),
            //            0:f0 (ts: 1100), 1:f1 (ts: 1100), 2:f2 (ts: 1100), 3:f3 (ts: 1100) }
            time = 1000 + 100L;
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, "f" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+f0", 1100),
                new KeyValueTimestamp<>(1, "A1+f1", 1100),
                new KeyValueTimestamp<>(2, "A2+f2", 1100),
                new KeyValueTimestamp<>(3, "A3+f3", 1100));

            // push four items with increased timestamps to the secondary stream; this should produce three items
            // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            // w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999),
            //        0:b0 (ts: 1000), 1:b1 (ts: 1000), 2:b2 (ts: 1000), 3:b3 (ts: 1000),
            //        0:c0 (ts: 1001), 1:c1 (ts: 1001), 2:c2 (ts: 1001), 3:c3 (ts: 1001),
            //        0:d0 (ts: 1002), 1:d1 (ts: 1002), 2:d2 (ts: 1002), 3:d3 (ts: 1002),
            //        0:e0 (ts: 1003), 1:e1 (ts: 1003), 2:e2 (ts: 1003), 3:e3 (ts: 1003),
            //        0:f0 (ts: 1100), 1:f1 (ts: 1100), 2:f2 (ts: 1100), 3:f3 (ts: 1100) }
            // --> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            //     w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999),
            //            0:b0 (ts: 1000), 1:b1 (ts: 1000), 2:b2 (ts: 1000), 3:b3 (ts: 1000),
            //            0:c0 (ts: 1001), 1:c1 (ts: 1001), 2:c2 (ts: 1001), 3:c3 (ts: 1001),
            //            0:d0 (ts: 1002), 1:d1 (ts: 1002), 2:d2 (ts: 1002), 3:d3 (ts: 1002),
            //            0:e0 (ts: 1003), 1:e1 (ts: 1003), 2:e2 (ts: 1003), 3:e3 (ts: 1003),
            //            0:f0 (ts: 1100), 1:f1 (ts: 1100), 2:f2 (ts: 1100), 3:f3 (ts: 1100),
            //            0:g0 (ts: 1101), 1:g1 (ts: 1101), 2:g2 (ts: 1101), 3:g3 (ts: 1101) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, "g" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(1, "A1+g1", 1101),
                new KeyValueTimestamp<>(2, "A2+g2", 1101),
                new KeyValueTimestamp<>(3, "A3+g3", 1101));

            // push four items with increased timestamps to the secondary stream; this should produce two items
            // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            // w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999),
            //        0:b0 (ts: 1000), 1:b1 (ts: 1000), 2:b2 (ts: 1000), 3:b3 (ts: 1000),
            //        0:c0 (ts: 1001), 1:c1 (ts: 1001), 2:c2 (ts: 1001), 3:c3 (ts: 1001),
            //        0:d0 (ts: 1002), 1:d1 (ts: 1002), 2:d2 (ts: 1002), 3:d3 (ts: 1002),
            //        0:e0 (ts: 1003), 1:e1 (ts: 1003), 2:e2 (ts: 1003), 3:e3 (ts: 1003),
            //        0:f0 (ts: 1100), 1:f1 (ts: 1100), 2:f2 (ts: 1100), 3:f3 (ts: 1100),
            //        0:g0 (ts: 1101), 1:g1 (ts: 1101), 2:g2 (ts: 1101), 3:g3 (ts: 1101) }
            // --> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            //     w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999),
            //            0:b0 (ts: 1000), 1:b1 (ts: 1000), 2:b2 (ts: 1000), 3:b3 (ts: 1000),
            //            0:c0 (ts: 1001), 1:c1 (ts: 1001), 2:c2 (ts: 1001), 3:c3 (ts: 1001),
            //            0:d0 (ts: 1002), 1:d1 (ts: 1002), 2:d2 (ts: 1002), 3:d3 (ts: 1002),
            //            0:e0 (ts: 1003), 1:e1 (ts: 1003), 2:e2 (ts: 1003), 3:e3 (ts: 1003),
            //            0:f0 (ts: 1100), 1:f1 (ts: 1100), 2:f2 (ts: 1100), 3:f3 (ts: 1100),
            //            0:g0 (ts: 1101), 1:g1 (ts: 1101), 2:g2 (ts: 1101), 3:g3 (ts: 1101),
            //            0:h0 (ts: 1102), 1:h1 (ts: 1102), 2:h2 (ts: 1102), 3:h3 (ts: 1102) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, "h" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(2, "A2+h2", 1102),
                new KeyValueTimestamp<>(3, "A3+h3", 1102));

            // push four items with increased timestamps to the secondary stream; this should produce one item
            // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            // w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999),
            //        0:b0 (ts: 1000), 1:b1 (ts: 1000), 2:b2 (ts: 1000), 3:b3 (ts: 1000),
            //        0:c0 (ts: 1001), 1:c1 (ts: 1001), 2:c2 (ts: 1001), 3:c3 (ts: 1001),
            //        0:d0 (ts: 1002), 1:d1 (ts: 1002), 2:d2 (ts: 1002), 3:d3 (ts: 1002),
            //        0:e0 (ts: 1003), 1:e1 (ts: 1003), 2:e2 (ts: 1003), 3:e3 (ts: 1003),
            //        0:f0 (ts: 1100), 1:f1 (ts: 1100), 2:f2 (ts: 1100), 3:f3 (ts: 1100),
            //        0:g0 (ts: 1101), 1:g1 (ts: 1101), 2:g2 (ts: 1101), 3:g3 (ts: 1101),
            //        0:h0 (ts: 1102), 1:h1 (ts: 1102), 2:h2 (ts: 1102), 3:h3 (ts: 1102) }
            // --> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            //     w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999),
            //            0:b0 (ts: 1000), 1:b1 (ts: 1000), 2:b2 (ts: 1000), 3:b3 (ts: 1000),
            //            0:c0 (ts: 1001), 1:c1 (ts: 1001), 2:c2 (ts: 1001), 3:c3 (ts: 1001),
            //            0:d0 (ts: 1002), 1:d1 (ts: 1002), 2:d2 (ts: 1002), 3:d3 (ts: 1002),
            //            0:e0 (ts: 1003), 1:e1 (ts: 1003), 2:e2 (ts: 1003), 3:e3 (ts: 1003),
            //            0:f0 (ts: 1100), 1:f1 (ts: 1100), 2:f2 (ts: 1100), 3:f3 (ts: 1100),
            //            0:g0 (ts: 1101), 1:g1 (ts: 1101), 2:g2 (ts: 1101), 3:g3 (ts: 1101),
            //            0:h0 (ts: 1102), 1:h1 (ts: 1102), 2:h2 (ts: 1102), 3:h3 (ts: 1102),
            //            0:i0 (ts: 1103), 1:i1 (ts: 1103), 2:i2 (ts: 1103), 3:i3 (ts: 1103) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, "i" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(3, "A3+i3", 1103));

            // push four items with increased timestamps (no out of window) to the secondary stream; this should produce no items
            // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            // w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999),
            //        0:b0 (ts: 1000), 1:b1 (ts: 1000), 2:b2 (ts: 1000), 3:b3 (ts: 1000),
            //        0:c0 (ts: 1001), 1:c1 (ts: 1001), 2:c2 (ts: 1001), 3:c3 (ts: 1001),
            //        0:d0 (ts: 1002), 1:d1 (ts: 1002), 2:d2 (ts: 1002), 3:d3 (ts: 1002),
            //        0:e0 (ts: 1003), 1:e1 (ts: 1003), 2:e2 (ts: 1003), 3:e3 (ts: 1003),
            //        0:f0 (ts: 1100), 1:f1 (ts: 1100), 2:f2 (ts: 1100), 3:f3 (ts: 1100),
            //        0:g0 (ts: 1101), 1:g1 (ts: 1101), 2:g2 (ts: 1101), 3:g3 (ts: 1101),
            //        0:h0 (ts: 1102), 1:h1 (ts: 1102), 2:h2 (ts: 1102), 3:h3 (ts: 1102),
            //        0:i0 (ts: 1103), 1:i1 (ts: 1103), 2:i2 (ts: 1103), 3:i3 (ts: 1103) }
            // --> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            //     w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999),
            //            0:b0 (ts: 1000), 1:b1 (ts: 1000), 2:b2 (ts: 1000), 3:b3 (ts: 1000),
            //            0:c0 (ts: 1001), 1:c1 (ts: 1001), 2:c2 (ts: 1001), 3:c3 (ts: 1001),
            //            0:d0 (ts: 1002), 1:d1 (ts: 1002), 2:d2 (ts: 1002), 3:d3 (ts: 1002),
            //            0:e0 (ts: 1003), 1:e1 (ts: 1003), 2:e2 (ts: 1003), 3:e3 (ts: 1003),
            //            0:f0 (ts: 1100), 1:f1 (ts: 1100), 2:f2 (ts: 1100), 3:f3 (ts: 1100),
            //            0:g0 (ts: 1101), 1:g1 (ts: 1101), 2:g2 (ts: 1101), 3:g3 (ts: 1101),
            //            0:h0 (ts: 1102), 1:h1 (ts: 1102), 2:h2 (ts: 1102), 3:h3 (ts: 1102),
            //            0:i0 (ts: 1103), 1:i1 (ts: 1103), 2:i2 (ts: 1103), 3:i3 (ts: 1103),
            //            0:j0 (ts: 1104), 1:j1 (ts: 1104), 2:j2 (ts: 1104), 3:j3 (ts: 1104) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, "j" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(EMPTY);
        }
    }

    @Test
    public void testAsymmetricWindowingBefore() {
        final StreamsBuilder builder = new StreamsBuilder();

        final int[] expectedKeys = new int[] {0, 1, 2, 3};

        final KStream<Integer, String> stream1;
        final KStream<Integer, String> stream2;
        final KStream<Integer, String> joined;
        final MockProcessorSupplier<Integer, String> supplier = new MockProcessorSupplier<>();

        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed);

        joined = stream1.join(
            stream2,
            MockValueJoiner.TOSTRING_JOINER,
            JoinWindows.of(ofMillis(0)).before(ofMillis(100)),
            StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.String()));
        joined.process(supplier);

        final Collection<Set<String>> copartitionGroups =
            TopologyWrapper.getInternalTopologyBuilder(builder.build()).copartitionGroups();

        assertEquals(1, copartitionGroups.size());
        assertEquals(new HashSet<>(Arrays.asList(topic1, topic2)), copartitionGroups.iterator().next());

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<Integer, String> inputTopic1 =
                    driver.createInputTopic(topic1, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<Integer, String> inputTopic2 =
                    driver.createInputTopic(topic2, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockProcessor<Integer, String> processor = supplier.theCapturedProcessor();
            long time = 1000L;

            // push four items with increasing timestamps to the primary stream; the other window is empty; this should produce no items
            // w1 = {}
            // w2 = {}
            // --> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            //     w2 = {}
            for (int i = 0; i < expectedKeys.length; i++) {
                inputTopic1.pipeInput(expectedKeys[i], "A" + expectedKeys[i], time + i);
            }
            processor.checkAndClearProcessResult(EMPTY);

            // push four items with smaller timestamps (before the window) to the other stream; this should produce no items
            // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            // w2 = {}
            // --> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            //     w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899) }
            time = 1000L - 100L - 1L;
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, "a" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(EMPTY);

            // push four items with increased timestamp to the other stream; this should produce one item
            // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            // w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899) }
            // --> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            //     w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899),
            //            0:b0 (ts: 900), 1:b1 (ts: 900), 2:b2 (ts: 900), 3:b3 (ts: 900) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, "b" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+b0", 1000));

            // push four items with increased timestamp to the other stream; this should produce two items
            // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            // w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899),
            //        0:b0 (ts: 900), 1:b1 (ts: 900), 2:b2 (ts: 900), 3:b3 (ts: 900) }
            // --> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            //     w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899),
            //            0:b0 (ts: 900), 1:b1 (ts: 900), 2:b2 (ts: 900), 3:b3 (ts: 900),
            //            0:c0 (ts: 901), 1:c1 (ts: 901), 2:c2 (ts: 901), 3:c3 (ts: 901) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, "c" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+c0", 1000),
                new KeyValueTimestamp<>(1, "A1+c1", 1001));

            // push four items with increased timestamp to the other stream; this should produce three items
            // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            // w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899),
            //        0:b0 (ts: 900), 1:b1 (ts: 900), 2:b2 (ts: 900), 3:b3 (ts: 900),
            //        0:c0 (ts: 901), 1:c1 (ts: 901), 2:c2 (ts: 901), 3:c3 (ts: 901) }
            // --> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            //     w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899),
            //            0:b0 (ts: 900), 1:b1 (ts: 900), 2:b2 (ts: 900), 3:b3 (ts: 900),
            //            0:c0 (ts: 901), 1:c1 (ts: 901), 2:c2 (ts: 901), 3:c3 (ts: 901),
            //            0:d0 (ts: 902), 1:d1 (ts: 902), 2:d2 (ts: 902), 3:d3 (ts: 902) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, "d" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+d0", 1000),
                new KeyValueTimestamp<>(1, "A1+d1", 1001),
                new KeyValueTimestamp<>(2, "A2+d2", 1002));

            // push four items with increased timestamp to the other stream; this should produce four items
            // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            // w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899),
            //        0:b0 (ts: 900), 1:b1 (ts: 900), 2:b2 (ts: 900), 3:b3 (ts: 900),
            //        0:c0 (ts: 901), 1:c1 (ts: 901), 2:c2 (ts: 901), 3:c3 (ts: 901),
            //        0:d0 (ts: 902), 1:d1 (ts: 902), 2:d2 (ts: 902), 3:d3 (ts: 902) }
            // --> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            //     w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899),
            //            0:b0 (ts: 900), 1:b1 (ts: 900), 2:b2 (ts: 900), 3:b3 (ts: 900),
            //            0:c0 (ts: 901), 1:c1 (ts: 901), 2:c2 (ts: 901), 3:c3 (ts: 901),
            //            0:d0 (ts: 902), 1:d1 (ts: 902), 2:d2 (ts: 902), 3:d3 (ts: 902),
            //            0:e0 (ts: 903), 1:e1 (ts: 903), 2:e2 (ts: 903), 3:e3 (ts: 903) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, "e" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+e0", 1000),
                new KeyValueTimestamp<>(1, "A1+e1", 1001),
                new KeyValueTimestamp<>(2, "A2+e2", 1002),
                new KeyValueTimestamp<>(3, "A3+e3", 1003));

            // push four items with larger timestamp to the other stream; this should produce four items
            // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            // w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899),
            //        0:b0 (ts: 900), 1:b1 (ts: 900), 2:b2 (ts: 900), 3:b3 (ts: 900),
            //        0:c0 (ts: 901), 1:c1 (ts: 901), 2:c2 (ts: 901), 3:c3 (ts: 901),
            //        0:d0 (ts: 902), 1:d1 (ts: 902), 2:d2 (ts: 902), 3:d3 (ts: 902),
            //        0:e0 (ts: 903), 1:e1 (ts: 903), 2:e2 (ts: 903), 3:e3 (ts: 903) }
            // --> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            //     w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899),
            //            0:b0 (ts: 900), 1:b1 (ts: 900), 2:b2 (ts: 900), 3:b3 (ts: 900),
            //            0:c0 (ts: 901), 1:c1 (ts: 901), 2:c2 (ts: 901), 3:c3 (ts: 901),
            //            0:d0 (ts: 902), 1:d1 (ts: 902), 2:d2 (ts: 902), 3:d3 (ts: 902),
            //            0:e0 (ts: 903), 1:e1 (ts: 903), 2:e2 (ts: 903), 3:e3 (ts: 903),
            //            0:f0 (ts: 1000), 1:f1 (ts: 1000), 2:f2 (ts: 1000), 3:f3 (ts: 1000) }
            time = 1000L;
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, "f" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+f0", 1000),
                new KeyValueTimestamp<>(1, "A1+f1", 1001),
                new KeyValueTimestamp<>(2, "A2+f2", 1002),
                new KeyValueTimestamp<>(3, "A3+f3", 1003));

            // push four items with increase timestamp to the other stream; this should produce three items
            // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            // w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899),
            //        0:b0 (ts: 900), 1:b1 (ts: 900), 2:b2 (ts: 900), 3:b3 (ts: 900),
            //        0:c0 (ts: 901), 1:c1 (ts: 901), 2:c2 (ts: 901), 3:c3 (ts: 901),
            //        0:d0 (ts: 902), 1:d1 (ts: 902), 2:d2 (ts: 902), 3:d3 (ts: 902),
            //        0:e0 (ts: 903), 1:e1 (ts: 903), 2:e2 (ts: 903), 3:e3 (ts: 903),
            //        0:f0 (ts: 1000), 1:f1 (ts: 1000), 2:f2 (ts: 1000), 3:f3 (ts: 1000) }
            // --> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            //     w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899),
            //            0:b0 (ts: 900), 1:b1 (ts: 900), 2:b2 (ts: 900), 3:b3 (ts: 900),
            //            0:c0 (ts: 901), 1:c1 (ts: 901), 2:c2 (ts: 901), 3:c3 (ts: 901),
            //            0:d0 (ts: 902), 1:d1 (ts: 902), 2:d2 (ts: 902), 3:d3 (ts: 902),
            //            0:e0 (ts: 903), 1:e1 (ts: 903), 2:e2 (ts: 903), 3:e3 (ts: 903),
            //            0:f0 (ts: 1000), 1:f1 (ts: 1000), 2:f2 (ts: 1000), 3:f3 (ts: 1000),
            //            0:g0 (ts: 1001), 1:g1 (ts: 1001), 2:g2 (ts: 1001), 3:g3 (ts: 1001) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, "g" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(1, "A1+g1", 1001),
                new KeyValueTimestamp<>(2, "A2+g2", 1002),
                new KeyValueTimestamp<>(3, "A3+g3", 1003));

            // push four items with increase timestamp to the other stream; this should produce two items
            // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            // w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899),
            //        0:b0 (ts: 900), 1:b1 (ts: 900), 2:b2 (ts: 900), 3:b3 (ts: 900),
            //        0:c0 (ts: 901), 1:c1 (ts: 901), 2:c2 (ts: 901), 3:c3 (ts: 901),
            //        0:d0 (ts: 902), 1:d1 (ts: 902), 2:d2 (ts: 902), 3:d3 (ts: 902),
            //        0:e0 (ts: 903), 1:e1 (ts: 903), 2:e2 (ts: 903), 3:e3 (ts: 903),
            //        0:f0 (ts: 1000), 1:f1 (ts: 1000), 2:f2 (ts: 1000), 3:f3 (ts: 1000),
            //        0:g0 (ts: 1001), 1:g1 (ts: 1001), 2:g2 (ts: 1001), 3:g3 (ts: 1001) }
            // --> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            //     w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899),
            //            0:b0 (ts: 900), 1:b1 (ts: 900), 2:b2 (ts: 900), 3:b3 (ts: 900),
            //            0:c0 (ts: 901), 1:c1 (ts: 901), 2:c2 (ts: 901), 3:c3 (ts: 901),
            //            0:d0 (ts: 902), 1:d1 (ts: 902), 2:d2 (ts: 902), 3:d3 (ts: 902),
            //            0:e0 (ts: 903), 1:e1 (ts: 903), 2:e2 (ts: 903), 3:e3 (ts: 903),
            //            0:f0 (ts: 1000), 1:f1 (ts: 1000), 2:f2 (ts: 1000), 3:f3 (ts: 1000),
            //            0:g0 (ts: 1001), 1:g1 (ts: 1001), 2:g2 (ts: 1001), 3:g3 (ts: 1001),
            //            0:h0 (ts: 1002), 1:h1 (ts: 1002), 2:h2 (ts: 1002), 3:h3 (ts: 1002) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, "h" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(2, "A2+h2", 1002),
                new KeyValueTimestamp<>(3, "A3+h3", 1003));

            // push four items with increase timestamp to the other stream; this should produce one item
            // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            // w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899),
            //        0:b0 (ts: 900), 1:b1 (ts: 900), 2:b2 (ts: 900), 3:b3 (ts: 900),
            //        0:c0 (ts: 901), 1:c1 (ts: 901), 2:c2 (ts: 901), 3:c3 (ts: 901),
            //        0:d0 (ts: 902), 1:d1 (ts: 902), 2:d2 (ts: 902), 3:d3 (ts: 902),
            //        0:e0 (ts: 903), 1:e1 (ts: 903), 2:e2 (ts: 903), 3:e3 (ts: 903),
            //        0:f0 (ts: 1000), 1:f1 (ts: 1000), 2:f2 (ts: 1000), 3:f3 (ts: 1000),
            //        0:g0 (ts: 1001), 1:g1 (ts: 1001), 2:g2 (ts: 1001), 3:g3 (ts: 1001),
            //        0:h0 (ts: 1002), 1:h1 (ts: 1002), 2:h2 (ts: 1002), 3:h3 (ts: 1002) }
            // --> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            //     w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899),
            //            0:b0 (ts: 900), 1:b1 (ts: 900), 2:b2 (ts: 900), 3:b3 (ts: 900),
            //            0:c0 (ts: 901), 1:c1 (ts: 901), 2:c2 (ts: 901), 3:c3 (ts: 901),
            //            0:d0 (ts: 902), 1:d1 (ts: 902), 2:d2 (ts: 902), 3:d3 (ts: 902),
            //            0:e0 (ts: 903), 1:e1 (ts: 903), 2:e2 (ts: 903), 3:e3 (ts: 903),
            //            0:f0 (ts: 1000), 1:f1 (ts: 1000), 2:f2 (ts: 1000), 3:f3 (ts: 1000),
            //            0:g0 (ts: 1001), 1:g1 (ts: 1001), 2:g2 (ts: 1001), 3:g3 (ts: 1001),
            //            0:h0 (ts: 1002), 1:h1 (ts: 1002), 2:h2 (ts: 1002), 3:h3 (ts: 1002),
            //            0:i0 (ts: 1003), 1:i1 (ts: 1003), 2:i2 (ts: 1003), 3:i3 (ts: 1003) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, "i" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(3, "A3+i3", 1003));

            // push four items with increase timestamp (no out of window) to the other stream; this should produce no items
            // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            // w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899),
            //        0:b0 (ts: 900), 1:b1 (ts: 900), 2:b2 (ts: 900), 3:b3 (ts: 900),
            //        0:c0 (ts: 901), 1:c1 (ts: 901), 2:c2 (ts: 901), 3:c3 (ts: 901),
            //        0:d0 (ts: 902), 1:d1 (ts: 902), 2:d2 (ts: 902), 3:d3 (ts: 902),
            //        0:e0 (ts: 903), 1:e1 (ts: 903), 2:e2 (ts: 903), 3:e3 (ts: 903),
            //        0:f0 (ts: 1000), 1:f1 (ts: 1000), 2:f2 (ts: 1000), 3:f3 (ts: 1000),
            //        0:g0 (ts: 1001), 1:g1 (ts: 1001), 2:g2 (ts: 1001), 3:g3 (ts: 1001),
            //        0:h0 (ts: 1002), 1:h1 (ts: 1002), 2:h2 (ts: 1002), 3:h3 (ts: 1002),
            //        0:i0 (ts: 1003), 1:i1 (ts: 1003), 2:i2 (ts: 1003), 3:i3 (ts: 1003) }
            // --> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
            //     w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899),
            //            0:b0 (ts: 900), 1:b1 (ts: 900), 2:b2 (ts: 900), 3:b3 (ts: 900),
            //            0:c0 (ts: 901), 1:c1 (ts: 901), 2:c2 (ts: 901), 3:c3 (ts: 901),
            //            0:d0 (ts: 902), 1:d1 (ts: 902), 2:d2 (ts: 902), 3:d3 (ts: 902),
            //            0:e0 (ts: 903), 1:e1 (ts: 903), 2:e2 (ts: 903), 3:e3 (ts: 903),
            //            0:f0 (ts: 1000), 1:f1 (ts: 1000), 2:f2 (ts: 1000), 3:f3 (ts: 1000),
            //            0:g0 (ts: 1001), 1:g1 (ts: 1001), 2:g2 (ts: 1001), 3:g3 (ts: 1001),
            //            0:h0 (ts: 1002), 1:h1 (ts: 1002), 2:h2 (ts: 1002), 3:h3 (ts: 1002),
            //            0:i0 (ts: 1003), 1:i1 (ts: 1003), 2:i2 (ts: 1003), 3:i3 (ts: 1003),
            //            0:j0 (ts: 1004), 1:j1 (ts: 1004), 2:j2 (ts: 1004), 3:j3 (ts: 1004) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, "j" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(EMPTY);
        }
    }

    private void buildStreamsJoinThatShouldThrow(final StreamJoined<String, Integer, Integer> streamJoined,
                                                 final JoinWindows joinWindows,
                                                 final String expectedExceptionMessagePrefix) {

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, Integer> left = builder.stream("left", Consumed.with(Serdes.String(), Serdes.Integer()));
        final KStream<String, Integer> right = builder.stream("right", Consumed.with(Serdes.String(), Serdes.Integer()));

        final StreamsException streamsException = assertThrows(StreamsException.class, () -> left.join(
            right,
            (value1, value2) -> value1 + value2,
            joinWindows,
            streamJoined));

        assertTrue(streamsException.getMessage().startsWith(expectedExceptionMessagePrefix));
    }

    private WindowBytesStoreSupplier buildWindowBytesStoreSupplier(final String name,
                                                                   final long retentionPeriod,
                                                                   final long windowSize,
                                                                   final boolean retainDuplicates) {
        return  Stores.inMemoryWindowStore(name,
                                           Duration.ofMillis(retentionPeriod),
                                           Duration.ofMillis(windowSize),
                                           retainDuplicates);
    }


    private final String expectedTopologyWithUserNamedRepartitionTopics = "Topologies:\n" +
            "   Sub-topology: 0\n" +
            "    Source: KSTREAM-SOURCE-0000000000 (topics: [topic])\n" +
            "      --> KSTREAM-MAP-0000000003\n" +
            "    Processor: KSTREAM-MAP-0000000003 (stores: [])\n" +
            "      --> second-join-left-repartition-filter, first-join-left-repartition-filter\n" +
            "      <-- KSTREAM-SOURCE-0000000000\n" +
            "    Processor: first-join-left-repartition-filter (stores: [])\n" +
            "      --> first-join-left-repartition-sink\n" +
            "      <-- KSTREAM-MAP-0000000003\n" +
            "    Processor: second-join-left-repartition-filter (stores: [])\n" +
            "      --> second-join-left-repartition-sink\n" +
            "      <-- KSTREAM-MAP-0000000003\n" +
            "    Sink: first-join-left-repartition-sink (topic: first-join-left-repartition)\n" +
            "      <-- first-join-left-repartition-filter\n" +
            "    Sink: second-join-left-repartition-sink (topic: second-join-left-repartition)\n" +
            "      <-- second-join-left-repartition-filter\n" +
            "\n" +
            "  Sub-topology: 1\n" +
            "    Source: KSTREAM-SOURCE-0000000001 (topics: [topic2])\n" +
            "      --> first-join-other-windowed\n" +
            "    Source: first-join-left-repartition-source (topics: [first-join-left-repartition])\n" +
            "      --> first-join-this-windowed\n" +
            "    Processor: first-join-other-windowed (stores: [KSTREAM-JOINOTHER-0000000010-store])\n" +
            "      --> first-join-other-join\n" +
            "      <-- KSTREAM-SOURCE-0000000001\n" +
            "    Processor: first-join-this-windowed (stores: [KSTREAM-JOINTHIS-0000000009-store])\n" +
            "      --> first-join-this-join\n" +
            "      <-- first-join-left-repartition-source\n" +
            "    Processor: first-join-other-join (stores: [KSTREAM-JOINTHIS-0000000009-store])\n" +
            "      --> first-join-merge\n" +
            "      <-- first-join-other-windowed\n" +
            "    Processor: first-join-this-join (stores: [KSTREAM-JOINOTHER-0000000010-store])\n" +
            "      --> first-join-merge\n" +
            "      <-- first-join-this-windowed\n" +
            "    Processor: first-join-merge (stores: [])\n" +
            "      --> KSTREAM-SINK-0000000012\n" +
            "      <-- first-join-this-join, first-join-other-join\n" +
            "    Sink: KSTREAM-SINK-0000000012 (topic: out-one)\n" +
            "      <-- first-join-merge\n" +
            "\n" +
            "  Sub-topology: 2\n" +
            "    Source: KSTREAM-SOURCE-0000000002 (topics: [topic3])\n" +
            "      --> second-join-other-windowed\n" +
            "    Source: second-join-left-repartition-source (topics: [second-join-left-repartition])\n" +
            "      --> second-join-this-windowed\n" +
            "    Processor: second-join-other-windowed (stores: [KSTREAM-JOINOTHER-0000000019-store])\n" +
            "      --> second-join-other-join\n" +
            "      <-- KSTREAM-SOURCE-0000000002\n" +
            "    Processor: second-join-this-windowed (stores: [KSTREAM-JOINTHIS-0000000018-store])\n" +
            "      --> second-join-this-join\n" +
            "      <-- second-join-left-repartition-source\n" +
            "    Processor: second-join-other-join (stores: [KSTREAM-JOINTHIS-0000000018-store])\n" +
            "      --> second-join-merge\n" +
            "      <-- second-join-other-windowed\n" +
            "    Processor: second-join-this-join (stores: [KSTREAM-JOINOTHER-0000000019-store])\n" +
            "      --> second-join-merge\n" +
            "      <-- second-join-this-windowed\n" +
            "    Processor: second-join-merge (stores: [])\n" +
            "      --> KSTREAM-SINK-0000000021\n" +
            "      <-- second-join-this-join, second-join-other-join\n" +
            "    Sink: KSTREAM-SINK-0000000021 (topic: out-two)\n" +
            "      <-- second-join-merge\n\n";
    
    private final String expectedTopologyWithGeneratedRepartitionTopic = "Topologies:\n" +
            "   Sub-topology: 0\n" +
            "    Source: KSTREAM-SOURCE-0000000000 (topics: [topic])\n" +
            "      --> KSTREAM-MAP-0000000003\n" +
            "    Processor: KSTREAM-MAP-0000000003 (stores: [])\n" +
            "      --> KSTREAM-FILTER-0000000005\n" +
            "      <-- KSTREAM-SOURCE-0000000000\n" +
            "    Processor: KSTREAM-FILTER-0000000005 (stores: [])\n" +
            "      --> KSTREAM-SINK-0000000004\n" +
            "      <-- KSTREAM-MAP-0000000003\n" +
            "    Sink: KSTREAM-SINK-0000000004 (topic: KSTREAM-MAP-0000000003-repartition)\n" +
            "      <-- KSTREAM-FILTER-0000000005\n" +
            "\n" +
            "  Sub-topology: 1\n" +
            "    Source: KSTREAM-SOURCE-0000000006 (topics: [KSTREAM-MAP-0000000003-repartition])\n" +
            "      --> KSTREAM-WINDOWED-0000000007, KSTREAM-WINDOWED-0000000016\n" +
            "    Source: KSTREAM-SOURCE-0000000001 (topics: [topic2])\n" +
            "      --> KSTREAM-WINDOWED-0000000008\n" +
            "    Source: KSTREAM-SOURCE-0000000002 (topics: [topic3])\n" +
            "      --> KSTREAM-WINDOWED-0000000017\n" +
            "    Processor: KSTREAM-WINDOWED-0000000007 (stores: [KSTREAM-JOINTHIS-0000000009-store])\n" +
            "      --> KSTREAM-JOINTHIS-0000000009\n" +
            "      <-- KSTREAM-SOURCE-0000000006\n" +
            "    Processor: KSTREAM-WINDOWED-0000000008 (stores: [KSTREAM-JOINOTHER-0000000010-store])\n" +
            "      --> KSTREAM-JOINOTHER-0000000010\n" +
            "      <-- KSTREAM-SOURCE-0000000001\n" +
            "    Processor: KSTREAM-WINDOWED-0000000016 (stores: [KSTREAM-JOINTHIS-0000000018-store])\n" +
            "      --> KSTREAM-JOINTHIS-0000000018\n" +
            "      <-- KSTREAM-SOURCE-0000000006\n" +
            "    Processor: KSTREAM-WINDOWED-0000000017 (stores: [KSTREAM-JOINOTHER-0000000019-store])\n" +
            "      --> KSTREAM-JOINOTHER-0000000019\n" +
            "      <-- KSTREAM-SOURCE-0000000002\n" +
            "    Processor: KSTREAM-JOINOTHER-0000000010 (stores: [KSTREAM-JOINTHIS-0000000009-store])\n" +
            "      --> KSTREAM-MERGE-0000000011\n" +
            "      <-- KSTREAM-WINDOWED-0000000008\n" +
            "    Processor: KSTREAM-JOINOTHER-0000000019 (stores: [KSTREAM-JOINTHIS-0000000018-store])\n" +
            "      --> KSTREAM-MERGE-0000000020\n" +
            "      <-- KSTREAM-WINDOWED-0000000017\n" +
            "    Processor: KSTREAM-JOINTHIS-0000000009 (stores: [KSTREAM-JOINOTHER-0000000010-store])\n" +
            "      --> KSTREAM-MERGE-0000000011\n" +
            "      <-- KSTREAM-WINDOWED-0000000007\n" +
            "    Processor: KSTREAM-JOINTHIS-0000000018 (stores: [KSTREAM-JOINOTHER-0000000019-store])\n" +
            "      --> KSTREAM-MERGE-0000000020\n" +
            "      <-- KSTREAM-WINDOWED-0000000016\n" +
            "    Processor: KSTREAM-MERGE-0000000011 (stores: [])\n" +
            "      --> KSTREAM-SINK-0000000012\n" +
            "      <-- KSTREAM-JOINTHIS-0000000009, KSTREAM-JOINOTHER-0000000010\n" +
            "    Processor: KSTREAM-MERGE-0000000020 (stores: [])\n" +
            "      --> KSTREAM-SINK-0000000021\n" +
            "      <-- KSTREAM-JOINTHIS-0000000018, KSTREAM-JOINOTHER-0000000019\n" +
            "    Sink: KSTREAM-SINK-0000000012 (topic: out-one)\n" +
            "      <-- KSTREAM-MERGE-0000000011\n" +
            "    Sink: KSTREAM-SINK-0000000021 (topic: out-to)\n" +
            "      <-- KSTREAM-MERGE-0000000020\n\n";
}