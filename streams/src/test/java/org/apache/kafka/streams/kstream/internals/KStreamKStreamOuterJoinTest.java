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
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TopologyWrapper;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.test.MockApiProcessor;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static java.time.Duration.ZERO;
import static java.time.Duration.ofMillis;
import static org.junit.Assert.assertEquals;

public class KStreamKStreamOuterJoinTest {
    private final String topic1 = "topic1";
    private final String topic2 = "topic2";
    private final Consumed<Integer, String> consumed = Consumed.with(Serdes.Integer(), Serdes.String());
    private final static Properties PROPS = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());

    @BeforeClass
    public static void beforeClass() {
        PROPS.put(InternalConfig.EMIT_INTERVAL_MS_KSTREAMS_OUTER_JOIN_SPURIOUS_RESULTS_FIX, 0L);
    }

    @SuppressWarnings("deprecation") // old join semantics; can be removed when `JoinWindows.of()` is removed
    @Test
    public void testOuterJoinDuplicatesWithFixDisabledOldApi() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Integer, String> stream1;
        final KStream<Integer, String> stream2;
        final KStream<Integer, String> joined;
        final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed);

        joined = stream1.outerJoin(
                stream2,
                MockValueJoiner.TOSTRING_JOINER,
                JoinWindows.of(ofMillis(100L)).grace(ofMillis(10L)),
                StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.String())
        );
        joined.process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(PROPS), PROPS)) {
            final TestInputTopic<Integer, String> inputTopic1 =
                    driver.createInputTopic(topic1, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<Integer, String> inputTopic2 =
                    driver.createInputTopic(topic2, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockApiProcessor<Integer, String, Void, Void> processor = supplier.theCapturedProcessor();

            // Only 2 window stores should be available
            assertEquals(2, driver.getAllStateStores().size());

            inputTopic1.pipeInput(0, "A0", 0L);
            inputTopic1.pipeInput(0, "A0-0", 0L);
            inputTopic2.pipeInput(0, "a0", 0L);
            inputTopic2.pipeInput(1, "b1", 0L);

            processor.checkAndClearProcessResult(
                    new KeyValueTimestamp<>(0, "A0+null", 0L),
                    new KeyValueTimestamp<>(0, "A0-0+null", 0L),
                    new KeyValueTimestamp<>(0, "A0+a0", 0L),
                    new KeyValueTimestamp<>(0, "A0-0+a0", 0L),
                    new KeyValueTimestamp<>(1, "null+b1", 0L)
            );
        }
    }

    @Test
    public void testOuterJoinDuplicates() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Integer, String> stream1;
        final KStream<Integer, String> stream2;
        final KStream<Integer, String> joined;
        final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed);

        joined = stream1.outerJoin(
            stream2,
            MockValueJoiner.TOSTRING_JOINER,
            JoinWindows.ofTimeDifferenceAndGrace(ofMillis(100L), ofMillis(10L)),
            StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.String())
        );
        joined.process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), PROPS)) {
            final TestInputTopic<Integer, String> inputTopic1 =
                driver.createInputTopic(topic1, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<Integer, String> inputTopic2 =
                driver.createInputTopic(topic2, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockApiProcessor<Integer, String, Void, Void> processor = supplier.theCapturedProcessor();

            // verifies non-joined duplicates are emitted when window has closed
            inputTopic1.pipeInput(0, "A0", 0L);
            inputTopic1.pipeInput(0, "A0-0", 0L);
            inputTopic2.pipeInput(1, "a1", 0L);
            inputTopic2.pipeInput(1, "a1-0", 0L);
            inputTopic2.pipeInput(1, "a0", 111L);
            // bump stream-time to trigger outer-join results
            inputTopic2.pipeInput(3, "dummy", 211);

            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(1, "null+a1", 0L),
                new KeyValueTimestamp<>(1, "null+a1-0", 0L),
                new KeyValueTimestamp<>(0, "A0+null", 0L),
                new KeyValueTimestamp<>(0, "A0-0+null", 0L)
            );

            // verifies joined duplicates are emitted
            inputTopic1.pipeInput(2, "A2", 200L);
            inputTopic1.pipeInput(2, "A2-0", 200L);
            inputTopic2.pipeInput(2, "a2", 201L);
            inputTopic2.pipeInput(2, "a2-0", 201L);

            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(2, "A2+a2", 201L),
                new KeyValueTimestamp<>(2, "A2-0+a2", 201L),
                new KeyValueTimestamp<>(2, "A2+a2-0", 201L),
                new KeyValueTimestamp<>(2, "A2-0+a2-0", 201L)
            );

            // this record should expired non-joined records; only null+a0 will be emitted because
            // it did not have a join
            inputTopic2.pipeInput(3, "dummy", 1500L);

            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(1, "null+a0", 111L),
                new KeyValueTimestamp<>(3, "null+dummy", 211)
            );
        }
    }

    @Test
    public void testLeftExpiredNonJoinedRecordsAreEmittedByTheLeftProcessor() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Integer, String> stream1;
        final KStream<Integer, String> stream2;
        final KStream<Integer, String> joined;
        final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();

        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed);
        joined = stream1.outerJoin(
            stream2,
            MockValueJoiner.TOSTRING_JOINER,
            JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100L)),
            StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.String())
        );
        joined.process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), PROPS)) {
            final TestInputTopic<Integer, String> inputTopic1 =
                driver.createInputTopic(topic1, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<Integer, String> inputTopic2 =
                driver.createInputTopic(topic2, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockApiProcessor<Integer, String, Void, Void> processor = supplier.theCapturedProcessor();

            final long windowStart = 0L;

            // No joins detected; No null-joins emitted
            inputTopic1.pipeInput(0, "A0", windowStart + 1L);
            inputTopic1.pipeInput(1, "A1", windowStart + 2L);
            inputTopic1.pipeInput(0, "A0-0", windowStart + 3L);
            processor.checkAndClearProcessResult();

            // Join detected; No null-joins emitted
            inputTopic2.pipeInput(1, "a1", windowStart + 3L);
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(1, "A1+a1", windowStart + 3L)
            );

            // Dummy record in left topic will emit expired non-joined records from the left topic
            inputTopic1.pipeInput(2, "dummy", windowStart + 401L);
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(0, "A0+null", windowStart + 1L),
                new KeyValueTimestamp<>(0, "A0-0+null", windowStart + 3L)
            );

            // Flush internal non-joined state store by joining the dummy record
            inputTopic2.pipeInput(2, "dummy", windowStart + 401L);
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(2, "dummy+dummy", windowStart + 401L)
            );
        }
    }

    @Test
    public void testLeftExpiredNonJoinedRecordsAreEmittedByTheRightProcessor() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Integer, String> stream1;
        final KStream<Integer, String> stream2;
        final KStream<Integer, String> joined;
        final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();

        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed);
        joined = stream1.outerJoin(
            stream2,
            MockValueJoiner.TOSTRING_JOINER,
            JoinWindows.ofTimeDifferenceAndGrace(ofMillis(100L), ofMillis(0L)),
            StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.String())
        );
        joined.process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), PROPS)) {
            final TestInputTopic<Integer, String> inputTopic1 =
                driver.createInputTopic(topic1, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<Integer, String> inputTopic2 =
                driver.createInputTopic(topic2, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockApiProcessor<Integer, String, Void, Void> processor = supplier.theCapturedProcessor();

            final long windowStart = 0L;

            // No joins detected; No null-joins emitted
            inputTopic1.pipeInput(0, "A0", windowStart + 1L);
            inputTopic1.pipeInput(1, "A1", windowStart + 2L);
            inputTopic1.pipeInput(0, "A0-0", windowStart + 3L);
            processor.checkAndClearProcessResult();

            // Join detected; No null-joins emitted
            inputTopic2.pipeInput(1, "a1", windowStart + 3L);
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(1, "A1+a1", windowStart + 3L)
            );

            // Dummy record in right topic will emit expired non-joined records from the left topic
            inputTopic2.pipeInput(2, "dummy", windowStart + 401L);
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(0, "A0+null", windowStart + 1L),
                new KeyValueTimestamp<>(0, "A0-0+null", windowStart + 3L)
            );

            // Flush internal non-joined state store by joining the dummy record
            inputTopic1.pipeInput(2, "dummy", windowStart + 402L);
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(2, "dummy+dummy", windowStart + 402L)
            );
        }
    }

    @Test
    public void testRightExpiredNonJoinedRecordsAreEmittedByTheLeftProcessor() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Integer, String> stream1;
        final KStream<Integer, String> stream2;
        final KStream<Integer, String> joined;
        final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();

        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed);
        joined = stream1.outerJoin(
            stream2,
            MockValueJoiner.TOSTRING_JOINER,
            JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100L)),
            StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.String())
        );
        joined.process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), PROPS)) {
            final TestInputTopic<Integer, String> inputTopic1 =
                driver.createInputTopic(topic1, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<Integer, String> inputTopic2 =
                driver.createInputTopic(topic2, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockApiProcessor<Integer, String, Void, Void> processor = supplier.theCapturedProcessor();

            final long windowStart = 0L;

            // No joins detected; No null-joins emitted
            inputTopic2.pipeInput(0, "A0", windowStart + 1L);
            inputTopic2.pipeInput(1, "A1", windowStart + 2L);
            inputTopic2.pipeInput(0, "A0-0", windowStart + 3L);
            processor.checkAndClearProcessResult();

            // Join detected; No null-joins emitted
            inputTopic1.pipeInput(1, "a1", windowStart + 3L);
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(1, "a1+A1", windowStart + 3L)
            );

            // Dummy record in left topic will emit expired non-joined records from the right topic
            inputTopic1.pipeInput(2, "dummy", windowStart + 401L);
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(0, "null+A0", windowStart + 1L),
                new KeyValueTimestamp<>(0, "null+A0-0", windowStart + 3L)
            );

            // Process the dummy joined record
            inputTopic2.pipeInput(2, "dummy", windowStart + 402L);
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(2, "dummy+dummy", windowStart + 402L)
            );
        }
    }

    @Test
    public void testRightExpiredNonJoinedRecordsAreEmittedByTheRightProcessor() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Integer, String> stream1;
        final KStream<Integer, String> stream2;
        final KStream<Integer, String> joined;
        final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();

        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed);
        joined = stream1.outerJoin(
            stream2,
            MockValueJoiner.TOSTRING_JOINER,
            JoinWindows.ofTimeDifferenceAndGrace(ofMillis(100L), ofMillis(0L)),
            StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.String())
        );
        joined.process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), PROPS)) {
            final TestInputTopic<Integer, String> inputTopic1 =
                driver.createInputTopic(topic1, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<Integer, String> inputTopic2 =
                driver.createInputTopic(topic2, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockApiProcessor<Integer, String, Void, Void> processor = supplier.theCapturedProcessor();

            final long windowStart = 0L;

            // No joins detected; No null-joins emitted
            inputTopic2.pipeInput(0, "A0", windowStart + 1L);
            inputTopic2.pipeInput(1, "A1", windowStart + 2L);
            inputTopic2.pipeInput(0, "A0-0", windowStart + 3L);
            processor.checkAndClearProcessResult();

            // Join detected; No null-joins emitted
            inputTopic1.pipeInput(1, "a1", windowStart + 3L);
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(1, "a1+A1", windowStart + 3L)
            );

            // Dummy record in right topic will emit expired non-joined records from the right topic
            inputTopic2.pipeInput(2, "dummy", windowStart + 401L);
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(0, "null+A0", windowStart + 1L),
                new KeyValueTimestamp<>(0, "null+A0-0", windowStart + 3L)
            );

            // Process the dummy joined record
            inputTopic1.pipeInput(2, "dummy", windowStart + 402L);
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(2, "dummy+dummy", windowStart + 402L)
            );
        }
    }

    @Test
    public void testOrdering() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Integer, String> stream1;
        final KStream<Integer, String> stream2;
        final KStream<Integer, String> joined;
        final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed);

        joined = stream1.outerJoin(
            stream2,
            MockValueJoiner.TOSTRING_JOINER,
            JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100)),
            StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.String())
        );
        joined.process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), PROPS)) {
            final TestInputTopic<Integer, String> inputTopic1 =
                driver.createInputTopic(topic1, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<Integer, String> inputTopic2 =
                driver.createInputTopic(topic2, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockApiProcessor<Integer, String, Void, Void> processor = supplier.theCapturedProcessor();

            // push two items to the primary stream; the other window is empty; this should not produce any item yet
            // w1 = {}
            // w2 = {}
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 100) }
            // --> w2 = {}
            inputTopic1.pipeInput(0, "A0", 0L);
            inputTopic1.pipeInput(1, "A1", 100L);
            processor.checkAndClearProcessResult();

            // push one item to the other window that has a join; this should produce non-joined records with a closed window first, then
            // the joined records
            // by the time they were produced before
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 100) }
            // w2 = { }
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
            // --> w2 = { 0:a0 (ts: 110) }
            inputTopic2.pipeInput(1, "a1", 110L);
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(0, "A0+null", 0L),
                new KeyValueTimestamp<>(1, "A1+a1", 110L)
            );
        }
    }

    @Test
    public void testGracePeriod() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Integer, String> stream1;
        final KStream<Integer, String> stream2;
        final KStream<Integer, String> joined;
        final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed);

        joined = stream1.outerJoin(
            stream2,
            MockValueJoiner.TOSTRING_JOINER,
            JoinWindows.ofTimeDifferenceAndGrace(ofMillis(100), ofMillis(10)),
            StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.String())
        );
        joined.process(supplier);

        final Collection<Set<String>> copartitionGroups =
            TopologyWrapper.getInternalTopologyBuilder(builder.build()).copartitionGroups();

        assertEquals(1, copartitionGroups.size());
        assertEquals(new HashSet<>(Arrays.asList(topic1, topic2)), copartitionGroups.iterator().next());

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), PROPS)) {
            final TestInputTopic<Integer, String> inputTopic1 =
                driver.createInputTopic(topic1, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<Integer, String> inputTopic2 =
                driver.createInputTopic(topic2, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockApiProcessor<Integer, String, Void, Void> processor = supplier.theCapturedProcessor();

            // push one item to the primary stream; and one item in other stream; this should not produce items because there are no joins
            // and window has not ended
            // w1 = {}
            // w2 = {}
            // --> w1 = { 0:A0 (ts: 0) }
            // --> w2 = { 1:a1 (ts: 0) }
            inputTopic1.pipeInput(0, "A0", 0L);
            inputTopic2.pipeInput(1, "a1", 0L);
            processor.checkAndClearProcessResult();

            // push one item on each stream with a window time after the previous window ended (not closed); this should not produce
            // joined records because the window has ended, but will not produce non-joined records because the window has not closed.
            // w1 = { 0:A0 (ts: 0) }
            // w2 = { 1:a1 (ts: 0) }
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
            // --> w2 = { 0:a0 (ts: 101), 1:a1 (ts: 101) }
            inputTopic2.pipeInput(0, "a0", 101L);
            inputTopic1.pipeInput(1, "A1", 101L);
            processor.checkAndClearProcessResult();

            // push a dummy item to the any stream after the window is closed; this should produced all expired non-joined records because
            // the window has closed
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
            // w2 = { 0:a0 (ts: 101), 1:a1 (ts: 101) }
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
            // --> w2 = { 0:a0 (ts: 101), 1:a1 (ts: 101), 0:dummy (ts: 112) }
            inputTopic2.pipeInput(0, "dummy", 211);
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(1, "null+a1", 0L),
                new KeyValueTimestamp<>(0, "A0+null", 0L)
            );
        }
    }

    @Test
    public void testOuterJoinWithInMemoryCustomSuppliers() {
        final JoinWindows joinWindows = JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100L));

        final WindowBytesStoreSupplier thisStoreSupplier = Stores.inMemoryWindowStore(
            "in-memory-join-store",
            Duration.ofMillis(joinWindows.size() + joinWindows.gracePeriodMs()),
            Duration.ofMillis(joinWindows.size()),
            true
        );

        final WindowBytesStoreSupplier otherStoreSupplier = Stores.inMemoryWindowStore(
            "in-memory-join-store-other",
            Duration.ofMillis(joinWindows.size() + joinWindows.gracePeriodMs()),
            Duration.ofMillis(joinWindows.size()),
            true
        );

        final StreamJoined<Integer, String, String> streamJoined = StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.String());

        runOuterJoin(streamJoined.withThisStoreSupplier(thisStoreSupplier).withOtherStoreSupplier(otherStoreSupplier), joinWindows);
    }

    @Test
    public void testOuterJoinWithDefaultSuppliers() {
        final JoinWindows joinWindows = JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100L));
        final StreamJoined<Integer, String, String> streamJoined = StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.String());

        runOuterJoin(streamJoined, joinWindows);
    }

    public void runOuterJoin(final StreamJoined<Integer, String, String> streamJoined,
                             final JoinWindows joinWindows) {
        final StreamsBuilder builder = new StreamsBuilder();

        final int[] expectedKeys = new int[] {0, 1, 2, 3};

        final KStream<Integer, String> stream1;
        final KStream<Integer, String> stream2;
        final KStream<Integer, String> joined;
        final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed);

        joined = stream1.outerJoin(
            stream2,
            MockValueJoiner.TOSTRING_JOINER,
            joinWindows,
            streamJoined
        );
        joined.process(supplier);

        final Collection<Set<String>> copartitionGroups =
            TopologyWrapper.getInternalTopologyBuilder(builder.build()).copartitionGroups();

        assertEquals(1, copartitionGroups.size());
        assertEquals(new HashSet<>(Arrays.asList(topic1, topic2)), copartitionGroups.iterator().next());

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), PROPS)) {
            final TestInputTopic<Integer, String> inputTopic1 =
                driver.createInputTopic(topic1, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<Integer, String> inputTopic2 =
                driver.createInputTopic(topic2, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockApiProcessor<Integer, String, Void, Void> processor = supplier.theCapturedProcessor();

            // 2 window stores + 1 shared window store should be available
            assertEquals(3, driver.getAllStateStores().size());

            // push two items to the primary stream; the other window is empty; this should not
            // produce any items because window has not expired
            // w1 {}
            // w2 {}
            // --> w1 = { 0:A0, 1:A1 }
            // --> w2 = {}
            for (int i = 0; i < 2; i++) {
                inputTopic1.pipeInput(expectedKeys[i], "A" + expectedKeys[i]);
            }
            processor.checkAndClearProcessResult();

            // push two items to the other stream; this should produce two full-joined items
            // w1 = { 0:A0, 1:A1 }
            // w2 {}
            // --> w1 = { 0:A0, 1:A1 }
            // --> w2 = { 0:a0, 1:a1 }
            for (int i = 0; i < 2; i++) {
                inputTopic2.pipeInput(expectedKeys[i], "a" + expectedKeys[i]);
            }
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(0, "A0+a0", 0L),
                new KeyValueTimestamp<>(1, "A1+a1", 0L)
            );

            // push three items to the primary stream; this should produce two full-joined items
            // w1 = { 0:A0, 1:A1 }
            // w2 = { 0:a0, 1:a1 }
            // --> w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2 }
            // --> w2 = { 0:a0, 1:a1 }
            for (int i = 0; i < 3; i++) {
                inputTopic1.pipeInput(expectedKeys[i], "B" + expectedKeys[i]);
            }
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(0, "B0+a0", 0L),
                new KeyValueTimestamp<>(1, "B1+a1", 0L)
            );

            // push all items to the other stream; this should produce five full-joined items
            // w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2 }
            // w2 = { 0:a0, 1:a1 }
            // --> w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2 }
            // --> w2 = { 0:a0, 1:a1, 0:b0, 1:b1, 2:b2, 3:b3 }
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, "b" + expectedKey);
            }
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(0, "A0+b0", 0L),
                new KeyValueTimestamp<>(0, "B0+b0", 0L),
                new KeyValueTimestamp<>(1, "A1+b1", 0L),
                new KeyValueTimestamp<>(1, "B1+b1", 0L),
                new KeyValueTimestamp<>(2, "B2+b2", 0L)
            );

            // push all four items to the primary stream; this should produce six full-joined items
            // w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2 }
            // w2 = { 0:a0, 1:a1, 0:b0, 1:b1, 2:b2, 3:b3 }
            // --> w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 0:C0, 1:C1, 2:C2, 3:C3 }
            // --> w2 = { 0:a0, 1:a1, 0:b0, 1:b1, 2:b2, 3:b3 }
            for (final int expectedKey : expectedKeys) {
                inputTopic1.pipeInput(expectedKey, "C" + expectedKey);
            }
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(0, "C0+a0", 0L),
                new KeyValueTimestamp<>(0, "C0+b0", 0L),
                new KeyValueTimestamp<>(1, "C1+a1", 0L),
                new KeyValueTimestamp<>(1, "C1+b1", 0L),
                new KeyValueTimestamp<>(2, "C2+b2", 0L),
                new KeyValueTimestamp<>(3, "C3+b3", 0L)
            );

            // push a dummy record that should expire non-joined items; it should not produce any items because
            // all of them are joined
            inputTopic1.pipeInput(0, "dummy", 400L);
            processor.checkAndClearProcessResult();
        }
    }

    @Test
    public void testWindowing() {
        final StreamsBuilder builder = new StreamsBuilder();
        final int[] expectedKeys = new int[]{0, 1, 2, 3};

        final KStream<Integer, String> stream1;
        final KStream<Integer, String> stream2;
        final KStream<Integer, String> joined;
        final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed);

        joined = stream1.outerJoin(
            stream2,
            MockValueJoiner.TOSTRING_JOINER,
            JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100)),
            StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.String())
        );
        joined.process(supplier);

        final Collection<Set<String>> copartitionGroups =
            TopologyWrapper.getInternalTopologyBuilder(builder.build()).copartitionGroups();

        assertEquals(1, copartitionGroups.size());
        assertEquals(new HashSet<>(Arrays.asList(topic1, topic2)), copartitionGroups.iterator().next());

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), PROPS)) {
            final TestInputTopic<Integer, String> inputTopic1 =
                driver.createInputTopic(topic1, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<Integer, String> inputTopic2 =
                driver.createInputTopic(topic2, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockApiProcessor<Integer, String, Void, Void> processor = supplier.theCapturedProcessor();
            final long time = 0L;

            // push two items to the primary stream; the other window is empty; this should not produce items because window has not closed
            // w1 = {}
            // w2 = {}
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
            // --> w2 = {}
            for (int i = 0; i < 2; i++) {
                inputTopic1.pipeInput(expectedKeys[i], "A" + expectedKeys[i], time);
            }
            processor.checkAndClearProcessResult();

            // push four items to the other stream; this should produce two full-join items
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
            // w2 = {}
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
            // --> w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0) }
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, "a" + expectedKey, time);
            }
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(0, "A0+a0", 0L),
                new KeyValueTimestamp<>(1, "A1+a1", 0L)
            );

            testUpperWindowBound(expectedKeys, driver, processor);
            testLowerWindowBound(expectedKeys, driver, processor);
        }
    }

    @Test
    public void shouldNotEmitLeftJoinResultForAsymmetricBeforeWindow() {
        final StreamsBuilder builder = new StreamsBuilder();
        final int[] expectedKeys = new int[] {0, 1, 2, 3};

        final KStream<Integer, String> stream1;
        final KStream<Integer, String> stream2;
        final KStream<Integer, String> joined;
        final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed);

        joined = stream1.outerJoin(
            stream2,
            MockValueJoiner.TOSTRING_JOINER,
            JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100)).before(ZERO),
            StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.String())
        );
        joined.process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), PROPS)) {
            final TestInputTopic<Integer, String> inputTopic1 =
                driver.createInputTopic(topic1, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<Integer, String> inputTopic2 =
                driver.createInputTopic(topic2, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockApiProcessor<Integer, String, Void, Void> processor = supplier.theCapturedProcessor();
            long time = 0L;

            // push two items to the primary stream; the other window is empty; this should not produce any items
            // w1 = {}
            // w2 = {}
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 1) }
            // --> w2 = {}
            for (int i = 0; i < 2; i++) {
                inputTopic1.pipeInput(expectedKeys[i], "A" + expectedKeys[i], time + i);
            }
            processor.checkAndClearProcessResult();

            // push one item to the other stream; this should produce one full-join item
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 1) }
            // w2 = {}
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 1) }
            // --> w2 = { 0:a0 (ts: 100) }
            time += 100L;
            inputTopic2.pipeInput(expectedKeys[0], "a" + expectedKeys[0], time);

            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(0, "A0+a0", 100L)
            );

            // push one item to the other stream; this should produce one left-join item
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 1) }
            // w2 = { 0:a0 (ts: 100) }
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 1) }
            // --> w2 = { 0:a0 (ts: 100), 1:a1 (ts: 102) }
            time += 2;
            inputTopic2.pipeInput(expectedKeys[1], "a" + expectedKeys[1], time);

            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(1, "A1+null", 1L)
            );

            // push one item to the other stream; this should not produce any items
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 1) }
            // w2 = { 0:a0 (ts: 100), 1:a1 (ts: 102) }
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 1) }
            // --> w2 = { 0:a0 (ts: 100), 1:a1 (ts: 102), 2:a2 (ts: 103) }
            time += 1;
            inputTopic2.pipeInput(expectedKeys[2], "a" + expectedKeys[2], time);

            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(1, "null+a1", 102L)
            );

            // push one item to the first stream; this should not produce one full-join item
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 1) }
            // w2 = { 0:a0 (ts: 100), 1:a1 (ts: 102), 2:a2 (ts: 103) }
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 1), 2:A2 (ts: 103) }
            // --> w2 = { 0:a0 (ts: 100), 1:a1 (ts: 102), 2:a2 (ts: 103)  }
            inputTopic1.pipeInput(expectedKeys[2], "A" + expectedKeys[2], time);

            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(2, "A2+a2", 103L)
            );
        }
    }

    @Test
    public void shouldNotEmitLeftJoinResultForAsymmetricAfterWindow() {
        final StreamsBuilder builder = new StreamsBuilder();
        final int[] expectedKeys = new int[] {0, 1, 2, 3};

        final KStream<Integer, String> stream1;
        final KStream<Integer, String> stream2;
        final KStream<Integer, String> joined;
        final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed);

        joined = stream1.outerJoin(
            stream2,
            MockValueJoiner.TOSTRING_JOINER,
            JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100)).after(ZERO),
            StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.String())
        );
        joined.process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), PROPS)) {
            final TestInputTopic<Integer, String> inputTopic1 =
                driver.createInputTopic(topic1, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<Integer, String> inputTopic2 =
                driver.createInputTopic(topic2, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockApiProcessor<Integer, String, Void, Void> processor = supplier.theCapturedProcessor();
            long time = 0L;

            // push two items to the primary stream; the other window is empty; this should not produce any item
            // w1 = {}
            // w2 = {}
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 1) }
            // --> w2 = {}
            for (int i = 0; i < 2; i++) {
                inputTopic1.pipeInput(expectedKeys[i], "A" + expectedKeys[i], time + i);
            }
            processor.checkAndClearProcessResult();

            // push one item to the other stream; this should produce one full-join item
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 1) }
            // w2 = {}
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 1) }
            // --> w2 = { 1:a1 (ts: 1) }
            time += 1;
            inputTopic2.pipeInput(expectedKeys[1], "a" + expectedKeys[1], time);

            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(1, "A1+a1", 1L)
            );

            // push one item to the other stream; this should produce one left-join item
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 1) }
            // w2 = { 1:a1 (ts: 1) }
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 1) }
            // --> w2 = { 1:a1 (ts: 1), 2:a2 (ts: 101) }
            time += 100;
            inputTopic2.pipeInput(expectedKeys[2], "a" + expectedKeys[2], time);

            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(0, "A0+null", 0L)
            );

            // push one item to the other stream; this should not produce any item
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 1) }
            // w2 = { 1:a1 (ts: 1), 2:a2 (ts: 101) }
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 1) }
            // --> w2 = { 1:a1 (ts: 1), 2:a2 (ts: 101), 3:a3 (ts: 101) }
            inputTopic2.pipeInput(expectedKeys[3], "a" + expectedKeys[3], time);

            processor.checkAndClearProcessResult();

            // push one item to the first stream; this should produce one full-join item
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 1) }
            // w2 = { 1:a1 (ts: 1), 2:a2 (ts: 101), 3:a3 (ts: 101) }
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 1), 2:A2 (ts: 201) }
            // --> w2 = { 1:a1 (ts: 1), 2:a2 (ts: 101), 3:a3 (ts: 101 }
            time += 100;
            inputTopic1.pipeInput(expectedKeys[2], "A" + expectedKeys[2], time);

            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(2, "A2+a2", 201L)
            );
        }
    }

    private void testUpperWindowBound(final int[] expectedKeys,
                                      final TopologyTestDriver driver,
                                      final MockApiProcessor<Integer, String, Void, Void> processor) {
        long time;

        final TestInputTopic<Integer, String> inputTopic1 =
            driver.createInputTopic(topic1, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
        final TestInputTopic<Integer, String> inputTopic2 =
            driver.createInputTopic(topic2, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);

        // push four items with larger and increasing timestamp (out of window) to the other stream; this should produced 2 expired non-joined records
        // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
        // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0) }
        // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
        // --> w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //            0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        time = 1000L;
        for (int i = 0; i < expectedKeys.length; i++) {
            inputTopic2.pipeInput(expectedKeys[i], "b" + expectedKeys[i], time + i);
        }
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(2, "null+a2", 0L),
            new KeyValueTimestamp<>(3, "null+a3", 0L)
        );

        // push four items with larger timestamp to the primary stream; this should produce four full-join items
        // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
        // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //        0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100) }
        // --> w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //            0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        time = 1000L + 100L;
        for (final int expectedKey : expectedKeys) {
            inputTopic1.pipeInput(expectedKey, "B" + expectedKey, time);
        }
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(0, "B0+b0", 1100L),
            new KeyValueTimestamp<>(1, "B1+b1", 1100L),
            new KeyValueTimestamp<>(2, "B2+b2", 1100L),
            new KeyValueTimestamp<>(3, "B3+b3", 1100L)
        );

        // push four items with increased timestamp to the primary stream; this should produce three full-join items (non-joined item is not produced yet)
        // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //        0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100) }
        // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //        0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //            0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101) }
        // --> w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //            0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        time += 1L;
        for (final int expectedKey : expectedKeys) {
            inputTopic1.pipeInput(expectedKey, "C" + expectedKey, time);
        }
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(1, "C1+b1", 1101L),
            new KeyValueTimestamp<>(2, "C2+b2", 1101L),
            new KeyValueTimestamp<>(3, "C3+b3", 1101L)
        );

        // push four items with increased timestamp to the primary stream; this should produce two full-join items (non-joined items are not produced yet)
        // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //        0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //        0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101) }
        // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //        0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //            0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //            0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102) }
        // --> w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //            0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        time += 1L;
        for (final int expectedKey : expectedKeys) {
            inputTopic1.pipeInput(expectedKey, "D" + expectedKey, time);
        }
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(2, "D2+b2", 1102L),
            new KeyValueTimestamp<>(3, "D3+b3", 1102L)
        );

        // push four items with increased timestamp to the primary stream; this should produce one full-join items (three non-joined left-join are not produced yet)
        // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //        0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //        0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //        0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102) }
        // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //        0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //            0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //            0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //            0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103) }
        // --> w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //            0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        time += 1L;
        for (final int expectedKey : expectedKeys) {
            inputTopic1.pipeInput(expectedKey, "E" + expectedKey, time);
        }
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(3, "E3+b3", 1103L)
        );

        // push four items with increased timestamp to the primary stream; this should produce no full-join items (four non-joined left-join are not produced yet)
        // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //        0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //        0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //        0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //        0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103) }
        // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //        0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //            0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //            0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //            0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
        //            0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104) }
        // --> w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //            0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        time += 1L;
        for (final int expectedKey : expectedKeys) {
            inputTopic1.pipeInput(expectedKey, "F" + expectedKey, time);
        }
        processor.checkAndClearProcessResult();

        // push a dummy record to produce all left-join non-joined items
        time += 301L;
        inputTopic1.pipeInput(0, "dummy", time);
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(0, "C0+null", 1101L),
            new KeyValueTimestamp<>(0, "D0+null", 1102L),
            new KeyValueTimestamp<>(1, "D1+null", 1102L),
            new KeyValueTimestamp<>(0, "E0+null", 1103L),
            new KeyValueTimestamp<>(1, "E1+null", 1103L),
            new KeyValueTimestamp<>(2, "E2+null", 1103L),
            new KeyValueTimestamp<>(0, "F0+null", 1104L),
            new KeyValueTimestamp<>(1, "F1+null", 1104L),
            new KeyValueTimestamp<>(2, "F2+null", 1104L),
            new KeyValueTimestamp<>(3, "F3+null", 1104L)
        );
    }

    private void testLowerWindowBound(final int[] expectedKeys,
                                      final TopologyTestDriver driver,
                                      final MockApiProcessor<Integer, String, Void, Void> processor) {
        long time;
        final TestInputTopic<Integer, String> inputTopic1 = driver.createInputTopic(topic1, new IntegerSerializer(), new StringSerializer());

        // push four items with smaller timestamp (before the window) to the primary stream; this should produce four left-join and no full-join items
        // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //        0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //        0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //        0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //        0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
        //        0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104) }
        // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //        0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //            0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //            0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //            0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
        //            0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104),
        //            0:G0 (ts: 899), 1:G1 (ts: 899), 2:G2 (ts: 899), 3:G3 (ts: 899) }
        // --> w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //            0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        time = 1000L - 100L - 1L;
        for (final int expectedKey : expectedKeys) {
            inputTopic1.pipeInput(expectedKey, "G" + expectedKey, time);
        }
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(0, "G0+null", 899L),
            new KeyValueTimestamp<>(1, "G1+null", 899L),
            new KeyValueTimestamp<>(2, "G2+null", 899L),
            new KeyValueTimestamp<>(3, "G3+null", 899L)
        );

        // push four items with increase timestamp to the primary stream; this should produce three left-join and one full-join items
        // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //        0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //        0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //        0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //        0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
        //        0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104),
        //        0:G0 (ts: 899), 1:G1 (ts: 899), 2:G2 (ts: 899), 3:G3 (ts: 899) }
        // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //        0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //            0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //            0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //            0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
        //            0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104),
        //            0:G0 (ts: 899), 1:G1 (ts: 899), 2:G2 (ts: 899), 3:G3 (ts: 899),
        //            0:H0 (ts: 900), 1:H1 (ts: 900), 2:H2 (ts: 900), 3:H3 (ts: 900) }
        // --> w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //            0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        time += 1L;
        for (final int expectedKey : expectedKeys) {
            inputTopic1.pipeInput(expectedKey, "H" + expectedKey, time);
        }
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(0, "H0+b0", 1000L),
            new KeyValueTimestamp<>(1, "H1+null", 900L),
            new KeyValueTimestamp<>(2, "H2+null", 900L),
            new KeyValueTimestamp<>(3, "H3+null", 900L)
        );

        // push four items with increase timestamp to the primary stream; this should produce two left-join and two full-join items
        // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //        0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //        0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //        0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //        0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
        //        0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104),
        //        0:G0 (ts: 899), 1:G1 (ts: 899), 2:G2 (ts: 899), 3:G3 (ts: 899),
        //        0:H0 (ts: 900), 1:H1 (ts: 900), 2:H2 (ts: 900), 3:H3 (ts: 900) }
        // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //        0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //            0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //            0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //            0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
        //            0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104),
        //            0:G0 (ts: 899), 1:G1 (ts: 899), 2:G2 (ts: 899), 3:G3 (ts: 899),
        //            0:H0 (ts: 900), 1:H1 (ts: 900), 2:H2 (ts: 900), 3:H3 (ts: 900),
        //            0:I0 (ts: 901), 1:I1 (ts: 901), 2:I2 (ts: 901), 3:I3 (ts: 901) }
        // --> w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //            0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        time += 1L;
        for (final int expectedKey : expectedKeys) {
            inputTopic1.pipeInput(expectedKey, "I" + expectedKey, time);
        }
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(0, "I0+b0", 1000L),
            new KeyValueTimestamp<>(1, "I1+b1", 1001L),
            new KeyValueTimestamp<>(2, "I2+null", 901L),
            new KeyValueTimestamp<>(3, "I3+null", 901L)
        );

        // push four items with increase timestamp to the primary stream; this should produce one left-join and three full-join items
        // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //        0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //        0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //        0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //        0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
        //        0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104),
        //        0:G0 (ts: 899), 1:G1 (ts: 899), 2:G2 (ts: 899), 3:G3 (ts: 899),
        //        0:H0 (ts: 900), 1:H1 (ts: 900), 2:H2 (ts: 900), 3:H3 (ts: 900),
        //        0:I0 (ts: 901), 1:I1 (ts: 901), 2:I2 (ts: 901), 3:I3 (ts: 901) }
        // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //        0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //            0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //            0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //            0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
        //            0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104),
        //            0:G0 (ts: 899), 1:G1 (ts: 899), 2:G2 (ts: 899), 3:G3 (ts: 899),
        //            0:H0 (ts: 900), 1:H1 (ts: 900), 2:H2 (ts: 900), 3:H3 (ts: 900),
        //            0:I0 (ts: 901), 1:I1 (ts: 901), 2:I2 (ts: 901), 3:I3 (ts: 901),
        //            0:J0 (ts: 902), 1:J1 (ts: 902), 2:J2 (ts: 902), 3:J3 (ts: 902) }
        // --> w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //            0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        time += 1L;
        for (final int expectedKey : expectedKeys) {
            inputTopic1.pipeInput(expectedKey, "J" + expectedKey, time);
        }
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(0, "J0+b0", 1000L),
            new KeyValueTimestamp<>(1, "J1+b1", 1001L),
            new KeyValueTimestamp<>(2, "J2+b2", 1002L),
            new KeyValueTimestamp<>(3, "J3+null", 902L)
        );

        // push four items with increase timestamp to the primary stream; this should produce one left-join and three full-join items
        // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //        0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //        0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //        0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //        0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
        //        0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104),
        //        0:G0 (ts: 899), 1:G1 (ts: 899), 2:G2 (ts: 899), 3:G3 (ts: 899),
        //        0:H0 (ts: 900), 1:H1 (ts: 900), 2:H2 (ts: 900), 3:H3 (ts: 900),
        //        0:I0 (ts: 901), 1:I1 (ts: 901), 2:I2 (ts: 901), 3:I3 (ts: 901),
        //        0:J0 (ts: 902), 1:J1 (ts: 902), 2:J2 (ts: 902), 3:J3 (ts: 902) }
        // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //        0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //            0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //            0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //            0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
        //            0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104),
        //            0:G0 (ts: 899), 1:G1 (ts: 899), 2:G2 (ts: 899), 3:G3 (ts: 899),
        //            0:H0 (ts: 900), 1:H1 (ts: 900), 2:H2 (ts: 900), 3:H3 (ts: 900),
        //            0:I0 (ts: 901), 1:I1 (ts: 901), 2:I2 (ts: 901), 3:I3 (ts: 901),
        //            0:J0 (ts: 902), 1:J1 (ts: 902), 2:J2 (ts: 902), 3:J3 (ts: 902),
        //            0:K0 (ts: 903), 1:K1 (ts: 903), 2:K2 (ts: 903), 3:K3 (ts: 903) }
        // --> w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //            0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        time += 1L;
        for (final int expectedKey : expectedKeys) {
            inputTopic1.pipeInput(expectedKey, "K" + expectedKey, time);
        }
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(0, "K0+b0", 1000L),
            new KeyValueTimestamp<>(1, "K1+b1", 1001L),
            new KeyValueTimestamp<>(2, "K2+b2", 1002L),
            new KeyValueTimestamp<>(3, "K3+b3", 1003L)
        );

        // push a dummy record to verify there are no expired records to produce
        // dummy window is behind the max. stream time seen (1205 used in testUpperWindowBound)
        inputTopic1.pipeInput(0, "dummy", time + 200L);
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(0, "dummy+null", 1103L)
        );
    }
}
