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
package org.apache.kafka.streams.kstream.internals.suppress;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.TimeWindowedSerializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.KTableImpl;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.state.internals.InMemoryTimeOrderedKeyValueChangeBuffer;
import org.apache.kafka.test.MockInternalNewProcessorContext;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;

import static java.time.Duration.ZERO;
import static java.time.Duration.ofMillis;
import static org.apache.kafka.common.serialization.Serdes.Long;
import static org.apache.kafka.common.serialization.Serdes.String;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.maxBytes;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.maxRecords;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.Suppressed.untilTimeLimit;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;
import static org.apache.kafka.streams.kstream.WindowedSerdes.sessionWindowedSerdeFrom;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class KTableSuppressProcessorTest {
    private static final long ARBITRARY_LONG = 5L;

    private static final Change<Long> ARBITRARY_CHANGE = new Change<>(7L, 14L);

    private static class Harness<K, V> {
        private final Processor<K, Change<V>, K, Change<V>> processor;
        private final MockInternalNewProcessorContext<K, Change<V>> context;


        Harness(final Suppressed<K> suppressed,
                final Serde<K> keySerde,
                final Serde<V> valueSerde) {

            final String storeName = "test-store";

            final StateStore buffer = new InMemoryTimeOrderedKeyValueChangeBuffer.Builder<>(storeName, keySerde, valueSerde)
                .withLoggingDisabled()
                .build();

            @SuppressWarnings("unchecked")
            final KTableImpl<K, ?, V> parent = mock(KTableImpl.class);
            final Processor<K, Change<V>, K, Change<V>> processor =
                new KTableSuppressProcessorSupplier<>((SuppressedInternal<K>) suppressed, storeName, parent).get();

            final MockInternalNewProcessorContext<K, Change<V>> context = new MockInternalNewProcessorContext<>();
            context.setCurrentNode(new ProcessorNode("testNode"));

            buffer.init((StateStoreContext) context, buffer);
            processor.init(context);

            this.processor = processor;
            this.context = context;
        }
    }

    @Test
    public void zeroTimeLimitShouldImmediatelyEmit() {
        final Harness<String, Long> harness =
            new Harness<>(untilTimeLimit(ZERO, unbounded()), String(), Long());
        final MockInternalNewProcessorContext<String, Change<Long>> context = harness.context;

        final long timestamp = ARBITRARY_LONG;
        context.setRecordMetadata("", 0, 0L);
        context.setTimestamp(timestamp);
        final String key = "hey";
        final Change<Long> value = ARBITRARY_CHANGE;
        harness.processor.process(new Record<>(key, value, timestamp));

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.record(), is(new Record<>(key, value, timestamp)));
    }

    @Test
    public void windowedZeroTimeLimitShouldImmediatelyEmit() {
        final Harness<Windowed<String>, Long> harness =
            new Harness<>(untilTimeLimit(ZERO, unbounded()), timeWindowedSerdeFrom(String.class, 100L), Long());
        final MockInternalNewProcessorContext<Windowed<String>, Change<Long>> context = harness.context;

        final long timestamp = ARBITRARY_LONG;
        context.setRecordMetadata("", 0, 0L);
        context.setTimestamp(timestamp);
        final Windowed<String> key = new Windowed<>("hey", new TimeWindow(0L, 100L));
        final Change<Long> value = ARBITRARY_CHANGE;
        harness.processor.process(new Record<>(key, value, timestamp));

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.record(), is(new Record<>(key, value, timestamp)));
    }

    @Test
    public void intermediateSuppressionShouldBufferAndEmitLater() {
        final Harness<String, Long> harness =
            new Harness<>(untilTimeLimit(ofMillis(1), unbounded()), String(), Long());
        final MockInternalNewProcessorContext<String, Change<Long>> context = harness.context;

        final long timestamp = 0L;
        context.setRecordMetadata("topic", 0, 0);
        context.setTimestamp(timestamp);
        final String key = "hey";
        final Change<Long> value = new Change<>(null, 1L);
        harness.processor.process(new Record<>(key, value, timestamp));
        assertThat(context.forwarded(), hasSize(0));

        context.setRecordMetadata("topic", 0, 1);
        context.setTimestamp(1L);
        harness.processor.process(new Record<>("tick", new Change<>(null, null), 1L));

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.record(), is(new Record<>(key, value, timestamp)));
    }

    @Test
    public void finalResultsSuppressionShouldBufferAndEmitAtGraceExpiration() {
        final Harness<Windowed<String>, Long> harness =
            new Harness<>(finalResults(ofMillis(1L)), timeWindowedSerdeFrom(String.class, 1L), Long());
        final MockInternalNewProcessorContext<Windowed<String>, Change<Long>> context = harness.context;

        final long windowStart = 99L;
        final long recordTime = 99L;
        final long windowEnd = 100L;
        context.setRecordMetadata("topic", 0, 0);
        context.setTimestamp(recordTime);
        final Windowed<String> key = new Windowed<>("hey", new TimeWindow(windowStart, windowEnd));
        final Change<Long> value = ARBITRARY_CHANGE;
        harness.processor.process(new Record<>(key, value, recordTime));
        assertThat(context.forwarded(), hasSize(0));

        // although the stream time is now 100, we have to wait 1 ms after the window *end* before we
        // emit "hey", so we don't emit yet.
        final long windowStart2 = 100L;
        final long recordTime2 = 100L;
        final long windowEnd2 = 101L;
        context.setRecordMetadata("topic", 0, 1);
        context.setTimestamp(recordTime2);
        harness.processor.process(new Record<>(new Windowed<>("dummyKey1", new TimeWindow(windowStart2, windowEnd2)), ARBITRARY_CHANGE, recordTime2));
        assertThat(context.forwarded(), hasSize(0));

        // ok, now it's time to emit "hey"
        final long windowStart3 = 101L;
        final long recordTime3 = 101L;
        final long windowEnd3 = 102L;
        context.setRecordMetadata("topic", 0, 1);
        context.setTimestamp(recordTime3);
        harness.processor.process(new Record<>(new Windowed<>("dummyKey2", new TimeWindow(windowStart3, windowEnd3)), ARBITRARY_CHANGE, recordTime3));

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.record(), is(new Record<>(key, value, recordTime)));
    }

    /**
     * Testing a special case of final results: that even with a grace period of 0,
     * it will still buffer events and emit only after the end of the window.
     * As opposed to emitting immediately the way regular suppression would with a time limit of 0.
     */
    @Test
    public void finalResultsWithZeroGraceShouldStillBufferUntilTheWindowEnd() {
        final Harness<Windowed<String>, Long> harness =
            new Harness<>(finalResults(ofMillis(0L)), timeWindowedSerdeFrom(String.class, 100L), Long());
        final MockInternalNewProcessorContext<Windowed<String>, Change<Long>> context = harness.context;

        // note the record is in the past, but the window end is in the future, so we still have to buffer,
        // even though the grace period is 0.
        final long timestamp = 5L;
        final long windowEnd = 100L;
        context.setRecordMetadata("", 0, 0L);
        context.setTimestamp(timestamp);
        final Windowed<String> key = new Windowed<>("hey", new TimeWindow(0, windowEnd));
        final Change<Long> value = ARBITRARY_CHANGE;
        harness.processor.process(new Record<>(key, value, timestamp));
        assertThat(context.forwarded(), hasSize(0));

        context.setRecordMetadata("", 0, 1L);
        context.setTimestamp(windowEnd);
        harness.processor.process(new Record<>(new Windowed<>("dummyKey", new TimeWindow(windowEnd, windowEnd + 100L)), ARBITRARY_CHANGE, windowEnd));

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.record(), is(new Record<>(key, value, timestamp)));
    }

    @Test
    public void finalResultsWithZeroGraceAtWindowEndShouldImmediatelyEmit() {
        final Harness<Windowed<String>, Long> harness =
            new Harness<>(finalResults(ofMillis(0L)), timeWindowedSerdeFrom(String.class, 100L), Long());
        final MockInternalNewProcessorContext<Windowed<String>, Change<Long>> context = harness.context;

        final long timestamp = 100L;
        context.setRecordMetadata("", 0, 0L);
        context.setTimestamp(timestamp);
        final Windowed<String> key = new Windowed<>("hey", new TimeWindow(0, 100L));
        final Change<Long> value = ARBITRARY_CHANGE;
        harness.processor.process(new Record<>(key, value, timestamp));

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.record(), is(new Record<>(key, value, timestamp)));
    }

    /**
     * It's desirable to drop tombstones for final-results windowed streams, since (as described in the
     * {@link SuppressedInternal} javadoc), they are unnecessary to emit.
     */
    @Test
    public void finalResultsShouldDropTombstonesForTimeWindows() {
        final Harness<Windowed<String>, Long> harness =
            new Harness<>(finalResults(ofMillis(0L)), timeWindowedSerdeFrom(String.class, 100L), Long());
        final MockInternalNewProcessorContext<Windowed<String>, Change<Long>> context = harness.context;

        final long timestamp = 100L;
        context.setRecordMetadata("", 0, 0L);
        context.setTimestamp(timestamp);
        final Windowed<String> key = new Windowed<>("hey", new TimeWindow(0, 100L));
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        harness.processor.process(new Record<>(key, value, timestamp));

        assertThat(context.forwarded(), hasSize(0));
    }


    /**
     * It's desirable to drop tombstones for final-results windowed streams, since (as described in the
     * {@link SuppressedInternal} javadoc), they are unnecessary to emit.
     */
    @Test
    public void finalResultsShouldDropTombstonesForSessionWindows() {
        final Harness<Windowed<String>, Long> harness =
            new Harness<>(finalResults(ofMillis(0L)), sessionWindowedSerdeFrom(String.class), Long());
        final MockInternalNewProcessorContext<Windowed<String>, Change<Long>> context = harness.context;

        final long timestamp = 100L;
        context.setRecordMetadata("", 0, 0L);
        context.setTimestamp(timestamp);
        final Windowed<String> key = new Windowed<>("hey", new SessionWindow(0L, 0L));
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        harness.processor.process(new Record<>(key, value, timestamp));

        assertThat(context.forwarded(), hasSize(0));
    }

    /**
     * It's NOT OK to drop tombstones for non-final-results windowed streams, since we may have emitted some results for
     * the window before getting the tombstone (see the {@link SuppressedInternal} javadoc).
     */
    @Test
    public void suppressShouldNotDropTombstonesForTimeWindows() {
        final Harness<Windowed<String>, Long> harness =
            new Harness<>(untilTimeLimit(ofMillis(0), maxRecords(0)), timeWindowedSerdeFrom(String.class, 100L), Long());
        final MockInternalNewProcessorContext<Windowed<String>, Change<Long>> context = harness.context;

        final long timestamp = 100L;
        final Headers headers = new RecordHeaders().add("k", "v".getBytes(StandardCharsets.UTF_8));
        context.setRecordMetadata("", 0, 0L);
        context.setTimestamp(timestamp);
        context.setHeaders(headers);
        final Windowed<String> key = new Windowed<>("hey", new TimeWindow(0L, 100L));
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        harness.processor.process(new Record<>(key, value, timestamp));

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.record(), is(new Record<>(key, value, timestamp, headers)));
    }


    /**
     * It's NOT OK to drop tombstones for non-final-results windowed streams, since we may have emitted some results for
     * the window before getting the tombstone (see the {@link SuppressedInternal} javadoc).
     */
    @Test
    public void suppressShouldNotDropTombstonesForSessionWindows() {
        final Harness<Windowed<String>, Long> harness =
            new Harness<>(untilTimeLimit(ofMillis(0), maxRecords(0)), sessionWindowedSerdeFrom(String.class), Long());
        final MockInternalNewProcessorContext<Windowed<String>, Change<Long>> context = harness.context;

        final long timestamp = 100L;
        context.setRecordMetadata("", 0, 0L);
        context.setTimestamp(timestamp);
        final Windowed<String> key = new Windowed<>("hey", new SessionWindow(0L, 0L));
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        harness.processor.process(new Record<>(key, value, timestamp));

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.record(), is(new Record<>(key, value, timestamp)));
    }


    /**
     * It's SUPER NOT OK to drop tombstones for non-windowed streams, since we may have emitted some results for
     * the key before getting the tombstone (see the {@link SuppressedInternal} javadoc).
     */
    @Test
    public void suppressShouldNotDropTombstonesForKTable() {
        final Harness<String, Long> harness =
            new Harness<>(untilTimeLimit(ofMillis(0), maxRecords(0)), String(), Long());
        final MockInternalNewProcessorContext<String, Change<Long>> context = harness.context;

        final long timestamp = 100L;
        context.setRecordMetadata("", 0, 0L);
        context.setTimestamp(timestamp);
        final String key = "hey";
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        harness.processor.process(new Record<>(key, value, timestamp));

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.record(), is(new Record<>(key, value, timestamp)));
    }

    @Test
    public void suppressShouldEmitWhenOverRecordCapacity() {
        final Harness<String, Long> harness =
            new Harness<>(untilTimeLimit(Duration.ofDays(100), maxRecords(1)), String(), Long());
        final MockInternalNewProcessorContext<String, Change<Long>> context = harness.context;

        final long timestamp = 100L;
        context.setRecordMetadata("", 0, 0L);
        context.setTimestamp(timestamp);
        final String key = "hey";
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        harness.processor.process(new Record<>(key, value, timestamp));

        context.setRecordMetadata("", 0, 1L);
        context.setTimestamp(timestamp + 1);
        harness.processor.process(new Record<>("dummyKey", value, timestamp + 1));

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.record(), is(new Record<>(key, value, timestamp)));
    }

    @Test
    public void suppressShouldEmitWhenOverByteCapacity() {
        final Harness<String, Long> harness =
            new Harness<>(untilTimeLimit(Duration.ofDays(100), maxBytes(60L)), String(), Long());
        final MockInternalNewProcessorContext<String, Change<Long>> context = harness.context;

        final long timestamp = 100L;
        context.setRecordMetadata("", 0, 0L);
        context.setTimestamp(timestamp);
        final String key = "hey";
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        harness.processor.process(new Record<>(key, value, timestamp));

        context.setRecordMetadata("", 0, 1L);
        context.setTimestamp(timestamp + 1);
        harness.processor.process(new Record<>("dummyKey", value, timestamp + 1));

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.record(), is(new Record<>(key, value, timestamp)));
    }

    @Test
    public void suppressShouldShutDownWhenOverRecordCapacity() {
        final Harness<String, Long> harness =
            new Harness<>(untilTimeLimit(Duration.ofDays(100), maxRecords(1).shutDownWhenFull()), String(), Long());
        final MockInternalNewProcessorContext<String, Change<Long>> context = harness.context;

        final long timestamp = 100L;
        context.setRecordMetadata("", 0, 0L);
        context.setTimestamp(timestamp);
        context.setCurrentNode(new ProcessorNode("testNode"));
        final String key = "hey";
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        harness.processor.process(new Record<>(key, value, timestamp));

        context.setRecordMetadata("", 0, 1L);
        context.setTimestamp(timestamp);
        try {
            harness.processor.process(new Record<>("dummyKey", value, timestamp));
            fail("expected an exception");
        } catch (final StreamsException e) {
            assertThat(e.getMessage(), containsString("buffer exceeded its max capacity"));
        }
    }

    @Test
    public void suppressShouldShutDownWhenOverByteCapacity() {
        final Harness<String, Long> harness =
            new Harness<>(untilTimeLimit(Duration.ofDays(100), maxBytes(60L).shutDownWhenFull()), String(), Long());
        final MockInternalNewProcessorContext<String, Change<Long>> context = harness.context;

        final long timestamp = 100L;
        context.setRecordMetadata("", 0, 0L);
        context.setTimestamp(timestamp);
        context.setCurrentNode(new ProcessorNode("testNode"));
        final String key = "hey";
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        harness.processor.process(new Record<>(key, value, timestamp));

        context.setRecordMetadata("", 0, 1L);
        context.setTimestamp(1L);
        try {
            harness.processor.process(new Record<>("dummyKey", value, timestamp));
            fail("expected an exception");
        } catch (final StreamsException e) {
            assertThat(e.getMessage(), containsString("buffer exceeded its max capacity"));
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <K extends Windowed> SuppressedInternal<K> finalResults(final Duration grace) {
        return ((FinalResultsSuppressionBuilder) untilWindowCloses(unbounded())).buildFinalResultsSuppression(grace);
    }

    private static <E> Matcher<Collection<E>> hasSize(final int i) {
        return new BaseMatcher<Collection<E>>() {
            @Override
            public void describeTo(final Description description) {
                description.appendText("a collection of size " + i);
            }

            @SuppressWarnings("unchecked")
            @Override
            public boolean matches(final Object item) {
                if (item == null) {
                    return false;
                } else {
                    return ((Collection<E>) item).size() == i;
                }
            }

        };
    }

    private static <K> Serde<Windowed<K>> timeWindowedSerdeFrom(final Class<K> rawType, final long windowSize) {
        final Serde<K> kSerde = Serdes.serdeFrom(rawType);
        return new Serdes.WrapperSerde<>(
            new TimeWindowedSerializer<>(kSerde.serializer()),
            new TimeWindowedDeserializer<>(kSerde.deserializer(), windowSize)
        );
    }
}