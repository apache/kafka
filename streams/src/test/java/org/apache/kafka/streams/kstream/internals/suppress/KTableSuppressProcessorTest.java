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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.TimeWindowedSerializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.FullChangeSerde;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.state.internals.InMemoryTimeOrderedKeyValueBuffer;
import org.apache.kafka.test.MockInternalProcessorContext;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Test;

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

public class KTableSuppressProcessorTest {
    private static final long ARBITRARY_LONG = 5L;

    private static final Change<Long> ARBITRARY_CHANGE = new Change<>(7L, 14L);

    private static class Harness<K, V> {
        private final KTableSuppressProcessor<K, V> processor;
        private final MockInternalProcessorContext context;


        Harness(final Suppressed<K> suppressed,
                final Serde<K> keySerde,
                final Serde<V> valueSerde) {

            final String storeName = "test-store";

            final StateStore buffer = new InMemoryTimeOrderedKeyValueBuffer.Builder<>(storeName, keySerde, FullChangeSerde.castOrWrap(valueSerde))
                .withLoggingDisabled()
                .build();

            final KTableSuppressProcessor<K, V> processor =
                new KTableSuppressProcessor<>((SuppressedInternal<K>) suppressed, storeName);

            final MockInternalProcessorContext context = new MockInternalProcessorContext();
            context.setCurrentNode(new ProcessorNode("testNode"));

            buffer.init(context, buffer);
            processor.init(context);

            this.processor = processor;
            this.context = context;
        }
    }

    @Test
    public void zeroTimeLimitShouldImmediatelyEmit() {
        final Harness<String, Long> harness =
            new Harness<>(untilTimeLimit(ZERO, unbounded()), String(), Long());
        final MockInternalProcessorContext context = harness.context;
        final KTableSuppressProcessor<String, Long> processor = harness.processor;

        final long timestamp = ARBITRARY_LONG;
        context.setRecordMetadata("", 0, 0L, null, timestamp);
        final String key = "hey";
        final Change<Long> value = ARBITRARY_CHANGE;
        processor.process(key, value);

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.keyValue(), is(new KeyValue<>(key, value)));
        assertThat(capturedForward.timestamp(), is(timestamp));
    }

    @Test
    public void windowedZeroTimeLimitShouldImmediatelyEmit() {
        final Harness<Windowed<String>, Long> harness =
            new Harness<>(untilTimeLimit(ZERO, unbounded()), timeWindowedSerdeFrom(String.class, 100L), Long());
        final MockInternalProcessorContext context = harness.context;
        final KTableSuppressProcessor<Windowed<String>, Long> processor = harness.processor;

        final long timestamp = ARBITRARY_LONG;
        context.setRecordMetadata("", 0, 0L, null, timestamp);
        final Windowed<String> key = new Windowed<>("hey", new TimeWindow(0L, 100L));
        final Change<Long> value = ARBITRARY_CHANGE;
        processor.process(key, value);

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.keyValue(), is(new KeyValue<>(key, value)));
        assertThat(capturedForward.timestamp(), is(timestamp));
    }

    @Test
    public void intermediateSuppressionShouldBufferAndEmitLater() {
        final Harness<String, Long> harness =
            new Harness<>(untilTimeLimit(ofMillis(1), unbounded()), String(), Long());
        final MockInternalProcessorContext context = harness.context;
        final KTableSuppressProcessor<String, Long> processor = harness.processor;

        final long timestamp = 0L;
        context.setRecordMetadata("topic", 0, 0, null, timestamp);
        final String key = "hey";
        final Change<Long> value = new Change<>(null, 1L);
        processor.process(key, value);
        assertThat(context.forwarded(), hasSize(0));

        context.setRecordMetadata("topic", 0, 1, null, 1L);
        processor.process("tick", new Change<>(null, null));

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.keyValue(), is(new KeyValue<>(key, value)));
        assertThat(capturedForward.timestamp(), is(timestamp));
    }

    @Test
    public void finalResultsSuppressionShouldBufferAndEmitAtGraceExpiration() {
        final Harness<Windowed<String>, Long> harness =
            new Harness<>(finalResults(ofMillis(1L)), timeWindowedSerdeFrom(String.class, 1L), Long());
        final MockInternalProcessorContext context = harness.context;
        final KTableSuppressProcessor<Windowed<String>, Long> processor = harness.processor;

        final long windowStart = 99L;
        final long recordTime = 99L;
        final long windowEnd = 100L;
        context.setRecordMetadata("topic", 0, 0, null, recordTime);
        final Windowed<String> key = new Windowed<>("hey", new TimeWindow(windowStart, windowEnd));
        final Change<Long> value = ARBITRARY_CHANGE;
        processor.process(key, value);
        assertThat(context.forwarded(), hasSize(0));

        // although the stream time is now 100, we have to wait 1 ms after the window *end* before we
        // emit "hey", so we don't emit yet.
        final long windowStart2 = 100L;
        final long recordTime2 = 100L;
        final long windowEnd2 = 101L;
        context.setRecordMetadata("topic", 0, 1, null, recordTime2);
        processor.process(new Windowed<>("dummyKey1", new TimeWindow(windowStart2, windowEnd2)), ARBITRARY_CHANGE);
        assertThat(context.forwarded(), hasSize(0));

        // ok, now it's time to emit "hey"
        final long windowStart3 = 101L;
        final long recordTime3 = 101L;
        final long windowEnd3 = 102L;
        context.setRecordMetadata("topic", 0, 1, null, recordTime3);
        processor.process(new Windowed<>("dummyKey2", new TimeWindow(windowStart3, windowEnd3)), ARBITRARY_CHANGE);

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.keyValue(), is(new KeyValue<>(key, value)));
        assertThat(capturedForward.timestamp(), is(recordTime));
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
        final MockInternalProcessorContext context = harness.context;
        final KTableSuppressProcessor<Windowed<String>, Long> processor = harness.processor;

        // note the record is in the past, but the window end is in the future, so we still have to buffer,
        // even though the grace period is 0.
        final long timestamp = 5L;
        final long windowEnd = 100L;
        context.setRecordMetadata("", 0, 0L, null, timestamp);
        final Windowed<String> key = new Windowed<>("hey", new TimeWindow(0, windowEnd));
        final Change<Long> value = ARBITRARY_CHANGE;
        processor.process(key, value);
        assertThat(context.forwarded(), hasSize(0));

        context.setRecordMetadata("", 0, 1L, null, windowEnd);
        processor.process(new Windowed<>("dummyKey", new TimeWindow(windowEnd, windowEnd + 100L)), ARBITRARY_CHANGE);

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.keyValue(), is(new KeyValue<>(key, value)));
        assertThat(capturedForward.timestamp(), is(timestamp));
    }

    @Test
    public void finalResultsWithZeroGraceAtWindowEndShouldImmediatelyEmit() {
        final Harness<Windowed<String>, Long> harness =
            new Harness<>(finalResults(ofMillis(0L)), timeWindowedSerdeFrom(String.class, 100L), Long());
        final MockInternalProcessorContext context = harness.context;
        final KTableSuppressProcessor<Windowed<String>, Long> processor = harness.processor;

        final long timestamp = 100L;
        context.setRecordMetadata("", 0, 0L, null, timestamp);
        final Windowed<String> key = new Windowed<>("hey", new TimeWindow(0, 100L));
        final Change<Long> value = ARBITRARY_CHANGE;
        processor.process(key, value);

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.keyValue(), is(new KeyValue<>(key, value)));
        assertThat(capturedForward.timestamp(), is(timestamp));
    }

    /**
     * It's desirable to drop tombstones for final-results windowed streams, since (as described in the
     * {@link SuppressedInternal} javadoc), they are unnecessary to emit.
     */
    @Test
    public void finalResultsShouldDropTombstonesForTimeWindows() {
        final Harness<Windowed<String>, Long> harness =
            new Harness<>(finalResults(ofMillis(0L)), timeWindowedSerdeFrom(String.class, 100L), Long());
        final MockInternalProcessorContext context = harness.context;
        final KTableSuppressProcessor<Windowed<String>, Long> processor = harness.processor;

        final long timestamp = 100L;
        context.setRecordMetadata("", 0, 0L, null, timestamp);
        final Windowed<String> key = new Windowed<>("hey", new TimeWindow(0, 100L));
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        processor.process(key, value);

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
        final MockInternalProcessorContext context = harness.context;
        final KTableSuppressProcessor<Windowed<String>, Long> processor = harness.processor;

        final long timestamp = 100L;
        context.setRecordMetadata("", 0, 0L, null, timestamp);
        final Windowed<String> key = new Windowed<>("hey", new SessionWindow(0L, 0L));
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        processor.process(key, value);

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
        final MockInternalProcessorContext context = harness.context;
        final KTableSuppressProcessor<Windowed<String>, Long> processor = harness.processor;

        final long timestamp = 100L;
        context.setRecordMetadata("", 0, 0L, null, timestamp);
        final Windowed<String> key = new Windowed<>("hey", new TimeWindow(0L, 100L));
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        processor.process(key, value);

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.keyValue(), is(new KeyValue<>(key, value)));
        assertThat(capturedForward.timestamp(), is(timestamp));
    }


    /**
     * It's NOT OK to drop tombstones for non-final-results windowed streams, since we may have emitted some results for
     * the window before getting the tombstone (see the {@link SuppressedInternal} javadoc).
     */
    @Test
    public void suppressShouldNotDropTombstonesForSessionWindows() {
        final Harness<Windowed<String>, Long> harness =
            new Harness<>(untilTimeLimit(ofMillis(0), maxRecords(0)), sessionWindowedSerdeFrom(String.class), Long());
        final MockInternalProcessorContext context = harness.context;
        final KTableSuppressProcessor<Windowed<String>, Long> processor = harness.processor;

        final long timestamp = 100L;
        context.setRecordMetadata("", 0, 0L, null, timestamp);
        final Windowed<String> key = new Windowed<>("hey", new SessionWindow(0L, 0L));
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        processor.process(key, value);

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.keyValue(), is(new KeyValue<>(key, value)));
        assertThat(capturedForward.timestamp(), is(timestamp));
    }


    /**
     * It's SUPER NOT OK to drop tombstones for non-windowed streams, since we may have emitted some results for
     * the key before getting the tombstone (see the {@link SuppressedInternal} javadoc).
     */
    @Test
    public void suppressShouldNotDropTombstonesForKTable() {
        final Harness<String, Long> harness =
            new Harness<>(untilTimeLimit(ofMillis(0), maxRecords(0)), String(), Long());
        final MockInternalProcessorContext context = harness.context;
        final KTableSuppressProcessor<String, Long> processor = harness.processor;

        final long timestamp = 100L;
        context.setRecordMetadata("", 0, 0L, null, timestamp);
        final String key = "hey";
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        processor.process(key, value);

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.keyValue(), is(new KeyValue<>(key, value)));
        assertThat(capturedForward.timestamp(), is(timestamp));
    }

    @Test
    public void suppressShouldEmitWhenOverRecordCapacity() {
        final Harness<String, Long> harness =
            new Harness<>(untilTimeLimit(Duration.ofDays(100), maxRecords(1)), String(), Long());
        final MockInternalProcessorContext context = harness.context;
        final KTableSuppressProcessor<String, Long> processor = harness.processor;

        final long timestamp = 100L;
        context.setRecordMetadata("", 0, 0L, null, timestamp);
        final String key = "hey";
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        processor.process(key, value);

        context.setRecordMetadata("", 0, 1L, null, timestamp + 1);
        processor.process("dummyKey", value);

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.keyValue(), is(new KeyValue<>(key, value)));
        assertThat(capturedForward.timestamp(), is(timestamp));
    }

    @Test
    public void suppressShouldEmitWhenOverByteCapacity() {
        final Harness<String, Long> harness =
            new Harness<>(untilTimeLimit(Duration.ofDays(100), maxBytes(60L)), String(), Long());
        final MockInternalProcessorContext context = harness.context;
        final KTableSuppressProcessor<String, Long> processor = harness.processor;

        final long timestamp = 100L;
        context.setRecordMetadata("", 0, 0L, null, timestamp);
        final String key = "hey";
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        processor.process(key, value);

        context.setRecordMetadata("", 0, 1L, null, timestamp + 1);
        processor.process("dummyKey", value);

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.keyValue(), is(new KeyValue<>(key, value)));
        assertThat(capturedForward.timestamp(), is(timestamp));
    }

    @Test
    public void suppressShouldShutDownWhenOverRecordCapacity() {
        final Harness<String, Long> harness =
            new Harness<>(untilTimeLimit(Duration.ofDays(100), maxRecords(1).shutDownWhenFull()), String(), Long());
        final MockInternalProcessorContext context = harness.context;
        final KTableSuppressProcessor<String, Long> processor = harness.processor;

        final long timestamp = 100L;
        context.setRecordMetadata("", 0, 0L, null, timestamp);
        context.setCurrentNode(new ProcessorNode("testNode"));
        final String key = "hey";
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        processor.process(key, value);

        context.setRecordMetadata("", 0, 1L, null, timestamp);
        try {
            processor.process("dummyKey", value);
            fail("expected an exception");
        } catch (final StreamsException e) {
            assertThat(e.getMessage(), containsString("buffer exceeded its max capacity"));
        }
    }

    @Test
    public void suppressShouldShutDownWhenOverByteCapacity() {
        final Harness<String, Long> harness =
            new Harness<>(untilTimeLimit(Duration.ofDays(100), maxBytes(60L).shutDownWhenFull()), String(), Long());
        final MockInternalProcessorContext context = harness.context;
        final KTableSuppressProcessor<String, Long> processor = harness.processor;

        final long timestamp = 100L;
        context.setRecordMetadata("", 0, 0L, null, timestamp);
        context.setCurrentNode(new ProcessorNode("testNode"));
        final String key = "hey";
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        processor.process(key, value);

        context.setRecordMetadata("", 0, 1L, null, timestamp);
        try {
            processor.process("dummyKey", value);
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