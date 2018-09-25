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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.FullChangeSerde;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.MockProcessorContext;
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
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.maxRecords;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.Suppressed.untilTimeLimit;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;
import static org.apache.kafka.streams.kstream.WindowedSerdes.sessionWindowedSerdeFrom;
import static org.apache.kafka.streams.kstream.WindowedSerdes.timeWindowedSerdeFrom;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@SuppressWarnings("PointlessArithmeticExpression")
public class KTableSuppressProcessorTest {
    private static final long ARBITRARY_LONG = 5L;

    private static final long ARBITRARY_TIMESTAMP = 1993L;

    private static final Change<Long> ARBITRARY_CHANGE = new Change<>(7L, 14L);

    private static final TimeWindow ARBITRARY_WINDOW = new TimeWindow(0L, 100L);

    @Test
    public void zeroTimeLimitShouldImmediatelyEmit() {
        final KTableSuppressProcessor<String, Long> processor =
            new KTableSuppressProcessor<>(getImpl(untilTimeLimit(ZERO, unbounded())), String(), new FullChangeSerde<>(Long()));

        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        processor.init(context);

        final long timestamp = ARBITRARY_LONG;
        context.setTimestamp(timestamp);
        context.setStreamTime(timestamp);
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
        final KTableSuppressProcessor<Windowed<String>, Long> processor =
            new KTableSuppressProcessor<>(
                getImpl(untilTimeLimit(ZERO, unbounded())),
                timeWindowedSerdeFrom(String.class),
                new FullChangeSerde<>(Long())
            );

        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        processor.init(context);

        final long timestamp = ARBITRARY_LONG;
        context.setTimestamp(timestamp);
        context.setStreamTime(timestamp);
        final Windowed<String> key = new Windowed<>("hey", ARBITRARY_WINDOW);
        final Change<Long> value = ARBITRARY_CHANGE;
        processor.process(key, value);

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.keyValue(), is(new KeyValue<>(key, value)));
        assertThat(capturedForward.timestamp(), is(timestamp));
    }

    @Test(expected = KTableSuppressProcessor.NotImplementedException.class)
    public void intermediateSuppressionShouldBufferAndEmitLater() {
        final KTableSuppressProcessor<String, Long> processor =
            new KTableSuppressProcessor<>(
                getImpl(untilTimeLimit(ofMillis(1), unbounded())),
                String(),
                new FullChangeSerde<>(Long())
            );

        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        processor.init(context);

        final long timestamp = 0L;
        context.setRecordMetadata("topic", 0, 0, null, timestamp);
        final String key = "hey";
        final Change<Long> value = new Change<>(null, 1L);
        processor.process(key, value);
        assertThat(context.forwarded(), hasSize(0));

        assertThat(context.scheduledPunctuators(), hasSize(1));
        context.scheduledPunctuators().get(0).getPunctuator().punctuate(1);

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.keyValue(), is(new KeyValue<>(key, value)));
        assertThat(capturedForward.timestamp(), is(timestamp));
    }


    @SuppressWarnings("unchecked")
    private <K extends Windowed> SuppressedImpl<K> finalResults(final Duration grace) {
        return ((FinalResultsSuppressionBuilder) untilWindowCloses(unbounded())).buildFinalResultsSuppression(grace);
    }


    @Test(expected = KTableSuppressProcessor.NotImplementedException.class)
    public void finalResultsSuppressionShouldBufferAndEmitAtGraceExpiration() {
        final KTableSuppressProcessor<Windowed<String>, Long> processor = new KTableSuppressProcessor<>(
            finalResults(ofMillis(1L)),
            timeWindowedSerdeFrom(String.class),
            new FullChangeSerde<>(Long())
        );

        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        processor.init(context);

        final long timestamp = ARBITRARY_TIMESTAMP;
        context.setRecordMetadata("topic", 0, 0, null, timestamp);
        final Windowed<String> key = new Windowed<>("hey", ARBITRARY_WINDOW);
        final Change<Long> value = ARBITRARY_CHANGE;
        processor.process(key, value);
        assertThat(context.forwarded(), hasSize(0));

        assertThat(context.scheduledPunctuators(), hasSize(1));
        context.scheduledPunctuators().get(0).getPunctuator().punctuate(timestamp + 1L);

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.keyValue(), is(new KeyValue<>(key, value)));
        assertThat(capturedForward.timestamp(), is(timestamp));
    }

    /**
     * Testing a special case of final results: that even with a grace period of 0,
     * it will still buffer events and emit only after the end of the window.
     * As opposed to emitting immediately the way regular suppresion would with a time limit of 0.
     */
    @Test(expected = KTableSuppressProcessor.NotImplementedException.class)
    public void finalResultsWithZeroGraceShouldStillBufferUntilTheWindowEnd() {
        final KTableSuppressProcessor<Windowed<String>, Long> processor = new KTableSuppressProcessor<>(
            finalResults(ofMillis(0)),
            timeWindowedSerdeFrom(String.class),
            new FullChangeSerde<>(Long())
        );

        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        processor.init(context);

        final long timestamp = 5L;
        context.setRecordMetadata("", 0, 0L, null, timestamp);
        final long windowEnd = 100L;
        final Windowed<String> key = new Windowed<>("hey", new TimeWindow(0, windowEnd));
        final Change<Long> value = ARBITRARY_CHANGE;
        processor.process(key, value);
        assertThat(context.forwarded(), hasSize(0));

        assertThat(context.scheduledPunctuators(), hasSize(1));
        context.scheduledPunctuators().get(0).getPunctuator().punctuate(windowEnd);

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.keyValue(), is(new KeyValue<>(key, value)));
        assertThat(capturedForward.timestamp(), is(timestamp));
    }

    @Test
    public void finalResultsWithZeroGraceAtWindowEndShouldImmediatelyEmit() {
        final KTableSuppressProcessor<Windowed<String>, Long> processor = new KTableSuppressProcessor<>(
            finalResults(ofMillis(0)),
            timeWindowedSerdeFrom(String.class),
            new FullChangeSerde<>(Long())
        );

        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        processor.init(context);

        final long timestamp = 100L;
        context.setTimestamp(timestamp);
        context.setStreamTime(timestamp);
        final Windowed<String> key = new Windowed<>("hey", new TimeWindow(0, 100L));
        final Change<Long> value = ARBITRARY_CHANGE;
        processor.process(key, value);

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.keyValue(), is(new KeyValue<>(key, value)));
        assertThat(capturedForward.timestamp(), is(timestamp));
    }

    @Test
    public void finalResultsShouldSuppressTombstonesForTimeWindows() {
        final KTableSuppressProcessor<Windowed<String>, Long> processor = new KTableSuppressProcessor<>(
            finalResults(ofMillis(0)),
            timeWindowedSerdeFrom(String.class),
            new FullChangeSerde<>(Long())
        );

        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        processor.init(context);

        final long timestamp = 100L;
        context.setTimestamp(timestamp);
        context.setStreamTime(timestamp);
        final Windowed<String> key = new Windowed<>("hey", new TimeWindow(0, 100L));
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        processor.process(key, value);

        assertThat(context.forwarded(), hasSize(0));
    }

    @Test
    public void finalResultsShouldSuppressTombstonesForSessionWindows() {
        final KTableSuppressProcessor<Windowed<String>, Long> processor = new KTableSuppressProcessor<>(
            finalResults(ofMillis(0)),
            sessionWindowedSerdeFrom(String.class),
            new FullChangeSerde<>(Long())
        );

        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        processor.init(context);

        final long timestamp = 100L;
        context.setTimestamp(timestamp);
        context.setStreamTime(timestamp);
        final Windowed<String> key = new Windowed<>("hey", new SessionWindow(0L, 0L));
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        processor.process(key, value);

        assertThat(context.forwarded(), hasSize(0));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void suppressShouldNotSuppressTombstonesForTimeWindows() {
        final KTableSuppressProcessor<Windowed<String>, Long> processor = new KTableSuppressProcessor<Windowed<String>, Long>(
            (SuppressedImpl) untilTimeLimit(ofMillis(0), maxRecords(0)),
            timeWindowedSerdeFrom(String.class),
            new FullChangeSerde<>(Long())
        );

        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        processor.init(context);

        final long timestamp = 100L;
        context.setTimestamp(timestamp);
        context.setStreamTime(timestamp);
        final Windowed<String> key = new Windowed<>("hey", new TimeWindow(0L, 100L));
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        processor.process(key, value);

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.keyValue(), is(new KeyValue<>(key, value)));
        assertThat(capturedForward.timestamp(), is(timestamp));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void suppressShouldNotSuppressTombstonesForSessionWindows() {
        final KTableSuppressProcessor<Windowed<String>, Long> processor = new KTableSuppressProcessor<Windowed<String>, Long>(
            (SuppressedImpl) untilTimeLimit(ofMillis(0), maxRecords(0)),
            sessionWindowedSerdeFrom(String.class),
            new FullChangeSerde<>(Long())
        );

        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        processor.init(context);

        final long timestamp = 100L;
        context.setTimestamp(timestamp);
        context.setStreamTime(timestamp);
        final Windowed<String> key = new Windowed<>("hey", new SessionWindow(0L, 0L));
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        processor.process(key, value);

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.keyValue(), is(new KeyValue<>(key, value)));
        assertThat(capturedForward.timestamp(), is(timestamp));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void suppressShouldNotSuppressTombstonesForKTable() {
        final KTableSuppressProcessor<String, Long> processor = new KTableSuppressProcessor<String, Long>(
            (SuppressedImpl) untilTimeLimit(ofMillis(0), maxRecords(0)),
            Serdes.String(),
            new FullChangeSerde<>(Long())
        );

        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        processor.init(context);

        final long timestamp = 100L;
        context.setTimestamp(timestamp);
        context.setStreamTime(timestamp);
        final String key = "hey";
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        processor.process(key, value);

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.keyValue(), is(new KeyValue<>(key, value)));
        assertThat(capturedForward.timestamp(), is(timestamp));
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

    private static <K> SuppressedImpl<K> getImpl(final Suppressed<K> suppressed) {
        return (SuppressedImpl<K>) suppressed;
    }
}