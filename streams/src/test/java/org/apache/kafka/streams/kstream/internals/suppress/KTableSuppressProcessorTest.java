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

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.Change;
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
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.Suppressed.untilTimeLimit;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

@SuppressWarnings("PointlessArithmeticExpression")
public class KTableSuppressProcessorTest {
    /**
     * Use this value to indicate that the test correctness does not depend on any particular number
     */
    private static final long ARBITRARY_LONG = 5L;

    /**
     * Use this value to indicate that the test correctness does not depend on any particular window
     */
    private static final TimeWindow ARBITRARY_WINDOW = new TimeWindow(0L, 100L);

    @Test
    public void zeroTimeLimitShouldImmediatelyEmit() {
        final KTableSuppressProcessor<String, Long> processor =
            new KTableSuppressProcessor<>(getImpl(untilTimeLimit(ZERO, unbounded())));

        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        processor.init(context);

        final long timestamp = ARBITRARY_LONG;
        context.setTimestamp(timestamp);
        context.setStreamTime(timestamp);
        final String key = "hey";
        final Change<Long> value = new Change<>(ARBITRARY_LONG, ARBITRARY_LONG);
        processor.process(key, value);

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.keyValue(), is(new KeyValue<>(key, value)));
        assertThat(capturedForward.timestamp(), is(timestamp));
    }

    @Test
    public void windowedZeroTimeLimitShouldImmediatelyEmit() {
        final KTableSuppressProcessor<Windowed<String>, Long> processor =
            new KTableSuppressProcessor<>(getImpl(untilTimeLimit(ZERO, unbounded())));

        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        processor.init(context);

        final long timestamp = ARBITRARY_LONG;
        context.setTimestamp(timestamp);
        context.setStreamTime(timestamp);
        final Windowed<String> key = new Windowed<>("hey", ARBITRARY_WINDOW);
        final Change<Long> value = new Change<>(ARBITRARY_LONG, ARBITRARY_LONG);
        processor.process(key, value);

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.keyValue(), is(new KeyValue<>(key, value)));
        assertThat(capturedForward.timestamp(), is(timestamp));
    }

    @Test
    public void intermediateSuppressionShouldThrow() {
        final KTableSuppressProcessor<String, Long> processor =
            new KTableSuppressProcessor<>(getImpl(untilTimeLimit(Duration.ofMillis(1), unbounded())));

        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        processor.init(context);

        try {
            processor.process("hey", new Change<>(null, 1L));
            fail("expected an exception for now");
        } catch (final KTableSuppressProcessor.NotImplementedException e) {
            // expected
        }
        assertThat(context.forwarded(), hasSize(0));
    }


    @SuppressWarnings("unchecked")
    private <K extends Windowed> SuppressedImpl<K> finalResults(final Duration grace) {
        return ((FinalResultsSuppressionBuilder) untilWindowCloses(unbounded())).buildFinalResultsSuppression(grace);
    }


    @Test
    public void finalResultsSuppressionShouldThrow() {
        final KTableSuppressProcessor<Windowed<String>, Long> processor =
            new KTableSuppressProcessor<>(finalResults(ofMillis(1)));

        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        processor.init(context);

        context.setTimestamp(ARBITRARY_LONG);
        try {
            processor.process(new Windowed<>("hey", ARBITRARY_WINDOW), new Change<>(ARBITRARY_LONG, ARBITRARY_LONG));
            fail("expected an exception for now");
        } catch (final KTableSuppressProcessor.NotImplementedException e) {
            // expected
        }
        assertThat(context.forwarded(), hasSize(0));
    }

    @Test
    public void finalResultsWith0GraceBeforeWindowEndShouldThrow() {
        final KTableSuppressProcessor<Windowed<String>, Long> processor =
            new KTableSuppressProcessor<>(finalResults(ofMillis(0)));

        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        processor.init(context);

        final long timestamp = 5L;
        context.setTimestamp(timestamp);
        final Windowed<String> key = new Windowed<>("hey", new TimeWindow(0, 100L));
        final Change<Long> value = new Change<>(ARBITRARY_LONG, ARBITRARY_LONG);
        try {
            processor.process(key, value);
            fail("expected an exception");
        } catch (final KTableSuppressProcessor.NotImplementedException e) {
            // expected
        }
        assertThat(context.forwarded(), hasSize(0));
    }

    @Test
    public void finalResultsWith0GraceAtWindowEndShouldImmediatelyEmit() {
        final KTableSuppressProcessor<Windowed<String>, Long> processor =
            new KTableSuppressProcessor<>(finalResults(ofMillis(0)));

        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        processor.init(context);

        final long timestamp = 100L;
        context.setTimestamp(timestamp);
        context.setStreamTime(timestamp);
        final Windowed<String> key = new Windowed<>("hey", new TimeWindow(0, 100L));
        final Change<Long> value = new Change<>(ARBITRARY_LONG, ARBITRARY_LONG);
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