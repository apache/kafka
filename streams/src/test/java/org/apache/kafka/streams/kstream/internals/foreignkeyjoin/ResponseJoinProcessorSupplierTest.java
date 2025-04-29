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

package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.internals.KTableValueGetter;
import org.apache.kafka.streams.kstream.internals.KTableValueGetterSupplier;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.Murmur3;
import org.apache.kafka.test.MockInternalProcessorContext;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class ResponseJoinProcessorSupplierTest {
    private static final StringSerializer STRING_SERIALIZER = new StringSerializer();
    private static final ValueJoiner<String, String, String> JOINER =
        (value1, value2) -> "(" + value1 + "," + value2 + ")";

    private static class TestKTableValueGetterSupplier<K, V> implements KTableValueGetterSupplier<K, V> {
        private final Map<K, V> map = new HashMap<>();

        @Override
        public KTableValueGetter<K, V> get() {
            return new KTableValueGetter<K, V>() {
                @Override
                public void init(final ProcessorContext<?, ?> context) {
                }

                @Override
                public ValueAndTimestamp<V> get(final K key) {
                    return ValueAndTimestamp.make(map.get(key), -1);
                }

                @Override
                public boolean isVersioned() {
                    return false;
                }
            };
        }

        @Override
        public String[] storeNames() {
            return new String[0];
        }

        void put(final K key, final V value) {
            map.put(key, value);
        }
    }

    @Test
    public void shouldNotForwardWhenHashDoesNotMatch() {
        final TestKTableValueGetterSupplier<String, String> valueGetterSupplier =
            new TestKTableValueGetterSupplier<>();
        final boolean leftJoin = false;
        final ResponseJoinProcessorSupplier<String, String, String, String> processorSupplier =
            new ResponseJoinProcessorSupplier<>(
                valueGetterSupplier,
                STRING_SERIALIZER,
                () -> "value-hash-dummy-topic",
                JOINER,
                leftJoin
            );
        final Processor<String, SubscriptionResponseWrapper<String>, String, String> processor = processorSupplier.get();
        final MockInternalProcessorContext<String, String> context = new MockInternalProcessorContext<>();
        processor.init(context);
        context.setRecordMetadata("topic", 0, 0);

        valueGetterSupplier.put("lhs1", "lhsValue");
        final long[] oldHash = Murmur3.hash128(STRING_SERIALIZER.serialize("topic-join-resolver", "oldLhsValue"));
        processor.process(new Record<>("lhs1", new SubscriptionResponseWrapper<>(oldHash, "rhsValue", 0), 0));
        final List<MockProcessorContext.CapturedForward<? extends String, ? extends String>> forwarded = context.forwarded();
        assertThat(forwarded, empty());

        // test dropped-records sensors
        assertEquals(1.0, getDroppedRecordsTotalMetric(context));
        assertNotEquals(0.0, getDroppedRecordsRateMetric(context));
    }

    @Test
    public void shouldIgnoreUpdateWhenLeftHasBecomeNull() {
        final TestKTableValueGetterSupplier<String, String> valueGetterSupplier =
            new TestKTableValueGetterSupplier<>();
        final boolean leftJoin = false;
        final ResponseJoinProcessorSupplier<String, String, String, String> processorSupplier =
            new ResponseJoinProcessorSupplier<>(
                valueGetterSupplier,
                STRING_SERIALIZER,
                () -> "value-hash-dummy-topic",
                JOINER,
                leftJoin
            );
        final Processor<String, SubscriptionResponseWrapper<String>, String, String> processor = processorSupplier.get();
        final MockInternalProcessorContext<String, String> context = new MockInternalProcessorContext<>();
        processor.init(context);
        context.setRecordMetadata("topic", 0, 0);

        valueGetterSupplier.put("lhs1", null);
        final long[] hash = Murmur3.hash128(STRING_SERIALIZER.serialize("topic-join-resolver", "lhsValue"));
        processor.process(new Record<>("lhs1", new SubscriptionResponseWrapper<>(hash, "rhsValue", 0), 0));
        final List<MockProcessorContext.CapturedForward<? extends String, ? extends String>> forwarded = context.forwarded();
        assertThat(forwarded, empty());

        // test dropped-records sensors
        assertEquals(1.0, getDroppedRecordsTotalMetric(context));
        assertNotEquals(0.0, getDroppedRecordsRateMetric(context));
    }

    @Test
    public void shouldForwardWhenHashMatches() {
        final TestKTableValueGetterSupplier<String, String> valueGetterSupplier =
            new TestKTableValueGetterSupplier<>();
        final boolean leftJoin = false;
        final ResponseJoinProcessorSupplier<String, String, String, String> processorSupplier =
            new ResponseJoinProcessorSupplier<>(
                valueGetterSupplier,
                STRING_SERIALIZER,
                () -> "value-hash-dummy-topic",
                JOINER,
                leftJoin
            );
        final Processor<String, SubscriptionResponseWrapper<String>, String, String> processor = processorSupplier.get();
        final MockInternalProcessorContext<String, String> context = new MockInternalProcessorContext<>();
        processor.init(context);
        context.setRecordMetadata("topic", 0, 0);

        valueGetterSupplier.put("lhs1", "lhsValue");
        final long[] hash = Murmur3.hash128(STRING_SERIALIZER.serialize("topic-join-resolver", "lhsValue"));
        processor.process(new Record<>("lhs1", new SubscriptionResponseWrapper<>(hash, "rhsValue", 0), 0));
        final List<MockProcessorContext.CapturedForward<? extends String, ? extends String>> forwarded = context.forwarded();
        assertThat(forwarded.size(), is(1));
        assertThat(forwarded.get(0).record(), is(new Record<>("lhs1", "(lhsValue,rhsValue)", 0)));
    }

    @Test
    public void shouldEmitTombstoneForInnerJoinWhenRightIsNull() {
        final TestKTableValueGetterSupplier<String, String> valueGetterSupplier =
            new TestKTableValueGetterSupplier<>();
        final boolean leftJoin = false;
        final ResponseJoinProcessorSupplier<String, String, String, String> processorSupplier =
            new ResponseJoinProcessorSupplier<>(
                valueGetterSupplier,
                STRING_SERIALIZER,
                () -> "value-hash-dummy-topic",
                JOINER,
                leftJoin
            );
        final Processor<String, SubscriptionResponseWrapper<String>, String, String> processor = processorSupplier.get();
        final MockInternalProcessorContext<String, String> context = new MockInternalProcessorContext<>();
        processor.init(context);
        context.setRecordMetadata("topic", 0, 0);

        valueGetterSupplier.put("lhs1", "lhsValue");
        final long[] hash = Murmur3.hash128(STRING_SERIALIZER.serialize("topic-join-resolver", "lhsValue"));
        processor.process(new Record<>("lhs1", new SubscriptionResponseWrapper<>(hash, null, 0), 0));
        final List<MockProcessorContext.CapturedForward<? extends String, ? extends String>> forwarded = context.forwarded();
        assertThat(forwarded.size(), is(1));
        assertThat(forwarded.get(0).record(), is(new Record<>("lhs1", null, 0)));
    }

    @Test
    public void shouldEmitResultForLeftJoinWhenRightIsNull() {
        final TestKTableValueGetterSupplier<String, String> valueGetterSupplier =
            new TestKTableValueGetterSupplier<>();
        final boolean leftJoin = true;
        final ResponseJoinProcessorSupplier<String, String, String, String> processorSupplier =
            new ResponseJoinProcessorSupplier<>(
                valueGetterSupplier,
                STRING_SERIALIZER,
                () -> "value-hash-dummy-topic",
                JOINER,
                leftJoin
            );
        final Processor<String, SubscriptionResponseWrapper<String>, String, String> processor = processorSupplier.get();
        final MockInternalProcessorContext<String, String> context = new MockInternalProcessorContext<>();
        processor.init(context);
        context.setRecordMetadata("topic", 0, 0);

        valueGetterSupplier.put("lhs1", "lhsValue");
        final long[] hash = Murmur3.hash128(STRING_SERIALIZER.serialize("topic-join-resolver", "lhsValue"));
        processor.process(new Record<>("lhs1", new SubscriptionResponseWrapper<>(hash, null, 0), 0));
        final List<MockProcessorContext.CapturedForward<? extends String, ? extends String>> forwarded = context.forwarded();
        assertThat(forwarded.size(), is(1));
        assertThat(forwarded.get(0).record(), is(new Record<>("lhs1", "(lhsValue,null)", 0)));
    }

    @Test
    public void shouldEmitTombstoneForLeftJoinWhenRightIsNullAndLeftIsNull() {
        final TestKTableValueGetterSupplier<String, String> valueGetterSupplier =
            new TestKTableValueGetterSupplier<>();
        final boolean leftJoin = true;
        final ResponseJoinProcessorSupplier<String, String, String, String> processorSupplier =
            new ResponseJoinProcessorSupplier<>(
                valueGetterSupplier,
                STRING_SERIALIZER,
                () -> "value-hash-dummy-topic",
                JOINER,
                leftJoin
            );
        final Processor<String, SubscriptionResponseWrapper<String>, String, String> processor = processorSupplier.get();
        final MockInternalProcessorContext<String, String> context = new MockInternalProcessorContext<>();
        processor.init(context);
        context.setRecordMetadata("topic", 0, 0);

        valueGetterSupplier.put("lhs1", null);
        final long[] hash = null;
        processor.process(new Record<>("lhs1", new SubscriptionResponseWrapper<>(hash, null, 0), 0));
        final List<MockProcessorContext.CapturedForward<? extends String, ? extends String>> forwarded = context.forwarded();
        assertThat(forwarded.size(), is(1));
        assertThat(forwarded.get(0).record(), is(new Record<>("lhs1", null, 0)));
    }

    static Object getDroppedRecordsTotalMetric(final InternalProcessorContext<String, ?> context) {
        final MetricName dropTotalMetric = new MetricName(
            "dropped-records-total",
            "stream-task-metrics",
            "The total number of dropped records",
            mkMap(
                mkEntry("thread-id", Thread.currentThread().getName()),
                mkEntry("task-id", "0_0")
            )
        );

        return context.metrics().metrics().get(dropTotalMetric).metricValue();
    }

    static Object getDroppedRecordsRateMetric(final InternalProcessorContext<String, ?> context) {
        final MetricName dropRateMetric = new MetricName(
            "dropped-records-rate",
            "stream-task-metrics",
            "The average number of dropped records per second",
            mkMap(
                mkEntry("thread-id", Thread.currentThread().getName()),
                mkEntry("task-id", "0_0")
            )
        );

        return context.metrics().metrics().get(dropRateMetric).metricValue();
    }

}
