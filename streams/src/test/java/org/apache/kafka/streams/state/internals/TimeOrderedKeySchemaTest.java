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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.test.KeyValueIteratorStub;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TimeOrderedKeySchemaTest {
    final private String key = "key";
    final private long startTime = 50L;
    final private long endTime = 100L;
    final private Serde<String> serde = Serdes.String();

    final private Window window = new TimeWindow(startTime, endTime);
    final private Windowed<String> windowedKey = new Windowed<>(key, window);
    final private TimeOrderedKeySchema timeOrderedKeySchema = new TimeOrderedKeySchema();
    final private StateSerdes<String, byte[]> stateSerdes = new StateSerdes<>("dummy", serde, Serdes.ByteArray());

    @Test
    public void testHasNextConditionUsingNullKeys() {
        final List<KeyValue<Bytes, Integer>> keys = Arrays.asList(
            KeyValue.pair(TimeOrderedKeySchema.toStoreKeyBinary(new Windowed<>(Bytes.wrap(new byte[] {0, 0}), new TimeWindow(0, 1)), 0), 1),
            KeyValue.pair(TimeOrderedKeySchema.toStoreKeyBinary(new Windowed<>(Bytes.wrap(new byte[] {0}), new TimeWindow(0, 1)), 0), 2),
            KeyValue.pair(TimeOrderedKeySchema.toStoreKeyBinary(new Windowed<>(Bytes.wrap(new byte[] {0, 0, 0}), new TimeWindow(0, 1)), 0), 3),
            KeyValue.pair(TimeOrderedKeySchema.toStoreKeyBinary(new Windowed<>(Bytes.wrap(new byte[] {0}), new TimeWindow(10, 20)), 4), 4),
            KeyValue.pair(TimeOrderedKeySchema.toStoreKeyBinary(new Windowed<>(Bytes.wrap(new byte[] {0, 0}), new TimeWindow(10, 20)), 5), 5),
            KeyValue.pair(TimeOrderedKeySchema.toStoreKeyBinary(new Windowed<>(Bytes.wrap(new byte[] {0, 0, 0}), new TimeWindow(10, 20)), 6), 6));
        final DelegatingPeekingKeyValueIterator<Bytes, Integer> iterator = new DelegatingPeekingKeyValueIterator<>("foo", new KeyValueIteratorStub<>(keys.iterator()));

        final HasNextCondition hasNextCondition = timeOrderedKeySchema.hasNextCondition(null, null, 0, Long.MAX_VALUE);
        final List<Integer> results = new ArrayList<>();
        while (hasNextCondition.hasNext(iterator)) {
            results.add(iterator.next().value);
        }
        assertThat(results, equalTo(Arrays.asList(1, 2, 3, 4, 5, 6)));
    }

    @Test
    public void testUpperBoundWithLargeTimestamps() {
        final Bytes upper = timeOrderedKeySchema.upperRange(Bytes.wrap(new byte[] {0xA, 0xB, 0xC}), Long.MAX_VALUE);

        assertThat(
            "shorter key with max timestamp should be in range",
            upper.compareTo(
                TimeOrderedKeySchema.toStoreKeyBinary(
                    new byte[] {0xA},
                    Long.MAX_VALUE,
                    Integer.MAX_VALUE
                )
            ) >= 0
        );

        assertThat(
            "shorter key with max timestamp should be in range",
            upper.compareTo(
                TimeOrderedKeySchema.toStoreKeyBinary(
                    new byte[] {0xA, 0xB},
                    Long.MAX_VALUE,
                    Integer.MAX_VALUE
                )
            ) >= 0
        );

        assertThat(upper, equalTo(TimeOrderedKeySchema.toStoreKeyBinary(new byte[] {0xA, 0xB, 0xC}, Long.MAX_VALUE, Integer.MAX_VALUE)));
    }

    @Test
    public void testUpperBoundWithZeroTimestamp() {
        final Bytes upper = timeOrderedKeySchema.upperRange(Bytes.wrap(new byte[] {0xA, 0xB, 0xC}), 0);
        assertThat(upper, equalTo(TimeOrderedKeySchema.toStoreKeyBinary(new byte[] {0xA, 0xB, 0xC}, 0, Integer.MAX_VALUE)));
    }

    @Test
    public void testLowerBoundWithZeroTimestamp() {
        final Bytes lower = timeOrderedKeySchema.lowerRange(Bytes.wrap(new byte[] {0xA, 0xB, 0xC}), 0);
        assertThat(lower, equalTo(TimeOrderedKeySchema.toStoreKeyBinary(new byte[] {0xA, 0xB, 0xC}, 0, 0)));
    }

    @Test
    public void testLowerBoundWithNonZeroTimestamp() {
        final Bytes lower = timeOrderedKeySchema.lowerRange(Bytes.wrap(new byte[] {0xA, 0xB, 0xC}), 42);
        assertThat(lower, equalTo(TimeOrderedKeySchema.toStoreKeyBinary(new byte[] {0xA, 0xB, 0xC}, 42, 0)));
    }

    @Test
    public void testLowerBoundMatchesTrailingZeros() {
        final Bytes lower = timeOrderedKeySchema.lowerRange(Bytes.wrap(new byte[] {0xA, 0xB, 0xC}), Long.MAX_VALUE - 1);

        assertThat(
            "appending zeros to key should still be in range",
            lower.compareTo(
                TimeOrderedKeySchema.toStoreKeyBinary(
                    new byte[] {0xA, 0xB, 0xC, 0, 0, 0, 0, 0, 0, 0, 0},
                    Long.MAX_VALUE - 1,
                    0
                )
            ) < 0
        );

        assertThat(lower, equalTo(TimeOrderedKeySchema.toStoreKeyBinary(new byte[] {0xA, 0xB, 0xC}, Long.MAX_VALUE - 1, 0)));
    }

    @Test
    public void shouldConvertToBinaryAndBack() {
        final Bytes serialized = TimeOrderedKeySchema.toStoreKeyBinary(windowedKey, 0, stateSerdes);
        final Windowed<String> result = TimeOrderedKeySchema.fromStoreKey(serialized.get(), endTime - startTime, stateSerdes.keyDeserializer(), stateSerdes.topic());
        assertEquals(windowedKey, result);
    }

    @Test
    public void shouldExtractSequenceFromBinary() {
        final Bytes serialized = TimeOrderedKeySchema.toStoreKeyBinary(windowedKey, 0, stateSerdes);
        assertEquals(0, TimeOrderedKeySchema.extractStoreSequence(serialized.get()));
    }

    @Test
    public void shouldExtractStartTimeFromBinary() {
        final Bytes serialized = TimeOrderedKeySchema.toStoreKeyBinary(windowedKey, 0, stateSerdes);
        assertEquals(startTime, TimeOrderedKeySchema.extractStoreTimestamp(serialized.get()));
    }

    @Test
    public void shouldExtractWindowFromBinary() {
        final Bytes serialized = TimeOrderedKeySchema.toStoreKeyBinary(windowedKey, 0, stateSerdes);
        assertEquals(window, TimeOrderedKeySchema.extractStoreWindow(serialized.get(), endTime - startTime));
    }

    @Test
    public void shouldExtractKeyBytesFromBinary() {
        final Bytes serialized = TimeOrderedKeySchema.toStoreKeyBinary(windowedKey, 0, stateSerdes);
        assertArrayEquals(key.getBytes(), TimeOrderedKeySchema.extractStoreKeyBytes(serialized.get()));
    }

    @Test
    public void shouldExtractKeyFromBinary() {
        final Bytes serialized = TimeOrderedKeySchema.toStoreKeyBinary(windowedKey, 0, stateSerdes);
        assertEquals(windowedKey, TimeOrderedKeySchema.fromStoreKey(serialized.get(), endTime - startTime, stateSerdes.keyDeserializer(), stateSerdes.topic()));
    }

    @Test
    public void shouldExtractBytesKeyFromBinary() {
        final Windowed<Bytes> windowedBytesKey = new Windowed<>(Bytes.wrap(key.getBytes()), window);
        final Bytes serialized = TimeOrderedKeySchema.toStoreKeyBinary(windowedBytesKey, 0);
        assertEquals(windowedBytesKey, TimeOrderedKeySchema.fromStoreBytesKey(serialized.get(), endTime - startTime));
    }
}
