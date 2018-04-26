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
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.test.KeyValueIteratorStub;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WindowKeySchemaTest {

    final private String key = "key";
    final private String topic = "topic";
    final private long startTime = 50L;
    final private long endTime = 100L;
    final private Serde<String> serde = Serdes.String();

    final private Window window = new TimeWindow(startTime, endTime);
    final private Windowed<String> windowedKey = new Windowed<>(key, window);
    final private WindowKeySchema windowKeySchema = new WindowKeySchema();
    final private Serde<Windowed<String>> keySerde = new WindowedSerdes.TimeWindowedSerde<>(serde);
    final private StateSerdes<String, byte[]> stateSerdes = new StateSerdes<>("dummy", serde, Serdes.ByteArray());

    @Before
    public void before() {
        windowKeySchema.init("topic");
    }

    @Test
    public void testHasNextConditionUsingNullKeys() {
        final List<KeyValue<Bytes, Integer>> keys = Arrays.asList(
                KeyValue.pair(WindowKeySchema.toStoreKeyBinary(new Windowed<>(Bytes.wrap(new byte[]{0, 0}), new TimeWindow(0, 1)), 0), 1),
                KeyValue.pair(WindowKeySchema.toStoreKeyBinary(new Windowed<>(Bytes.wrap(new byte[]{0}), new TimeWindow(0, 1)), 0), 2),
                KeyValue.pair(WindowKeySchema.toStoreKeyBinary(new Windowed<>(Bytes.wrap(new byte[]{0, 0, 0}), new TimeWindow(0, 1)), 0), 3),
                KeyValue.pair(WindowKeySchema.toStoreKeyBinary(new Windowed<>(Bytes.wrap(new byte[]{0}), new TimeWindow(10, 20)), 4), 4),
                KeyValue.pair(WindowKeySchema.toStoreKeyBinary(new Windowed<>(Bytes.wrap(new byte[]{0, 0}), new TimeWindow(10, 20)), 5), 5),
                KeyValue.pair(WindowKeySchema.toStoreKeyBinary(new Windowed<>(Bytes.wrap(new byte[]{0, 0, 0}), new TimeWindow(10, 20)), 6), 6));
        final DelegatingPeekingKeyValueIterator<Bytes, Integer> iterator = new DelegatingPeekingKeyValueIterator<>("foo", new KeyValueIteratorStub<>(keys.iterator()));

        final HasNextCondition hasNextCondition = windowKeySchema.hasNextCondition(null, null, 0, Long.MAX_VALUE);
        final List<Integer> results = new ArrayList<>();
        while (hasNextCondition.hasNext(iterator)) {
            results.add(iterator.next().value);
        }
        assertThat(results, equalTo(Arrays.asList(1, 2, 3, 4, 5, 6)));
    }

    @Test
    public void testUpperBoundWithLargeTimestamps() {
        Bytes upper = windowKeySchema.upperRange(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), Long.MAX_VALUE);

        assertThat(
                "shorter key with max timestamp should be in range",
                windowKeySchema.bytesComparator().compare(upper.get(),
                        WindowKeySchema.toStoreKeyBinary(
                                Bytes.wrap(new byte[]{0xA}),
                                Long.MAX_VALUE,
                                Integer.MAX_VALUE
                        ).get()
                ) >= 0
        );

        assertThat(
                "shorter key with max timestamp should be in range",
                windowKeySchema.bytesComparator().compare(upper.get(),
                        WindowKeySchema.toStoreKeyBinary(
                                Bytes.wrap(new byte[]{0xA, 0xB}),
                                Long.MAX_VALUE,
                                Integer.MAX_VALUE
                        ).get()
                ) >= 0
        );
    }

    @Test
    public void testUpperBoundWithKeyBytesLargerThanFirstTimestampByte() {
        Bytes upper = windowKeySchema.upperRange(Bytes.wrap(new byte[]{0xA, (byte) 0x8F, (byte) 0x9F}), Long.MAX_VALUE);

        assertThat(
                "shorter key with max timestamp should be in range",
                windowKeySchema.bytesComparator().compare(upper.get(),
                        WindowKeySchema.toStoreKeyBinary(
                                Bytes.wrap(new byte[]{0xA, (byte) 0x8F}),
                                Long.MAX_VALUE,
                                Integer.MAX_VALUE
                        ).get()
                ) >= 0
        );
    }

    @Test
    public void testUpperBoundWithKeyBytesLargerAndSmallerThanFirstTimestampByte() {
        Bytes upper = windowKeySchema.upperRange(Bytes.wrap(new byte[]{0xC, 0xC, 0x9}), 0x0AffffffffffffffL);

        assertThat(
                "shorter key with max timestamp should be in range",
                windowKeySchema.bytesComparator().compare(upper.get(),
                        WindowKeySchema.toStoreKeyBinary(
                                Bytes.wrap(new byte[]{0xA, 0xC}),
                                0x0AffffffffffffffL,
                                Integer.MAX_VALUE
                        ).get()
                ) >= 0
        );
    }

    @Test
    public void testUpperBound() {
        Bytes upper = windowKeySchema.upperRange(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), 0);
        assertThat(upper, equalTo(WindowKeySchema.toStoreKeyBinary(new byte[]{0xA, 0xB, 0xC}, 0, Integer.MAX_VALUE)));
    }

    @Test
    public void testLowerBound() {
        Bytes lower = windowKeySchema.lowerRange(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), 5);
        assertThat(lower, equalTo(WindowKeySchema.toStoreKeyBinary(new byte[]{0xA, 0xB, 0xC}, 5, 0)));
    }

    @Test
    public void shouldSerializeDeserialize() {
        final byte[] bytes = keySerde.serializer().serialize(topic, windowedKey);
        final Windowed<String> result = keySerde.deserializer().deserialize(topic, bytes);
        // TODO: fix this part as last bits of KAFKA-4468
        assertEquals(new Windowed<>(key, new TimeWindow(startTime, Long.MAX_VALUE)), result);
    }

    @Test
    public void testSerializeDeserializeOverflowWindowSize() {
        final byte[] bytes = keySerde.serializer().serialize(topic, windowedKey);
        final Windowed<String> result = new TimeWindowedDeserializer<>(serde.deserializer(), Long.MAX_VALUE - 1)
                .deserialize(topic, bytes);
        assertEquals(new Windowed<>(key, new TimeWindow(startTime, Long.MAX_VALUE)), result);
    }

    @Test
    public void shouldSerializeDeserializeExpectedWindowSize() {
        final byte[] bytes = keySerde.serializer().serialize(topic, windowedKey);
        final Windowed<String> result = new TimeWindowedDeserializer<>(serde.deserializer(), endTime - startTime)
                .deserialize(topic, bytes);
        assertEquals(windowedKey, result);
    }

    @Test
    public void shouldSerializeNullToNull() {
        assertNull(keySerde.serializer().serialize(topic, null));
    }

    @Test
    public void shouldDeSerializeEmtpyByteArrayToNull() {
        assertNull(keySerde.deserializer().deserialize(topic, new byte[0]));
    }

    @Test
    public void shouldDeSerializeNullToNull() {
        assertNull(keySerde.deserializer().deserialize(topic, null));
    }

    @Test
    public void shouldConvertToBinaryAndBack() {
        final Bytes serialized = WindowKeySchema.toStoreKeyBinary(windowedKey, 0, stateSerdes);
        final Windowed<String> result = WindowKeySchema.fromStoreKey(serialized.get(), endTime - startTime, stateSerdes);
        assertEquals(windowedKey, result);
    }

    @Test
    public void shouldExtractEndTimeFromBinary() {
        final Bytes serialized = WindowKeySchema.toStoreKeyBinary(windowedKey, 0, stateSerdes);
        assertEquals(0, WindowKeySchema.extractStoreSequence(serialized.get()));
    }

    @Test
    public void shouldExtractStartTimeFromBinary() {
        final Bytes serialized = WindowKeySchema.toStoreKeyBinary(windowedKey, 0, stateSerdes);
        assertEquals(startTime, WindowKeySchema.extractStoreTimestamp(serialized.get()));
    }

    @Test
    public void shouldExtractWindowFromBindary() {
        final Bytes serialized = WindowKeySchema.toStoreKeyBinary(windowedKey, 0, stateSerdes);
        assertEquals(window, WindowKeySchema.extractStoreWindow(serialized.get(), endTime - startTime));
    }

    @Test
    public void shouldExtractKeyBytesFromBinary() {
        final Bytes serialized = WindowKeySchema.toStoreKeyBinary(windowedKey, 0, stateSerdes);
        assertArrayEquals(key.getBytes(), WindowKeySchema.extractStoreKeyBytes(serialized.get()));
    }

    @Test
    public void shouldExtractKeyFromBinary() {
        final Bytes serialized = WindowKeySchema.toStoreKeyBinary(windowedKey, 0, stateSerdes);
        assertEquals(windowedKey, WindowKeySchema.fromStoreKey(serialized.get(), endTime - startTime, stateSerdes));
    }

    @Test
    public void shouldExtractBytesKeyFromBinary() {
        final Windowed<Bytes> windowedBytesKey = new Windowed<>(Bytes.wrap(key.getBytes()), window);
        final Bytes serialized = WindowKeySchema.toStoreKeyBinary(windowedBytesKey, 0);
        assertEquals(windowedBytesKey, WindowKeySchema.fromStoreKey(serialized.get(), endTime - startTime));
    }
}
