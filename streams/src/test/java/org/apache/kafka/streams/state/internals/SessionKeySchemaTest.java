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
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.test.KeyValueIteratorStub;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SessionKeySchemaTest {

    private final String key = "key";
    private final String topic = "topic";
    private final long startTime = 50L;
    private final long endTime = 100L;
    private final Serde<String> serde = Serdes.String();

    private final Window window = new SessionWindow(startTime, endTime);
    private final Windowed<String> windowedKey = new Windowed<>(key, window);
    private final Serde<Windowed<String>> keySerde = new WindowedSerdes.SessionWindowedSerde<>(serde);

    private final SessionKeySchema sessionKeySchema = new SessionKeySchema();
    private DelegatingPeekingKeyValueIterator<Bytes, Integer> iterator;

    @Before
    public void before() {
        final List<KeyValue<Bytes, Integer>> keys = Arrays.asList(KeyValue.pair(SessionKeySchema.toBinary(new Windowed<>(Bytes.wrap(new byte[]{0, 0}), new SessionWindow(0, 0))), 1),
                                                                  KeyValue.pair(SessionKeySchema.toBinary(new Windowed<>(Bytes.wrap(new byte[]{0}), new SessionWindow(0, 0))), 2),
                                                                  KeyValue.pair(SessionKeySchema.toBinary(new Windowed<>(Bytes.wrap(new byte[]{0, 0, 0}), new SessionWindow(0, 0))), 3),
                                                                  KeyValue.pair(SessionKeySchema.toBinary(new Windowed<>(Bytes.wrap(new byte[]{0}), new SessionWindow(10, 20))), 4),
                                                                  KeyValue.pair(SessionKeySchema.toBinary(new Windowed<>(Bytes.wrap(new byte[]{0, 0}), new SessionWindow(10, 20))), 5),
                                                                  KeyValue.pair(SessionKeySchema.toBinary(new Windowed<>(Bytes.wrap(new byte[]{0, 0, 0}), new SessionWindow(10, 20))), 6));
        iterator = new DelegatingPeekingKeyValueIterator<>("foo", new KeyValueIteratorStub<>(keys.iterator()));
    }

    @Test
    public void shouldFetchExactKeysSkippingLongerKeys() {
        final Bytes key = Bytes.wrap(new byte[]{0});
        final List<Integer> result = getValues(sessionKeySchema.hasNextCondition(key, key, 0, Long.MAX_VALUE));
        assertThat(result, equalTo(Arrays.asList(2, 4)));
    }

    @Test
    public void shouldFetchExactKeySkippingShorterKeys() {
        final Bytes key = Bytes.wrap(new byte[]{0, 0});
        final HasNextCondition hasNextCondition = sessionKeySchema.hasNextCondition(key, key, 0, Long.MAX_VALUE);
        final List<Integer> results = getValues(hasNextCondition);
        assertThat(results, equalTo(Arrays.asList(1, 5)));
    }

    @Test
    public void shouldFetchAllKeysUsingNullKeys() {
        final HasNextCondition hasNextCondition = sessionKeySchema.hasNextCondition(null, null, 0, Long.MAX_VALUE);
        final List<Integer> results = getValues(hasNextCondition);
        assertThat(results, equalTo(Arrays.asList(1, 2, 3, 4, 5, 6)));
    }
    
    @Test
    public void testUpperBoundWithLargeTimestamps() {
        final Bytes upper = sessionKeySchema.upperRange(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), Long.MAX_VALUE);

        assertThat(
            "shorter key with max timestamp should be in range",
            upper.compareTo(SessionKeySchema.toBinary(
                new Windowed<>(
                    Bytes.wrap(new byte[]{0xA}),
                    new SessionWindow(Long.MAX_VALUE, Long.MAX_VALUE))
            )) >= 0
        );

        assertThat(
            "shorter key with max timestamp should be in range",
            upper.compareTo(SessionKeySchema.toBinary(
                new Windowed<>(
                    Bytes.wrap(new byte[]{0xA, 0xB}),
                    new SessionWindow(Long.MAX_VALUE, Long.MAX_VALUE))

            )) >= 0
        );

        assertThat(upper, equalTo(SessionKeySchema.toBinary(
            new Windowed<>(Bytes.wrap(new byte[]{0xA}), new SessionWindow(Long.MAX_VALUE, Long.MAX_VALUE))))
        );
    }

    @Test
    public void testUpperBoundWithKeyBytesLargerThanFirstTimestampByte() {
        final Bytes upper = sessionKeySchema.upperRange(Bytes.wrap(new byte[]{0xA, (byte) 0x8F, (byte) 0x9F}), Long.MAX_VALUE);

        assertThat(
            "shorter key with max timestamp should be in range",
            upper.compareTo(SessionKeySchema.toBinary(
                new Windowed<>(
                    Bytes.wrap(new byte[]{0xA, (byte) 0x8F}),
                    new SessionWindow(Long.MAX_VALUE, Long.MAX_VALUE))
                )
            ) >= 0
        );

        assertThat(upper, equalTo(SessionKeySchema.toBinary(
            new Windowed<>(Bytes.wrap(new byte[]{0xA, (byte) 0x8F, (byte) 0x9F}), new SessionWindow(Long.MAX_VALUE, Long.MAX_VALUE))))
        );
    }

    @Test
    public void testUpperBoundWithZeroTimestamp() {
        final Bytes upper = sessionKeySchema.upperRange(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), 0);

        assertThat(upper, equalTo(SessionKeySchema.toBinary(
            new Windowed<>(Bytes.wrap(new byte[]{0xA}), new SessionWindow(0, Long.MAX_VALUE))))
        );
    }

    @Test
    public void testLowerBoundWithZeroTimestamp() {
        final Bytes lower = sessionKeySchema.lowerRange(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), 0);
        assertThat(lower, equalTo(SessionKeySchema.toBinary(new Windowed<>(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), new SessionWindow(0, 0)))));
    }

    @Test
    public void testLowerBoundMatchesTrailingZeros() {
        final Bytes lower = sessionKeySchema.lowerRange(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), Long.MAX_VALUE);

        assertThat(
            "appending zeros to key should still be in range",
            lower.compareTo(SessionKeySchema.toBinary(
                new Windowed<>(
                    Bytes.wrap(new byte[]{0xA, 0xB, 0xC, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
                    new SessionWindow(Long.MAX_VALUE, Long.MAX_VALUE))
            )) < 0
        );

        assertThat(lower, equalTo(SessionKeySchema.toBinary(new Windowed<>(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), new SessionWindow(0, 0)))));
    }

    @Test
    public void shouldSerializeDeserialize() {
        final byte[] bytes = keySerde.serializer().serialize(topic, windowedKey);
        final Windowed<String> result = keySerde.deserializer().deserialize(topic, bytes);
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
        final byte[] serialized = SessionKeySchema.toBinary(windowedKey, serde.serializer(), "dummy");
        final Windowed<String> result = SessionKeySchema.from(serialized, Serdes.String().deserializer(), "dummy");
        assertEquals(windowedKey, result);
    }

    @Test
    public void shouldExtractEndTimeFromBinary() {
        final byte[] serialized = SessionKeySchema.toBinary(windowedKey, serde.serializer(), "dummy");
        assertEquals(endTime, SessionKeySchema.extractEndTimestamp(serialized));
    }

    @Test
    public void shouldExtractStartTimeFromBinary() {
        final byte[] serialized = SessionKeySchema.toBinary(windowedKey, serde.serializer(), "dummy");
        assertEquals(startTime, SessionKeySchema.extractStartTimestamp(serialized));
    }

    @Test
    public void shouldExtractWindowFromBindary() {
        final byte[] serialized = SessionKeySchema.toBinary(windowedKey, serde.serializer(), "dummy");
        assertEquals(window, SessionKeySchema.extractWindow(serialized));
    }

    @Test
    public void shouldExtractKeyBytesFromBinary() {
        final byte[] serialized = SessionKeySchema.toBinary(windowedKey, serde.serializer(), "dummy");
        assertArrayEquals(key.getBytes(), SessionKeySchema.extractKeyBytes(serialized));
    }

    @Test
    public void shouldExtractKeyFromBinary() {
        final byte[] serialized = SessionKeySchema.toBinary(windowedKey, serde.serializer(), "dummy");
        assertEquals(windowedKey, SessionKeySchema.from(serialized, serde.deserializer(), "dummy"));
    }

    @Test
    public void shouldExtractBytesKeyFromBinary() {
        final Bytes bytesKey = Bytes.wrap(key.getBytes());
        final Windowed<Bytes> windowedBytesKey = new Windowed<>(bytesKey, window);
        final Bytes serialized = SessionKeySchema.toBinary(windowedBytesKey);
        assertEquals(windowedBytesKey, SessionKeySchema.from(serialized));
    }

    private List<Integer> getValues(final HasNextCondition hasNextCondition) {
        final List<Integer> results = new ArrayList<>();
        while (hasNextCondition.hasNext(iterator)) {
            results.add(iterator.next().value);
        }
        return results;
    }

}
