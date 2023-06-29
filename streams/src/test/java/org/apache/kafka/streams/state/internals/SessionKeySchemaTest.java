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

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.state.internals.PrefixedSessionKeySchemas.KeyFirstSessionKeySchema;
import org.apache.kafka.streams.state.internals.PrefixedSessionKeySchemas.TimeFirstSessionKeySchema;
import org.apache.kafka.streams.state.internals.SegmentedBytesStore.KeySchema;
import org.apache.kafka.test.KeyValueIteratorStub;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(Parameterized.class)
public class SessionKeySchemaTest {
    private static final Map<SchemaType, KeySchema> SCHEMA_TYPE_MAP = mkMap(
        mkEntry(SchemaType.SessionKeySchema, new SessionKeySchema()),
        mkEntry(SchemaType.PrefixedKeyFirstSchema, new KeyFirstSessionKeySchema()),
        mkEntry(SchemaType.PrefixedTimeFirstSchema, new TimeFirstSessionKeySchema())
    );

    private static final Map<SchemaType, Function<Windowed<Bytes>, Bytes>> WINDOW_TO_STORE_BINARY_MAP = mkMap(
        mkEntry(SchemaType.SessionKeySchema, SessionKeySchema::toBinary),
        mkEntry(SchemaType.PrefixedKeyFirstSchema, KeyFirstSessionKeySchema::toBinary),
        mkEntry(SchemaType.PrefixedTimeFirstSchema, TimeFirstSessionKeySchema::toBinary)
    );

    private static final Map<SchemaType, Function<byte[], Long>> EXTRACT_END_TS_MAP = mkMap(
        mkEntry(SchemaType.SessionKeySchema, SessionKeySchema::extractEndTimestamp),
        mkEntry(SchemaType.PrefixedKeyFirstSchema, KeyFirstSessionKeySchema::extractEndTimestamp),
        mkEntry(SchemaType.PrefixedTimeFirstSchema, TimeFirstSessionKeySchema::extractEndTimestamp)
    );

    private static final Map<SchemaType, Function<byte[], Long>> EXTRACT_START_TS_MAP = mkMap(
        mkEntry(SchemaType.SessionKeySchema, SessionKeySchema::extractStartTimestamp),
        mkEntry(SchemaType.PrefixedKeyFirstSchema, KeyFirstSessionKeySchema::extractStartTimestamp),
        mkEntry(SchemaType.PrefixedTimeFirstSchema, TimeFirstSessionKeySchema::extractStartTimestamp)
    );

    @FunctionalInterface
    interface TriFunction<A, B, C, R> {
        R apply(A a, B b, C c);
    }

    private static final Map<SchemaType, TriFunction<Windowed<String>, Serializer<String>, String, byte[]>> SERDE_TO_STORE_BINARY_MAP = mkMap(
        mkEntry(SchemaType.SessionKeySchema, SessionKeySchema::toBinary),
        mkEntry(SchemaType.PrefixedKeyFirstSchema, KeyFirstSessionKeySchema::toBinary),
        mkEntry(SchemaType.PrefixedTimeFirstSchema, TimeFirstSessionKeySchema::toBinary)
    );

    private static final Map<SchemaType, TriFunction<byte[], Deserializer<String>, String, Windowed<String>>> SERDE_FROM_BYTES_MAP = mkMap(
        mkEntry(SchemaType.SessionKeySchema, SessionKeySchema::from),
        mkEntry(SchemaType.PrefixedKeyFirstSchema, KeyFirstSessionKeySchema::from),
        mkEntry(SchemaType.PrefixedTimeFirstSchema, TimeFirstSessionKeySchema::from)
    );

    private static final Map<SchemaType, Function<Bytes, Windowed<Bytes>>> FROM_BYTES_MAP = mkMap(
        mkEntry(SchemaType.SessionKeySchema, SessionKeySchema::from),
        mkEntry(SchemaType.PrefixedKeyFirstSchema, KeyFirstSessionKeySchema::from),
        mkEntry(SchemaType.PrefixedTimeFirstSchema, TimeFirstSessionKeySchema::from)
    );

    private static final Map<SchemaType, Function<byte[], Window>> EXTRACT_WINDOW = mkMap(
        mkEntry(SchemaType.SessionKeySchema, SessionKeySchema::extractWindow),
        mkEntry(SchemaType.PrefixedKeyFirstSchema, KeyFirstSessionKeySchema::extractWindow),
        mkEntry(SchemaType.PrefixedTimeFirstSchema, TimeFirstSessionKeySchema::extractWindow)
    );

    private static final Map<SchemaType, Function<byte[], byte[]>> EXTRACT_KEY_BYTES = mkMap(
        mkEntry(SchemaType.SessionKeySchema, SessionKeySchema::extractKeyBytes),
        mkEntry(SchemaType.PrefixedKeyFirstSchema, KeyFirstSessionKeySchema::extractKeyBytes),
        mkEntry(SchemaType.PrefixedTimeFirstSchema, TimeFirstSessionKeySchema::extractKeyBytes)
    );

    private final String key = "key";
    private final String topic = "topic";
    private final long startTime = 50L;
    private final long endTime = 100L;
    private final Serde<String> serde = Serdes.String();

    private final Window window = new SessionWindow(startTime, endTime);
    private final Windowed<String> windowedKey = new Windowed<>(key, window);
    private final Serde<Windowed<String>> keySerde = new WindowedSerdes.SessionWindowedSerde<>(serde);

    private final KeySchema keySchema;
    private DelegatingPeekingKeyValueIterator<Bytes, Integer> iterator;
    private final SchemaType schemaType;
    private final Function<Windowed<Bytes>, Bytes> toBinary;
    private final TriFunction<Windowed<String>, Serializer<String>, String, byte[]> serdeToBinary;
    private final TriFunction<byte[], Deserializer<String>, String, Windowed<String>> serdeFromBytes;
    private final Function<Bytes, Windowed<Bytes>> fromBytes;
    private final Function<byte[], Long> extractStartTS;
    private final Function<byte[], Long> extractEndTS;
    private final Function<byte[], byte[]> extractKeyBytes;
    private final Function<byte[], Window> extractWindow;

    private enum SchemaType {
        SessionKeySchema,
        PrefixedTimeFirstSchema,
        PrefixedKeyFirstSchema
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return asList(new Object[][] {
            {SchemaType.SessionKeySchema},
            {SchemaType.PrefixedTimeFirstSchema},
            {SchemaType.PrefixedKeyFirstSchema}
        });
    }

    public SessionKeySchemaTest(final SchemaType type) {
        schemaType = type;
        keySchema = SCHEMA_TYPE_MAP.get(type);
        toBinary = WINDOW_TO_STORE_BINARY_MAP.get(schemaType);
        serdeToBinary = SERDE_TO_STORE_BINARY_MAP.get(schemaType);
        serdeFromBytes = SERDE_FROM_BYTES_MAP.get(schemaType);
        fromBytes = FROM_BYTES_MAP.get(schemaType);
        extractStartTS = EXTRACT_START_TS_MAP.get(schemaType);
        extractEndTS = EXTRACT_END_TS_MAP.get(schemaType);
        extractKeyBytes = EXTRACT_KEY_BYTES.get(schemaType);
        extractWindow = EXTRACT_WINDOW.get(schemaType);
    }

    @After
    public void after() {
        if (iterator != null) {
            iterator.close();
        }
    }

    @Before
    public void before() {
        final List<KeyValue<Bytes, Integer>> keys = asList(KeyValue.pair(toBinary.apply(new Windowed<>(Bytes.wrap(new byte[]{0, 0}), new SessionWindow(0, 0))), 1),
                                                                  KeyValue.pair(toBinary.apply(new Windowed<>(Bytes.wrap(new byte[]{0}), new SessionWindow(0, 0))), 2),
                                                                  KeyValue.pair(toBinary.apply(new Windowed<>(Bytes.wrap(new byte[]{0, 0, 0}), new SessionWindow(0, 0))), 3),
                                                                  KeyValue.pair(toBinary.apply(new Windowed<>(Bytes.wrap(new byte[]{0}), new SessionWindow(10, 20))), 4),
                                                                  KeyValue.pair(toBinary.apply(new Windowed<>(Bytes.wrap(new byte[]{0, 0}), new SessionWindow(10, 20))), 5),
                                                                  KeyValue.pair(toBinary.apply(new Windowed<>(Bytes.wrap(new byte[]{0, 0, 0}), new SessionWindow(10, 20))), 6));
        iterator = new DelegatingPeekingKeyValueIterator<>("foo", new KeyValueIteratorStub<>(keys.iterator()));
    }

    @Test
    public void shouldFetchExactKeysSkippingLongerKeys() {
        final Bytes key = Bytes.wrap(new byte[]{0});
        final List<Integer> result = getValues(keySchema.hasNextCondition(key, key, 0, Long.MAX_VALUE, true));
        assertThat(result, equalTo(asList(2, 4)));
    }

    @Test
    public void shouldFetchExactKeySkippingShorterKeys() {
        final Bytes key = Bytes.wrap(new byte[]{0, 0});
        final HasNextCondition hasNextCondition = keySchema.hasNextCondition(key, key, 0, Long.MAX_VALUE, true);
        final List<Integer> results = getValues(hasNextCondition);
        assertThat(results, equalTo(asList(1, 5)));
    }

    @Test
    public void shouldFetchAllKeysUsingNullKeys() {
        final HasNextCondition hasNextCondition = keySchema.hasNextCondition(null, null, 0, Long.MAX_VALUE, true);
        final List<Integer> results = getValues(hasNextCondition);
        assertThat(results, equalTo(asList(1, 2, 3, 4, 5, 6)));
    }
    
    @Test
    public void testUpperBoundWithLargeTimestamps() {
        final Bytes upper = keySchema.upperRange(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), Long.MAX_VALUE);

        assertThat(
            "shorter key with max timestamp should be in range",
            upper.compareTo(toBinary.apply(
                new Windowed<>(
                    Bytes.wrap(new byte[]{0xA}),
                    new SessionWindow(Long.MAX_VALUE, Long.MAX_VALUE))
            )) >= 0
        );

        assertThat(
            "shorter key with max timestamp should be in range",
            upper.compareTo(toBinary.apply(
                new Windowed<>(
                    Bytes.wrap(new byte[]{0xA, 0xB}),
                    new SessionWindow(Long.MAX_VALUE, Long.MAX_VALUE))

            )) >= 0
        );

        if (schemaType == SchemaType.PrefixedTimeFirstSchema) {
            assertThat(upper, equalTo(toBinary.apply(
                new Windowed<>(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}),
                    new SessionWindow(Long.MAX_VALUE, Long.MAX_VALUE))))
            );
        } else {
            assertThat(upper, equalTo(toBinary.apply(
                new Windowed<>(Bytes.wrap(new byte[]{0xA}),
                    new SessionWindow(Long.MAX_VALUE, Long.MAX_VALUE))))
            );
        }
    }

    @Test
    public void testUpperBoundWithKeyBytesLargerThanFirstTimestampByte() {
        final Bytes upper = keySchema.upperRange(Bytes.wrap(new byte[]{0xA, (byte) 0x8F, (byte) 0x9F}), Long.MAX_VALUE);

        assertThat(
            "shorter key with max timestamp should be in range",
            upper.compareTo(toBinary.apply(
                new Windowed<>(
                    Bytes.wrap(new byte[]{0xA, (byte) 0x8F}),
                    new SessionWindow(Long.MAX_VALUE, Long.MAX_VALUE))
                )
            ) >= 0
        );

        assertThat(upper, equalTo(toBinary.apply(
            new Windowed<>(Bytes.wrap(new byte[]{0xA, (byte) 0x8F, (byte) 0x9F}), new SessionWindow(Long.MAX_VALUE, Long.MAX_VALUE))))
        );
    }

    @Test
    public void testUpperBoundWithZeroTimestamp() {
        final Bytes upper = keySchema.upperRange(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), 0);
        final Function<Windowed<Bytes>, Bytes> toBinary = WINDOW_TO_STORE_BINARY_MAP.get(schemaType);

        if (schemaType == SchemaType.PrefixedTimeFirstSchema) {
            assertThat(upper, equalTo(toBinary.apply(
                new Windowed<>(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), new SessionWindow(0, Long.MAX_VALUE))))
            );
        } else {
            assertThat(upper, equalTo(toBinary.apply(
                new Windowed<>(Bytes.wrap(new byte[]{0xA}), new SessionWindow(0, Long.MAX_VALUE))))
            );
        }
    }

    @Test
    public void testLowerBoundWithZeroTimestamp() {
        final Bytes lower = keySchema.lowerRange(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), 0);
        assertThat(lower, equalTo(toBinary.apply(new Windowed<>(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), new SessionWindow(0, 0)))));
    }

    @Test
    public void testLowerBoundMatchesTrailingZeros() {
        final Bytes lower = keySchema.lowerRange(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), Long.MAX_VALUE);

        assertThat(
            "appending zeros to key should still be in range",
            lower.compareTo(toBinary.apply(
                new Windowed<>(
                    Bytes.wrap(new byte[]{0xA, 0xB, 0xC, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
                    new SessionWindow(Long.MAX_VALUE, Long.MAX_VALUE))
            )) < 0
        );

        if (schemaType == SchemaType.PrefixedTimeFirstSchema) {
            assertThat(lower, equalTo(toBinary.apply(
                new Windowed<>(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), new SessionWindow(0, Long.MAX_VALUE)))));
        } else {
            assertThat(lower, equalTo(toBinary.apply(
                new Windowed<>(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), new SessionWindow(0, 0)))));
        }
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
    public void shouldDeSerializeEmptyByteArrayToNull() {
        assertNull(keySerde.deserializer().deserialize(topic, new byte[0]));
    }

    @Test
    public void shouldDeSerializeNullToNull() {
        assertNull(keySerde.deserializer().deserialize(topic, null));
    }

    @Test
    public void shouldConvertToBinaryAndBack() {
        final byte[] serialized = serdeToBinary.apply(windowedKey, serde.serializer(), "dummy");
        final Windowed<String> result = serdeFromBytes.apply(serialized, Serdes.String().deserializer(), "dummy");
        assertEquals(windowedKey, result);
    }

    @Test
    public void shouldExtractEndTimeFromBinary() {
        final byte[] serialized = serdeToBinary.apply(windowedKey, serde.serializer(), "dummy");
        assertEquals(endTime, (long) extractEndTS.apply(serialized));
    }

    @Test
    public void shouldExtractStartTimeFromBinary() {
        final byte[] serialized = serdeToBinary.apply(windowedKey, serde.serializer(), "dummy");
        assertEquals(startTime, (long) extractStartTS.apply(serialized));
    }

    @Test
    public void shouldExtractWindowFromBindary() {
        final byte[] serialized = serdeToBinary.apply(windowedKey, serde.serializer(), "dummy");
        assertEquals(window, extractWindow.apply(serialized));
    }

    @Test
    public void shouldExtractKeyBytesFromBinary() {
        final byte[] serialized = serdeToBinary.apply(windowedKey, serde.serializer(), "dummy");
        assertArrayEquals(key.getBytes(), extractKeyBytes.apply(serialized));
    }

    @Test
    public void shouldExtractKeyFromBinary() {
        final byte[] serialized = serdeToBinary.apply(windowedKey, serde.serializer(), "dummy");
        assertEquals(windowedKey, serdeFromBytes.apply(serialized, serde.deserializer(), "dummy"));
    }

    @Test
    public void shouldExtractBytesKeyFromBinary() {
        final Bytes bytesKey = Bytes.wrap(key.getBytes());
        final Windowed<Bytes> windowedBytesKey = new Windowed<>(bytesKey, window);
        final Bytes serialized = toBinary.apply(windowedBytesKey);
        assertEquals(windowedBytesKey, fromBytes.apply(serialized));
    }

    private List<Integer> getValues(final HasNextCondition hasNextCondition) {
        final List<Integer> results = new ArrayList<>();
        while (hasNextCondition.hasNext(iterator)) {
            results.add(iterator.next().value);
        }
        return results;
    }

}
