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
import org.apache.kafka.streams.state.internals.PrefixedWindowKeySchemas.KeyFirstWindowKeySchema;
import org.apache.kafka.streams.state.internals.PrefixedWindowKeySchemas.TimeFirstWindowKeySchema;
import org.apache.kafka.streams.state.internals.SegmentedBytesStore.KeySchema;
import org.apache.kafka.test.KeyValueIteratorStub;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class WindowKeySchemaTest {

    private static final Map<SchemaType, KeySchema> SCHEMA_TYPE_MAP = mkMap(
        mkEntry(SchemaType.WindowKeySchema, new WindowKeySchema()),
        mkEntry(SchemaType.PrefixedKeyFirstSchema, new KeyFirstWindowKeySchema()),
        mkEntry(SchemaType.PrefixedTimeFirstSchema, new TimeFirstWindowKeySchema())
    );

    private static final Map<SchemaType, Function<byte[], byte[]>> EXTRACT_STORE_KEY_MAP = mkMap(
        mkEntry(SchemaType.WindowKeySchema, WindowKeySchema::extractStoreKeyBytes),
        mkEntry(SchemaType.PrefixedKeyFirstSchema, KeyFirstWindowKeySchema::extractStoreKeyBytes),
        mkEntry(SchemaType.PrefixedTimeFirstSchema, TimeFirstWindowKeySchema::extractStoreKeyBytes)
    );

    private static final Map<SchemaType, BiFunction<byte[], Long, Windowed<Bytes>>> FROM_STORAGE_BYTES_KEY = mkMap(
        mkEntry(SchemaType.WindowKeySchema, WindowKeySchema::fromStoreBytesKey),
        mkEntry(SchemaType.PrefixedKeyFirstSchema, KeyFirstWindowKeySchema::fromStoreBytesKey),
        mkEntry(SchemaType.PrefixedTimeFirstSchema, TimeFirstWindowKeySchema::fromStoreBytesKey)
    );

    private static final Map<SchemaType, BiFunction<Windowed<Bytes>, Integer, Bytes>> WINDOW_TO_STORE_BINARY_MAP = mkMap(
        mkEntry(SchemaType.WindowKeySchema, WindowKeySchema::toStoreKeyBinary),
        mkEntry(SchemaType.PrefixedKeyFirstSchema, KeyFirstWindowKeySchema::toStoreKeyBinary),
        mkEntry(SchemaType.PrefixedTimeFirstSchema, TimeFirstWindowKeySchema::toStoreKeyBinary)
    );

    private static final Map<SchemaType, BiFunction<byte[], Long, Window>> EXTRACT_STORE_WINDOW_MAP = mkMap(
        mkEntry(SchemaType.WindowKeySchema, WindowKeySchema::extractStoreWindow),
        mkEntry(SchemaType.PrefixedKeyFirstSchema, KeyFirstWindowKeySchema::extractStoreWindow),
        mkEntry(SchemaType.PrefixedTimeFirstSchema, TimeFirstWindowKeySchema::extractStoreWindow)
    );

    @FunctionalInterface
    interface TriFunction<A, B, C, R> {
        R apply(A a, B b, C c);
    }

    private static final Map<SchemaType, TriFunction<byte[], Long, Integer, Bytes>> BYTES_TO_STORE_BINARY_MAP = mkMap(
        mkEntry(SchemaType.WindowKeySchema, WindowKeySchema::toStoreKeyBinary),
        mkEntry(SchemaType.PrefixedKeyFirstSchema, KeyFirstWindowKeySchema::toStoreKeyBinary),
        mkEntry(SchemaType.PrefixedTimeFirstSchema, TimeFirstWindowKeySchema::toStoreKeyBinary)
    );

    private static final Map<SchemaType, TriFunction<Windowed<String>, Integer, StateSerdes<String, byte[]>, Bytes>> SERDE_TO_STORE_BINARY_MAP = mkMap(
        mkEntry(SchemaType.WindowKeySchema, WindowKeySchema::toStoreKeyBinary),
        mkEntry(SchemaType.PrefixedKeyFirstSchema, KeyFirstWindowKeySchema::toStoreKeyBinary),
        mkEntry(SchemaType.PrefixedTimeFirstSchema, TimeFirstWindowKeySchema::toStoreKeyBinary)
    );

    private static final Map<SchemaType, Function<byte[], Long>> EXTRACT_TS_MAP = mkMap(
        mkEntry(SchemaType.WindowKeySchema, WindowKeySchema::extractStoreTimestamp),
        mkEntry(SchemaType.PrefixedKeyFirstSchema, KeyFirstWindowKeySchema::extractStoreTimestamp),
        mkEntry(SchemaType.PrefixedTimeFirstSchema, TimeFirstWindowKeySchema::extractStoreTimestamp)
    );

    private static final Map<SchemaType, Function<byte[], Integer>> EXTRACT_SEQ_MAP = mkMap(
        mkEntry(SchemaType.WindowKeySchema, WindowKeySchema::extractStoreSequence),
        mkEntry(SchemaType.PrefixedKeyFirstSchema, KeyFirstWindowKeySchema::extractStoreSequence),
        mkEntry(SchemaType.PrefixedTimeFirstSchema, TimeFirstWindowKeySchema::extractStoreSequence)
    );

    private static final Map<SchemaType, Function<byte[], byte[]>> FROM_WINDOW_KEY_MAP = mkMap(
        mkEntry(SchemaType.PrefixedKeyFirstSchema, KeyFirstWindowKeySchema::fromNonPrefixWindowKey),
        mkEntry(SchemaType.PrefixedTimeFirstSchema, TimeFirstWindowKeySchema::fromNonPrefixWindowKey)
    );

    private final String key = "key";
    private final String topic = "topic";
    private final long startTime = 50L;
    private final long endTime = 100L;
    private final Serde<String> serde = Serdes.String();

    private final Window window = new TimeWindow(startTime, endTime);
    private final Windowed<String> windowedKey = new Windowed<>(key, window);
    private KeySchema keySchema;
    private final Serde<Windowed<String>> keySerde = new WindowedSerdes.TimeWindowedSerde<>(serde, endTime - startTime);
    private final StateSerdes<String, byte[]> stateSerdes = new StateSerdes<>("dummy", serde, Serdes.ByteArray());
    public SchemaType schemaType;

    private enum SchemaType {
        WindowKeySchema,
        PrefixedTimeFirstSchema,
        PrefixedKeyFirstSchema
    }

    public void setup(final SchemaType type) {
        schemaType = type;
        keySchema = SCHEMA_TYPE_MAP.get(type);
    }

    private BiFunction<byte[], Long, Windowed<Bytes>> getFromStorageKey() {
        return FROM_STORAGE_BYTES_KEY.get(schemaType);
    }

    private BiFunction<byte[], Long, Window> getExtractStoreWindow() {
        return EXTRACT_STORE_WINDOW_MAP.get(schemaType);
    }

    private Function<byte[], byte[]> getExtractStorageKey() {
        return EXTRACT_STORE_KEY_MAP.get(schemaType);
    }

    private BiFunction<Windowed<Bytes>, Integer, Bytes> getToStoreKeyBinaryWindowParam() {
        return WINDOW_TO_STORE_BINARY_MAP.get(schemaType);
    }

    private TriFunction<byte[], Long, Integer, Bytes> getToStoreKeyBinaryBytesParam() {
        return BYTES_TO_STORE_BINARY_MAP.get(schemaType);
    }

    private Function<byte[], Long> getExtractTimestampFunc() {
        return EXTRACT_TS_MAP.get(schemaType);
    }

    private Function<byte[], Integer> getExtractSeqFunc() {
        return EXTRACT_SEQ_MAP.get(schemaType);
    }

    private TriFunction<Windowed<String>, Integer, StateSerdes<String, byte[]>, Bytes> getSerdeToStoreKey() {
        return SERDE_TO_STORE_BINARY_MAP.get(schemaType);
    }

    @EnumSource(SchemaType.class)
    @ParameterizedTest
    public void testHasNextConditionUsingNullKeys(final SchemaType type) {
        setup(type);
        final BiFunction<Windowed<Bytes>, Integer, Bytes> toStoreKeyBinary = getToStoreKeyBinaryWindowParam();
        final List<KeyValue<Bytes, Integer>> keys = asList(
            KeyValue.pair(toStoreKeyBinary.apply(new Windowed<>(Bytes.wrap(new byte[] {0, 0}), new TimeWindow(0, 1)), 0), 1),
            KeyValue.pair(toStoreKeyBinary.apply(new Windowed<>(Bytes.wrap(new byte[] {0}), new TimeWindow(0, 1)), 0), 2),
            KeyValue.pair(toStoreKeyBinary.apply(new Windowed<>(Bytes.wrap(new byte[] {0, 0, 0}), new TimeWindow(0, 1)), 0), 3),
            KeyValue.pair(toStoreKeyBinary.apply(new Windowed<>(Bytes.wrap(new byte[] {0}), new TimeWindow(10, 20)), 4), 4),
            KeyValue.pair(toStoreKeyBinary.apply(new Windowed<>(Bytes.wrap(new byte[] {0, 0}), new TimeWindow(10, 20)), 5), 5),
            KeyValue.pair(toStoreKeyBinary.apply(new Windowed<>(Bytes.wrap(new byte[] {0, 0, 0}), new TimeWindow(10, 20)), 6), 6));
        try (final DelegatingPeekingKeyValueIterator<Bytes, Integer> iterator = new DelegatingPeekingKeyValueIterator<>("foo", new KeyValueIteratorStub<>(keys.iterator()))) {

            final HasNextCondition hasNextCondition = keySchema.hasNextCondition(null, null, 0, Long.MAX_VALUE, true);
            final List<Integer> results = new ArrayList<>();
            while (hasNextCondition.hasNext(iterator)) {
                results.add(iterator.next().value);
            }

            assertThat(results, equalTo(asList(1, 2, 3, 4, 5, 6)));
        }
    }
    
    @EnumSource(SchemaType.class)
    @ParameterizedTest
    public void testUpperBoundWithLargeTimestamps(final SchemaType type) {
        setup(type);
        final Bytes upper = keySchema.upperRange(Bytes.wrap(new byte[] {0xA, 0xB, 0xC}), Long.MAX_VALUE);
        final TriFunction<byte[], Long, Integer, Bytes> toStoreKeyBinary = getToStoreKeyBinaryBytesParam();

        assertThat(
            "shorter key with max timestamp should be in range",
            upper.compareTo(
                toStoreKeyBinary.apply(
                    new byte[] {0xA},
                    Long.MAX_VALUE,
                    Integer.MAX_VALUE
                )
            ) >= 0
        );

        assertThat(
            "shorter key with max timestamp should be in range",
            upper.compareTo(
                toStoreKeyBinary.apply(
                    new byte[] {0xA, 0xB},
                    Long.MAX_VALUE,
                    Integer.MAX_VALUE
                )
            ) >= 0
        );

        if (schemaType == SchemaType.PrefixedTimeFirstSchema) {
            assertThat(upper, equalTo(
                toStoreKeyBinary.apply(new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF}, Long.MAX_VALUE, Integer.MAX_VALUE)));
        } else {
            assertThat(upper, equalTo(
                toStoreKeyBinary.apply(new byte[]{0xA}, Long.MAX_VALUE, Integer.MAX_VALUE)));
        }
    }

    @EnumSource(SchemaType.class)
    @ParameterizedTest
    public void testUpperBoundWithKeyBytesLargerThanFirstTimestampByte(final SchemaType type) {
        setup(type);
        final Bytes upper = keySchema.upperRange(Bytes.wrap(new byte[] {0xA, (byte) 0x8F, (byte) 0x9F}), Long.MAX_VALUE);
        final TriFunction<byte[], Long, Integer, Bytes> toStoreKeyBinary = getToStoreKeyBinaryBytesParam();

        assertThat(
            "shorter key with max timestamp should be in range",
            upper.compareTo(
                toStoreKeyBinary.apply(
                    new byte[] {0xA, (byte) 0x8F},
                    Long.MAX_VALUE,
                    Integer.MAX_VALUE
                )
            ) >= 0
        );

        if (schemaType == SchemaType.PrefixedTimeFirstSchema) {
            assertThat(upper, equalTo(
                toStoreKeyBinary.apply(new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF}, Long.MAX_VALUE, Integer.MAX_VALUE)));
        } else {
            assertThat(upper, equalTo(
                toStoreKeyBinary.apply(new byte[]{0xA, (byte) 0x8F, (byte) 0x9F}, Long.MAX_VALUE,
                    Integer.MAX_VALUE)));
        }
    }

    @EnumSource(SchemaType.class)
    @ParameterizedTest
    public void testUpperBoundWithKeyBytesLargerAndSmallerThanFirstTimestampByte(final SchemaType type) {
        setup(type);
        final Bytes upper = keySchema.upperRange(Bytes.wrap(new byte[] {0xC, 0xC, 0x9}), 0x0AffffffffffffffL);
        final TriFunction<byte[], Long, Integer, Bytes> toStoreKeyBinary = getToStoreKeyBinaryBytesParam();

        assertThat(
            "shorter key with customized timestamp should be in range",
            upper.compareTo(
                toStoreKeyBinary.apply(
                    new byte[] {0xC, 0xC},
                    0x0AffffffffffffffL,
                    Integer.MAX_VALUE
                )
            ) >= 0
        );
        if (schemaType == SchemaType.PrefixedTimeFirstSchema) {
            assertThat(upper, equalTo(
                toStoreKeyBinary.apply(new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF}, 0x0AffffffffffffffL, Integer.MAX_VALUE)));
        } else {
            assertThat(upper, equalTo(
                toStoreKeyBinary.apply(new byte[]{0xC, 0xC}, 0x0AffffffffffffffL,
                    Integer.MAX_VALUE)));
        }
    }

    @EnumSource(SchemaType.class)
    @ParameterizedTest
    public void testUpperBoundWithZeroTimestamp(final SchemaType type) {
        setup(type);
        final Bytes upper = keySchema.upperRange(Bytes.wrap(new byte[] {0xA, 0xB, 0xC}), 0);
        final TriFunction<byte[], Long, Integer, Bytes> toStoreKeyBinary = getToStoreKeyBinaryBytesParam();

        if (schemaType == SchemaType.PrefixedTimeFirstSchema) {
            assertThat(upper, equalTo(
                toStoreKeyBinary.apply(new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF}, 0x0L, Integer.MAX_VALUE)));
        } else {
            assertThat(upper,
                equalTo(toStoreKeyBinary.apply(new byte[]{0xA, 0xB, 0xC}, 0L, Integer.MAX_VALUE)));
        }
    }

    @EnumSource(SchemaType.class)
    @ParameterizedTest
    public void testLowerBoundWithZeroTimestamp(final SchemaType type) {
        setup(type);
        final Bytes lower = keySchema.lowerRange(Bytes.wrap(new byte[] {0xA, 0xB, 0xC}), 0);
        final TriFunction<byte[], Long, Integer, Bytes> toStoreKeyBinary = getToStoreKeyBinaryBytesParam();
        assertThat(
            "Larger key prefix should be in range.",
            lower.compareTo(
                toStoreKeyBinary.apply(
                    new byte[] {0xA, 0xB, 0xC, 0x0},
                    0L,
                    0
                )
            ) < 0
        );

        if (schemaType == SchemaType.PrefixedTimeFirstSchema) {
            final Bytes expected = Bytes.wrap(ByteBuffer.allocate(1 + 8 + 3)
                .put((byte) 0x0)
                .putLong(0)
                .put(new byte[] {0xA, 0xB, 0xC})
                .array());
            assertThat(lower, equalTo(expected));
        } else {
            assertThat(lower, equalTo(toStoreKeyBinary.apply(new byte[]{0xA, 0xB, 0xC}, 0L, 0)));
        }
    }

    @EnumSource(SchemaType.class)
    @ParameterizedTest
    public void testLowerBoundWithNonZeroTimestamp(final SchemaType type) {
        setup(type);
        final Bytes lower = keySchema.lowerRange(Bytes.wrap(new byte[] {0xA, 0xB, 0xC}), 42);
        final TriFunction<byte[], Long, Integer, Bytes> toStoreKeyBinary = getToStoreKeyBinaryBytesParam();

        assertThat(
            "Larger timestamp should be in range",
            lower.compareTo(
                toStoreKeyBinary.apply(
                    new byte[] {0xA, 0xB, 0xC, 0x0},
                    43L,
                    0
                )
            ) < 0
        );

        if (schemaType == SchemaType.PrefixedTimeFirstSchema) {
            final Bytes expected = Bytes.wrap(ByteBuffer.allocate(1 + 8 + 3)
                .put((byte) 0x0)
                .putLong(42)
                .put(new byte[] {0xA, 0xB, 0xC})
                .array());
            assertThat(lower, equalTo(expected));
        } else {
            assertThat(lower, equalTo(toStoreKeyBinary.apply(new byte[]{0xA, 0xB, 0xC}, 0L, 0)));
        }
    }

    @EnumSource(SchemaType.class)
    @ParameterizedTest
    public void testLowerBoundMatchesTrailingZeros(final SchemaType type) {
        setup(type);
        final Bytes lower = keySchema.lowerRange(Bytes.wrap(new byte[] {0xA, 0xB, 0xC}), Long.MAX_VALUE - 1);
        final TriFunction<byte[], Long, Integer, Bytes> toStoreKeyBinary = getToStoreKeyBinaryBytesParam();

        assertThat(
            "appending zeros to key should still be in range",
            lower.compareTo(
                toStoreKeyBinary.apply(
                    new byte[] {0xA, 0xB, 0xC, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    Long.MAX_VALUE - 1,
                    0
                )
            ) < 0
        );
        if (schemaType == SchemaType.PrefixedTimeFirstSchema) {
            final Bytes expected = Bytes.wrap(ByteBuffer.allocate(1 + 8 + 3)
                .put((byte) 0x0)
                .putLong(Long.MAX_VALUE - 1)
                .put(new byte[] {0xA, 0xB, 0xC})
                .array());
            assertThat(lower, equalTo(expected));
        } else {
            assertThat(lower, equalTo(toStoreKeyBinary.apply(new byte[]{0xA, 0xB, 0xC}, 0L, 0)));
        }
    }

    @EnumSource(SchemaType.class)
    @ParameterizedTest
    public void shouldSerializeDeserialize(final SchemaType type) {
        setup(type);
        final byte[] bytes = keySerde.serializer().serialize(topic, windowedKey);
        final Windowed<String> result = keySerde.deserializer().deserialize(topic, bytes);
        // TODO: fix this part as last bits of KAFKA-4468
        assertEquals(new Windowed<>(key, new TimeWindow(startTime, endTime)), result);
    }

    @EnumSource(SchemaType.class)
    @ParameterizedTest
    public void testSerializeDeserializeOverflowWindowSize(final SchemaType type) {
        setup(type);
        final byte[] bytes = keySerde.serializer().serialize(topic, windowedKey);
        final Windowed<String> result = new TimeWindowedDeserializer<>(serde.deserializer(), Long.MAX_VALUE - 1)
            .deserialize(topic, bytes);
        assertEquals(new Windowed<>(key, new TimeWindow(startTime, Long.MAX_VALUE)), result);
    }

    @EnumSource(SchemaType.class)
    @ParameterizedTest
    public void shouldSerializeDeserializeExpectedWindowSize(final SchemaType type) {
        setup(type);
        final byte[] bytes = keySerde.serializer().serialize(topic, windowedKey);
        final Windowed<String> result = new TimeWindowedDeserializer<>(serde.deserializer(), endTime - startTime)
            .deserialize(topic, bytes);
        assertEquals(windowedKey, result);
    }

    @EnumSource(SchemaType.class)
    @ParameterizedTest
    public void shouldSerializeDeserializeExpectedChangelogWindowSize(final SchemaType type) {
        setup(type);
        if (schemaType != SchemaType.WindowKeySchema) {
            // Changelog key is serialized using WindowKeySchema
            return;
        }
        // Key-value containing serialized store key binary and the key's window size
        final List<KeyValue<Bytes, Integer>> keys = asList(
            KeyValue.pair(WindowKeySchema.toStoreKeyBinary(new Windowed<>(Bytes.wrap(new byte[] {0}), new TimeWindow(0, 1)), 0), 1),
            KeyValue.pair(WindowKeySchema.toStoreKeyBinary(new Windowed<>(Bytes.wrap(new byte[] {0, 0}), new TimeWindow(0, 10)), 0), 10),
            KeyValue.pair(WindowKeySchema.toStoreKeyBinary(new Windowed<>(Bytes.wrap(new byte[] {0, 0, 0}), new TimeWindow(10, 30)), 6), 20));

        final List<Long> results = new ArrayList<>();
        for (final KeyValue<Bytes, Integer> keyValue : keys) {
            // Let the deserializer know that it's deserializing a changelog windowed key
            final Serde<Windowed<String>> keySerde = new WindowedSerdes.TimeWindowedSerde<>(serde, keyValue.value).forChangelog(true);
            final Windowed<String> result = keySerde.deserializer().deserialize(topic, keyValue.key.get());
            final Window resultWindow = result.window();
            results.add(resultWindow.end() - resultWindow.start());
        }

        assertThat(results, equalTo(asList(1L, 10L, 20L)));
    }

    @EnumSource(SchemaType.class)
    @ParameterizedTest
    public void shouldSerializeNullToNull(final SchemaType type) {
        setup(type);
        assertNull(keySerde.serializer().serialize(topic, null));
    }

    @EnumSource(SchemaType.class)
    @ParameterizedTest
    public void shouldDeserializeEmptyByteArrayToNull(final SchemaType type) {
        setup(type);
        assertNull(keySerde.deserializer().deserialize(topic, new byte[0]));
    }

    @EnumSource(SchemaType.class)
    @ParameterizedTest
    public void shouldDeserializeNullToNull(final SchemaType type) {
        setup(type);
        assertNull(keySerde.deserializer().deserialize(topic, null));
    }

    @EnumSource(SchemaType.class)
    @ParameterizedTest
    public void shouldConvertToBinaryAndBack(final SchemaType type) {
        setup(type);
        final TriFunction<Windowed<String>, Integer, StateSerdes<String, byte[]>, Bytes> toStoreKeyBinary = getSerdeToStoreKey();
        final Bytes serialized = toStoreKeyBinary.apply(windowedKey, 0, stateSerdes);
        final Windowed<String> result;
        if (schemaType == SchemaType.WindowKeySchema) {
            result = WindowKeySchema.fromStoreKey(serialized.get(),
                endTime - startTime, stateSerdes.keyDeserializer(), stateSerdes.topic());
        } else if (schemaType == SchemaType.PrefixedTimeFirstSchema) {
            result = TimeFirstWindowKeySchema.fromStoreKey(serialized.get(),
                endTime - startTime, stateSerdes.keyDeserializer(), stateSerdes.topic());
        } else {
            result = KeyFirstWindowKeySchema.fromStoreKey(serialized.get(),
                endTime - startTime, stateSerdes.keyDeserializer(), stateSerdes.topic());
        }
        assertEquals(windowedKey, result);
    }

    @EnumSource(SchemaType.class)
    @ParameterizedTest
    public void shouldExtractSequenceFromBinary(final SchemaType type) {
        setup(type);
        final TriFunction<Windowed<String>, Integer, StateSerdes<String, byte[]>, Bytes> toStoreKeyBinary = getSerdeToStoreKey();
        final Bytes serialized = toStoreKeyBinary.apply(windowedKey, 0, stateSerdes);
        final Function<byte[], Integer> extractStoreSequence = getExtractSeqFunc();
        assertEquals(0, (int) extractStoreSequence.apply(serialized.get()));
    }

    @EnumSource(SchemaType.class)
    @ParameterizedTest
    public void shouldExtractStartTimeFromBinary(final SchemaType type) {
        setup(type);
        final TriFunction<Windowed<String>, Integer, StateSerdes<String, byte[]>, Bytes> toStoreKeyBinary = getSerdeToStoreKey();
        final Bytes serialized = toStoreKeyBinary.apply(windowedKey, 0, stateSerdes);
        final Function<byte[], Long> extractStoreTimestamp = getExtractTimestampFunc();
        assertEquals(startTime, (long) extractStoreTimestamp.apply(serialized.get()));
    }

    @EnumSource(SchemaType.class)
    @ParameterizedTest
    public void shouldExtractWindowFromBinary(final SchemaType type) {
        setup(type);
        final TriFunction<Windowed<String>, Integer, StateSerdes<String, byte[]>, Bytes> toStoreKeyBinary = getSerdeToStoreKey();
        final Bytes serialized = toStoreKeyBinary.apply(windowedKey, 0, stateSerdes);
        final BiFunction<byte[], Long, Window> extractStoreWindow = getExtractStoreWindow();
        assertEquals(window, extractStoreWindow.apply(serialized.get(), endTime - startTime));
    }

    @EnumSource(SchemaType.class)
    @ParameterizedTest
    public void shouldExtractKeyBytesFromBinary(final SchemaType type) {
        setup(type);
        final TriFunction<Windowed<String>, Integer, StateSerdes<String, byte[]>, Bytes> toStoreKeyBinary = getSerdeToStoreKey();
        final Bytes serialized = toStoreKeyBinary.apply(windowedKey, 0, stateSerdes);
        final Function<byte[], byte[]> extractStoreKeyBytes = getExtractStorageKey();
        assertArrayEquals(key.getBytes(), extractStoreKeyBytes.apply(serialized.get()));
    }

    @EnumSource(SchemaType.class)
    @ParameterizedTest
    public void shouldExtractBytesKeyFromBinary(final SchemaType type) {
        setup(type);
        final Windowed<Bytes> windowedBytesKey = new Windowed<>(Bytes.wrap(key.getBytes()), window);
        final BiFunction<Windowed<Bytes>, Integer, Bytes> toStoreKeyBinary = getToStoreKeyBinaryWindowParam();
        final Bytes serialized = toStoreKeyBinary.apply(windowedBytesKey, 0);
        final BiFunction<byte[], Long, Windowed<Bytes>> fromStoreBytesKey = getFromStorageKey();
        assertEquals(windowedBytesKey, fromStoreBytesKey.apply(serialized.get(), endTime - startTime));
    }
    
    @EnumSource(SchemaType.class)
    @ParameterizedTest
    public void shouldConvertFromNonPrefixWindowKey(final SchemaType type) {
        setup(type);
        final Function<byte[], byte[]> fromWindowKey = FROM_WINDOW_KEY_MAP.get(schemaType);
        final TriFunction<byte[], Long, Integer, Bytes> toStoreKeyBinary = BYTES_TO_STORE_BINARY_MAP.get(SchemaType.WindowKeySchema);
        if (fromWindowKey != null) {
            final Bytes windowKeyBytes = toStoreKeyBinary.apply(key.getBytes(), startTime, 0);
            final byte[] convertedBytes = fromWindowKey.apply(windowKeyBytes.get());
            final Function<byte[], Long> extractStoreTimestamp = getExtractTimestampFunc();
            final Function<byte[], Integer> extractStoreSequence = getExtractSeqFunc();
            final Function<byte[], byte[]> extractStoreKeyBytes = getExtractStorageKey();

            final byte[] rawkey = extractStoreKeyBytes.apply(convertedBytes);
            final long timestamp = extractStoreTimestamp.apply(convertedBytes);
            final int seq = extractStoreSequence.apply(convertedBytes);

            assertEquals(0, seq);
            assertEquals(startTime, timestamp);
            assertEquals(Bytes.wrap(key.getBytes()), Bytes.wrap(rawkey));
        }
    }
}
