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

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static org.apache.kafka.streams.state.internals.Utils.rawPlainValue;
import static org.apache.kafka.streams.state.internals.Utils.rawTimestampedValue;
import static org.apache.kafka.streams.state.internals.Utils.readBytes;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class UtilsTest {
    private static final String TOPIC = "test-topic";
    private static final String VALUE_STR = "test-value";
    private static final byte[] VALUE = VALUE_STR.getBytes(StandardCharsets.UTF_8);
    private static final long TIMESTAMP = 123456789L;
    private static final byte[] HEADERS = "test-headers".getBytes(StandardCharsets.UTF_8);
    // Header size's varint encoding cannot exceed 5 bytes (see @{link ByteUtils#readVarint(ByteBuffer)})
    private static final int MAX_VARINT_SIZE = 5;
    private static final int OVERFLOW_HEADERS_SIZE = (1 + MAX_VARINT_SIZE) + HEADERS.length + StateSerdes.TIMESTAMP_SIZE + VALUE.length;

    @Test
    public void shouldExtractRawPlainValue() {
        // Format: [headersSize(varint)][headers][timestamp(8)][value]
        // Create a value with headers size=0, timestamp=-1, value="test"
        final byte[] value = "test".getBytes();
        final ByteBuffer buffer = ByteBuffer.allocate(1 + 8 + value.length);
        buffer.put((byte) 0x00); // headers size = 0
        buffer.putLong(-1L); // timestamp = -1
        buffer.put(value);

        final byte[] result = rawPlainValue(buffer.array());

        assertArrayEquals(value, result);
    }

    @Test
    public void shouldReturnNullForNullRawPlainValue() {
        assertNull(rawPlainValue(null));
    }

    @Test
    public void shouldReturnNullForNullRawTimestampedValue() {
        assertNull(rawPlainValue(null));
    }

    @ParameterizedTest
    @ValueSource(strings = { VALUE_STR, "" })
    public void testRawTimestampedValue(final String valueStr) {
        final byte[] value = valueStr.getBytes(StandardCharsets.UTF_8);
        final byte[] headers = headersOf(HEADERS);

        final byte[] inputBytes = headersTimestampValueOf(headers, value);
        final byte[] outputBytes = rawTimestampedValue(inputBytes);

        assertArrayEquals(timestampedValueOf(value), outputBytes);
    }

    @ParameterizedTest
    @MethodSource("invalidHeaderSizes")
    public void testRawTimestampedValueWithInvalidHeadersSize(final int invalidHeaderSize) {
        final byte[] invalidHeaders = headersOf(HEADERS, invalidHeaderSize);
        final byte[] inputBytes = headersTimestampValueOf(invalidHeaders, VALUE);
        assertThrows(SerializationException.class, () -> rawTimestampedValue(inputBytes));
    }

    @Test
    public void testRawTimestampedValueWithoutTimestamp() {
        assertThrows(SerializationException.class, () -> rawTimestampedValue(VALUE));
    }

    @Test
    public void testRawTimestampedValueWithSerdes() {
        final Headers headers = new RecordHeaders().add("key1", "value1".getBytes(StandardCharsets.UTF_8));
        final ValueTimestampHeaders<String> input = ValueTimestampHeaders.make(VALUE_STR, TIMESTAMP, headers);
        try (
            final ValueTimestampHeadersSerializer<String> serializer = new ValueTimestampHeadersSerializer<>(Serdes.String().serializer());
            final ValueAndTimestampSerde<String> stringSerde = new ValueAndTimestampSerde<>(Serdes.String())
        ) {
            final byte[] inputBytes = serializer.serialize(TOPIC, input);
            final byte[] outputBytes = rawTimestampedValue(inputBytes);
            final ValueAndTimestamp<String> output = stringSerde.deserializer().deserialize(TOPIC, outputBytes);

            assertEquals(TIMESTAMP, output.timestamp());
            assertEquals(VALUE_STR, output.value());
        }
    }

    @Test
    public void testReadBytes() {
        final ByteBuffer buf = ByteBuffer.wrap(VALUE);

        assertThrows(SerializationException.class, () -> readBytes(buf, -1));
        assertThrows(SerializationException.class, () -> readBytes(buf, VALUE.length + 1));
        
        assertEquals('t', readBytes(buf, 1)[0]);
        assertEquals('e', readBytes(buf, 1)[0]);

        final byte[] nextTwo = readBytes(buf, 2);
        assertEquals(2, nextTwo.length);
        assertEquals('s', nextTwo[0]);
        assertEquals('t', nextTwo[1]);

        final byte[] tail = readBytes(buf, buf.remaining());
        assertEquals(6, tail.length);
        assertArrayEquals("-value".getBytes(StandardCharsets.UTF_8), tail);

        assertThrows(SerializationException.class, () -> readBytes(buf, 1));
    }

    private static Stream<Arguments> invalidHeaderSizes() {
        return Stream.of(-1, Integer.MAX_VALUE, Integer.MIN_VALUE, OVERFLOW_HEADERS_SIZE).map(Arguments::of);
    }

    private static byte[] headersOf(final byte[] headersBytes) {
        return headersOf(headersBytes, headersBytes.length);
    }

    private static byte[] headersOf(final byte[] headerBytes, final int injectedHeadersSize) {
        final ByteBuffer buf = ByteBuffer.allocate(MAX_VARINT_SIZE + headerBytes.length);
        ByteUtils.writeVarint(injectedHeadersSize, buf);
        buf.put(headerBytes);
        buf.flip();
        final byte[] res = new byte[buf.limit()];
        buf.get(res);
        return res;
    }

    private static byte[] headersTimestampValueOf(final byte[] headers, final byte[] value) {
        final byte[] res = new byte[headers.length + StateSerdes.TIMESTAMP_SIZE + value.length];
        final ByteBuffer buf = ByteBuffer.wrap(res);
        buf.put(headers);
        buf.putLong(TIMESTAMP);
        buf.put(value);
        return res;
    }

    private static byte[] timestampedValueOf(final byte[] value) {
        final byte[] res = new byte[StateSerdes.TIMESTAMP_SIZE + value.length];
        final ByteBuffer buf = ByteBuffer.wrap(res);
        buf.putLong(TIMESTAMP);
        buf.put(value);
        return res;
    }
}
