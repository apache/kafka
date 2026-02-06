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
package org.apache.kafka.streams.state;

import org.apache.kafka.common.utils.ByteUtils;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HeadersBytesStoreTest {

    @Test
    public void shouldReturnNullWhenConvertingNullValue() {
        final byte[] result = HeadersBytesStore.convertToHeaderFormat(null);
        assertNull(result);
    }

    @Test
    public void shouldConvertLegacyValueToHeaderFormat() {
        final byte[] legacyValue = "test-value".getBytes();

        final byte[] converted = HeadersBytesStore.convertToHeaderFormat(legacyValue);

        assertNotNull(converted);
        assertTrue(converted.length > legacyValue.length, "converted bytes should have empty header bytes");
    }

    @Test
    public void shouldConvertTimestampedValueToHeaderFormat() {
        // Legacy timestamped format: [timestamp(8)][value]
        final long timestamp = 123456789L;
        final byte[] value = "test-value".getBytes();

        final ByteBuffer legacyBuffer = ByteBuffer.allocate(Long.BYTES + value.length);
        legacyBuffer.putLong(timestamp);
        legacyBuffer.put(value);
        final byte[] legacyValue = legacyBuffer.array();

        final byte[] converted = HeadersBytesStore.convertToHeaderFormat(legacyValue);

        assertNotNull(converted);

        // Verify format: [headersSize(varint)][headersBytes][payload]
        final ByteBuffer buffer = ByteBuffer.wrap(converted);

        final int headersSize = ByteUtils.readVarint(buffer);
        assertEquals(0, headersSize, "Empty headers should have headersSize = 0");
        final byte[] payload = new byte[buffer.remaining()];
        buffer.get(payload);
        assertArrayEquals(legacyValue, payload, "should keep original payload");
    }

    @Test
    public void shouldConvertEmptyValueToHeaderFormat() {
        final byte[] emptyValue = new byte[0];

        final byte[] converted = HeadersBytesStore.convertToHeaderFormat(emptyValue);

        assertNotNull(converted);
        assertTrue(converted.length > 0, "Converted value should have headers metadata");

        final ByteBuffer buffer = ByteBuffer.wrap(converted);
        final int headersSize = ByteUtils.readVarint(buffer);
        assertEquals(0, headersSize, "Empty headers should have headersSize = 0");
        assertEquals(0, buffer.remaining(), "No payload bytes for empty value");
    }

    @Test
    public void shouldHandleLargeValues() {
        final byte[] largeValue = new byte[10000];
        for (int i = 0; i < largeValue.length; i++) {
            largeValue[i] = (byte) (i % 256);
        }

        final byte[] converted = HeadersBytesStore.convertToHeaderFormat(largeValue);

        assertNotNull(converted);
        assertTrue(converted.length > largeValue.length);

        // Verify the payload is intact
        final ByteBuffer buffer = ByteBuffer.wrap(converted);
        final int headersSize = ByteUtils.readVarint(buffer);
        buffer.position(buffer.position() + headersSize);

        final byte[] payload = new byte[buffer.remaining()];
        buffer.get(payload);
        assertArrayEquals(largeValue, payload);
    }
}
