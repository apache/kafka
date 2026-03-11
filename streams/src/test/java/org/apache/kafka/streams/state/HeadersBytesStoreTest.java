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
import java.util.Arrays;

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
        assertEquals(legacyValue.length + 1, converted.length, "converted bytes should have empty header bytes");
        assertEquals(0x00, converted[0], "First byte for empty header should be the 0x00");
        final byte[] actualPayload = Arrays.copyOfRange(converted, 1, converted.length);
        assertArrayEquals(legacyValue, actualPayload);
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
    public void shouldReturnNullWhenConvertingNullPlainValue() {
        final byte[] result = HeadersBytesStore.convertFromPlainToHeaderFormat(null);
        assertNull(result);
    }

    @Test
    public void shouldConvertPlainValueToHeaderFormatWithTimestamp() {
        final byte[] plainValue = "test-value".getBytes();

        final byte[] converted = HeadersBytesStore.convertFromPlainToHeaderFormat(plainValue);

        assertNotNull(converted);
        // Expected format: [0x00 (1 byte)][timestamp=-1 (8 bytes)][value]
        assertEquals(1 + 8 + plainValue.length, converted.length);

        // Verify empty headers marker
        assertEquals(0x00, converted[0], "First byte for empty header should be 0x00");

        // Verify timestamp = -1 (all 0xFF bytes)
        for (int i = 1; i <= 8; i++) {
            assertEquals((byte) 0xFF, converted[i], "Timestamp byte " + (i - 1) + " should be 0xFF for -1");
        }

        // Verify payload
        final byte[] actualPayload = Arrays.copyOfRange(converted, 9, converted.length);
        assertArrayEquals(plainValue, actualPayload);
    }

    @Test
    public void shouldConvertEmptyPlainValueToHeaderFormat() {
        final byte[] emptyValue = new byte[0];

        final byte[] converted = HeadersBytesStore.convertFromPlainToHeaderFormat(emptyValue);

        assertNotNull(converted);
        // Expected format: [0x00 (1 byte)][timestamp=-1 (8 bytes)]
        assertEquals(9, converted.length, "Converted empty value should have headers + timestamp");

        final ByteBuffer buffer = ByteBuffer.wrap(converted);
        final int headersSize = ByteUtils.readVarint(buffer);
        assertEquals(0, headersSize, "Empty headers should have headersSize = 0");

        // Verify timestamp = -1
        final long timestamp = buffer.getLong();
        assertEquals(-1L, timestamp, "Timestamp should be -1 for plain value upgrade");
    }

    @Test
    public void shouldConvertPlainValueWithCorrectByteOrder() {
        final byte[] plainValue = new byte[]{0x01, 0x02, 0x03};

        final byte[] converted = HeadersBytesStore.convertFromPlainToHeaderFormat(plainValue);

        // Expected: [0x00][0xFF x 8][0x01, 0x02, 0x03]
        final byte[] expected = new byte[]{
            0x00,                                           // empty headers
            (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,  // timestamp -1 (high 4 bytes)
            (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,  // timestamp -1 (low 4 bytes)
            0x01, 0x02, 0x03                                // payload
        };

        assertArrayEquals(expected, converted);
    }
}
