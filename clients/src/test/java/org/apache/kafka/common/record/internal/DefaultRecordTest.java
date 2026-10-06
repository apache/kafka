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
package org.apache.kafka.common.record.internal;

import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.utils.internals.ByteBufferInputStream;
import org.apache.kafka.common.utils.internals.ByteBufferOutputStream;
import org.apache.kafka.common.utils.internals.ByteUtils;
import org.apache.kafka.common.utils.internals.SingleByteBufferOutputStream;

import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DefaultRecordTest {
    @Test
    public void testBasicSerde() throws IOException {
        Header[] headers = new Header[] {
            new RecordHeader("foo", "value".getBytes()),
            new RecordHeader("bar", null),
            new RecordHeader("\"A\\u00ea\\u00f1\\u00fcC\"", "value".getBytes())
        };

        SimpleRecord[] records = new SimpleRecord[] {
            new SimpleRecord("hi".getBytes(), "there".getBytes()),
            new SimpleRecord(null, "there".getBytes()),
            new SimpleRecord("hi".getBytes(), null),
            new SimpleRecord(null, null),
            new SimpleRecord(15L, "hi".getBytes(), "there".getBytes(), headers)
        };

        for (SimpleRecord record : records) {
            int baseSequence = 723;
            long baseOffset = 37;
            int offsetDelta = 10;
            long baseTimestamp = System.currentTimeMillis();
            long timestampDelta = 323;

            ByteBufferOutputStream out = new SingleByteBufferOutputStream(1024);
            DefaultRecord.writeTo(new DataOutputStream(out), offsetDelta, timestampDelta, record.key(), record.value(),
                    record.headers());
            ByteBuffer buffer = out.buffer();
            buffer.flip();

            DefaultRecord logRecord = DefaultRecord.readFrom(buffer, baseOffset, baseTimestamp, baseSequence, null);
            assertNotNull(logRecord);
            assertEquals(baseOffset + offsetDelta, logRecord.offset());
            assertEquals(baseSequence + offsetDelta, logRecord.sequence());
            assertEquals(baseTimestamp + timestampDelta, logRecord.timestamp());
            assertEquals(record.key(), logRecord.key());
            assertEquals(record.value(), logRecord.value());
            assertArrayEquals(record.headers(), logRecord.headers());
            assertEquals(DefaultRecord.sizeInBytes(offsetDelta, timestampDelta, record.key(), record.value(),
                    record.headers()), logRecord.sizeInBytes());
        }
    }

    @Test
    public void testBasicSerdeInvalidHeaderCountTooHigh() throws IOException {
        Header[] headers = new Header[] {
            new RecordHeader("foo", "value".getBytes()),
            new RecordHeader("bar", null),
            new RecordHeader("\"A\\u00ea\\u00f1\\u00fcC\"", "value".getBytes())
        };

        SimpleRecord record = new SimpleRecord(15L, "hi".getBytes(), "there".getBytes(), headers);

        int baseSequence = 723;
        long baseOffset = 37;
        int offsetDelta = 10;
        long baseTimestamp = System.currentTimeMillis();
        long timestampDelta = 323;

        ByteBufferOutputStream out = new SingleByteBufferOutputStream(1024);
        DefaultRecord.writeTo(new DataOutputStream(out), offsetDelta, timestampDelta, record.key(), record.value(),
                record.headers());
        ByteBuffer buffer = out.buffer();
        buffer.flip();
        buffer.put(14, (byte) 8);
        // test for input stream input
        try (ByteBufferInputStream inpStream = new ByteBufferInputStream(buffer.asReadOnlyBuffer())) {
            assertThrows(InvalidRecordException.class,
                () -> DefaultRecord.readFrom(inpStream, baseOffset, baseTimestamp, baseSequence, null, Records.SOFT_MAX_ARRAY_LENGTH));
        }
        // test for buffer input
        assertThrows(InvalidRecordException.class,
            () -> DefaultRecord.readFrom(buffer, baseOffset, baseTimestamp, baseSequence, null));
    }

    @Test
    public void testBasicSerdeInvalidHeaderCountTooLow() throws IOException {
        Header[] headers = new Header[] {
            new RecordHeader("foo", "value".getBytes()),
            new RecordHeader("bar", null),
            new RecordHeader("\"A\\u00ea\\u00f1\\u00fcC\"", "value".getBytes())
        };

        SimpleRecord record = new SimpleRecord(15L, "hi".getBytes(), "there".getBytes(), headers);

        int baseSequence = 723;
        long baseOffset = 37;
        int offsetDelta = 10;
        long baseTimestamp = System.currentTimeMillis();
        long timestampDelta = 323;

        ByteBufferOutputStream out = new SingleByteBufferOutputStream(1024);
        DefaultRecord.writeTo(new DataOutputStream(out), offsetDelta, timestampDelta, record.key(), record.value(),
                record.headers());
        ByteBuffer buffer = out.buffer();
        buffer.flip();
        buffer.put(14, (byte) 4);

        assertThrows(InvalidRecordException.class,
            () -> DefaultRecord.readFrom(buffer, baseOffset, baseTimestamp, baseSequence, null));
    }

    @Test
    public void testInvalidKeySize() throws IOException {
        byte attributes = 0;
        long timestampDelta = 2;
        int offsetDelta = 1;
        int sizeOfBodyInBytes = 100;
        int keySize = 105; // use a key size larger than the full message

        ByteBuffer buf = ByteBuffer.allocate(sizeOfBodyInBytes + ByteUtils.sizeOfVarint(sizeOfBodyInBytes));
        ByteUtils.writeVarint(sizeOfBodyInBytes, buf);
        buf.put(attributes);
        ByteUtils.writeVarlong(timestampDelta, buf);
        ByteUtils.writeVarint(offsetDelta, buf);
        ByteUtils.writeVarint(keySize, buf);
        buf.position(buf.limit());

        buf.flip();
        assertDecodingRecordFromBufferThrowsInvalidRecordException(buf);
    }

    @Test
    public void testInvalidKeySizePartial() throws IOException {
        byte attributes = 0;
        long timestampDelta = 2;
        int offsetDelta = 1;
        int sizeOfBodyInBytes = 100;
        int keySize = 105; // use a key size larger than the full message

        ByteBuffer buf = ByteBuffer.allocate(sizeOfBodyInBytes + ByteUtils.sizeOfVarint(sizeOfBodyInBytes));
        ByteUtils.writeVarint(sizeOfBodyInBytes, buf);
        buf.put(attributes);
        ByteUtils.writeVarlong(timestampDelta, buf);
        ByteUtils.writeVarint(offsetDelta, buf);
        ByteUtils.writeVarint(keySize, buf);
        buf.position(buf.limit());

        buf.flip();
        assertPartiallyDecodingRecordsFromBufferThrowsInvalidRecordException(buf);
    }

    @Test
    public void testInvalidValueSize() throws IOException {
        byte attributes = 0;
        long timestampDelta = 2;
        int offsetDelta = 1;
        int sizeOfBodyInBytes = 100;
        int valueSize = 105; // use a value size larger than the full message

        ByteBuffer buf = ByteBuffer.allocate(sizeOfBodyInBytes + ByteUtils.sizeOfVarint(sizeOfBodyInBytes));
        ByteUtils.writeVarint(sizeOfBodyInBytes, buf);
        buf.put(attributes);
        ByteUtils.writeVarlong(timestampDelta, buf);
        ByteUtils.writeVarint(offsetDelta, buf);
        ByteUtils.writeVarint(-1, buf); // null key
        ByteUtils.writeVarint(valueSize, buf);
        buf.position(buf.limit());

        buf.flip();
        assertDecodingRecordFromBufferThrowsInvalidRecordException(buf);
    }

    @Test
    public void testInvalidValueSizePartial() throws IOException {
        byte attributes = 0;
        long timestampDelta = 2;
        int offsetDelta = 1;
        int sizeOfBodyInBytes = 100;
        int valueSize = 105; // use a value size larger than the full message

        ByteBuffer buf = ByteBuffer.allocate(sizeOfBodyInBytes + ByteUtils.sizeOfVarint(sizeOfBodyInBytes));
        ByteUtils.writeVarint(sizeOfBodyInBytes, buf);
        buf.put(attributes);
        ByteUtils.writeVarlong(timestampDelta, buf);
        ByteUtils.writeVarint(offsetDelta, buf);
        ByteUtils.writeVarint(-1, buf); // null key
        ByteUtils.writeVarint(valueSize, buf);
        buf.position(buf.limit());

        buf.flip();
        assertPartiallyDecodingRecordsFromBufferThrowsInvalidRecordException(buf);
    }

    @Test
    public void testInvalidNumHeaders() throws IOException {
        byte attributes = 0;
        long timestampDelta = 2;
        int offsetDelta = 1;
        int sizeOfBodyInBytes = 100;

        ByteBuffer buf = ByteBuffer.allocate(sizeOfBodyInBytes + ByteUtils.sizeOfVarint(sizeOfBodyInBytes));
        ByteUtils.writeVarint(sizeOfBodyInBytes, buf);
        buf.put(attributes);
        ByteUtils.writeVarlong(timestampDelta, buf);
        ByteUtils.writeVarint(offsetDelta, buf);
        ByteUtils.writeVarint(-1, buf); // null key
        ByteUtils.writeVarint(-1, buf); // null value
        ByteUtils.writeVarint(-1, buf); // -1 num.headers, not allowed
        buf.position(buf.limit());

        buf.flip();
        assertDecodingRecordFromBufferThrowsInvalidRecordException(buf);

        ByteBuffer buf2 = ByteBuffer.allocate(sizeOfBodyInBytes + ByteUtils.sizeOfVarint(sizeOfBodyInBytes));
        ByteUtils.writeVarint(sizeOfBodyInBytes, buf2);
        buf2.put(attributes);
        ByteUtils.writeVarlong(timestampDelta, buf2);
        ByteUtils.writeVarint(offsetDelta, buf2);
        ByteUtils.writeVarint(-1, buf2); // null key
        ByteUtils.writeVarint(-1, buf2); // null value
        ByteUtils.writeVarint(sizeOfBodyInBytes, buf2); // more headers than remaining buffer size, not allowed
        buf2.position(buf2.limit());

        buf2.flip();
        assertDecodingRecordFromBufferThrowsInvalidRecordException(buf2);
    }

    @Test
    public void testInvalidNumHeadersPartial() throws IOException {
        byte attributes = 0;
        long timestampDelta = 2;
        int offsetDelta = 1;
        int sizeOfBodyInBytes = 100;

        ByteBuffer buf = ByteBuffer.allocate(sizeOfBodyInBytes + ByteUtils.sizeOfVarint(sizeOfBodyInBytes));
        ByteUtils.writeVarint(sizeOfBodyInBytes, buf);
        buf.put(attributes);
        ByteUtils.writeVarlong(timestampDelta, buf);
        ByteUtils.writeVarint(offsetDelta, buf);
        ByteUtils.writeVarint(-1, buf); // null key
        ByteUtils.writeVarint(-1, buf); // null value
        ByteUtils.writeVarint(-1, buf); // -1 num.headers, not allowed
        buf.position(buf.limit());

        buf.flip();
        assertPartiallyDecodingRecordsFromBufferThrowsInvalidRecordException(buf);
    }

    @Test
    public void testInvalidHeaderKey() throws IOException {
        byte attributes = 0;
        long timestampDelta = 2;
        int offsetDelta = 1;
        int sizeOfBodyInBytes = 100;

        ByteBuffer buf = ByteBuffer.allocate(sizeOfBodyInBytes + ByteUtils.sizeOfVarint(sizeOfBodyInBytes));
        ByteUtils.writeVarint(sizeOfBodyInBytes, buf);
        buf.put(attributes);
        ByteUtils.writeVarlong(timestampDelta, buf);
        ByteUtils.writeVarint(offsetDelta, buf);
        ByteUtils.writeVarint(-1, buf); // null key
        ByteUtils.writeVarint(-1, buf); // null value
        ByteUtils.writeVarint(1, buf);
        ByteUtils.writeVarint(105, buf); // header key too long
        buf.position(buf.limit());

        buf.flip();

        assertDecodingRecordFromBufferThrowsInvalidRecordException(buf);
    }

    @Test
    public void testInvalidHeaderKeyPartial() throws IOException {
        byte attributes = 0;
        long timestampDelta = 2;
        int offsetDelta = 1;
        int sizeOfBodyInBytes = 100;

        ByteBuffer buf = ByteBuffer.allocate(sizeOfBodyInBytes + ByteUtils.sizeOfVarint(sizeOfBodyInBytes));
        ByteUtils.writeVarint(sizeOfBodyInBytes, buf);
        buf.put(attributes);
        ByteUtils.writeVarlong(timestampDelta, buf);
        ByteUtils.writeVarint(offsetDelta, buf);
        ByteUtils.writeVarint(-1, buf); // null key
        ByteUtils.writeVarint(-1, buf); // null value
        ByteUtils.writeVarint(1, buf);
        ByteUtils.writeVarint(105, buf); // header key too long
        buf.position(buf.limit());

        buf.flip();
        assertPartiallyDecodingRecordsFromBufferThrowsInvalidRecordException(buf);
    }

    @Test
    public void testNullHeaderKey() throws IOException {
        byte attributes = 0;
        long timestampDelta = 2;
        int offsetDelta = 1;
        int sizeOfBodyInBytes = 100;

        ByteBuffer buf = ByteBuffer.allocate(sizeOfBodyInBytes + ByteUtils.sizeOfVarint(sizeOfBodyInBytes));
        ByteUtils.writeVarint(sizeOfBodyInBytes, buf);
        buf.put(attributes);
        ByteUtils.writeVarlong(timestampDelta, buf);
        ByteUtils.writeVarint(offsetDelta, buf);
        ByteUtils.writeVarint(-1, buf); // null key
        ByteUtils.writeVarint(-1, buf); // null value
        ByteUtils.writeVarint(1, buf);
        ByteUtils.writeVarint(-1, buf); // null header key not allowed
        buf.position(buf.limit());

        buf.flip();

        assertDecodingRecordFromBufferThrowsInvalidRecordException(buf);
    }

    @Test
    public void testNullHeaderKeyPartial() throws IOException {
        byte attributes = 0;
        long timestampDelta = 2;
        int offsetDelta = 1;
        int sizeOfBodyInBytes = 100;

        ByteBuffer buf = ByteBuffer.allocate(sizeOfBodyInBytes + ByteUtils.sizeOfVarint(sizeOfBodyInBytes));
        ByteUtils.writeVarint(sizeOfBodyInBytes, buf);
        buf.put(attributes);
        ByteUtils.writeVarlong(timestampDelta, buf);
        ByteUtils.writeVarint(offsetDelta, buf);
        ByteUtils.writeVarint(-1, buf); // null key
        ByteUtils.writeVarint(-1, buf); // null value
        ByteUtils.writeVarint(1, buf);
        ByteUtils.writeVarint(-1, buf); // null header key not allowed
        buf.position(buf.limit());

        buf.flip();
        assertPartiallyDecodingRecordsFromBufferThrowsInvalidRecordException(buf);
    }

    @Test
    public void testInvalidHeaderValue() throws IOException {
        byte attributes = 0;
        long timestampDelta = 2;
        int offsetDelta = 1;
        int sizeOfBodyInBytes = 100;

        ByteBuffer buf = ByteBuffer.allocate(sizeOfBodyInBytes + ByteUtils.sizeOfVarint(sizeOfBodyInBytes));
        ByteUtils.writeVarint(sizeOfBodyInBytes, buf);
        buf.put(attributes);
        ByteUtils.writeVarlong(timestampDelta, buf);
        ByteUtils.writeVarint(offsetDelta, buf);
        ByteUtils.writeVarint(-1, buf); // null key
        ByteUtils.writeVarint(-1, buf); // null value
        ByteUtils.writeVarint(1, buf);
        ByteUtils.writeVarint(1, buf);
        buf.put((byte) 1);
        ByteUtils.writeVarint(105, buf); // header value too long
        buf.position(buf.limit());
        buf.flip();

        assertDecodingRecordFromBufferThrowsInvalidRecordException(buf);
    }

    @Test
    public void testInvalidHeaderValuePartial() throws IOException {
        byte attributes = 0;
        long timestampDelta = 2;
        int offsetDelta = 1;
        int sizeOfBodyInBytes = 100;

        ByteBuffer buf = ByteBuffer.allocate(sizeOfBodyInBytes + ByteUtils.sizeOfVarint(sizeOfBodyInBytes));
        ByteUtils.writeVarint(sizeOfBodyInBytes, buf);
        buf.put(attributes);
        ByteUtils.writeVarlong(timestampDelta, buf);
        ByteUtils.writeVarint(offsetDelta, buf);
        ByteUtils.writeVarint(-1, buf); // null key
        ByteUtils.writeVarint(-1, buf); // null value
        ByteUtils.writeVarint(1, buf);
        ByteUtils.writeVarint(1, buf);
        buf.put((byte) 1);
        ByteUtils.writeVarint(105, buf); // header value too long
        buf.position(buf.limit());

        buf.flip();
        assertPartiallyDecodingRecordsFromBufferThrowsInvalidRecordException(buf);
    }

    @Test
    public void testUnderflowReadingTimestamp() throws IOException {
        byte attributes = 0;
        int sizeOfBodyInBytes = 1;
        ByteBuffer buf = ByteBuffer.allocate(sizeOfBodyInBytes + ByteUtils.sizeOfVarint(sizeOfBodyInBytes));
        ByteUtils.writeVarint(sizeOfBodyInBytes, buf);
        buf.put(attributes);
        buf.flip();
        assertDecodingRecordFromBufferThrowsInvalidRecordException(buf);
    }

    @Test
    public void testUnderflowReadingVarlong() throws IOException {
        byte attributes = 0;
        int sizeOfBodyInBytes = 2; // one byte for attributes, one byte for partial timestamp
        ByteBuffer buf = ByteBuffer.allocate(sizeOfBodyInBytes + ByteUtils.sizeOfVarint(sizeOfBodyInBytes) + 1);
        ByteUtils.writeVarint(sizeOfBodyInBytes, buf);
        buf.put(attributes);
        ByteUtils.writeVarlong(156, buf); // needs 2 bytes to represent
        buf.position(buf.limit() - 1);
        buf.flip();
        assertDecodingRecordFromBufferThrowsInvalidRecordException(buf);
    }

    @Test
    public void testInvalidVarlong() throws IOException {
        byte attributes = 0;
        int sizeOfBodyInBytes = 11; // one byte for attributes, 10 bytes for max timestamp
        ByteBuffer buf = ByteBuffer.allocate(sizeOfBodyInBytes + ByteUtils.sizeOfVarint(sizeOfBodyInBytes) + 1);
        ByteUtils.writeVarint(sizeOfBodyInBytes, buf);
        int recordStartPosition = buf.position();

        buf.put(attributes);
        ByteUtils.writeVarlong(Long.MAX_VALUE, buf); // takes 10 bytes
        buf.put(recordStartPosition + 10, Byte.MIN_VALUE); // use an invalid final byte

        buf.flip();

        assertDecodingRecordFromBufferThrowsInvalidRecordException(buf);
    }

    @Test
    public void testSerdeNoSequence() throws IOException {
        ByteBuffer key = ByteBuffer.wrap("hi".getBytes());
        ByteBuffer value = ByteBuffer.wrap("there".getBytes());
        long baseOffset = 37;
        int offsetDelta = 10;
        long baseTimestamp = System.currentTimeMillis();
        long timestampDelta = 323;

        ByteBufferOutputStream out = new SingleByteBufferOutputStream(1024);
        DefaultRecord.writeTo(new DataOutputStream(out), offsetDelta, timestampDelta, key, value, new Header[0]);
        ByteBuffer buffer = out.buffer();
        buffer.flip();

        // test for input stream input
        try (ByteBufferInputStream inpStream = new ByteBufferInputStream(buffer.asReadOnlyBuffer())) {
            DefaultRecord record = DefaultRecord.readFrom(inpStream, baseOffset, baseTimestamp, RecordBatch.NO_SEQUENCE, null, Records.SOFT_MAX_ARRAY_LENGTH);
            assertNotNull(record);
            assertEquals(RecordBatch.NO_SEQUENCE, record.sequence());
        }

        // test for buffer input
        DefaultRecord record = DefaultRecord.readFrom(buffer, baseOffset, baseTimestamp, RecordBatch.NO_SEQUENCE, null);
        assertNotNull(record);
        assertEquals(RecordBatch.NO_SEQUENCE, record.sequence());
    }

    @Test
    public void testInvalidSizeOfBodyInBytes() throws IOException {
        int sizeOfBodyInBytes = 10;
        ByteBuffer buf = ByteBuffer.allocate(5);
        ByteUtils.writeVarint(sizeOfBodyInBytes, buf);
        buf.flip();

        // test for input stream input
        assertDecodingRecordFromBufferThrowsInvalidRecordException(buf);
    }

    // =============================================================================================
    // Configurable per-record decompressed-body-size limit (max.decompressed.message.bytes).
    // Broker decode paths pass the configured limit to the InputStream decoders; a record whose
    // declared (decompressed) body exceeds it is rejected with InvalidRecordException BEFORE
    // allocation, so the limit bounds the allocation. Callers that do not thread a limit use the
    // overloads without one, which apply Records.SOFT_MAX_ARRAY_LENGTH — the array-length ceiling.
    // =============================================================================================

    private static byte[] recordWithForgedBodySize(int declaredBodySize) {
        // Only the leading size varint matters: the guard fires before any body bytes are read.
        ByteBuffer buf = ByteBuffer.allocate(16);
        ByteUtils.writeVarint(declaredBodySize, buf);
        buf.put((byte) 0); // attribute byte, never reached when the guard fires
        buf.flip();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        return bytes;
    }

    // A negative size is forgeable via the zig-zag varint; sizes above the array length limit can
    // never be allocated. Both are rejected before any allocation. SOFT_MAX_ARRAY_LENGTH + 1 pins
    // the exact upper threshold without attempting a ~2 GiB allocation.
    @Test
    public void testReadFromStreamRejectsInvalidBodySize() throws IOException {
        assertReadFromStreamRejectsBodySize(-1, "is negative");
        assertReadFromStreamRejectsBodySize(Integer.MAX_VALUE, "exceeds the configured maximum record size");
        assertReadFromStreamRejectsBodySize(Records.SOFT_MAX_ARRAY_LENGTH + 1, "exceeds the configured maximum record size");
    }

    private static void assertReadFromStreamRejectsBodySize(int declaredBodySize, String expectedMessage) throws IOException {
        byte[] rec = recordWithForgedBodySize(declaredBodySize);
        try (InputStream in = new ByteBufferInputStream(ByteBuffer.wrap(rec))) {
            InvalidRecordException ex = assertThrows(InvalidRecordException.class,
                () -> DefaultRecord.readFrom(in, 0L, 0L, RecordBatch.NO_SEQUENCE, null, Records.SOFT_MAX_ARRAY_LENGTH));
            assertTrue(ex.getMessage().contains(expectedMessage),
                "expected '" + expectedMessage + "', got: " + ex.getMessage());
        }
    }

    @Test
    public void testReadPartiallyFromStreamRejectsInvalidBodySize() throws IOException {
        assertReadPartiallyFromStreamRejectsBodySize(-1, "is negative");
        assertReadPartiallyFromStreamRejectsBodySize(Integer.MAX_VALUE, "exceeds the configured maximum record size");
        assertReadPartiallyFromStreamRejectsBodySize(Records.SOFT_MAX_ARRAY_LENGTH + 1, "exceeds the configured maximum record size");
    }

    private static void assertReadPartiallyFromStreamRejectsBodySize(int declaredBodySize, String expectedMessage) throws IOException {
        byte[] rec = recordWithForgedBodySize(declaredBodySize);
        try (InputStream in = new ByteBufferInputStream(ByteBuffer.wrap(rec))) {
            InvalidRecordException ex = assertThrows(InvalidRecordException.class,
                () -> DefaultRecord.readPartiallyFrom(in, 0L, 0L, RecordBatch.NO_SEQUENCE, null, Records.SOFT_MAX_ARRAY_LENGTH));
            assertTrue(ex.getMessage().contains(expectedMessage),
                "expected '" + expectedMessage + "', got: " + ex.getMessage());
        }
    }

    // The forged body size (1000) is well under the array-length limit but over the configured max
    // (100), so the configurable guard fires (not the array-length guard) before any allocation.
    @Test
    public void testReadFromStreamRejectsBodySizeExceedingConfiguredMax() throws IOException {
        byte[] rec = recordWithForgedBodySize(1000);
        try (InputStream in = new ByteBufferInputStream(ByteBuffer.wrap(rec))) {
            InvalidRecordException ex = assertThrows(InvalidRecordException.class,
                () -> DefaultRecord.readFrom(in, 0L, 0L, RecordBatch.NO_SEQUENCE, null, 100));
            assertTrue(ex.getMessage().contains("exceeds the configured maximum record size"),
                "expected the configured-maximum guard, got: " + ex.getMessage());
        }
    }

    @Test
    public void testReadPartiallyFromStreamRejectsBodySizeExceedingConfiguredMax() throws IOException {
        byte[] rec = recordWithForgedBodySize(1000);
        try (InputStream in = new ByteBufferInputStream(ByteBuffer.wrap(rec))) {
            InvalidRecordException ex = assertThrows(InvalidRecordException.class,
                () -> DefaultRecord.readPartiallyFrom(in, 0L, 0L, RecordBatch.NO_SEQUENCE, null, 100));
            assertTrue(ex.getMessage().contains("exceeds the configured maximum record size"),
                "expected the configured-maximum guard, got: " + ex.getMessage());
        }
    }

    // A genuinely-serialized record decodes when the configured limit is generous, and is rejected
    // (before allocation) when the limit is below its body, proving the guard enforces the limit on
    // real records without rejecting valid ones.
    @Test
    public void testReadFromStreamWithConfiguredMaxAcceptsValidRecordAndRejectsWhenTooSmall() throws IOException {
        ByteBuffer key = ByteBuffer.wrap("hi".getBytes());
        ByteBuffer value = ByteBuffer.wrap("there".getBytes());
        ByteBufferOutputStream out = new SingleByteBufferOutputStream(1024);
        DefaultRecord.writeTo(new DataOutputStream(out), 0, 0L, key, value, new Header[0]);
        ByteBuffer buffer = out.buffer();
        buffer.flip();

        // A generous limit accepts the valid record.
        try (InputStream in = new ByteBufferInputStream(buffer.duplicate())) {
            DefaultRecord record = DefaultRecord.readFrom(in, 0L, 0L, RecordBatch.NO_SEQUENCE, null, 1024);
            assertNotNull(record);
        }
        // A 1-byte limit is below the real body, so the record is rejected before allocation.
        try (InputStream in = new ByteBufferInputStream(buffer.duplicate())) {
            InvalidRecordException ex = assertThrows(InvalidRecordException.class,
                () -> DefaultRecord.readFrom(in, 0L, 0L, RecordBatch.NO_SEQUENCE, null, 1));
            assertTrue(ex.getMessage().contains("exceeds the configured maximum record size"),
                "expected the configured-maximum guard, got: " + ex.getMessage());
        }
    }

    private static void assertPartiallyDecodingRecordsFromBufferThrowsInvalidRecordException(ByteBuffer buf) throws IOException {
        try (InputStream inputStream = new ByteBufferInputStream(buf)) {
            assertThrows(InvalidRecordException.class,
                () -> DefaultRecord.readPartiallyFrom(inputStream, 0L, 0L, RecordBatch.NO_SEQUENCE, null, Records.SOFT_MAX_ARRAY_LENGTH));
        }
    }

    private static void assertDecodingRecordFromBufferThrowsInvalidRecordException(ByteBuffer buf) throws IOException {
        // test for input stream input
        try (ByteBufferInputStream inpStream = new ByteBufferInputStream(buf.asReadOnlyBuffer())) {
            assertThrows(InvalidRecordException.class,
                () -> DefaultRecord.readFrom(inpStream, 0L, 0L, RecordBatch.NO_SEQUENCE, null, Records.SOFT_MAX_ARRAY_LENGTH));
        }
        // test for buffer input
        assertThrows(InvalidRecordException.class,
            () -> DefaultRecord.readFrom(buf, 0L, 0L, RecordBatch.NO_SEQUENCE, null));
    }
}
