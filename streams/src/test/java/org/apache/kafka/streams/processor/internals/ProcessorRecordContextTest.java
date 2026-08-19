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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ProcessorRecordContextTest {
    // timestamp + offset + partition: 8 + 8 + 4
    private static final long MIN_SIZE = 20L;

    @Test
    public void shouldNotAllowNullHeaders() {
        assertThrows(
            NullPointerException.class,
            () -> new ProcessorRecordContext(
                42L,
                73L,
                0,
                "topic",
                null
            )
        );
    }

    @Test
    public void shouldEstimateNullTopicAndEmptyHeadersAsZeroLength() {
        final ProcessorRecordContext context = new ProcessorRecordContext(
            42L,
            73L,
            0,
            null,
            new RecordHeaders()
        );

        assertEquals(MIN_SIZE, context.residentMemorySizeEstimate());
    }

    @Test
    public void shouldEstimateEmptyHeaderAsZeroLength() {
        final ProcessorRecordContext context = new ProcessorRecordContext(
            42L,
            73L,
            0,
            null,
            new RecordHeaders()
        );

        assertEquals(MIN_SIZE, context.residentMemorySizeEstimate());
    }

    @Test
    public void shouldEstimateTopicLength() {
        final ProcessorRecordContext context = new ProcessorRecordContext(
            42L,
            73L,
            0,
            "topic",
            new RecordHeaders()
        );

        assertEquals(MIN_SIZE + 5L, context.residentMemorySizeEstimate());
    }

    @Test
    public void shouldEstimateHeadersLength() {
        final Headers headers = new RecordHeaders();
        headers.add("header-key", "header-value".getBytes());
        final ProcessorRecordContext context = new ProcessorRecordContext(
            42L,
            73L,
            0,
            null,
            headers
        );

        assertEquals(MIN_SIZE + 10L + 12L, context.residentMemorySizeEstimate());
    }

    @Test
    public void shouldEstimateNullValueInHeaderAsZero() {
        final Headers headers = new RecordHeaders();
        headers.add("header-key", null);
        final ProcessorRecordContext context = new ProcessorRecordContext(
            42L,
            73L,
            0,
            null,
            headers
        );

        assertEquals(MIN_SIZE + 10L, context.residentMemorySizeEstimate());
    }

    @Test
    public void shouldRejectHeaderCountLargerThanRemainingBuffer() {
        final Headers headers = new RecordHeaders();
        headers.add("header-key", "header-value".getBytes(UTF_8));
        final ProcessorRecordContext context = new ProcessorRecordContext(
                42L, 73L, 0, "topic", headers);

        final byte[] serialized = context.serialize();
        final ByteBuffer buffer = ByteBuffer.wrap(serialized);

        // Locate headerCount's position within the real serialized bytes
        // timestamp(8) + offset(8) + topicLen(4) + "topic"(5) + partition(4)
        final int headerCountOffset = 8 + 8 + 4 + "topic".getBytes(UTF_8).length + 4;
        final int bytesAfterHeaderCount = serialized.length - (headerCountOffset + 4);
        final int maxPlausibleHeaderCount = bytesAfterHeaderCount / (2 * Integer.BYTES);

        // Overwrite the real headerCount with a little more that it could support
        buffer.putInt(headerCountOffset, maxPlausibleHeaderCount + 1);

        assertThrows(SerializationException.class, () -> ProcessorRecordContext.deserialize(buffer));
    }

    @Test
    public void shouldDeserializeValidHeaderCountWithoutRejecting() {
        final Headers headers = new RecordHeaders();
        headers.add("header-key", "header-value".getBytes(UTF_8));
        final ProcessorRecordContext context = new ProcessorRecordContext(
                42L, 73L, 0, "topic", headers);

        final ProcessorRecordContext roundTripped =
                ProcessorRecordContext.deserialize(ByteBuffer.wrap(context.serialize()));

        assertEquals(context, roundTripped);
    }

    @Test
    public void shouldRejectNegativeHeaderCount() {
        final ByteBuffer buffer = ByteBuffer.allocate(28);
        buffer.putLong(0L);    // timestamp
        buffer.putLong(0L);    // offset
        buffer.putInt(0);      // topicLen
        buffer.putInt(0);      // partition
        buffer.putInt(-2);     // headerCount, negative but not the -1 sentinel
        buffer.flip();

        assertThrows(SerializationException.class, () -> ProcessorRecordContext.deserialize(buffer));
    }
}
