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

package org.apache.kafka.common.protocol.types;

import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@Timeout(120)
public class RawTaggedFieldWriterTest {

    @Test
    public void testWritingZeroRawTaggedFields() {
        RawTaggedFieldWriter writer = RawTaggedFieldWriter.forFields(null);
        assertEquals(0, writer.numFields());
        ByteBufferAccessor accessor = new ByteBufferAccessor(ByteBuffer.allocate(0));
        writer.writeRawTags(accessor, Integer.MAX_VALUE);
    }

    @Test
    public void testWritingSeveralRawTaggedFields() {
        List<RawTaggedField> tags = Arrays.asList(
            new RawTaggedField(2, new byte[] {0x1, 0x2, 0x3}),
            new RawTaggedField(5, new byte[] {0x4, 0x5})
        );
        RawTaggedFieldWriter writer = RawTaggedFieldWriter.forFields(tags);
        assertEquals(2, writer.numFields());
        byte[] arr = new byte[9];
        ByteBufferAccessor accessor = new ByteBufferAccessor(ByteBuffer.wrap(arr));
        writer.writeRawTags(accessor, 1);
        assertArrayEquals(new byte[] {0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}, arr);
        writer.writeRawTags(accessor, 3);
        assertArrayEquals(new byte[] {0x2, 0x3, 0x1, 0x2, 0x3, 0x0, 0x0, 0x0, 0x0}, arr);
        writer.writeRawTags(accessor, 7);
        assertArrayEquals(new byte[] {0x2, 0x3, 0x1, 0x2, 0x3, 0x5, 0x2, 0x4, 0x5}, arr);
        writer.writeRawTags(accessor, Integer.MAX_VALUE);
        assertArrayEquals(new byte[] {0x2, 0x3, 0x1, 0x2, 0x3, 0x5, 0x2, 0x4, 0x5}, arr);
    }

    @Test
    public void testInvalidNextDefinedTag() {
        List<RawTaggedField> tags = Arrays.asList(
            new RawTaggedField(2, new byte[] {0x1, 0x2, 0x3}),
            new RawTaggedField(5, new byte[] {0x4, 0x5, 0x6}),
            new RawTaggedField(7, new byte[] {0x0})
        );
        RawTaggedFieldWriter writer = RawTaggedFieldWriter.forFields(tags);
        assertEquals(3, writer.numFields());
        try {
            writer.writeRawTags(new ByteBufferAccessor(ByteBuffer.allocate(1024)), 2);
            fail("expected to get RuntimeException");
        } catch (RuntimeException e) {
            assertEquals("Attempted to use tag 2 as an undefined tag.", e.getMessage());
        }
    }

    @Test
    public void testOutOfOrderTags() {
        List<RawTaggedField> tags = Arrays.asList(
            new RawTaggedField(5, new byte[] {0x4, 0x5, 0x6}),
            new RawTaggedField(2, new byte[] {0x1, 0x2, 0x3}),
            new RawTaggedField(7, new byte[] {0x0 })
        );
        RawTaggedFieldWriter writer = RawTaggedFieldWriter.forFields(tags);
        assertEquals(3, writer.numFields());
        try {
            writer.writeRawTags(new ByteBufferAccessor(ByteBuffer.allocate(1024)), 8);
            fail("expected to get RuntimeException");
        } catch (RuntimeException e) {
            assertEquals("Invalid raw tag field list: tag 2 comes after tag 5, but is " +
                "not higher than it.", e.getMessage());
        }
    }
}
