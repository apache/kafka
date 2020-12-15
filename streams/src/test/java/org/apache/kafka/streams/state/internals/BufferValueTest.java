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

import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class BufferValueTest {
    @Test
    public void shouldDeduplicateNullValues() {
        final BufferValue bufferValue = new BufferValue(null, null, null, null);
        assertSame(bufferValue.priorValue(), bufferValue.oldValue());
    }

    @Test
    public void shouldDeduplicateIndenticalValues() {
        final byte[] bytes = {(byte) 0};
        final BufferValue bufferValue = new BufferValue(bytes, bytes, null, null);
        assertSame(bufferValue.priorValue(), bufferValue.oldValue());
    }

    @Test
    public void shouldDeduplicateEqualValues() {
        final BufferValue bufferValue = new BufferValue(new byte[] {(byte) 0}, new byte[] {(byte) 0}, null, null);
        assertSame(bufferValue.priorValue(), bufferValue.oldValue());
    }

    @Test
    public void shouldStoreDifferentValues() {
        final byte[] priorValue = {(byte) 0};
        final byte[] oldValue = {(byte) 1};
        final BufferValue bufferValue = new BufferValue(priorValue, oldValue, null, null);
        assertSame(priorValue, bufferValue.priorValue());
        assertSame(oldValue, bufferValue.oldValue());
        assertNotEquals(bufferValue.priorValue(), bufferValue.oldValue());
    }

    @Test
    public void shouldStoreDifferentValuesWithPriorNull() {
        final byte[] priorValue = null;
        final byte[] oldValue = {(byte) 1};
        final BufferValue bufferValue = new BufferValue(priorValue, oldValue, null, null);
        assertNull(bufferValue.priorValue());
        assertSame(oldValue, bufferValue.oldValue());
        assertNotEquals(bufferValue.priorValue(), bufferValue.oldValue());
    }

    @Test
    public void shouldStoreDifferentValuesWithOldNull() {
        final byte[] priorValue = {(byte) 0};
        final byte[] oldValue = null;
        final BufferValue bufferValue = new BufferValue(priorValue, oldValue, null, null);
        assertSame(priorValue, bufferValue.priorValue());
        assertNull(bufferValue.oldValue());
        assertNotEquals(bufferValue.priorValue(), bufferValue.oldValue());
    }

    @Test
    public void shouldAccountForDeduplicationInSizeEstimate() {
        final ProcessorRecordContext context = new ProcessorRecordContext(0L, 0L, 0, "topic", null);
        assertEquals(25L, new BufferValue(null, null, null, context).residentMemorySizeEstimate());
        assertEquals(26L, new BufferValue(new byte[] {(byte) 0}, null, null, context).residentMemorySizeEstimate());
        assertEquals(26L, new BufferValue(null, new byte[] {(byte) 0}, null, context).residentMemorySizeEstimate());
        assertEquals(26L, new BufferValue(new byte[] {(byte) 0}, new byte[] {(byte) 0}, null, context).residentMemorySizeEstimate());
        assertEquals(27L, new BufferValue(new byte[] {(byte) 0}, new byte[] {(byte) 1}, null, context).residentMemorySizeEstimate());

        // new value should get counted, but doesn't get deduplicated
        assertEquals(28L, new BufferValue(new byte[] {(byte) 0}, new byte[] {(byte) 1}, new byte[] {(byte) 0}, context).residentMemorySizeEstimate());
    }

    @Test
    public void shouldSerializeNulls() {
        final ProcessorRecordContext context = new ProcessorRecordContext(0L, 0L, 0, "topic", null);
        final byte[] serializedContext = context.serialize();
        final byte[] bytes = new BufferValue(null, null, null, context).serialize(0).array();
        final byte[] withoutContext = Arrays.copyOfRange(bytes, serializedContext.length, bytes.length);

        assertThat(withoutContext, is(ByteBuffer.allocate(Integer.BYTES * 3).putInt(-1).putInt(-1).putInt(-1).array()));
    }

    @Test
    public void shouldSerializePrior() {
        final ProcessorRecordContext context = new ProcessorRecordContext(0L, 0L, 0, "topic", null);
        final byte[] serializedContext = context.serialize();
        final byte[] priorValue = {(byte) 5};
        final byte[] bytes = new BufferValue(priorValue, null, null, context).serialize(0).array();
        final byte[] withoutContext = Arrays.copyOfRange(bytes, serializedContext.length, bytes.length);

        assertThat(withoutContext, is(ByteBuffer.allocate(Integer.BYTES * 3 + 1).putInt(1).put(priorValue).putInt(-1).putInt(-1).array()));
    }

    @Test
    public void shouldSerializeOld() {
        final ProcessorRecordContext context = new ProcessorRecordContext(0L, 0L, 0, "topic", null);
        final byte[] serializedContext = context.serialize();
        final byte[] oldValue = {(byte) 5};
        final byte[] bytes = new BufferValue(null, oldValue, null, context).serialize(0).array();
        final byte[] withoutContext = Arrays.copyOfRange(bytes, serializedContext.length, bytes.length);

        assertThat(withoutContext, is(ByteBuffer.allocate(Integer.BYTES * 3 + 1).putInt(-1).putInt(1).put(oldValue).putInt(-1).array()));
    }

    @Test
    public void shouldSerializeNew() {
        final ProcessorRecordContext context = new ProcessorRecordContext(0L, 0L, 0, "topic", null);
        final byte[] serializedContext = context.serialize();
        final byte[] newValue = {(byte) 5};
        final byte[] bytes = new BufferValue(null, null, newValue, context).serialize(0).array();
        final byte[] withoutContext = Arrays.copyOfRange(bytes, serializedContext.length, bytes.length);

        assertThat(withoutContext, is(ByteBuffer.allocate(Integer.BYTES * 3 + 1).putInt(-1).putInt(-1).putInt(1).put(newValue).array()));
    }

    @Test
    public void shouldCompactDuplicates() {
        final ProcessorRecordContext context = new ProcessorRecordContext(0L, 0L, 0, "topic", null);
        final byte[] serializedContext = context.serialize();
        final byte[] duplicate = {(byte) 5};
        final byte[] bytes = new BufferValue(duplicate, duplicate, null, context).serialize(0).array();
        final byte[] withoutContext = Arrays.copyOfRange(bytes, serializedContext.length, bytes.length);

        assertThat(withoutContext, is(ByteBuffer.allocate(Integer.BYTES * 3 + 1).putInt(1).put(duplicate).putInt(-2).putInt(-1).array()));
    }

    @Test
    public void shouldDeserializePrior() {
        final ProcessorRecordContext context = new ProcessorRecordContext(0L, 0L, 0, "topic", null);
        final byte[] serializedContext = context.serialize();
        final byte[] priorValue = {(byte) 5};
        final ByteBuffer serialValue =
            ByteBuffer
                .allocate(serializedContext.length + Integer.BYTES * 3 + priorValue.length)
                .put(serializedContext).putInt(1).put(priorValue).putInt(-1).putInt(-1);
        serialValue.position(0);

        final BufferValue deserialize = BufferValue.deserialize(serialValue);
        assertThat(deserialize, is(new BufferValue(priorValue, null, null, context)));
    }

    @Test
    public void shouldDeserializeOld() {
        final ProcessorRecordContext context = new ProcessorRecordContext(0L, 0L, 0, "topic", null);
        final byte[] serializedContext = context.serialize();
        final byte[] oldValue = {(byte) 5};
        final ByteBuffer serialValue =
            ByteBuffer
                .allocate(serializedContext.length + Integer.BYTES * 3 + oldValue.length)
                .put(serializedContext).putInt(-1).putInt(1).put(oldValue).putInt(-1);
        serialValue.position(0);

        assertThat(BufferValue.deserialize(serialValue), is(new BufferValue(null, oldValue, null, context)));
    }

    @Test
    public void shouldDeserializeNew() {
        final ProcessorRecordContext context = new ProcessorRecordContext(0L, 0L, 0, "topic", null);
        final byte[] serializedContext = context.serialize();
        final byte[] newValue = {(byte) 5};
        final ByteBuffer serialValue =
            ByteBuffer
                .allocate(serializedContext.length + Integer.BYTES * 3 + newValue.length)
                .put(serializedContext).putInt(-1).putInt(-1).putInt(1).put(newValue);
        serialValue.position(0);

        assertThat(BufferValue.deserialize(serialValue), is(new BufferValue(null, null, newValue, context)));
    }

    @Test
    public void shouldDeserializeCompactedDuplicates() {
        final ProcessorRecordContext context = new ProcessorRecordContext(0L, 0L, 0, "topic", null);
        final byte[] serializedContext = context.serialize();
        final byte[] duplicate = {(byte) 5};
        final ByteBuffer serialValue =
            ByteBuffer
                .allocate(serializedContext.length + Integer.BYTES * 3 + duplicate.length)
                .put(serializedContext).putInt(1).put(duplicate).putInt(-2).putInt(-1);
        serialValue.position(0);

        final BufferValue bufferValue = BufferValue.deserialize(serialValue);
        assertThat(bufferValue, is(new BufferValue(duplicate, duplicate, null, context)));
        assertSame(bufferValue.priorValue(), bufferValue.oldValue());
    }
}
