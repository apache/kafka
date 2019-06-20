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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public final class BufferValue {
    private static final int NULL_VALUE_SENTINEL = -1;
    private static final int OLD_PREV_DUPLICATE_VALUE_SENTINEL = -2;
    private final byte[] priorValue;
    private final byte[] oldValue;
    private final byte[] newValue;
    private final ProcessorRecordContext recordContext;

    BufferValue(final byte[] priorValue,
                final byte[] oldValue,
                final byte[] newValue,
                final ProcessorRecordContext recordContext) {
        this.oldValue = oldValue;
        this.newValue = newValue;
        this.recordContext = recordContext;

        // This de-duplicates the prior and old references.
        // If they were already the same reference, the comparison is trivially fast, so we don't specifically check
        // for that case.
        if (Arrays.equals(priorValue, oldValue)) {
            this.priorValue = oldValue;
        } else {
            this.priorValue = priorValue;
        }
    }

    byte[] priorValue() {
        return priorValue;
    }

    byte[] oldValue() {
        return oldValue;
    }

    byte[] newValue() {
        return newValue;
    }

    ProcessorRecordContext context() {
        return recordContext;
    }

    static BufferValue deserialize(final ByteBuffer buffer) {
        final ProcessorRecordContext context = ProcessorRecordContext.deserialize(buffer);

        final byte[] priorValue = extractValue(buffer);

        final byte[] oldValue;
        final int oldValueLength = buffer.getInt();
        if (oldValueLength == NULL_VALUE_SENTINEL) {
            oldValue = null;
        } else if (oldValueLength == OLD_PREV_DUPLICATE_VALUE_SENTINEL) {
            oldValue = priorValue;
        } else {
            oldValue = new byte[oldValueLength];
            buffer.get(oldValue);
        }

        final byte[] newValue = extractValue(buffer);

        return new BufferValue(priorValue, oldValue, newValue, context);
    }

    private static byte[] extractValue(final ByteBuffer buffer) {
        final int valueLength = buffer.getInt();
        if (valueLength == NULL_VALUE_SENTINEL) {
            return null;
        } else {
            final byte[] value = new byte[valueLength];
            buffer.get(value);
            return value;
        }
    }

    ByteBuffer serialize(final int endPadding) {

        final int sizeOfValueLength = Integer.BYTES;

        final int sizeOfPriorValue = priorValue == null ? 0 : priorValue.length;
        final int sizeOfOldValue = oldValue == null || priorValue == oldValue ? 0 : oldValue.length;
        final int sizeOfNewValue = newValue == null ? 0 : newValue.length;

        final byte[] serializedContext = recordContext.serialize();

        final ByteBuffer buffer = ByteBuffer.allocate(
            serializedContext.length
                + sizeOfValueLength + sizeOfPriorValue
                + sizeOfValueLength + sizeOfOldValue
                + sizeOfValueLength + sizeOfNewValue
                + endPadding
        );

        buffer.put(serializedContext);

        addValue(buffer, priorValue);

        if (oldValue == null) {
            buffer.putInt(NULL_VALUE_SENTINEL);
        } else if (priorValue == oldValue) {
            buffer.putInt(OLD_PREV_DUPLICATE_VALUE_SENTINEL);
        } else {
            buffer.putInt(sizeOfOldValue);
            buffer.put(oldValue);
        }

        addValue(buffer, newValue);

        return buffer;
    }

    private static void addValue(final ByteBuffer buffer, final byte[] value) {
        if (value == null) {
            buffer.putInt(NULL_VALUE_SENTINEL);
        } else {
            buffer.putInt(value.length);
            buffer.put(value);
        }
    }

    long residentMemorySizeEstimate() {
        return (priorValue == null ? 0 : priorValue.length)
            + (oldValue == null || priorValue == oldValue ? 0 : oldValue.length)
            + (newValue == null ? 0 : newValue.length)
            + recordContext.residentMemorySizeEstimate();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final BufferValue that = (BufferValue) o;
        return Arrays.equals(priorValue, that.priorValue) &&
            Arrays.equals(oldValue, that.oldValue) &&
            Arrays.equals(newValue, that.newValue) &&
            Objects.equals(recordContext, that.recordContext);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(recordContext);
        result = 31 * result + Arrays.hashCode(priorValue);
        result = 31 * result + Arrays.hashCode(oldValue);
        result = 31 * result + Arrays.hashCode(newValue);
        return result;
    }

    @Override
    public String toString() {
        return "BufferValue{" +
            "priorValue=" + Arrays.toString(priorValue) +
            ", oldValue=" + Arrays.toString(oldValue) +
            ", newValue=" + Arrays.toString(newValue) +
            ", recordContext=" + recordContext +
            '}';
    }
}
