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

import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.FullChangeSerde;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class BufferValue {
    private static final int NULL_VALUE_SENTINEL = -1;
    private static final int OLD_PREV_DUPLICATE_VALUE_SENTINEL = -2;
    private final byte[] priorValue;
    private final byte[] oldValue;
    private final byte[] newValue;
    private final ProcessorRecordContext recordContext;

    public static void main(String[] args) {
        final String str="00000172D1434655000000000000006F0000000C6C6F67732D696E6772657373000000180000000000000059FFFFFFFF00000051080110AC051A2436346139616437662D313838352D346234342D613163302D633134346565633162636665222436346139616437662D313838352D346234342D613163302D633134346565633162636665FFFFFFFF00000172D1434655";
        final byte[] arr = hexStringToByteArray(str);

        // in this case, the changelog value is a serialized ContextualRecord
        final ByteBuffer timeAndValue = ByteBuffer.wrap(arr);
        final long time = timeAndValue.getLong();
        final byte[] changelogValue = new byte[arr.length - 8];
        timeAndValue.get(changelogValue);

        final ContextualRecord contextualRecord = ContextualRecord.deserialize(ByteBuffer.wrap(changelogValue));
        final Change<byte[]> change = requireNonNull(FullChangeSerde.decomposeLegacyFormattedArrayIntoChangeArrays(contextualRecord.value()));

        System.out.println(change);

        final BufferValue deserialize = deserialize(ByteBuffer.wrap(arr));
        System.out.println(deserialize);
    }

    public static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                + Character.digit(s.charAt(i+1), 16));
        }
        return data;
    }

    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    public static String bytesToHex(byte[] bytes) {
        if (bytes == null) {
            return "null";
        }
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }

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
        } else if (Arrays.equals(priorValue, oldValue)) {
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
            "priorValue=0x" + bytesToHex(priorValue) +
            ", oldValue=0x" + bytesToHex(oldValue) +
            ", newValue=0x" + bytesToHex(newValue) +
            ", recordContext=" + recordContext +
            '}';
    }
}
