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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public final class BufferValue {
    private final ContextualRecord record;
    private final byte[] priorValue;

    BufferValue(final ContextualRecord record,
                final byte[] priorValue) {
        this.record = record;
        this.priorValue = priorValue;
    }

    ContextualRecord record() {
        return record;
    }

    byte[] priorValue() {
        return priorValue;
    }

    static BufferValue deserialize(final ByteBuffer buffer) {
        final ContextualRecord record = ContextualRecord.deserialize(buffer);

        final int priorValueLength = buffer.getInt();
        if (priorValueLength == -1) {
            return new BufferValue(record, null);
        } else {
            final byte[] priorValue = new byte[priorValueLength];
            buffer.get(priorValue);
            return new BufferValue(record, priorValue);
        }
    }

    ByteBuffer serialize(final int endPadding) {

        final int sizeOfPriorValueLength = Integer.BYTES;
        final int sizeOfPriorValue = priorValue == null ? 0 : priorValue.length;

        final ByteBuffer buffer = record.serialize(sizeOfPriorValueLength + sizeOfPriorValue + endPadding);

        if (priorValue == null) {
            buffer.putInt(-1);
        } else {
            buffer.putInt(priorValue.length);
            buffer.put(priorValue);
        }

        return buffer;
    }

    long sizeBytes() {
        return (priorValue == null ? 0 : priorValue.length) + record.sizeBytes();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final BufferValue that = (BufferValue) o;
        return Objects.equals(record, that.record) &&
            Arrays.equals(priorValue, that.priorValue);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(record);
        result = 31 * result + Arrays.hashCode(priorValue);
        return result;
    }

    @Override
    public String toString() {
        return "BufferValue{" +
            "record=" + record +
            ", priorValue=" + Arrays.toString(priorValue) +
            '}';
    }
}
