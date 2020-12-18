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
package org.apache.kafka.common.record;

import org.apache.kafka.common.message.EndTxnMarker;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.MessageUtil;

import java.nio.ByteBuffer;

/**
 * This class represents the control record which is written to the log to indicate the completion
 * of a transaction. The record key specifies the {@link ControlRecordType control type} and the
 * value embeds information useful for write validation (for now, just the coordinator epoch).
 */
public class EndTransactionMarker {

    private final ControlRecordType type;
    private final int coordinatorEpoch;
    private final ByteBuffer buffer;

    public EndTransactionMarker(ControlRecordType type, int coordinatorEpoch) {
        ensureTransactionMarkerControlType(type);
        this.type = type;
        this.coordinatorEpoch = coordinatorEpoch;
        EndTxnMarker marker = new EndTxnMarker().setCoordinatorEpoch(coordinatorEpoch);
        this.buffer = MessageUtil.toVersionPrefixedByteBuffer(EndTxnMarker.HIGHEST_SUPPORTED_VERSION, marker);
    }

    public int coordinatorEpoch() {
        return coordinatorEpoch;
    }

    public ControlRecordType controlType() {
        return type;
    }

    public ByteBuffer serializeValue() {
        return buffer.duplicate();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EndTransactionMarker that = (EndTransactionMarker) o;
        return coordinatorEpoch == that.coordinatorEpoch && type == that.type;
    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + coordinatorEpoch;
        return result;
    }

    private static void ensureTransactionMarkerControlType(ControlRecordType type) {
        if (type != ControlRecordType.COMMIT && type != ControlRecordType.ABORT)
            throw new IllegalArgumentException("Invalid control record type for end transaction marker " + type);
    }

    public static EndTransactionMarker deserialize(Record record) {
        ControlRecordType type = ControlRecordType.parse(record.key());
        return deserializeValue(type, record.value());
    }

    // Visible for testing
    static EndTransactionMarker deserializeValue(ControlRecordType type, ByteBuffer value) {
        ensureTransactionMarkerControlType(type);

        short version = value.getShort();
        EndTxnMarker marker = new EndTxnMarker(new ByteBufferAccessor(value), version);
        return new EndTransactionMarker(type, marker.coordinatorEpoch());
    }

    public int endTxnMarkerValueSize() {
        return DefaultRecord.sizeInBytes(0, 0L,
                ControlRecordType.CURRENT_CONTROL_RECORD_KEY_SIZE,
                buffer.remaining(),
                Record.EMPTY_HEADERS);
    }

}
