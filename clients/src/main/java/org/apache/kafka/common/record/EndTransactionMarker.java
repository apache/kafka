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

import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * This class represents the control record which is written to the log to indicate the completion
 * of a transaction. The record key specifies the {@link ControlRecordType control type} and the
 * value embeds information useful for write validation (for now, just the coordinator epoch).
 */
public class EndTransactionMarker {
    private static final Logger log = LoggerFactory.getLogger(EndTransactionMarker.class);

    private static final short CURRENT_END_TXN_MARKER_VERSION = 0;
    private static final Schema END_TXN_MARKER_SCHEMA_VERSION_V0 = new Schema(
            new Field("version", Type.INT16),
            new Field("coordinator_epoch", Type.INT32));
    static final int CURRENT_END_TXN_MARKER_VALUE_SIZE = 6;
    static final int CURRENT_END_TXN_SCHEMA_RECORD_SIZE = DefaultRecord.sizeInBytes(0, 0L,
            ControlRecordType.CURRENT_CONTROL_RECORD_KEY_SIZE,
            EndTransactionMarker.CURRENT_END_TXN_MARKER_VALUE_SIZE,
            Record.EMPTY_HEADERS);

    private final ControlRecordType type;
    private final int coordinatorEpoch;

    public EndTransactionMarker(ControlRecordType type, int coordinatorEpoch) {
        ensureTransactionMarkerControlType(type);
        this.type = type;
        this.coordinatorEpoch = coordinatorEpoch;
    }

    public int coordinatorEpoch() {
        return coordinatorEpoch;
    }

    public ControlRecordType controlType() {
        return type;
    }

    private Struct buildRecordValue() {
        Struct struct = new Struct(END_TXN_MARKER_SCHEMA_VERSION_V0);
        struct.set("version", CURRENT_END_TXN_MARKER_VERSION);
        struct.set("coordinator_epoch", coordinatorEpoch);
        return struct;
    }

    public ByteBuffer serializeValue() {
        Struct valueStruct = buildRecordValue();
        ByteBuffer value = ByteBuffer.allocate(valueStruct.sizeOf());
        valueStruct.writeTo(value);
        value.flip();
        return value;
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
            throw new IllegalArgumentException("Invalid control record type for end transaction marker" + type);
    }

    public static EndTransactionMarker deserialize(Record record) {
        ControlRecordType type = ControlRecordType.parse(record.key());
        return deserializeValue(type, record.value());
    }

    static EndTransactionMarker deserializeValue(ControlRecordType type, ByteBuffer value) {
        ensureTransactionMarkerControlType(type);

        if (value.remaining() < CURRENT_END_TXN_MARKER_VALUE_SIZE)
            throw new InvalidRecordException("Invalid value size found for end transaction marker. Must have " +
                    "at least " + CURRENT_END_TXN_MARKER_VALUE_SIZE + " bytes, but found only " + value.remaining());

        short version = value.getShort(0);
        if (version < 0)
            throw new InvalidRecordException("Invalid version found for end transaction marker: " + version +
                    ". May indicate data corruption");

        if (version > CURRENT_END_TXN_MARKER_VERSION)
            log.debug("Received end transaction marker value version {}. Parsing as version {}", version,
                    CURRENT_END_TXN_MARKER_VERSION);

        int coordinatorEpoch = value.getInt(2);
        return new EndTransactionMarker(type, coordinatorEpoch);
    }

}
