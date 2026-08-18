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
import org.apache.kafka.common.message.ControlRecordTypeSchema;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.MessageUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Control records specify a schema for the record key which includes a version and type:
 *
 * Key => Version Type
 *   Version => Int16
 *   Type => Int16
 *
 * In the future, the version can be bumped to indicate a new schema, but it must be backwards compatible
 * with the current schema. In general, this means we can add new fields, but we cannot remove old ones.
 *
 * Note that control records are not considered for compaction by the log cleaner.
 *
 * The schema for the value field is left to the control record type to specify.
 */
public enum ControlRecordType {
    ABORT((short) 0),
    COMMIT((short) 1),

    // KRaft quorum related control messages
    LEADER_CHANGE((short) 2),
    SNAPSHOT_HEADER((short) 3),
    SNAPSHOT_FOOTER((short) 4),

    // KRaft membership changes messages
    KRAFT_VERSION((short) 5),
    KRAFT_VOTERS((short) 6),

    // UNKNOWN is used to indicate a control type which the client is not aware of and should be ignored
    UNKNOWN((short) -1);

    private static final Logger log = LoggerFactory.getLogger(ControlRecordType.class);
    private static final int CONTROL_RECORD_KEY_SIZE = 4;

    private final short type;
    private final ByteBuffer buffer;

    ControlRecordType(short type) {
        this.type = type;
        ControlRecordTypeSchema schema = new ControlRecordTypeSchema().setType(type);
        buffer = MessageUtil.toVersionPrefixedByteBuffer(ControlRecordTypeSchema.HIGHEST_SUPPORTED_VERSION, schema);
    }

    public short type() {
        return type;
    }

    public ByteBuffer recordKey() {
        if (this == UNKNOWN)
            throw new IllegalArgumentException("Cannot serialize UNKNOWN control record type");
        return buffer.duplicate();
    }

    public int controlRecordKeySize() {
        return buffer.remaining();
    }

    public static short parseTypeId(ByteBuffer key) {
        // We should duplicate the original buffer since it will be read again in some cases, for example,
        // read by KafkaRaftClient and RaftClient.Listener
        ByteBuffer buffer = key.duplicate();
        if (buffer.remaining() < CONTROL_RECORD_KEY_SIZE)
            throw new InvalidRecordException("Invalid value size found for control record key. " +
                    "Must have at least " + CONTROL_RECORD_KEY_SIZE + " bytes, but found only " + buffer.remaining());

        short version = buffer.getShort();
        if (version < ControlRecordTypeSchema.LOWEST_SUPPORTED_VERSION)
            throw new InvalidRecordException("Invalid version found for control record: " + version +
                    ". May indicate data corruption");

        if (version > ControlRecordTypeSchema.HIGHEST_SUPPORTED_VERSION) {
            log.debug("Received unknown control record key version {}. Parsing as version {}", version,
                    ControlRecordTypeSchema.HIGHEST_SUPPORTED_VERSION);
            version = ControlRecordTypeSchema.HIGHEST_SUPPORTED_VERSION;
        }
        ControlRecordTypeSchema schema = new ControlRecordTypeSchema(new ByteBufferAccessor(buffer), version);
        return schema.type();
    }

    public static ControlRecordType fromTypeId(short typeId) {
        switch (typeId) {
            case 0:
                return ABORT;
            case 1:
                return COMMIT;
            case 2:
                return LEADER_CHANGE;
            case 3:
                return SNAPSHOT_HEADER;
            case 4:
                return SNAPSHOT_FOOTER;
            case 5:
                return KRAFT_VERSION;
            case 6:
                return KRAFT_VOTERS;

            default:
                return UNKNOWN;
        }
    }

    public static ControlRecordType parse(ByteBuffer key) {
        return fromTypeId(parseTypeId(key));
    }
}
