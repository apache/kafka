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
    COMMIT((short) 0),
    ABORT((short) 1),

    // UNKNOWN is used to indicate a control type which the client is not aware of and should be ignored
    UNKNOWN((short) -1);

    private static final Logger log = LoggerFactory.getLogger(ControlRecordType.class);

    static final short CURRENT_CONTROL_RECORD_KEY_VERSION = 0;
    private static final Schema CONTROL_RECORD_KEY_SCHEMA_VERSION_V0 = new Schema(
            new Field("version", Type.INT16),
            new Field("type", Type.INT16));

    final short type;

    ControlRecordType(short type) {
        this.type = type;
    }

    public Struct recordKey() {
        if (this == UNKNOWN)
            throw new IllegalArgumentException("Cannot serialize UNKNOWN control record type");

        Struct struct = new Struct(CONTROL_RECORD_KEY_SCHEMA_VERSION_V0);
        struct.set("version", CURRENT_CONTROL_RECORD_KEY_VERSION);
        struct.set("type", type);
        return struct;
    }

    public static ControlRecordType parse(ByteBuffer key) {
        short version = key.getShort(0);
        if (version != CURRENT_CONTROL_RECORD_KEY_VERSION)
            log.debug("Received unknown control record key version {}. Parsing as version {}" +
                    version, CURRENT_CONTROL_RECORD_KEY_VERSION);
        short type = key.getShort(2);
        switch (type) {
            case 0:
                return COMMIT;
            case 1:
                return ABORT;
            default:
                return UNKNOWN;
        }
    }
}
