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

import org.apache.kafka.common.message.KRaftVersionRecord;
import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.message.SnapshotFooterRecord;
import org.apache.kafka.common.message.SnapshotHeaderRecord;
import org.apache.kafka.common.message.VotersRecord;
import org.apache.kafka.common.protocol.ByteBufferAccessor;

import java.nio.ByteBuffer;

/**
 * Utility class for easy interaction with control records.
 */
public class ControlRecordUtils {
    public static final short KRAFT_VERSION_CURRENT_VERSION = 0;
    public static final short LEADER_CHANGE_CURRENT_VERSION = 0;
    public static final short SNAPSHOT_FOOTER_CURRENT_VERSION = 0;
    public static final short SNAPSHOT_HEADER_CURRENT_VERSION = 0;
    public static final short KRAFT_VOTERS_CURRENT_VERSION = 0;

    public static LeaderChangeMessage deserializeLeaderChangeMessage(Record record) {
        ControlRecordType recordType = ControlRecordType.parse(record.key());
        validateControlRecordType(ControlRecordType.LEADER_CHANGE, recordType);

        return deserializeLeaderChangeMessage(record.value());
    }

    public static LeaderChangeMessage deserializeLeaderChangeMessage(ByteBuffer data) {
        return new LeaderChangeMessage(new ByteBufferAccessor(data.slice()), LEADER_CHANGE_CURRENT_VERSION);
    }

    public static SnapshotHeaderRecord deserializeSnapshotHeaderRecord(Record record) {
        ControlRecordType recordType = ControlRecordType.parse(record.key());
        validateControlRecordType(ControlRecordType.SNAPSHOT_HEADER, recordType);

        return deserializeSnapshotHeaderRecord(record.value());
    }

    public static SnapshotHeaderRecord deserializeSnapshotHeaderRecord(ByteBuffer data) {
        return new SnapshotHeaderRecord(new ByteBufferAccessor(data.slice()), SNAPSHOT_HEADER_CURRENT_VERSION);
    }

    public static SnapshotFooterRecord deserializeSnapshotFooterRecord(Record record) {
        ControlRecordType recordType = ControlRecordType.parse(record.key());
        validateControlRecordType(ControlRecordType.SNAPSHOT_FOOTER, recordType);

        return deserializeSnapshotFooterRecord(record.value());
    }

    public static SnapshotFooterRecord deserializeSnapshotFooterRecord(ByteBuffer data) {
        return new SnapshotFooterRecord(new ByteBufferAccessor(data.slice()), SNAPSHOT_FOOTER_CURRENT_VERSION);
    }

    public static KRaftVersionRecord deserializeKRaftVersionRecord(Record record) {
        ControlRecordType recordType = ControlRecordType.parse(record.key());
        validateControlRecordType(ControlRecordType.KRAFT_VERSION, recordType);

        return deserializeKRaftVersionRecord(record.value());
    }

    public static KRaftVersionRecord deserializeKRaftVersionRecord(ByteBuffer data) {
        return new KRaftVersionRecord(new ByteBufferAccessor(data.slice()), KRAFT_VERSION_CURRENT_VERSION);
    }

    public static VotersRecord deserializeVotersRecord(Record record) {
        ControlRecordType recordType = ControlRecordType.parse(record.key());
        validateControlRecordType(ControlRecordType.KRAFT_VOTERS, recordType);

        return deserializeVotersRecord(record.value());
    }

    public static VotersRecord deserializeVotersRecord(ByteBuffer data) {
        return new VotersRecord(new ByteBufferAccessor(data.slice()), KRAFT_VOTERS_CURRENT_VERSION);
    }

    private static void validateControlRecordType(ControlRecordType expected, ControlRecordType actual) {
        if (actual != expected) {
            throw new IllegalArgumentException(
                String.format(
                    "Expected %s control record type(%d), but found %s",
                    expected,
                    expected.type(),
                    actual
                )
            );
        }
    }
}
