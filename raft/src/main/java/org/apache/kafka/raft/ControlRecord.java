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
package org.apache.kafka.raft;

import java.util.Objects;
import org.apache.kafka.common.message.KRaftVersionRecord;
import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.message.SnapshotFooterRecord;
import org.apache.kafka.common.message.SnapshotHeaderRecord;
import org.apache.kafka.common.message.VotersRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.record.ControlRecordType;

public final class ControlRecord {
    private final ControlRecordType recordType;
    private final ApiMessage message;

    private static void throwIllegalArgument(ControlRecordType recordType, ApiMessage message) {
        throw new IllegalArgumentException(
            String.format(
                "Record type %s doesn't match message class %s",
                recordType,
                message.getClass()
            )
        );
    }

    public ControlRecord(ControlRecordType recordType, ApiMessage message) {
        switch (recordType) {
            case LEADER_CHANGE:
                if (!(message instanceof LeaderChangeMessage)) {
                    throwIllegalArgument(recordType, message);
                }
                break;
            case SNAPSHOT_HEADER:
                if (!(message instanceof SnapshotHeaderRecord)) {
                    throwIllegalArgument(recordType, message);
                }
                break;
            case SNAPSHOT_FOOTER:
                if (!(message instanceof SnapshotFooterRecord)) {
                    throwIllegalArgument(recordType, message);
                }
                break;
            case KRAFT_VERSION:
                if (!(message instanceof KRaftVersionRecord)) {
                    throwIllegalArgument(recordType, message);
                }
                break;
            case KRAFT_VOTERS:
                if (!(message instanceof VotersRecord)) {
                    throwIllegalArgument(recordType, message);
                }
                break;
            default:
                throw new IllegalArgumentException(String.format("Unknown control record type %s", recordType));
        }

        this.recordType = recordType;
        this.message = message;
    }

    public ControlRecordType type() {
        return recordType;
    }

    public short version() {
        switch (recordType) {
            case LEADER_CHANGE:
                return ((LeaderChangeMessage) message).version();
            case SNAPSHOT_HEADER:
                return ((SnapshotHeaderRecord) message).version();
            case SNAPSHOT_FOOTER:
                return ((SnapshotFooterRecord) message).version();
            case KRAFT_VERSION:
                return ((KRaftVersionRecord) message).version();
            case KRAFT_VOTERS:
                return ((VotersRecord) message).version();
            default:
                throw new IllegalStateException(String.format("Unknown control record type %s", recordType));
        }
    }

    public ApiMessage message() {
        return message;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;
        ControlRecord that = (ControlRecord) other;
        return Objects.equals(recordType, that.recordType) &&
            Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(recordType, message);
    }

    @Override
    public String toString() {
        return String.format("ControlRecord(recordType=%s, message=%s)", recordType, message);
    }
}
