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

import org.apache.kafka.common.message.KRaftVersionRecord;
import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.message.SnapshotFooterRecord;
import org.apache.kafka.common.message.SnapshotHeaderRecord;
import org.apache.kafka.common.message.VotersRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.record.internal.ControlRecordType;
import org.apache.kafka.common.record.internal.ControlRecordUtils;

import java.nio.ByteBuffer;
import java.util.Objects;


public final class ControlRecord {
    private final ControlRecordType recordType;
    private final ApiMessage message;

    public static ControlRecord of(ByteBuffer key, ByteBuffer value) {
        ControlRecordType recordType = ControlRecordType.parse(key);
        final ApiMessage message = switch (recordType) {
            case LEADER_CHANGE -> ControlRecordUtils.deserializeLeaderChangeMessage(value);
            case SNAPSHOT_HEADER -> ControlRecordUtils.deserializeSnapshotHeaderRecord(value);
            case SNAPSHOT_FOOTER -> ControlRecordUtils.deserializeSnapshotFooterRecord(value);
            case KRAFT_VERSION -> ControlRecordUtils.deserializeKRaftVersionRecord(value);
            case KRAFT_VOTERS -> ControlRecordUtils.deserializeVotersRecord(value);
            default -> throw new IllegalArgumentException(String.format("Unknown control record type %s", recordType));
        };
        return new ControlRecord(recordType, message);
    }

    //this is for test only
    public static ControlRecord of(ApiMessage message) {
        ControlRecordType recordType;
        if (message instanceof LeaderChangeMessage) {
            recordType = ControlRecordType.LEADER_CHANGE;
        } else if (message instanceof SnapshotHeaderRecord) {
            recordType = ControlRecordType.SNAPSHOT_HEADER;
        } else if (message instanceof SnapshotFooterRecord) {
            recordType = ControlRecordType.SNAPSHOT_FOOTER;
        } else if (message instanceof KRaftVersionRecord) {
            recordType = ControlRecordType.KRAFT_VERSION;
        } else if (message instanceof VotersRecord) {
            recordType = ControlRecordType.KRAFT_VOTERS;
        } else {
            throw new IllegalArgumentException(String.format("Unknown control record type %s", message.getClass()));
        }
        return new ControlRecord(recordType, message);
    }

    private ControlRecord(ControlRecordType recordType, ApiMessage message) {
        this.recordType = recordType;
        this.message = message;
    }

    public ControlRecordType type() {
        return recordType;
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
