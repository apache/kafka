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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.coordinator.group.generated.OffsetCommitValue;

import java.util.Objects;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static org.apache.kafka.coordinator.group.Utils.ofSentinel;

/**
 * Represents a committed offset with its metadata.
 */
public class OffsetAndMetadata {
    private static final String NO_METADATA = "";

    /**
     * The committed offset.
     */
    public final long committedOffset;

    /**
     * The leader epoch in use when the offset was committed.
     */
    public final OptionalInt leaderEpoch;

    /**
     * The committed metadata. The Kafka offset commit API allows users to provide additional
     * metadata (in the form of a string) when an offset is committed. This can be useful
     * (for example) to store information about which node made the commit, what time the
     * commit was made, etc.
     */
    public final String metadata;

    /**
     * The commit timestamp in milliseconds.
     */
    public final long commitTimestampMs;

    /**
     * The expire timestamp in milliseconds.
     */
    public final OptionalLong expireTimestampMs;

    /**
     * The offset of the commit record in the log.
     */
    public final long recordOffset;

    public OffsetAndMetadata(
        long committedOffset,
        OptionalInt leaderEpoch,
        String metadata,
        long commitTimestampMs,
        OptionalLong expireTimestampMs
    ) {
        this(
            -1L,
            committedOffset,
            leaderEpoch,
            metadata,
            commitTimestampMs,
            expireTimestampMs
        );
    }

    public OffsetAndMetadata(
        long recordOffset,
        long committedOffset,
        OptionalInt leaderEpoch,
        String metadata,
        long commitTimestampMs,
        OptionalLong expireTimestampMs
    ) {
        this.recordOffset = recordOffset;
        this.committedOffset = committedOffset;
        this.leaderEpoch = Objects.requireNonNull(leaderEpoch);
        this.metadata = Objects.requireNonNull(metadata);
        this.commitTimestampMs = commitTimestampMs;
        this.expireTimestampMs = Objects.requireNonNull(expireTimestampMs);
    }

    @Override
    public String toString() {
        return "OffsetAndMetadata(offset=" + committedOffset +
            ", leaderEpoch=" + leaderEpoch +
            ", metadata=" + metadata +
            ", commitTimestampMs=" + commitTimestampMs +
            ", expireTimestampMs=" + expireTimestampMs +
            ", recordOffset=" + recordOffset +
            ')';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OffsetAndMetadata that = (OffsetAndMetadata) o;

        if (committedOffset != that.committedOffset) return false;
        if (commitTimestampMs != that.commitTimestampMs) return false;
        if (recordOffset != that.recordOffset) return false;
        if (!Objects.equals(leaderEpoch, that.leaderEpoch)) return false;
        if (!Objects.equals(metadata, that.metadata)) return false;
        return Objects.equals(expireTimestampMs, that.expireTimestampMs);
    }

    @Override
    public int hashCode() {
        int result = (int) (committedOffset ^ (committedOffset >>> 32));
        result = 31 * result + (leaderEpoch != null ? leaderEpoch.hashCode() : 0);
        result = 31 * result + (metadata != null ? metadata.hashCode() : 0);
        result = 31 * result + (int) (commitTimestampMs ^ (commitTimestampMs >>> 32));
        result = 31 * result + (expireTimestampMs != null ? expireTimestampMs.hashCode() : 0);
        result = 31 * result + (int) (recordOffset ^ (recordOffset >>> 32));
        return result;
    }

    /**
     * @return An OffsetAndMetadata created from a OffsetCommitValue record.
     */
    public static OffsetAndMetadata fromRecord(
        long recordOffset,
        OffsetCommitValue record
    ) {
        return new OffsetAndMetadata(
            recordOffset,
            record.offset(),
            ofSentinel(record.leaderEpoch()),
            record.metadata(),
            record.commitTimestamp(),
            ofSentinel(record.expireTimestamp())
        );
    }

    /**
     * @return An OffsetAndMetadata created from an OffsetCommitRequestPartition request.
     */
    public static OffsetAndMetadata fromRequest(
        OffsetCommitRequestData.OffsetCommitRequestPartition partition,
        long currentTimeMs,
        OptionalLong expireTimestampMs
    ) {
        return new OffsetAndMetadata(
            partition.committedOffset(),
            ofSentinel(partition.committedLeaderEpoch()),
            partition.committedMetadata() == null ?
                OffsetAndMetadata.NO_METADATA : partition.committedMetadata(),
            partition.commitTimestamp() == OffsetCommitRequest.DEFAULT_TIMESTAMP ?
                currentTimeMs : partition.commitTimestamp(),
            expireTimestampMs
        );
    }

    /**
     * @return An OffsetAndMetadata created from an OffsetCommitRequestPartition request.
     */
    public static OffsetAndMetadata fromRequest(
        TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition partition,
        long currentTimeMs
    ) {
        return new OffsetAndMetadata(
            partition.committedOffset(),
            ofSentinel(partition.committedLeaderEpoch()),
            partition.committedMetadata() == null ?
                OffsetAndMetadata.NO_METADATA : partition.committedMetadata(),
            currentTimeMs,
            OptionalLong.empty()
        );
    }
}
