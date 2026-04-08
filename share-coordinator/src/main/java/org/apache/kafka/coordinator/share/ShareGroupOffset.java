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

package org.apache.kafka.coordinator.share;

import org.apache.kafka.common.message.InitializeShareGroupStateRequestData;
import org.apache.kafka.common.message.WriteShareGroupStateRequestData;
import org.apache.kafka.coordinator.share.generated.ShareSnapshotValue;
import org.apache.kafka.coordinator.share.generated.ShareUpdateValue;
import org.apache.kafka.server.share.persister.PersisterStateBatch;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Container class to represent data encapsulated in {@link ShareSnapshotValue} and {@link ShareUpdateValue}
 * This class is immutable (state batches is not modified out of context).
 */
public class ShareGroupOffset {
    public static final int NO_TIMESTAMP = 0;
    public static final int UNINITIALIZED_EPOCH = 0;
    public static final int UNINITIALIZED_DELIVERY_COMPLETE_COUNT = -1;
    public static final int UNINITIALIZED_START_OFFSET = -1;
    public static final int DEFAULT_EPOCH = 0;

    private final int snapshotEpoch;
    private final int stateEpoch;
    private final int leaderEpoch;
    private final long startOffset;
    private final int deliveryCompleteCount;
    private final List<PersisterStateBatch> stateBatches;
    private final long createTimestamp;
    private final long writeTimestamp;

    private ShareGroupOffset(
        int snapshotEpoch,
        int stateEpoch,
        int leaderEpoch,
        long startOffset,
        int deliveryCompleteCount,
        List<PersisterStateBatch> stateBatches,
        long createTimestamp,
        long writeTimestamp
    ) {
        this.snapshotEpoch = snapshotEpoch;
        this.stateEpoch = stateEpoch;
        this.leaderEpoch = leaderEpoch;
        this.startOffset = startOffset;
        this.deliveryCompleteCount = deliveryCompleteCount;
        this.stateBatches = stateBatches;
        this.createTimestamp = createTimestamp;
        this.writeTimestamp = writeTimestamp;
    }

    public int snapshotEpoch() {
        return snapshotEpoch;
    }

    public int stateEpoch() {
        return stateEpoch;
    }

    public int leaderEpoch() {
        return leaderEpoch;
    }

    public long startOffset() {
        return startOffset;
    }

    public int deliveryCompleteCount() {
        return deliveryCompleteCount;
    }

    public long createTimestamp() {
        return createTimestamp;
    }

    public long writeTimestamp() {
        return writeTimestamp;
    }

    public List<PersisterStateBatch> stateBatches() {
        return Collections.unmodifiableList(stateBatches);
    }

    private static PersisterStateBatch toPersisterOffsetsStateBatch(ShareSnapshotValue.StateBatch stateBatch) {
        return new PersisterStateBatch(stateBatch.firstOffset(), stateBatch.lastOffset(), stateBatch.deliveryState(), stateBatch.deliveryCount());
    }

    private static PersisterStateBatch toPersisterOffsetsStateBatch(ShareUpdateValue.StateBatch stateBatch) {
        return new PersisterStateBatch(stateBatch.firstOffset(), stateBatch.lastOffset(), stateBatch.deliveryState(), stateBatch.deliveryCount());
    }

    public static ShareGroupOffset fromRecord(ShareSnapshotValue record) {
        return new ShareGroupOffset(
            record.snapshotEpoch(),
            record.stateEpoch(),
            record.leaderEpoch(),
            record.startOffset(),
            record.deliveryCompleteCount(),
            record.stateBatches().stream()
                .map(ShareGroupOffset::toPersisterOffsetsStateBatch)
                .toList(),
            record.createTimestamp(),
            record.writeTimestamp()
        );
    }

    public static ShareGroupOffset fromRecord(ShareUpdateValue record) {
        return new ShareGroupOffset(
            record.snapshotEpoch(),
            UNINITIALIZED_EPOCH,
            record.leaderEpoch(),
            record.startOffset(),
            record.deliveryCompleteCount(),
            record.stateBatches().stream()
                .map(ShareGroupOffset::toPersisterOffsetsStateBatch)
                .toList(),
            NO_TIMESTAMP,
            NO_TIMESTAMP
        );
    }

    public static ShareGroupOffset fromRequest(WriteShareGroupStateRequestData.PartitionData data, long timestamp) {
        return fromRequest(data, DEFAULT_EPOCH, timestamp);
    }

    public static ShareGroupOffset fromRequest(WriteShareGroupStateRequestData.PartitionData data, int snapshotEpoch, long timestamp) {
        return new ShareGroupOffset(
            snapshotEpoch,
            data.stateEpoch(),
            data.leaderEpoch(),
            data.startOffset(),
            data.deliveryCompleteCount(),
            data.stateBatches().stream()
                .map(PersisterStateBatch::from)
                .toList(),
            timestamp,
            timestamp
        );
    }

    public static ShareGroupOffset fromRequest(InitializeShareGroupStateRequestData.PartitionData data, long timestamp) {
        return fromRequest(data, DEFAULT_EPOCH, timestamp);
    }

    public static ShareGroupOffset fromRequest(InitializeShareGroupStateRequestData.PartitionData data, int snapshotEpoch, long timestamp) {
        // This method is invoked during InitializeShareGroupStateRequest. If the start offset is uninitialized (when the 
        // share partition is being initialized for the first time), the consumption hasn't started yet, and lag cannot
        // be calculated. Thus, deliveryCompleteCount is also set as -1. But, if start offset is a non-negative value (when 
        // the start offset is altered), the lag can be calculated from that point onward. Hence, we set deliveryCompleteCount
        // to 0 in that case.
        int deliveryCompleteCount = data.startOffset() == UNINITIALIZED_START_OFFSET ? UNINITIALIZED_DELIVERY_COMPLETE_COUNT : 0;
        return new ShareGroupOffset(
            snapshotEpoch,
            data.stateEpoch(),
            UNINITIALIZED_EPOCH,
            data.startOffset(),
            deliveryCompleteCount,
            List.of(),
            timestamp,
            timestamp
        );
    }

    public static class Builder {
        private int snapshotEpoch;
        private int stateEpoch;
        private int leaderEpoch;
        private long startOffset;
        private int deliveryCompleteCount;
        private List<PersisterStateBatch> stateBatches;
        private long createTimestamp = NO_TIMESTAMP;
        private long writeTimestamp = NO_TIMESTAMP;

        public Builder setSnapshotEpoch(int snapshotEpoch) {
            this.snapshotEpoch = snapshotEpoch;
            return this;
        }

        public Builder setStateEpoch(int stateEpoch) {
            this.stateEpoch = stateEpoch;
            return this;
        }

        public Builder setLeaderEpoch(int leaderEpoch) {
            this.leaderEpoch = leaderEpoch;
            return this;
        }

        public Builder setStartOffset(long startOffset) {
            this.startOffset = startOffset;
            return this;
        }

        public Builder setDeliveryCompleteCount(int deliveryCompleteCount) {
            this.deliveryCompleteCount = deliveryCompleteCount;
            return this;
        }

        public Builder setStateBatches(List<PersisterStateBatch> stateBatches) {
            this.stateBatches = stateBatches == null ? List.of() : stateBatches.stream().toList();
            return this;
        }

        public Builder setCreateTimestamp(long createTimestamp) {
            this.createTimestamp = createTimestamp;
            return this;
        }

        public Builder setWriteTimestamp(long writeTimestamp) {
            this.writeTimestamp = writeTimestamp;
            return this;
        }

        public ShareGroupOffset build() {
            return new ShareGroupOffset(snapshotEpoch, stateEpoch, leaderEpoch, startOffset, deliveryCompleteCount, stateBatches, createTimestamp, writeTimestamp);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShareGroupOffset that = (ShareGroupOffset) o;
        return snapshotEpoch == that.snapshotEpoch &&
            stateEpoch == that.stateEpoch &&
            leaderEpoch == that.leaderEpoch &&
            startOffset == that.startOffset &&
            deliveryCompleteCount == that.deliveryCompleteCount &&
            Objects.equals(stateBatches, that.stateBatches) &&
            createTimestamp == that.createTimestamp &&
            writeTimestamp == that.writeTimestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotEpoch, stateEpoch, leaderEpoch, startOffset, deliveryCompleteCount, stateBatches, createTimestamp, writeTimestamp);
    }

    @Override
    public String toString() {
        return "ShareGroupOffset{" +
            "snapshotEpoch=" + snapshotEpoch +
            ", stateEpoch=" + stateEpoch +
            ", leaderEpoch=" + leaderEpoch +
            ", startOffset=" + startOffset +
            ", deliveryCompleteCount=" + deliveryCompleteCount +
            ", createTimestamp=" + createTimestamp +
            ", writeTimestamp=" + writeTimestamp +
            ", stateBatches=" + stateBatches +
            '}';
    }

    public Builder builderSupplier() {
        return new Builder()
            .setSnapshotEpoch(snapshotEpoch())
            .setStateEpoch(stateEpoch())
            .setLeaderEpoch(leaderEpoch())
            .setStartOffset(startOffset())
            .setDeliveryCompleteCount(deliveryCompleteCount())
            .setStateBatches(stateBatches())
            .setCreateTimestamp(createTimestamp())
            .setWriteTimestamp(writeTimestamp());
    }
}
