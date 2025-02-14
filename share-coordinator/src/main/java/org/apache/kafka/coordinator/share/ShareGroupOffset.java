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

import org.apache.kafka.common.message.WriteShareGroupStateRequestData;
import org.apache.kafka.coordinator.share.generated.ShareSnapshotValue;
import org.apache.kafka.coordinator.share.generated.ShareUpdateValue;
import org.apache.kafka.server.share.persister.PersisterStateBatch;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;

/**
 * Container class to represent data encapsulated in {@link ShareSnapshotValue} and {@link ShareUpdateValue}
 * This class is immutable (state batches is not modified out of context).
 */
public class ShareGroupOffset {
    private final int snapshotEpoch;
    private final int stateEpoch;
    private final int leaderEpoch;
    private final long startOffset;
    private final List<PersisterStateBatch> stateBatches;

    private ShareGroupOffset(int snapshotEpoch,
                            int stateEpoch,
                            int leaderEpoch,
                            long startOffset,
                            List<PersisterStateBatch> stateBatches) {
        this.snapshotEpoch = snapshotEpoch;
        this.stateEpoch = stateEpoch;
        this.leaderEpoch = leaderEpoch;
        this.startOffset = startOffset;
        this.stateBatches = stateBatches;
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
        return new ShareGroupOffset(record.snapshotEpoch(), record.stateEpoch(), record.leaderEpoch(), record.startOffset(), record.stateBatches().stream()
            .map(ShareGroupOffset::toPersisterOffsetsStateBatch).toList());
    }

    public static ShareGroupOffset fromRecord(ShareUpdateValue record) {
        return new ShareGroupOffset(record.snapshotEpoch(), -1, record.leaderEpoch(), record.startOffset(), record.stateBatches().stream()
            .map(ShareGroupOffset::toPersisterOffsetsStateBatch).toList());
    }

    public static ShareGroupOffset fromRequest(WriteShareGroupStateRequestData.PartitionData data) {
        return fromRequest(data, 0);
    }

    public static ShareGroupOffset fromRequest(WriteShareGroupStateRequestData.PartitionData data, int snapshotEpoch) {
        return new ShareGroupOffset(snapshotEpoch,
            data.stateEpoch(),
            data.leaderEpoch(),
            data.startOffset(),
            data.stateBatches().stream()
                .map(PersisterStateBatch::from)
                .toList());
    }

    public LinkedHashSet<PersisterStateBatch> stateBatchAsSet() {
        return new LinkedHashSet<>(stateBatches);
    }

    public static class Builder {
        private int snapshotEpoch;
        private int stateEpoch;
        private int leaderEpoch;
        private long startOffset;
        private List<PersisterStateBatch> stateBatches;

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

        public Builder setStateBatches(List<PersisterStateBatch> stateBatches) {
            this.stateBatches = stateBatches;
            return this;
        }

        public ShareGroupOffset build() {
            return new ShareGroupOffset(snapshotEpoch, stateEpoch, leaderEpoch, startOffset, stateBatches);
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
            Objects.equals(stateBatches, that.stateBatches);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotEpoch, stateEpoch, leaderEpoch, startOffset, stateBatches);
    }

    @Override
    public String toString() {
        return "ShareGroupOffset{" +
            "snapshotEpoch=" + snapshotEpoch +
            ", stateEpoch=" + stateEpoch +
            ", leaderEpoch=" + leaderEpoch +
            ", startOffset=" + startOffset +
            ", stateBatches=" + stateBatches +
            '}';
    }
}
