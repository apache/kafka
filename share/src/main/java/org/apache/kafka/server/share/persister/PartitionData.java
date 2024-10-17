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

package org.apache.kafka.server.share.persister;

import java.util.List;
import java.util.Objects;

/**
 * This class contains the data for a partition in the interface to {@link Persister}. The various interfaces
 * reflect the ways in which a subset of the data can be accessed for different purposes.
 */
public class PartitionData implements
        PartitionIdData, PartitionStateData, PartitionErrorData, PartitionStateErrorData,
        PartitionStateBatchData, PartitionIdLeaderEpochData, PartitionAllData {
    private final int partition;
    private final int stateEpoch;
    private final long startOffset;
    private final short errorCode;
    private final String errorMessage;
    private final int leaderEpoch;
    private final List<PersisterStateBatch> stateBatches;

    public PartitionData(int partition, int stateEpoch, long startOffset, short errorCode,
                         String errorMessage, int leaderEpoch, List<PersisterStateBatch> stateBatches) {
        this.partition = partition;
        this.stateEpoch = stateEpoch;
        this.startOffset = startOffset;
        this.errorCode = errorCode;
        this.leaderEpoch = leaderEpoch;
        this.errorMessage = errorMessage;
        this.stateBatches = stateBatches;
    }

    public int partition() {
        return partition;
    }

    public int stateEpoch() {
        return stateEpoch;
    }

    public long startOffset() {
        return startOffset;
    }

    public short errorCode() {
        return errorCode;
    }

    public String errorMessage() {
        return errorMessage;
    }

    public int leaderEpoch() {
        return leaderEpoch;
    }

    public List<PersisterStateBatch> stateBatches() {
        return stateBatches;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PartitionData that = (PartitionData) o;
        return Objects.equals(partition, that.partition) &&
                Objects.equals(stateEpoch, that.stateEpoch) &&
                Objects.equals(startOffset, that.startOffset) &&
                Objects.equals(errorCode, that.errorCode) &&
                Objects.equals(errorMessage, that.errorMessage) &&
                Objects.equals(leaderEpoch, that.leaderEpoch) &&
                Objects.equals(stateBatches, that.stateBatches);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, stateEpoch, startOffset, errorCode, leaderEpoch, errorMessage, stateBatches);
    }

    public static class Builder {
        private int partition;
        private int stateEpoch;
        private long startOffset;
        private short errorCode;
        private String errorMessage;
        private int leaderEpoch;
        private List<PersisterStateBatch> stateBatches;

        public Builder setPartition(int partition) {
            this.partition = partition;
            return this;
        }

        public Builder setStateEpoch(int stateEpoch) {
            this.stateEpoch = stateEpoch;
            return this;
        }

        public Builder setStartOffset(long startOffset) {
            this.startOffset = startOffset;
            return this;
        }

        public Builder setErrorCode(short errorCode) {
            this.errorCode = errorCode;
            return this;
        }

        public Builder setErrorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
            return this;
        }

        public Builder setLeaderEpoch(int leaderEpoch) {
            this.leaderEpoch = leaderEpoch;
            return this;
        }

        public Builder setStateBatches(List<PersisterStateBatch> stateBatches) {
            this.stateBatches = stateBatches;
            return this;
        }

        public PartitionData build() {
            return new PartitionData(partition, stateEpoch, startOffset, errorCode, errorMessage, leaderEpoch, stateBatches);
        }
    }

    @Override
    public String toString() {
        return "PartitionData(" +
                "partition=" + partition + "," +
                "stateEpoch=" + stateEpoch + "," +
                "startOffset=" + startOffset + "," +
                "errorCode=" + errorCode + "," +
                "errorMessage=" + errorMessage + "," +
                "leaderEpoch=" + leaderEpoch + "," +
                "stateBatches=" + stateBatches +
                ")";
    }
}
