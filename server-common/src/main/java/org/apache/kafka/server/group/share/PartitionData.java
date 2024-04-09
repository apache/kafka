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

package org.apache.kafka.server.group.share;

import java.util.List;
import java.util.Objects;

public class PartitionData implements
    PartitionIdData, PartitionStateData, PartitionErrorData, PartitionStateErrorData, PartitionStateBatchData, PartitionAllData {
  private final int partition;
  private final int stateEpoch;
  private final long startOffset;
  private final short errorCode;
  private final List<PersisterStateBatch> stateBatches;

  public PartitionData(int partition, int stateEpoch, long startOffset, short errorCode, List<PersisterStateBatch> stateBatches) {
    this.partition = partition;
    this.stateEpoch = stateEpoch;
    this.startOffset = startOffset;
    this.errorCode = errorCode;
    this.stateBatches = stateBatches;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PartitionData that = (PartitionData) o;
    return partition == that.partition && stateEpoch == that.stateEpoch && startOffset == that.startOffset && this.errorCode == that.errorCode && this.stateBatches == that
        .stateBatches;
  }

  @Override
  public int hashCode() {
    return Objects.hash(partition, stateEpoch, startOffset, errorCode, stateBatches);
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

  public List<PersisterStateBatch> stateBatches() {
    return stateBatches;
  }

  public static class Builder {
    private int partition;
    private int stateEpoch;
    private long startOffset;
    private short errorCode;
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

    public Builder setStateBatches(List<PersisterStateBatch> stateBatches) {
      this.stateBatches = stateBatches;
      return this;
    }

    public PartitionData build() {
      return new PartitionData(partition, stateEpoch, startOffset, errorCode, stateBatches);
    }
  }
}
