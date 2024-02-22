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
package kafka.server;

import org.apache.kafka.storage.internals.log.FetchPartitionData;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class SharePartition {

  public enum RecordState {
    AVAILABLE,
    ACQUIRED,
    ACKNOWLEDGED,
    ARCHIVED
  }

  private final Map<BatchRecord, InFlightRecord> cachedState;

  private int maxInFlightMessages;
  private int maxDeliveryCount;

  private long nextFetchOffset;

  SharePartition(int maxInFlightMessages, int maxDeliveryCount) {
    this.maxInFlightMessages = maxInFlightMessages;
    this.maxDeliveryCount = maxDeliveryCount;
    this.cachedState = new ConcurrentHashMap<>();
    assert this.maxInFlightMessages > 0;
    assert this.maxDeliveryCount > 0;
  }

  public void update(FetchPartitionData fetchPartitionData) {
      fetchPartitionData.records.batches().forEach(batch -> {
        BatchRecord batchRecord = new BatchRecord(batch.baseOffset(), batch.lastOffset());
        cachedState.put(batchRecord, new InFlightRecord(RecordState.ACQUIRED));
        nextFetchOffset = batch.lastOffset() + 1;
      });
  }

  public long nextFetchOffset() {
    return nextFetchOffset;
  }

  static class BatchRecord {
    private final long baseOffset;
    private final long lastOffset;

    public BatchRecord(long baseOffset, long lastOffset) {
      this.baseOffset = baseOffset;
      this.lastOffset = lastOffset;
    }

    public long baseOffset() {
      return baseOffset;
    }

    public long lastOffset() {
      return lastOffset;
    }

    @Override
    public int hashCode() {
      return Objects.hash(baseOffset, lastOffset);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      BatchRecord other = (BatchRecord) obj;
      return baseOffset == other.baseOffset && lastOffset == other.lastOffset;
    }
  }

  static class InFlightRecord {
    private RecordState state;
    private int deliveryCount;

    public InFlightRecord(RecordState state) {
      this(state, 0);
    }

    public InFlightRecord(RecordState state, int deliveryCount) {
      this.state = state;
      this.deliveryCount = deliveryCount;
    }

    public RecordState messageState() {
      return state;
    }

    public void setMessageState(RecordState newState, boolean incrementDeliveryCount) {
      state = newState;
      if (incrementDeliveryCount) {
        deliveryCount++;
      }
    }

    public int deliveryCount() {
      return deliveryCount;
    }

    @Override
    public int hashCode() {
      return Objects.hash(state, deliveryCount);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      InFlightRecord other = (InFlightRecord) obj;
      return state == other.state && deliveryCount == other.deliveryCount;
    }

    @Override
    public String toString() {
      return state.toString() + ((deliveryCount == 0) ? "" : ("(" + deliveryCount + ")"));
    }
  }
}
