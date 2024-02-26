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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * The SharePartition is used to track the state of a partition that is shared between multiple
 * consumers. The class maintains the state of the records that have been fetched from the leader
 * and are in-flight.
 */
public class SharePartition {

  private final static Logger log = LoggerFactory.getLogger(SharePartition.class);

  /**
   * The RecordState is used to track the state of a record that has been fetched from the leader.
   * The state of the records determines if the records should be re-delivered, move the next fetch
   * offset, or be state persisted to disk.
   */
  public enum RecordState {
    AVAILABLE,
    ACQUIRED,
    ACKNOWLEDGED,
    ARCHIVED
  }

  /**
   * The in-flight record is used to track the state of a record that has been fetched from the
   * leader. The state of the record is used to determine if the record should be re-fetched or if it
   * can be acknowledged or archived. Once share partition start offset is moved then the in-flight
   * records prior to the start offset are removed from the cache.
   */
  private final NavigableMap<BatchRecord, InFlightRecord> cachedState;

  /**
   * The max in-flight messages is used to limit the number of records that can be in-flight at any
   * given time. The max in-flight messages is used to prevent the consumer from fetching too many
   * records from the leader and running out of memory.
   */
  private int maxInFlightMessages;
  /**
   * The max delivery count is used to limit the number of times a record can be delivered to the
   * consumer. The max delivery count is used to prevent the consumer re-delivering the same record
   * indefinitely.
   */
  private int maxDeliveryCount;

  /**
   * The next fetch offset is used to track the next offset that should be fetched from the leader.
   */
  private long nextFetchOffset;

  SharePartition(int maxInFlightMessages, int maxDeliveryCount) {
    this.maxInFlightMessages = maxInFlightMessages;
    this.maxDeliveryCount = maxDeliveryCount;
    this.cachedState = new ConcurrentSkipListMap<>();
    assert this.maxInFlightMessages > 0;
    assert this.maxDeliveryCount > 0;
  }


  public void update(FetchPartitionData fetchPartitionData) {
      fetchPartitionData.records.batches().forEach(batch -> {
        BatchRecord batchRecord = new BatchRecord(batch.baseOffset(), batch.lastOffset());
        cachedState.put(batchRecord, new InFlightRecord(RecordState.ACQUIRED));
      });
      // As we currently only fetch from the leader, the last stable offset should be preferred over
      // the high watermark as high watermark tracks the end of the log as known by the follower.
      nextFetchOffset = fetchPartitionData.lastStableOffset.isPresent() ?
          fetchPartitionData.lastStableOffset.getAsLong() : fetchPartitionData.highWatermark;
  }

  public void acknowledge(List<AcknowledgementBatch> acknowledgementBatch) {
    log.debug("Acknowledgement batch request for share partition: " + acknowledgementBatch);
    // TODO: Implement the logic to handle the acknowledgement batch request.
  }

  public long nextFetchOffset() {
    return nextFetchOffset;
  }

  static class BatchRecord implements Comparable<BatchRecord> {
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

    public String toString() {
      return "BatchRecord(" +
          " baseOffset=" + baseOffset +
          ", lastOffset=" + lastOffset +
          ")";
    }

    @Override
    public int compareTo(BatchRecord o) {
        int res = Long.compare(this.baseOffset, o.baseOffset);
        if (res != 0) {
          return res;
        }
        return Long.compare(this.lastOffset, o.lastOffset);
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
      return "InFlightRecord(" +
          " state=" + state.toString() +
          ", deliveryCount=" + ((deliveryCount == 0) ? "" : ("(" + deliveryCount + ")")) +
          ")";
    }
  }

  static class AcknowledgementBatch {

    private final long startOffset;
    private final long lastOffset;
    private final List<Long> gapOffsets;
    private final byte acknowledgeType;

    public AcknowledgementBatch(long startOffset, long lastOffset, List<Long> gapOffsets, byte acknowledgeType) {
      this.startOffset = startOffset;
      this.lastOffset = lastOffset;
      this.gapOffsets = gapOffsets;
      this.acknowledgeType = acknowledgeType;
    }

    @Override
    public String toString() {
      return "AcknowledgementBatch(" +
          " startOffset=" + startOffset +
          ", lastOffset=" + lastOffset +
          ", gapOffsets=" + gapOffsets +
          ", acknowledgeType=" + acknowledgeType +
          ")";
    }
  }
}
