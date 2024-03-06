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

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.storage.internals.log.FetchPartitionData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
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
        AVAILABLE((byte) 0),
        ACQUIRED((byte) 1),
        ACKNOWLEDGED((byte) 2),
        ARCHIVED((byte) 3);

        public final byte id;

        RecordState(byte id) {
            this.id = id;
        }

        public static RecordState forId(byte id) {
            switch (id) {
                case 0:
                    return AVAILABLE;
                case 1:
                    return ACQUIRED;
                case 2:
                    return ACKNOWLEDGED;
                case 3:
                    return ARCHIVED;
                default:
                    throw new IllegalArgumentException("Unknown record state id: " + id);
            }
        }
    }

    /**
     * The in-flight record is used to track the state of a record that has been fetched from the
     * leader. The state of the record is used to determine if the record should be re-fetched or if it
     * can be acknowledged or archived. Once share partition start offset is moved then the in-flight
     * records prior to the start offset are removed from the cache.
     */
    private final NavigableMap<Long, InFlightFetchBatch> cachedState;

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

    private long nextFetchOffset = 0;

    SharePartition(int maxInFlightMessages, int maxDeliveryCount) {
        this.maxInFlightMessages = maxInFlightMessages;
        this.maxDeliveryCount = maxDeliveryCount;
        this.cachedState = new ConcurrentSkipListMap<>();
        // TODO: Just a placeholder for now.
        assert this.maxInFlightMessages > 0;
        assert this.maxDeliveryCount > 0;
    }

    public long nextFetchOffset() {
        return nextFetchOffset;
    }
    public void update(String memberId, FetchPartitionData fetchPartitionData) {
        synchronized (this) {
            long next = nextFetchOffset;
            for (RecordBatch batch : fetchPartitionData.records.batches()) {
                cachedState.put(batch.baseOffset(), new InFlightFetchBatch(memberId, batch.baseOffset(),
                    batch.lastOffset(), RecordState.ACQUIRED));
                next = batch.nextOffset();
            }
            nextFetchOffset = next;
        }
    }

    public CompletableFuture<Optional<Throwable>> acknowledge(String memberId, List<AcknowledgementBatch> acknowledgementBatch) {
        log.debug("Acknowledgement batch request for share partition: " + acknowledgementBatch);
        throw new UnsupportedOperationException("Not implemented yet");
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

  static class InFlightFetchBatch {

      // The member id of the client that is fetching the record.
      private final String memberId;
      // The offset of the first record in the batch that is fetched from the log.
      private final long startOffset;
      // The last offset of the batch that is fetched from the log.
      private final long lastOffset;

      // Must be null for most of the records hence lazily-initialize. Should hold the gap offsets for
      // the batch that are reported by the client.
      private List<Long> gapOffsets;

      // The state of the fetch batch records. If the subset records are present then the state is
      // derived from the subset records.
      private RecordState state;
      // The number of times the records has been delivered to the client. If the subset records are
      // present then the delivery count is derived from the subset records.
      private int deliveryCount;

      public InFlightFetchBatch(String memberId, long startOffset, long lastOffset, RecordState state) {
          this(memberId, startOffset, lastOffset, state, 0);
      }

      public InFlightFetchBatch(String memberId, long startOffset, long lastOffset, RecordState state, int deliveryCount) {
          this.memberId = memberId;
          this.startOffset = startOffset;
          this.lastOffset = lastOffset;
          this.state = state;
          this.deliveryCount = deliveryCount;
      }

      public String memberId() {
          return memberId;
      }

      public long startOffset() {
          return startOffset;
      }

      public long lastOffset() {
          return lastOffset;
      }

      public RecordState messageState() {
          return state;
      }

      public int deliveryCount() {
          return deliveryCount;
      }

      public List<Long> gapOffsets() {
          return gapOffsets;
      }

      public void setMessageState(RecordState newState, boolean incrementDeliveryCount) {
          state = newState;
          if (incrementDeliveryCount) {
              deliveryCount++;
          }
      }

      public void addGapOffsets(List<Long> gapOffsets) {
          if (this.gapOffsets == null) {
              this.gapOffsets = new ArrayList<>(gapOffsets);
          } else {
              this.gapOffsets.addAll(gapOffsets);
              // Maintain the order of the gap offsets as out of band acknowledgements can be received.
              Collections.sort(this.gapOffsets);
          }
      }

      @Override
      public String toString() {
          return "InFlightFetchBatch(" +
              " memberId=" + memberId +
              ", startOffset=" + startOffset +
              ", lastOffset=" + lastOffset +
              ", state=" + state.toString() +
              ", deliveryCount=" + ((deliveryCount == 0) ? "" : ("(" + deliveryCount + ")")) +
              ", gapOffsets=" + ((gapOffsets == null) ? "" : gapOffsets) +
              ")";
      }
  }

  static class AcknowledgementBatch {

      private final long baseOffset;
      private final long lastOffset;
      private final List<Long> gapOffsets;
      private final AcknowledgeType acknowledgeType;

      public AcknowledgementBatch(long baseOffset, long lastOffset, List<Long> gapOffsets, AcknowledgeType acknowledgeType) {
          this.baseOffset = baseOffset;
          this.lastOffset = lastOffset;
          this.gapOffsets = gapOffsets;
          this.acknowledgeType = acknowledgeType;
      }

      @Override
      public String toString() {
          return "AcknowledgementBatch(" +
              " baseOffset=" + baseOffset +
              ", lastOffset=" + lastOffset +
              ", gapOffsets=" + gapOffsets +
              ", acknowledgeType=" + acknowledgeType +
              ")";
      }
  }
}
