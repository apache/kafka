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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ShareFetchResponseData.AcquiredRecords;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.storage.internals.log.FetchPartitionData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The SharePartition is used to track the state of a partition that is shared between multiple
 * consumers. The class maintains the state of the records that have been fetched from the leader
 * and are in-flight.
 */
public class SharePartition {

    private static final Logger log = LoggerFactory.getLogger(SharePartition.class);

    /**
     * empty member id used to indicate when a record is not acquired by any member.
     */
    static final String EMPTY_MEMBER_ID = Uuid.ZERO_UUID.toString();

    /**
     * The RecordState is used to track the state of a record that has been fetched from the leader.
     * The state of the records determines if the records should be re-delivered, move the next fetch
     * offset, or be state persisted to disk.
     */
    public enum RecordState {
        AVAILABLE((byte) 0),
        ACQUIRED((byte) 1),
        ACKNOWLEDGED((byte) 2),
        ARCHIVED((byte) 4);

        public final byte id;

        RecordState(byte id) {
            this.id = id;
        }

        /**
         * Validates that the <code>newState</code> is one of the valid transition from the current
         * {@code RecordState}.
         *
         * @param newState State into which requesting to transition; must be non-<code>null</code>
         *
         * @return {@code RecordState} <code>newState</code> if validation succeeds. Returning
         *         <code>newState</code> helps state assignment chaining.
         *
         * @throws IllegalStateException if the state transition validation fails.
         */
        public RecordState validateTransition(RecordState newState) throws IllegalStateException {
            Objects.requireNonNull(newState, "newState cannot be null");
            if (this == newState) {
                throw new IllegalStateException("The state transition is invalid as the new state is"
                    + "the same as the current state");
            }

            if (this == ACKNOWLEDGED || this == ARCHIVED) {
                throw new IllegalStateException("The state transition is invalid from the current state: " + this);
            }

            if (this == AVAILABLE && newState != ACQUIRED) {
                throw new IllegalStateException("The state can only be transitioned to ACQUIRED from AVAILABLE");
            }

            // Either the transition is from Available -> Acquired or from Acquired -> Available/
            // Acknowledged/Archived.
            return newState;
        }

        public static RecordState forId(byte id) {
            switch (id) {
                case 0:
                    return AVAILABLE;
                case 1:
                    return ACQUIRED;
                case 2:
                    return ACKNOWLEDGED;
                case 4:
                    return ARCHIVED;
                default:
                    throw new IllegalArgumentException("Unknown record state id: " + id);
            }
        }
    }

    /**
     * The group id of the share partition belongs to.
     */
    private final String groupId;

    /**
     * The topic id partition of the share partition.
     */
    private final TopicIdPartition topicIdPartition;

    /**
     * The in-flight record is used to track the state of a record that has been fetched from the
     * leader. The state of the record is used to determine if the record should be re-fetched or if it
     * can be acknowledged or archived. Once share partition start offset is moved then the in-flight
     * records prior to the start offset are removed from the cache. The cache holds data against the
     * first offset of the in-flight batch.
     */
    private final NavigableMap<Long, InFlightBatch> cachedState;

    /**
     * The lock is used to synchronize access to the in-flight records. The lock is used to ensure that
     * the in-flight records are accessed in a thread-safe manner.
     */
    private final ReadWriteLock lock;

    /**
     * The find next fetch offset is used to indicate if the next fetch offset should be recomputed.
     */
    private final AtomicBoolean findNextFetchOffset;

    /**
     * The lock to ensure that the same share partition does not enter a fetch queue
     * while another one is being fetched within the queue.
     */
    private final AtomicBoolean fetchLock;

    /**
     * The max in-flight messages is used to limit the number of records that can be in-flight at any
     * given time. The max in-flight messages is used to prevent the consumer from fetching too many
     * records from the leader and running out of memory.
     */
    private final int maxInFlightMessages;

    /**
     * The max delivery count is used to limit the number of times a record can be delivered to the
     * consumer. The max delivery count is used to prevent the consumer re-delivering the same record
     * indefinitely.
     */
    private final int maxDeliveryCount;

    /**
     * The record lock duration is used to limit the duration for which a consumer can acquire a record.
     * Once this time period is elapsed, the record will be made available or archived depending on the delivery count.
     */
    private final int recordLockDurationMs;

    /**
     * Timer is used to implement acquisition lock on records that guarantees the movement of records from
     * acquired to available/archived state upon timeout
     */
    private final Timer timer;

    /**
     * Time is used to get the currentTime.
     */
    private final Time time;

    /**
     * The share partition start offset specifies the partition start offset from which the records
     * are cached in the cachedState of the sharePartition.
     */
    private long startOffset;

    /**
     * The share partition end offset specifies the partition end offset from which the records
     * are already fetched.
     */
    private long endOffset;

    /**
     * The state epoch is used to track the version of the state of the share partition.
     */
    private int stateEpoch;

    /**
     * The replica manager is used to get the earliest offset of the share partition, so we can adjust the start offset.
     */
    private final ReplicaManager replicaManager;

    SharePartition(
        String groupId,
        TopicIdPartition topicIdPartition,
        int maxInFlightMessages,
        int maxDeliveryCount,
        int recordLockDurationMs,
        Timer timer,
        Time time,
        ReplicaManager replicaManager
    ) {
        this.groupId = groupId;
        this.topicIdPartition = topicIdPartition;
        this.maxInFlightMessages = maxInFlightMessages;
        this.maxDeliveryCount = maxDeliveryCount;
        this.cachedState = new ConcurrentSkipListMap<>();
        this.lock = new ReentrantReadWriteLock();
        this.findNextFetchOffset = new AtomicBoolean(false);
        this.fetchLock = new AtomicBoolean(false);
        this.recordLockDurationMs = recordLockDurationMs;
        this.timer = timer;
        this.time = time;
        this.replicaManager = replicaManager;
        // Initialize the partition.
        initialize();
    }

    /**
     * The next fetch offset is used to determine the next offset that should be fetched from the leader.
     * The offset should be the next offset after the last fetched batch but there could be batches/
     * offsets that are either released by acknowledgement API or lock timed out hence the next fetch
     * offset might be different from the last batch next offset. Hence, method checks if the next
     * fetch offset should be recomputed else returns the last computed next fetch offset.
     *
     * @return The next fetch offset that should be fetched from the leader.
     */
    public long nextFetchOffset() {
        // TODO: Implement the logic to compute the next fetch offset.
        return 0;
    }

    /**
     * Acquire the fetched records for the share partition. The acquired records are added to the
     * in-flight records and the next fetch offset is updated to the next offset that should be
     * fetched from the leader.
     *
     * @param memberId The member id of the client that is fetching the record.
     * @param fetchPartitionData The fetched records for the share partition.
     *
     * @return A future which is completed when the records are acquired.
     */
    public CompletableFuture<List<AcquiredRecords>> acquire(
        String memberId,
        FetchPartitionData fetchPartitionData
    ) {
        log.trace("Received acquire request for share partition: {}-{}", memberId, fetchPartitionData);

        CompletableFuture<List<AcquiredRecords>> future = new CompletableFuture<>();
        future.completeExceptionally(new UnsupportedOperationException("Not implemented"));

        return future;
    }

    /**
     * Acknowledge the fetched records for the share partition. The accepted batches are removed
     * from the in-flight records once persisted. The next fetch offset is updated to the next offset
     * that should be fetched from the leader, if required.
     *
     * @param memberId The member id of the client that is fetching the record.
     * @param acknowledgementBatch The acknowledgement batch list for the share partition.
     *
     * @return A future which is completed when the records are acknowledged.
     */
    public CompletableFuture<Optional<Throwable>> acknowledge(
        String memberId,
        List<AcknowledgementBatch> acknowledgementBatch
    ) {
        log.trace("Acknowledgement batch request for share partition: {}-{}", groupId, topicIdPartition);

        CompletableFuture<Optional<Throwable>> future = new CompletableFuture<>();
        future.completeExceptionally(new UnsupportedOperationException("Not implemented"));

        return future;
    }

    /**
     * Release the acquired records for the share partition. The next fetch offset is updated to the next offset
     * that should be fetched from the leader.
     *
     * @param memberId The member id of the client whose records shall be released.
     *
     * @return A future which is completed when the records are released.
     */
    public CompletableFuture<Optional<Throwable>> releaseAcquiredRecords(String memberId) {
        log.trace("Release acquired records request for share partition: {}-{}-{}", groupId, memberId, topicIdPartition);

        CompletableFuture<Optional<Throwable>> future = new CompletableFuture<>();
        future.completeExceptionally(new UnsupportedOperationException("Not implemented"));

        return future;
    }

    private void initialize() {
        // Initialize the partition.
        log.debug("Initializing share partition: {}-{}", groupId, topicIdPartition);

        // TODO: Provide implementation to initialize the share partition.
    }

    /**
     * The InFlightBatch maintains the in-memory state of the fetched records i.e. in-flight records.
     */
    private static class InFlightBatch {
        /**
         * The offset of the first record in the batch that is fetched from the log.
         */
        private final long firstOffset;
        /**
         * The last offset of the batch that is fetched from the log.
         */
        private final long lastOffset;
        /**
         * The in-flight state of the fetched records. If the offset state map is empty then inflightState
         * determines the state of the complete batch else individual offset determines the state of
         * the respective records.
         */
        private InFlightState inFlightState;

        InFlightBatch(String memberId, long firstOffset, long lastOffset, RecordState state, int deliveryCount) {
            this.firstOffset = firstOffset;
            this.lastOffset = lastOffset;
            this.inFlightState = new InFlightState(state, deliveryCount, memberId);
        }

        @Override
        public String toString() {
            return "InFlightBatch(" +
                " firstOffset=" + firstOffset +
                ", lastOffset=" + lastOffset +
                ", inFlightState=" + inFlightState +
                ")";
        }
    }

    /**
     * The InFlightState is used to track the state and delivery count of a record that has been
     * fetched from the leader. The state of the record is used to determine if the record should
     * be re-deliver or if it can be acknowledged or archived.
     */
    private static class InFlightState {
        /**
         * The state of the fetch batch records.
         */
        private RecordState state;
        /**
         * The number of times the records has been delivered to the client.
         */
        private int deliveryCount;
        /**
         * The member id of the client that is fetching/acknowledging the record.
         */
        private String memberId;

        InFlightState(RecordState state, int deliveryCount, String memberId) {
            this.state = state;
            this.deliveryCount = deliveryCount;
            this.memberId = memberId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(state, deliveryCount, memberId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            InFlightState that = (InFlightState) o;
            return state == that.state && deliveryCount == that.deliveryCount && memberId.equals(that.memberId);
        }

        @Override
        public String toString() {
            return "InFlightState(" +
                " state=" + state.toString() +
                ", deliveryCount=" + deliveryCount +
                ", memberId=" + memberId +
                ")";
        }
    }

    /**
     * The AcknowledgementBatch containing the fields required to acknowledge the fetched records.
     */
    public static class AcknowledgementBatch {

        private final long firstOffset;
        private final long lastOffset;
        private final List<Byte> acknowledgeTypes;

        public AcknowledgementBatch(long firstOffset, long lastOffset, List<Byte> acknowledgeTypes) {
            this.firstOffset = firstOffset;
            this.lastOffset = lastOffset;
            this.acknowledgeTypes = acknowledgeTypes;
        }

        public long firstOffset() {
            return firstOffset;
        }

        public long lastOffset() {
            return lastOffset;
        }

        public List<Byte> acknowledgeTypes() {
            return acknowledgeTypes;
        }

        @Override
        public String toString() {
            return "AcknowledgementBatch(" +
                " firstOffset=" + firstOffset +
                ", lastOffset=" + lastOffset +
                ", acknowledgeTypes=" + ((acknowledgeTypes == null) ? "" : acknowledgeTypes) +
                ")";
        }
    }
}
