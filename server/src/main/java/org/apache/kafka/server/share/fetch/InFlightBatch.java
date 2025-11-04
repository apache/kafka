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
package org.apache.kafka.server.share.fetch;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.share.metrics.SharePartitionMetrics;
import org.apache.kafka.server.util.timer.Timer;

import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.apache.kafka.server.share.fetch.InFlightState.EMPTY_MEMBER_ID;

/**
 * The InFlightBatch maintains the in-memory state of the fetched records i.e. in-flight records.
 * <p>
 * This class is not thread-safe and caller should attain locks if concurrent updates on same batch
 * are expected.
 */
public class InFlightBatch {
    // The timer is used to schedule the acquisition lock timeout task for the batch.
    private final Timer timer;
    // The time is used to get the current time in milliseconds.
    private final Time time;
    // The offset of the first record in the batch that is fetched from the log.
    private final long firstOffset;
    // The last offset of the batch that is fetched from the log.
    private final long lastOffset;
    // The acquisition lock timeout handler is used to release the acquired records when the acquisition
    // lock timeout is reached.
    private final AcquisitionLockTimeoutHandler timeoutHandler;
    // The share partition metrics are used to track the metrics related to the share partition.
    private final SharePartitionMetrics sharePartitionMetrics;

    // The batch state of the fetched records. If the offset state map is empty then batchState
    // determines the state of the complete batch else individual offset determines the state of
    // the respective records.
    private InFlightState batchState;

    // The offset state map is used to track the state of the records per offset. However, the
    // offset state map is only required when the state of the offsets within same batch are
    // different. The states can be different when explicit offset acknowledgement is done which
    // is different from the batch state.
    private NavigableMap<Long, InFlightState> offsetState;

    public InFlightBatch(
        Timer timer,
        Time time,
        String memberId,
        long firstOffset,
        long lastOffset,
        RecordState state,
        int deliveryCount,
        AcquisitionLockTimerTask acquisitionLockTimeoutTask,
        AcquisitionLockTimeoutHandler timeoutHandler,
        SharePartitionMetrics sharePartitionMetrics
    ) {
        this.timer = timer;
        this.time = time;
        this.firstOffset = firstOffset;
        this.lastOffset = lastOffset;
        this.timeoutHandler = timeoutHandler;
        this.sharePartitionMetrics = sharePartitionMetrics;
        this.batchState = new InFlightState(state, deliveryCount, memberId, acquisitionLockTimeoutTask);
    }

    /**
     * @return the first offset of the batch.
     */
    public long firstOffset() {
        return firstOffset;
    }

    /**
     * @return the last offset of the batch.
     */
    public long lastOffset() {
        return lastOffset;
    }

    /**
     * @return the state of the batch.
     * @throws IllegalStateException if the offset state is maintained and the batch state is not available.
     */
    public RecordState batchState() {
        return inFlightState().state();
    }

    /**
     * @return the member id of the batch.
     * @throws IllegalStateException if the offset state is maintained and the batch state is not available.
     */
    public String batchMemberId() {
        return inFlightState().memberId();
    }

    /**
     * @return the delivery count of the batch.
     * @throws IllegalStateException if the offset state is maintained and the batch state is not available.
     */
    public int batchDeliveryCount() {
        return inFlightState().deliveryCount();
    }

    /**
     * @return the acquisition lock timeout task for the batch.
     * @throws IllegalStateException if the offset state is maintained and the batch state is not available.
     */
    public AcquisitionLockTimerTask batchAcquisitionLockTimeoutTask() {
        return inFlightState().acquisitionLockTimeoutTask();
    }

    /**
     * @return the offset state map which maintains the state of the records per offset.
     */
    public NavigableMap<Long, InFlightState> offsetState() {
        return offsetState;
    }

    /**
     * Cancel the acquisition lock timeout task and clear the reference to it.
     * This method is used to cancel the acquisition lock timeout task if it exists
     * and clear the reference to it.
     * @throws IllegalStateException if the offset state is maintained and the batch state is not available.
     */
    public void cancelAndClearAcquisitionLockTimeoutTask() {
        inFlightState().cancelAndClearAcquisitionLockTimeoutTask();
    }

    /**
     * @return true if the batch has an ongoing state transition, false otherwise.
     * @throws IllegalStateException if the offset state is maintained and the batch state is not available.
     */
    public boolean batchHasOngoingStateTransition() {
        return inFlightState().hasOngoingStateTransition();
    }

    /**
     * Archive the batch state. This is used to mark the batch as archived and no further updates
     * are allowed to the batch state.
     * @throws IllegalStateException if the offset state is maintained and the batch state is not available.
     */
    public void archiveBatch() {
        inFlightState().archive();
    }

    /**
     * Try to update the batch state. The state of the batch can only be updated if the new state is allowed
     * to be transitioned from old state. The delivery count is not changed if the state update is unsuccessful.
     *
     * @param newState The new state of the records.
     * @param ops      The behavior on the delivery count.
     * @param maxDeliveryCount The maximum delivery count for the records.
     * @param newMemberId The new member id for the records.
     * @return {@code InFlightState} if update succeeds, null otherwise. Returning state helps update chaining.
     * @throws IllegalStateException if the offset state is maintained and the batch state is not available.
     */
    public InFlightState tryUpdateBatchState(RecordState newState, DeliveryCountOps ops, int maxDeliveryCount, String newMemberId) {
        return inFlightState().tryUpdateState(newState, ops, maxDeliveryCount, newMemberId);
    }

    /**
     * Start a state transition for the batch. This is used to mark the batch as in-flight and
     * no further updates are allowed to the batch state.
     *
     * @param newState The new state of the records.
     * @param ops      The behavior on the delivery count.
     * @param maxDeliveryCount The maximum delivery count for the records.
     * @param newMemberId The new member id for the records.
     * @return {@code InFlightState} if update succeeds, null otherwise. Returning state helps update chaining.
     * @throws IllegalStateException if the offset state is maintained and the batch state is not available.
     */
    public InFlightState startBatchStateTransition(RecordState newState, DeliveryCountOps ops, int maxDeliveryCount,
        String newMemberId
    ) {
        return inFlightState().startStateTransition(newState, ops, maxDeliveryCount, newMemberId);
    }

    /**
     * This method initializes the offset states in two ranges:
     * [firstOffset to targetOffset]: initialize each offset using the current batch state and schedule
     * an acquisition lock timeout task for each offset.
     * (targetOffset to lastOffset]: initialize each offset to {@link RecordState#AVAILABLE}
     * and do not schedule any timer task for these offsets.
     *
     * @param targetOffset The target offset up to which the offset states are initialized using the current batch state.
     * @param delayMs The delay in milliseconds for the acquisition lock timeout task.
     */
    public void maybeInitializeOffsetStateUpdate(long targetOffset, int delayMs) {
        if (offsetState == null) {
            offsetState = new ConcurrentSkipListMap<>();
            for (long offset = this.firstOffset; offset <= this.lastOffset; offset++) {
                if (offset <= targetOffset) {
                    AcquisitionLockTimerTask timerTask = acquisitionLockTimerTask(batchState.memberId(), offset, offset, delayMs);
                    offsetState.put(offset, new InFlightState(batchState.state(), batchState.deliveryCount(), batchState.memberId(), timerTask));
                    timer.add(timerTask);
                } else {
                    offsetState.put(offset, new InFlightState(RecordState.AVAILABLE, batchState.deliveryCount() - 1, EMPTY_MEMBER_ID));
                }
            }
            batchState = null;
        }
    }

    /**
     * Initialize the offset state map if it is not already initialized. This is used to maintain the state of the
     * records per offset when the state of the offsets within same batch are different.
     */
    public void maybeInitializeOffsetStateUpdate() {
        if (offsetState == null) {
            offsetState = new ConcurrentSkipListMap<>();
            // The offset state map is not initialized hence initialize the state of the offsets
            // from the first offset to the last offset. Mark the batch inflightState to null as
            // the state of the records is maintained in the offset state map now.
            for (long offset = this.firstOffset; offset <= this.lastOffset; offset++) {
                if (batchState.acquisitionLockTimeoutTask() != null) {
                    // The acquisition lock timeout task is already scheduled for the batch, hence we need to schedule
                    // the acquisition lock timeout task for the offset as well.
                    long delayMs = batchState.acquisitionLockTimeoutTask().expirationMs() - time.hiResClockMs();
                    AcquisitionLockTimerTask timerTask = acquisitionLockTimerTask(batchState.memberId(), offset, offset, delayMs);
                    offsetState.put(offset, new InFlightState(batchState.state(), batchState.deliveryCount(), batchState.memberId(), timerTask));
                    timer.add(timerTask);
                } else {
                    offsetState.put(offset, new InFlightState(batchState.state(), batchState.deliveryCount(), batchState.memberId()));
                }
            }
            // Cancel the acquisition lock timeout task for the batch as the offset state is maintained.
            if (batchState.acquisitionLockTimeoutTask() != null) {
                batchState.cancelAndClearAcquisitionLockTimeoutTask();
            }
            batchState = null;
        }
    }

    /**
     * Update the acquisition lock timeout task for the batch. This is used to update the acquisition lock timeout
     * task for the batch when the acquisition lock timeout is changed.
     *
     * @param acquisitionLockTimeoutTask The new acquisition lock timeout task for the batch.
     * @throws IllegalStateException if the offset state is maintained and the batch state is not available.
     */
    public void updateAcquisitionLockTimeout(AcquisitionLockTimerTask acquisitionLockTimeoutTask) {
        inFlightState().updateAcquisitionLockTimeoutTask(acquisitionLockTimeoutTask);
    }

    private InFlightState inFlightState() {
        if (batchState == null) {
            throw new IllegalStateException("The batch state is not available as the offset state is maintained");
        }
        return batchState;
    }

    private AcquisitionLockTimerTask acquisitionLockTimerTask(
        String memberId,
        long firstOffset,
        long lastOffset,
        long delayMs
    ) {
        return new AcquisitionLockTimerTask(time, delayMs, memberId, firstOffset, lastOffset, timeoutHandler, sharePartitionMetrics);
    }

    @Override
    public String toString() {
        return "InFlightBatch(" +
            "firstOffset=" + firstOffset +
            ", lastOffset=" + lastOffset +
            ", inFlightState=" + batchState +
            ", offsetState=" + ((offsetState == null) ? "null" : offsetState) +
            ")";
    }
}
