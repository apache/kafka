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
package org.apache.kafka.raft;

import org.apache.kafka.raft.errors.BufferAllocationException;
import org.apache.kafka.raft.errors.NotLeaderException;
import org.apache.kafka.raft.errors.UnexpectedBaseOffsetException;
import org.apache.kafka.snapshot.SnapshotReader;
import org.apache.kafka.snapshot.SnapshotWriter;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

public interface RaftClient<T> extends AutoCloseable {

    interface Listener<T> {
        /**
         * Callback which is invoked for all records committed to the log.
         * It is the responsibility of this implementation to invoke {@link BatchReader#close()}
         * after consuming the reader.
         *
         * Note that there is not a one-to-one correspondence between writes through
         * {@link #scheduleAppend(int, List)} or {@link #scheduleAtomicAppend(int, OptionalLong, List)}
         * and this callback. The Raft implementation is free to batch together the records
         * from multiple append calls provided that batch boundaries are respected. Records
         * specified through {@link #scheduleAtomicAppend(int, OptionalLong, List)} are guaranteed to be a
         * subset of a batch provided by the {@link BatchReader}. Records specified through
         * {@link #scheduleAppend(int, List)} are guaranteed to be in the same order but
         * they can map to any number of batches provided by the {@link BatchReader}.
         *
         * @param reader reader instance which must be iterated and closed
         */
        void handleCommit(BatchReader<T> reader);

        /**
         * Callback which is invoked when the Listener needs to load a snapshot.
         * It is the responsibility of this implementation to invoke {@link SnapshotReader#close()}
         * after consuming the reader.
         *
         * When handling this call, the implementation must assume that all previous calls
         * to {@link #handleCommit} contain invalid data.
         *
         * @param reader snapshot reader instance which must be iterated and closed
         */
        void handleLoadSnapshot(SnapshotReader<T> reader);

        /**
         * Called on any change to leadership. This includes both when a leader is elected and
         * when a leader steps down or fails.
         *
         * If this node is the leader, then the notification of leadership will be delayed until
         * the implementation of this interface has caught up to the high-watermark through calls to
         * {@link #handleLoadSnapshot(SnapshotReader)} and {@link #handleCommit(BatchReader)}.
         *
         * If this node is not the leader, then this method will be called as soon as possible. In
         * this case the leader may or may not be known for the current epoch.
         *
         * Subsequent calls to this method will expose a monotonically increasing epoch. For a
         * given epoch the leader may be unknown, {@code leader.leaderId} is {@code OptionalInt#empty},
         * or known {@code leader.leaderId} is {@code OptionalInt#of}. Once a leader is known for
         * a given epoch it will remain the leader for that epoch. In other words, the implementation of
         * method should expect this method will be called at most twice for each epoch. Once if the
         * epoch changed but the leader is not known and once when the leader is known for the current
         * epoch.
         *
         * @param leader the current leader and epoch
         */
        default void handleLeaderChange(LeaderAndEpoch leader) {}

        default void beginShutdown() {}
    }

    /**
     * Register a listener to get commit, snapshot and leader notifications.
     *
     * The implementation of this interface assumes that each call to {@code register} uses
     * a different {@code Listener} instance. If the same instance is used for multiple calls
     * to this method, then only one {@code Listener} will be registered.
     *
     * @param listener the listener to register
     */
    void register(Listener<T> listener);

    /**
     * Unregisters a listener.
     *
     * To distinguish from events that happened before the call to {@code unregister} and a future
     * call to {@code register}, different {@code Listener} instances must be used.
     *
     * If the {@code Listener} provided was never registered then the unregistration is ignored. 
     *
     * @param listener the listener to unregister
     */
    void unregister(Listener<T> listener);

    /**
     * Returns the current high watermark, or OptionalLong.empty if it is not known.
     */
    OptionalLong highWatermark();

    /**
     * Return the current {@link LeaderAndEpoch}.
     *
     * @return the current leader and epoch
     */
    LeaderAndEpoch leaderAndEpoch();

    /**
     * Get local nodeId if one is defined. This may be absent when the client is used
     * as an anonymous observer, as in the case of the metadata shell.
     *
     * @return optional node id
     */
    OptionalInt nodeId();

    /**
     * Append a list of records to the log. The write will be scheduled for some time
     * in the future. There is no guarantee that appended records will be written to
     * the log and eventually committed. While the order of the records is preserve, they can
     * be appended to the log using one or more batches. Each record may be committed independently.
     * If a record is committed, then all records scheduled for append during this epoch
     * and prior to this record are also committed.
     *
     * If the provided current leader epoch does not match the current epoch, which
     * is possible when the state machine has yet to observe the epoch change, then
     * this method will throw an {@link NotLeaderException} to indicate the leader
     * to resign its leadership. The state machine is expected to discard all
     * uncommitted entries after observing an epoch change.
     *
     * @param epoch the current leader epoch
     * @param records the list of records to append
     * @return the expected offset of the last record if append succeed
     * @throws org.apache.kafka.common.errors.RecordBatchTooLargeException if the size of the records is greater than the maximum
     *         batch size; if this exception is throw none of the elements in records were
     *         committed
     * @throws NotLeaderException if we are not the current leader or the epoch doesn't match the leader epoch
     * @throws BufferAllocationException if we failed to allocate memory for the records
     */
    long scheduleAppend(int epoch, List<T> records);

    /**
     * Append a list of records to the log. The write will be scheduled for some time
     * in the future. There is no guarantee that appended records will be written to
     * the log and eventually committed. However, it is guaranteed that if any of the
     * records become committed, then all of them will be.
     *
     * If the provided current leader epoch does not match the current epoch, which
     * is possible when the state machine has yet to observe the epoch change, then
     * this method will throw an {@link NotLeaderException} to indicate the leader
     * to resign its leadership. The state machine is expected to discard all
     * uncommitted entries after observing an epoch change.
     *
     * If the current base offset does not match the supplied required base offset,
     * then this method will throw {@link UnexpectedBaseOffsetException}.
     *
     * @param epoch the current leader epoch
     * @param requiredBaseOffset if this is set, it is the offset we must use as the base offset.
     * @param records the list of records to append
     * @return the expected offset of the last record if append succeed
     * @throws org.apache.kafka.common.errors.RecordBatchTooLargeException if the size of the records is greater than the maximum
     *         batch size; if this exception is throw none of the elements in records were
     *         committed
     * @throws NotLeaderException if we are not the current leader or the epoch doesn't match the leader epoch
     * @throws BufferAllocationException we failed to allocate memory for the records
     * @throws UnexpectedBaseOffsetException the requested base offset could not be obtained.
     */
    long scheduleAtomicAppend(int epoch, OptionalLong requiredBaseOffset, List<T> records);

    /**
     * Attempt a graceful shutdown of the client. This allows the leader to proactively
     * resign and help a new leader to get elected rather than forcing the remaining
     * voters to wait for the fetch timeout.
     *
     * Note that if the client has hit an unexpected exception which has left it in an
     * indeterminate state, then the call to shutdown should be skipped. However, it
     * is still expected that {@link #close()} will be used to clean up any resources
     * in use.
     *
     * @param timeoutMs How long to wait for graceful completion of pending operations.
     * @return A future which is completed when shutdown completes successfully or the timeout expires.
     */
    CompletableFuture<Void> shutdown(int timeoutMs);

    /**
     * Resign the leadership. The leader will give up its leadership in the passed epoch
     * (if it matches the current epoch), and a new election will be held. Note that nothing
     * prevents this node from being reelected as the leader.
     *
     * Notification of successful resignation can be observed through
     * {@link Listener#handleLeaderChange(LeaderAndEpoch)}.
     *
     * @param epoch the epoch to resign from. If this epoch is smaller than the current epoch, this
     *              call will be ignored.
     *
     * @throws IllegalArgumentException - if the passed epoch is invalid (negative or greater than current) or
     * if the listener is not the leader associated with this epoch.
     */
    void resign(int epoch);

    /**
     * Create a writable snapshot file for a committed offset and epoch.
     *
     * The RaftClient assumes that the snapshot returned will contain the records up to, but not
     * including the committed offset and epoch. If no records have been committed, it is possible
     * to generate an empty snapshot using 0 for both the offset and epoch.
     *
     * See {@link SnapshotWriter} for details on how to use this object. If a snapshot already
     * exists then returns an {@link Optional#empty()}.
     *
     * @param snapshotId The ID of the new snapshot, which includes the (exclusive) last committed offset
     *                   and the last committed epoch.
     * @param lastContainedLogTime The append time of the highest record contained in this snapshot
     * @return a writable snapshot if it doesn't already exist
     * @throws IllegalArgumentException if the committed offset is greater than the high-watermark
     *         or less than the log start offset.
     */
    Optional<SnapshotWriter<T>> createSnapshot(OffsetAndEpoch snapshotId, long lastContainedLogTime);

    /**
     * The snapshot id for the latest snapshot.
     *
     * Returns the snapshot id of the latest snapshot, if it exists. If a snapshot doesn't exist, returns an
     * {@link Optional#empty()}.
     *
     * @return the id of the latest snapshot, if it exists
     */
    Optional<OffsetAndEpoch> latestSnapshotId();

    /**
     * Returns the current end of the log. This method is thread-safe.
     *
     * @return the log end offset, which is one greater than the offset of the last record written,
     * or 0 if there have not been any records written.
     */
    long logEndOffset();
}
