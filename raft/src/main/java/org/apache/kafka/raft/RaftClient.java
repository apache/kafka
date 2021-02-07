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

import org.apache.kafka.snapshot.SnapshotWriter;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface RaftClient<T> extends Closeable {

    interface Listener<T> {
        /**
         * Callback which is invoked for all records committed to the log.
         * It is the responsibility of the caller to invoke {@link BatchReader#close()}
         * after consuming the reader.
         *
         * Note that there is not a one-to-one correspondence between writes through
         * {@link #scheduleAppend(int, List)} and this callback. The Raft implementation
         * is free to batch together the records from multiple append calls provided
         * that batch boundaries are respected. This means that each batch specified
         * through {@link #scheduleAppend(int, List)} is guaranteed to be a subset of
         * a batch provided by the {@link BatchReader}.
         *
         * @param reader reader instance which must be iterated and closed
         */
        void handleCommit(BatchReader<T> reader);

        /**
         * Invoked after this node has become a leader. This is only called after
         * all commits up to the start of the leader's epoch have been sent to
         * {@link #handleCommit(BatchReader)}.
         *
         * After becoming a leader, the client is eligible to write to the log
         * using {@link #scheduleAppend(int, List)}.
         *
         * @param epoch the claimed leader epoch
         */
        default void handleClaim(int epoch) {}

        /**
         * Invoked after a leader has stepped down. This callback may or may not
         * fire before the next leader has been elected.
         */
        default void handleResign() {}
    }

    /**
     * Initialize the client.
     * This should only be called once on startup.
     *
     * @param raftConfig the Raft quorum configuration
     * @throws IOException For any IO errors during initialization
     */
    void initialize() throws IOException;

    /**
     * Register a listener to get commit/leader notifications.
     *
     * @param listener the listener
     */
    void register(Listener<T> listener);

    /**
     * Append a list of records to the log. The write will be scheduled for some time
     * in the future. There is no guarantee that appended records will be written to
     * the log and eventually committed. However, it is guaranteed that if any of the
     * records become committed, then all of them will be.
     *
     * If the provided current leader epoch does not match the current epoch, which
     * is possible when the state machine has yet to observe the epoch change, then
     * this method will return {@link Long#MAX_VALUE} to indicate an offset which is
     * not possible to become committed. The state machine is expected to discard all
     * uncommitted entries after observing an epoch change.
     *
     * @param epoch the current leader epoch
     * @param records the list of records to append
     * @return the offset within the current epoch that the log entries will be appended,
     *         or null if the leader was unable to accept the write (e.g. due to memory
     *         being reached).
     */
    Long scheduleAppend(int epoch, List<T> records);

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
     * Create a writable snapshot file for a given offset and epoch.
     *
     * The RaftClient assumes that the snapshot return will contain the records up to but
     * not including the end offset in the snapshot id. See {@link SnapshotWriter} for
     * details on how to use this object.
     *
     * @param snapshotId the end offset and epoch that identifies the snapshot
     * @return a writable snapshot
     */
    SnapshotWriter<T> createSnapshot(OffsetAndEpoch snapshotId) throws IOException;
}
