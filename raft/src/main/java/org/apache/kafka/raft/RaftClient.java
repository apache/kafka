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

import org.apache.kafka.common.record.Records;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface RaftClient<T> {

    interface Listener<T> {
        /**
         * Callback which is invoked when records written through {@link #scheduleAppend(int, List)}
         * become committed.
         *
         * Note that there is not a one-to-one correspondence between writes through
         * {@link #scheduleAppend(int, List)} and this callback. The Raft implementation
         * is free to batch together the records from multiple append calls provided
         * that batch boundaries are respected. This means that each batch specified
         * through {@link #scheduleAppend(int, List)} is guaranteed to be a subset of
         * a batch passed to {@link #handleCommit(int, long, List)}.
         *
         * @param epoch the epoch in which the write was accepted
         * @param lastOffset the offset of the last record in the record list
         * @param records the set of records that were committed
         */
        void handleCommit(int epoch, long lastOffset, List<T> records);
    }

    /**
     * Initialize the client. This should only be called once and it must be
     * called before any of the other APIs can be invoked.
     *
     * @throws IOException For any IO errors during initialization
     */
    void initialize(Listener<T> listener) throws IOException;

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
     * Read a set of records from the log. Note that it is the responsibility of the state machine
     * to filter control records added by the Raft client itself.
     *
     * If the fetch offset is no longer valid, then the future will be completed exceptionally
     * with a {@link LogTruncationException}.
     *
     * @param position The position to fetch from
     * @param isolation The isolation level to apply to the read
     * @param maxWaitTimeMs The maximum time to wait for new data to become available before completion
     * @return The record set, which may be empty if fetching from the end of the log
     */
    CompletableFuture<Records> read(OffsetAndEpoch position, Isolation isolation, long maxWaitTimeMs);

    /**
     * Get the current leader (if known) and the current epoch.
     *
     * @return Current leader and epoch information
     */
    LeaderAndEpoch currentLeaderAndEpoch();

    /**
     * Shutdown the client.
     *
     * @param timeoutMs How long to wait for graceful completion of pending operations.
     * @return A future which is completed when shutdown completes successfully or the timeout expires.
     */
    CompletableFuture<Void> shutdown(int timeoutMs);

}
