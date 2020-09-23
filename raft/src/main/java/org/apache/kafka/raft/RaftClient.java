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
import java.util.concurrent.CompletableFuture;

public interface RaftClient {

    /**
     * Initialize the client. This should only be called once and it must be
     * called before any of the other APIs can be invoked.
     *
     * @throws IOException For any IO errors during initialization
     */
    void initialize() throws IOException;

    /**
     * Append a new entry to the log. The client must be in the leader state to
     * accept an append: it is up to the state machine implementation
     * to ensure this using {@link #currentLeaderAndEpoch()}.
     *
     * TODO: One improvement we can make here is to allow the caller to specify
     * the current leader epoch in the record set. That would ensure that each
     * leader change must be "observed" by the state machine before new appends
     * are accepted.
     *
     * @param records The records to append to the log
     * @param timeoutMs Maximum time to wait for the append to complete
     * @return A future containing the last offset and epoch of the appended records (if successful)
     */
    CompletableFuture<OffsetAndEpoch> append(Records records, AckMode ackMode, long timeoutMs);

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
