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
     * Initialize the state machine that will be used by this client. This should
     * only be called once after creation and calls to {@link #append(Records)} should
     * be made until this method returns.
     *
     * @param stateMachine The state machine implementation
     * @throws IOException For any IO errors during initialization
     */
    void initialize(DistributedStateMachine stateMachine) throws IOException;

    /**
     * Append a new entry to the log. The client must be in the leader state to
     * accept an append: it is up to the {@link DistributedStateMachine} implementation
     * to ensure this using {@link DistributedStateMachine#becomeLeader(int)}. Note that
     * the records will only be appended to the log if they are accepted by the state
     * machine with {@link DistributedStateMachine#accept(Records)}.
     *
     * This method must be thread-safe.
     *
     * @param records The records to append to the log
     * @return A future containing the base offset and epoch of the appended records (if successful)
     */
    CompletableFuture<OffsetAndEpoch> append(Records records);

    /**
     * Shutdown the client.
     *
     * @param timeoutMs How long to wait for graceful completion of pending operations.
     */
    void shutdown(int timeoutMs);

}
