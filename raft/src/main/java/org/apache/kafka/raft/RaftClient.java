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
     * only be called once after creation and calls to {@link RecordAppender#append(Records)} should
     * be made until this method returns.
     *
     * @param stateMachine The state machine implementation
     * @throws IOException For any IO errors during initialization
     */
    void initialize(ReplicatedStateMachine stateMachine) throws IOException;

    /**
     * Shutdown the client.
     *
     * @param timeoutMs How long to wait for graceful completion of pending operations.
     * @return A future which is completed when shutdown completes successfully or the timeout expires.
     */
    CompletableFuture<Void> shutdown(int timeoutMs);
}
