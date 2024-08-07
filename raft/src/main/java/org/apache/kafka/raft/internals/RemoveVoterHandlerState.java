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
package org.apache.kafka.raft.internals;

import org.apache.kafka.common.message.RemoveRaftVoterResponseData;
import org.apache.kafka.common.utils.Timer;

import java.util.concurrent.CompletableFuture;

public final class RemoveVoterHandlerState {
    private final long lastOffset;
    private final Timer timeout;
    private final CompletableFuture<RemoveRaftVoterResponseData> future = new CompletableFuture<>();

    RemoveVoterHandlerState(long lastOffset, Timer timeout) {
        this.lastOffset = lastOffset;
        this.timeout = timeout;
    }

    public long timeUntilOperationExpiration(long currentTimeMs) {
        timeout.update(currentTimeMs);
        return timeout.remainingMs();
    }

    public CompletableFuture<RemoveRaftVoterResponseData> future() {
        return future;
    }

    public long lastOffset() {
        return lastOffset;
    }
}
