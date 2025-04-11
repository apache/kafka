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

import org.apache.kafka.common.message.AddRaftVoterResponseData;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.raft.Endpoints;
import org.apache.kafka.raft.ReplicaKey;

import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

public final class AddVoterHandlerState {
    private final ReplicaKey voterKey;
    private final Endpoints voterEndpoints;
    private final Timer timeout;
    private final CompletableFuture<AddRaftVoterResponseData> future = new CompletableFuture<>();

    private OptionalLong lastOffset = OptionalLong.empty();

    AddVoterHandlerState(
        ReplicaKey voterKey,
        Endpoints voterEndpoints,
        Timer timeout
    ) {
        this.voterKey = voterKey;
        this.voterEndpoints = voterEndpoints;
        this.timeout = timeout;
    }

    public long timeUntilOperationExpiration(long currentTimeMs) {
        timeout.update(currentTimeMs);
        return timeout.remainingMs();
    }

    public boolean expectingApiResponse(int replicaId) {
        return lastOffset.isEmpty() && replicaId == voterKey.id();
    }

    public void setLastOffset(long lastOffset) {
        if (this.lastOffset.isPresent()) {
            throw new IllegalStateException(
                String.format(
                    "Cannot override last offset to %s for adding voter %s because it is " +
                    "already set to %s",
                    lastOffset,
                    voterKey,
                    this.lastOffset
                )
            );
        }

        this.lastOffset = OptionalLong.of(lastOffset);
    }

    public ReplicaKey voterKey() {
        return voterKey;
    }

    public Endpoints voterEndpoints() {
        return voterEndpoints;
    }

    public OptionalLong lastOffset() {
        return lastOffset;
    }

    public CompletableFuture<AddRaftVoterResponseData> future() {
        return future;
    }
}
