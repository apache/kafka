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
import java.util.concurrent.CompletionStage;

/**
 * Tracks the state of a single pending add voter operation.
 * <p>
 * An instance is created by {@link AddVoterHandler#handleAddVoterRequest} once the API_VERSIONS
 * request has been sent to the new voter, and is held by {@link ChangeVoterHandlerState} until
 * the operation completes, is aborted, or expires.
 */
public final class AddVoterHandlerState {
    private final ReplicaKey voterKey;
    private final Endpoints voterEndpoints;
    private final boolean ackWhenCommitted;
    private final Timer timeout;
    private final CompletableFuture<AddRaftVoterResponseData> future = new CompletableFuture<>();

    private OptionalLong lastOffset = OptionalLong.empty();

    AddVoterHandlerState(
        ReplicaKey voterKey,
        Endpoints voterEndpoints,
        boolean ackWhenCommitted,
        Timer timeout
    ) {
        this.voterKey = voterKey;
        this.voterEndpoints = voterEndpoints;
        this.ackWhenCommitted = ackWhenCommitted;
        this.timeout = timeout;
    }

    /**
     * Returns the time in milliseconds until this operation expires.
     *
     * @param currentTimeMs the current time in milliseconds
     * @return the remaining time in milliseconds until expiration
     */
    public long timeUntilOperationExpiration(long currentTimeMs) {
        timeout.update(currentTimeMs);
        return timeout.remainingMs();
    }

    /**
     * Checks whether this handler state is expecting an API_VERSIONS response from the given replica.
     *
     * @param replicaId the replica id to check
     * @return true if expecting a response from this replica, false otherwise
     */
    public boolean expectingApiResponse(int replicaId) {
        return lastOffset.isEmpty() && replicaId == voterKey.id();
    }

    /**
     * Sets the offset of the VotersRecord that was appended to the log for this add voter operation.
     *
     * @param lastOffset the offset of the VotersRecord that was appended to the log
     * @throws IllegalStateException if the last offset has already been set
     */
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

    /**
     * Returns the voter key for the voter being added.
     *
     * @return the voter key
     */
    public ReplicaKey voterKey() {
        return voterKey;
    }

    /**
     * Returns the endpoints for the voter being added.
     *
     * @return the voter endpoints
     */
    public Endpoints voterEndpoints() {
        return voterEndpoints;
    }

    /**
     * Returns whether the AddVoter response should be withheld until the voter change commits.
     *
     * @return true if the response should be sent only after the change commits, false if it
     *         should be sent as soon as the change is appended to the log
     */
    public boolean ackWhenCommitted() {
        return ackWhenCommitted;
    }

    /**
     * Returns the offset of the VotersRecord if it has been appended to the log.
     *
     * @return the last offset, or empty if not yet appended
     */
    public OptionalLong lastOffset() {
        return lastOffset;
    }

    /**
     * Completes the future with the provided response.
     *
     * @param response the response to complete the future with
     */
    public void completeFuture(AddRaftVoterResponseData response) {
        future.complete(response);
    }

    CompletionStage<AddRaftVoterResponseData> future() {
        return future;
    }
}
