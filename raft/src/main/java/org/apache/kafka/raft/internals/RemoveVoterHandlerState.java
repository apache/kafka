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
import java.util.concurrent.CompletionStage;

/**
 * Tracks the state of a single pending remove voter operation.
 * <p>
 * An instance is created by {@link RemoveVoterHandler#handleRemoveVoterRequest} once the
 * updated VotersRecord has been appended to the log, and is held by
 * {@link ChangeVoterHandlerState} until the record commits or the operation expires.
 */
public final class RemoveVoterHandlerState {
    private final long lastOffset;
    private final Timer timeout;
    private final CompletableFuture<RemoveRaftVoterResponseData> future = new CompletableFuture<>();

    RemoveVoterHandlerState(long lastOffset, Timer timeout) {
        this.lastOffset = lastOffset;
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
     * Completes the future with the provided response.
     *
     * @param response the response to complete the future with
     */
    public void completeFuture(RemoveRaftVoterResponseData response) {
        future.complete(response);
    }

    /**
     * Returns the offset of the VotersRecord that was appended to the log for this remove voter
     * operation.
     *
     * @return the offset of the appended VotersRecord
     */
    public long lastOffset() {
        return lastOffset;
    }

    CompletionStage<RemoveRaftVoterResponseData> future() {
        return future;
    }
}
