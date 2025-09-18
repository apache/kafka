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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ApiVersions;

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

public class ThreadSafeAsyncConsumerState extends ThreadSafeConsumerState {

    private final SubscriptionState subscriptions;
    private final OffsetFetcherUtils offsetFetcherUtils;
    private final ThreadSafeAutoCommitState autoCommitState;

    /**
     * Exception that occurred while updating positions after the triggering event had already
     * expired. It will be propagated and cleared on the next call to update fetch positions.
     */
    private final ThreadSafeExceptionReference positionsUpdateError;

    public ThreadSafeAsyncConsumerState(ThreadSafeAutoCommitState autoCommitState,
                                        LogContext logContext,
                                        ConsumerMetadata metadata,
                                        SubscriptionState subscriptions,
                                        Time time,
                                        long retryBackoffMs,
                                        ApiVersions apiVersions) {
        this.autoCommitState = autoCommitState;
        this.subscriptions = subscriptions;
        this.offsetFetcherUtils = new OffsetFetcherUtils(
            logContext,
            metadata,
            subscriptions,
            time,
            retryBackoffMs,
            apiVersions
        );
        this.positionsUpdateError = new ThreadSafeExceptionReference();
    }

    public ThreadSafeAutoCommitState autoCommitState() {
        return autoCommitState;
    }

    OffsetFetcherUtils offsetFetcherUtils() {
        return offsetFetcherUtils;
    }

    public ThreadSafeExceptionReference positionsUpdateError() {
        return positionsUpdateError;
    }

    public boolean canSkipUpdateFetchPositions() {
        positionsUpdateError.maybeThrowException();
        metadataError.maybeClearAndThrowException();

        // In cases of metadata updates, getPartitionsToValidate() will review the partitions and
        // determine which, if any, need to be validated. If any partitions require validation, the
        // update fetch positions step can't be skipped.
        if (!offsetFetcherUtils.getPartitionsToValidate().isEmpty())
            return false;

        // If there are no partitions in the AWAIT_RESET, AWAIT_VALIDATION, or INITIALIZING states, it's ok to skip.
        return subscriptions.hasAllFetchPositions();
    }

    public boolean canSkipWaitingOnPoll(long currentTimeMs) {
        if (reconciliationState.isInProgress())
            return false;

        autoCommitState.updateTimer(currentTimeMs);
        return !autoCommitState.shouldAutoCommit();
    }
}
