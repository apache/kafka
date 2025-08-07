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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

class SharedOffsetsState implements MemberStateListener {

    // to keep from repeatedly scanning subscriptions in poll(), cache the result during metadata updates
    private final AtomicBoolean cachedSubscriptionHasAllFetchPositions = new AtomicBoolean();
    /**
     * Exception that occurred while updating positions after the triggering event had already
     * expired. It will be propagated and cleared on the next call to update fetch positions.
     */
    private final AtomicReference<Throwable> cachedUpdatePositionsException = new AtomicReference<>();
    private final OffsetFetcherUtils offsetFetcherUtils;

    SharedOffsetsState(LogContext logContext,
                              ConsumerMetadata metadata,
                              SubscriptionState subscriptions,
                              Time time,
                              long retryBackoffMs,
                              ApiVersions apiVersions) {
        this.offsetFetcherUtils = new OffsetFetcherUtils(
            logContext,
            metadata,
            subscriptions,
            time,
            retryBackoffMs,
            apiVersions
        );
    }

    Optional<Throwable> getAndClearCachedUpdatePositionsException() {
        return Optional.ofNullable(cachedUpdatePositionsException.getAndSet(null));
    }

    void setCachedUpdatePositionsException(Throwable t) {
        cachedUpdatePositionsException.set(t);
    }

    boolean subscriptionHasAllFetchPositions() {
        return cachedSubscriptionHasAllFetchPositions.get();
    }

    void setSubscriptionHasAllFetchPositions(boolean value) {
        cachedSubscriptionHasAllFetchPositions.set(value);
    }

    Map<TopicPartition, SubscriptionState.FetchPosition> getPartitionsToValidate() {
        return offsetFetcherUtils.getPartitionsToValidate();
    }

    boolean canSkipUpdateFetchPositions() {
        Optional<Throwable> error = getAndClearCachedUpdatePositionsException();

        if (error.isPresent())
            throw ConsumerUtils.maybeWrapAsKafkaException(error.get());

        return getPartitionsToValidate().isEmpty() && subscriptionHasAllFetchPositions();
    }

    OffsetFetcherUtils offsetFetcherUtils() {
        return offsetFetcherUtils;
    }

    @Override
    public void onMemberEpochUpdated(Optional<Integer> memberEpoch, String memberId) {
        // Ignore...
    }

    @Override
    public void onGroupAssignmentUpdated(Set<TopicPartition> partitions) {
        setSubscriptionHasAllFetchPositions(false);
    }
}
