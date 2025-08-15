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
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.internals.events.CheckAndUpdatePositionsEvent;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class stores shared state needed by both the application thread ({@link AsyncKafkaConsumer}) and the
 * background thread ({@link OffsetsRequestManager}) to determine if a costly call to check offsets can be skipped
 * inside {@link Consumer#poll(Duration)}.
 *
 * <p/>
 *
 * This class compromises on the ideal of keeping the state only in the background thread. However, this class only
 * relies on the {@link SubscriptionState} and {@link ConsumerMetadata} which are, unfortunately, already used
 * sparingly in both the application and background threads. Both of those classes are heavily synchronized given
 * their use by the {@link ClassicKafkaConsumer}, so their use in a multithreaded fashion is already established.
 */
public class CommitOffsetsSharedState implements MemberStateListener {

    /**
     * To keep from repeatedly scanning subscriptions in poll(), cache the result during metadata updates.
     */
    private final AtomicBoolean cachedSubscriptionHasAllFetchPositions = new AtomicBoolean();

    /**
     * Exception that occurred while updating positions after the triggering event had already
     * expired. It will be propagated and cleared on the next call to update fetch positions.
     */
    private final AtomicReference<Throwable> cachedUpdatePositionsException = new AtomicReference<>();
    private final OffsetFetcherUtils offsetFetcherUtils;
    private final SubscriptionState subscriptions;

    CommitOffsetsSharedState(LogContext logContext,
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
        this.subscriptions = subscriptions;
    }

    Throwable getAndClearCachedUpdatePositionsException() {
        return cachedUpdatePositionsException.getAndSet(null);
    }

    void setCachedUpdatePositionsException(Throwable exception) {
        cachedUpdatePositionsException.set(exception);
    }

    boolean subscriptionHasAllFetchPositions() {
        return cachedSubscriptionHasAllFetchPositions.get();
    }

    void setSubscriptionHasAllFetchPositions(boolean value) {
        cachedSubscriptionHasAllFetchPositions.set(value);
    }

    /**
     * This method is used by {@code AsyncKafkaConsumer#updateFetchPositions()} to determine if it can skip
     * the step of sending (and waiting for) a {@link CheckAndUpdatePositionsEvent}. {@code updateFetchPositions()}
     * is in the critical path for the {@link Consumer#poll(Duration)}, and if the application thread can determine
     * that it doesn't need to perform the {@link OffsetsRequestManager#updateFetchPositions(long)} call (via the
     * {@link CheckAndUpdatePositionsEvent}), that is a big performance savings.
     *
     * <p/>
     *
     * This method performs similar checks to the start of {@link OffsetsRequestManager#updateFetchPositions(long)}:
     *
     * <ol>
     *     <li>
     *         Checks for previous exceptions during update positions
     *         ({@code OffsetsRequestManager#cacheExceptionIfEventExpired()})
     *     </li>
     *     <li>
     *         Checks that the previously cached version of {@link #cachedSubscriptionHasAllFetchPositions} is still
     *         {@code true}. This covers a couple additional cases (like before first assignment), so although it's
     *         not completely optimal, the fallback is we force the check which will block the application thread
     *         while the background thread performs its checks which may result in the value being set back to
     *         {@code true} before the application thread resumes.
     *     </li>
     *     <li>
     *         Checks that there are no positions in the {@link SubscriptionState.FetchStates#AWAIT_VALIDATION}
     *         state ({@link OffsetFetcherUtils#getPartitionsToValidate()})
     *     </li>
     *     <li>
     *         Checks that all positions are in the {@link SubscriptionState.FetchStates#FETCHING} state
     *         ({@link SubscriptionState#hasAllFetchPositions()})
     *     </li>
     * </ol>
     *
     * If the first check fails, an exception will be thrown. If any of the second, third, or fourth checks fail, this
     * method will return {@code false}. Otherwise, this method will return {@code true}, which signals to the
     * application thread that the {@link CheckAndUpdatePositionsEvent} can be skipped.
     *
     * @return true if all checks pass, false if either of the latter two checks fail
     */
    boolean canSkipUpdateFetchPositions() {
        Throwable exception = getAndClearCachedUpdatePositionsException();

        if (exception != null)
            throw ConsumerUtils.maybeWrapAsKafkaException(exception);

        // If the cached value is set and there are no partitions in the AWAIT_RESET, AWAIT_VALIDATION, or
        // INITIALIZING states, it's ok to skip.
        if (cachedSubscriptionHasAllFetchPositions.get() && offsetFetcherUtils.getPartitionsToValidate().isEmpty() && subscriptions.hasAllFetchPositions())
            return true;

        // However, if even one partition is not set to FETCHING, set the cached value to false and signal the
        // application thread that it should call the background thread to perform the necessary logic to resolve the
        // state of the partitions.
        setSubscriptionHasAllFetchPositions(false);
        return false;
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
