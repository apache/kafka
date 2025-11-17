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
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.internals.events.CheckAndUpdatePositionsEvent;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import java.time.Duration;

import static java.util.Objects.requireNonNull;

/**
 * This class stores shared state needed by both the application thread and the background thread to avoid costly
 * inter-thread communication, where possible. This class compromises on the ideal of keeping state only in the
 * background thread. However, this class (and its subclasses) only relies on classes which are designed to be
 * thread-safe, thus they can be used in both the application and background threads.
 *
 * <p/>
 *
 * The following thread-safe classes are used by this class:
 *
 * <ul>
 *     <li>{@link ApiVersions}</li>
 *     <li>{@link ConsumerMetadata}</li>
 *     <li>{@link OffsetFetcherUtils}</li>
 *     <li>{@link SubscriptionState}</li>
 *     <li>{@link Time}</li>
 * </ul>
 *
 * <p/>
 *
 * In general, callers from the application thread should not mutate any of the state contained within this class.
 * It should be considered as <em>read-only</em>, and only the background thread should mutate the state.
 */
public class ThreadSafeAsyncConsumerState {

    private final SubscriptionState subscriptions;
    private final OffsetFetcherUtils offsetFetcherUtils;

    public ThreadSafeAsyncConsumerState(LogContext logContext,
                                        ConsumerMetadata metadata,
                                        SubscriptionState subscriptions,
                                        Time time,
                                        long retryBackoffMs) {
        this(logContext, metadata, subscriptions, time, retryBackoffMs, new ApiVersions());
    }

    public ThreadSafeAsyncConsumerState(LogContext logContext,
                                        ConsumerMetadata metadata,
                                        SubscriptionState subscriptions,
                                        Time time,
                                        long retryBackoffMs,
                                        ApiVersions apiVersions) {
        requireNonNull(logContext);
        requireNonNull(metadata);
        requireNonNull(subscriptions);
        requireNonNull(time);
        requireNonNull(apiVersions);

        this.subscriptions = subscriptions;
        this.offsetFetcherUtils = new OffsetFetcherUtils(
            logContext,
            metadata,
            subscriptions,
            time,
            retryBackoffMs,
            apiVersions
        );
    }

    OffsetFetcherUtils offsetFetcherUtils() {
        return offsetFetcherUtils;
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
     *
     * @exception InvalidTopicException         Thrown if one or more of the subscribed topics are invalid
     * @exception NoOffsetForPartitionException Thrown if no offset could be found for one or more partitions and no
     *                                          offset reset policy was configured
     * @exception TopicAuthorizationException   Thrown if the user is not authorized to fetch from one or more of the
     *                                          subscribed topics
     * @exception KafkaException                Thrown on other unexpected errors
     */
    public boolean canSkipUpdateFetchPositions() {
        // In cases of metadata updates, getPartitionsToValidate() will review the partitions and
        // determine which, if any, need to be validated. If any partitions require validation, the
        // update fetch positions step can't be skipped.
        if (!offsetFetcherUtils.getPartitionsToValidate().isEmpty())
            return false;

        // If there are no partitions in the AWAIT_RESET, AWAIT_VALIDATION, or INITIALIZING states, it's ok to skip.
        return subscriptions.hasAllFetchPositions();
    }
}
