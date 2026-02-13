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
import org.apache.kafka.clients.consumer.internals.events.AsyncPollEvent;
import org.apache.kafka.clients.consumer.internals.events.CheckAndUpdatePositionsEvent;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

/**
 * As named, this class validates positions in the {@link SubscriptionState} based on current {@link ConsumerMetadata}
 * version. It maintains just enough shared state to determine when it can avoid costly inter-thread communication
 * in the {@link Consumer#poll(Duration)} method.
 *
 * <p/>
 *
 * Callers from the application thread should not mutate any of the state contained within this class.
 * It should be considered as <em>read-only</em>, and only the background thread should mutate the state.
 */
public class PositionsValidator {

    private final Logger log;
    private final Time time;
    private final ConsumerMetadata metadata;
    private final SubscriptionState subscriptions;

    /**
     * Exception that occurred while validating positions, that will be propagated on the next
     * call to validate positions. This could be an error received in the
     * OffsetsForLeaderEpoch response, or a LogTruncationException detected when using a
     * successful response to validate the positions. It will be cleared when thrown.
     */
    private final AtomicReference<RuntimeException> cachedValidatePositionsException = new AtomicReference<>();

    private final AtomicInteger metadataUpdateVersion = new AtomicInteger(-1);

    public PositionsValidator(LogContext logContext,
                              Time time,
                              SubscriptionState subscriptions,
                              ConsumerMetadata metadata) {
        this.log = requireNonNull(logContext).logger(getClass());
        this.time = requireNonNull(time);
        this.metadata = requireNonNull(metadata);
        this.subscriptions = requireNonNull(subscriptions);
    }

    /**
     * This method is called by the background thread in response to {@link AsyncPollEvent} and
     * {@link CheckAndUpdatePositionsEvent}.
     */
    Map<TopicPartition, SubscriptionState.FetchPosition> refreshAndGetPartitionsToValidate(ApiVersions apiVersions) {
        maybeThrowError();

        // Validate each partition against the current leader and epoch
        // If we see a new metadata version, check all partitions
        validatePositionsOnMetadataChange(apiVersions);

        // Collect positions needing validation, with backoff
        return subscriptions.partitionsNeedingValidation(time.milliseconds());
    }

    /**
     * If we have seen new metadata (as tracked by {@link org.apache.kafka.clients.Metadata#updateVersion()}), then
     * we should check that all the assignments have a valid position.
     */
    void validatePositionsOnMetadataChange(ApiVersions apiVersions) {
        int newMetadataUpdateVersion = metadata.updateVersion();
        if (metadataUpdateVersion.getAndSet(newMetadataUpdateVersion) != newMetadataUpdateVersion) {
            subscriptions.assignedPartitions().forEach(topicPartition -> {
                ConsumerMetadata.LeaderAndEpoch leaderAndEpoch = metadata.currentLeader(topicPartition);
                subscriptions.maybeValidatePositionForCurrentLeader(apiVersions, topicPartition, leaderAndEpoch);
            });
        }
    }

    void maybeSetError(RuntimeException e) {
        if (!cachedValidatePositionsException.compareAndSet(null, e)) {
            log.error("Discarding error validating positions because another error is pending", e);
        }
    }

    void maybeThrowError() {
        RuntimeException exception = cachedValidatePositionsException.getAndSet(null);
        if (exception != null)
            throw exception;
    }

    /**
     * This method is used by {@code AsyncKafkaConsumer} to determine if it can skip the step of validating
     * positions as this is in the critical path for the {@link Consumer#poll(Duration)}. If the application thread
     * can safely and accurately determine that it doesn't need to perform the
     * {@link OffsetsRequestManager#updateFetchPositions(long)} call, a big performance savings can be realized.
     *
     * <p/>
     *
     * <ol>
     *     <li>
     *         Checks for previous errors from validation, and throws the error if present
     *     </li>
     *     <li>
     *         Checks that the current {@link ConsumerMetadata#updateVersion()} matches its current cached
     *         value to ensure that it is not stale
     *     </li>
     *     <li>
     *         Checks that all positions are in the {@link SubscriptionState.FetchStates#FETCHING} state
     *         ({@link SubscriptionState#hasAllFetchPositions()})
     *     </li>
     * </ol>
     *
     * If any checks fail, this method will return {@code false}, otherwise, it will return {@code true}, which
     * signals to the application thread that the position validation step can be skipped.
     *
     * @return true if all checks pass, false if any checks fail
     */
    boolean canSkipUpdateFetchPositions() {
        maybeThrowError();

        if (metadataUpdateVersion.get() != metadata.updateVersion()) {
            return false;
        }

        // If there are no partitions in the AWAIT_RESET, AWAIT_VALIDATION, or INITIALIZING states, it's ok to skip.
        return subscriptions.hasAllFetchPositions();
    }
}
