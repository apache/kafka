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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class CommitRequestManager implements RequestManager {
    // TODO: current in ConsumerConfig but inaccessible in the internal package.
    private static final String THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED = "internal.throw.on.fetch.stable.offset.unsupported";
    // TODO: We will need to refactor the subscriptionState
    private final SubscriptionState subscriptionState;
    private final Logger log;
    private final Optional<AutoCommitState> autoCommitState;
    private final CoordinatorRequestManager coordinatorRequestManager;
    private final GroupState groupState;
    private final long retryBackoffMs;
    private final boolean throwOnFetchStableOffsetUnsupported;
    final PendingRequests pendingRequests;

    public CommitRequestManager(
            final Time time,
            final LogContext logContext,
            final SubscriptionState subscriptionState,
            final ConsumerConfig config,
            final CoordinatorRequestManager coordinatorRequestManager,
            final GroupState groupState) {
        Objects.requireNonNull(coordinatorRequestManager, "Coordinator is needed upon committing offsets");
        this.log = logContext.logger(getClass());
        this.pendingRequests = new PendingRequests();
        if (config.getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)) {
            final long autoCommitInterval =
                    Integer.toUnsignedLong(config.getInt(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG));
            this.autoCommitState = Optional.of(new AutoCommitState(time, autoCommitInterval));
        } else {
            this.autoCommitState = Optional.empty();
        }
        this.coordinatorRequestManager = coordinatorRequestManager;
        this.groupState = groupState;
        this.subscriptionState = subscriptionState;
        this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        this.throwOnFetchStableOffsetUnsupported = config.getBoolean(THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED);
    }

    /**
     * Poll for the {@link OffsetFetchRequest} and {@link OffsetCommitRequest} request if there's any. The function will
     * also try to autocommit the offsets, if feature is enabled.
     */
    @Override
    public NetworkClientDelegate.PollResult poll(final long currentTimeMs) {
        maybeAutoCommit();
        if (!pendingRequests.hasUnsentRequests()) {
            return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.emptyList());
        }

        return new NetworkClientDelegate.PollResult(Long.MAX_VALUE,
                Collections.unmodifiableList(pendingRequests.drain(currentTimeMs)));
    }

    private void maybeAutoCommit() {
        if (!autoCommitState.isPresent()) {
            return;
        }

        AutoCommitState autocommit = autoCommitState.get();
        if (!autocommit.canSendAutocommit()) {
            return;
        }

        Map<TopicPartition, OffsetAndMetadata> allConsumedOffsets = subscriptionState.allConsumed();
        sendAutoCommit(allConsumedOffsets);
        autocommit.resetTimer();
        autocommit.setInflightCommitStatus(true);
    }

    /**
     * Handles {@link org.apache.kafka.clients.consumer.internals.events.CommitApplicationEvent}. It creates an
     * {@link OffsetCommitRequestState} and enqueue it to send later.
     */
    public CompletableFuture<ClientResponse> addOffsetCommitRequest(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        return pendingRequests.addOffsetCommitRequest(offsets);
    }

    /**
     * Handles {@link org.apache.kafka.clients.consumer.internals.events.OffsetFetchApplicationEvent}. It creates an
     * {@link OffsetFetchRequestState} and enqueue it to send later.
     */
    public CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> addOffsetFetchRequest(final Set<TopicPartition> partitions) {
        return pendingRequests.addOffsetFetchRequest(partitions);
    }

    public void updateAutoCommitTimer(final long currentTimeMs) {
        this.autoCommitState.ifPresent(t -> t.ack(currentTimeMs));
    }


    // Visible for testing
    List<OffsetFetchRequestState> unsentOffsetFetchRequests() {
        return pendingRequests.unsentOffsetFetches;
    }

    // Visible for testing
    Queue<OffsetCommitRequestState> unsentOffsetCommitRequests() {
        return pendingRequests.unsentOffsetCommits;
    }

    // Visible for testing
    CompletableFuture<ClientResponse> sendAutoCommit(final Map<TopicPartition, OffsetAndMetadata> allConsumedOffsets) {
        log.debug("Enqueuing autocommit offsets: {}", allConsumedOffsets);
        return this.addOffsetCommitRequest(allConsumedOffsets)
                .whenComplete((response, throwable) -> {
                    this.autoCommitState.ifPresent(autoCommitState -> autoCommitState.setInflightCommitStatus(false));

                    if (throwable == null) {
                        log.debug("Completed asynchronous auto-commit of offsets {}", allConsumedOffsets);
                    }
                })
                .exceptionally(t -> {
                    if (t instanceof RetriableCommitFailedException) {
                        log.debug("Asynchronous auto-commit of offsets {} failed due to retriable error: {}", allConsumedOffsets, t);
                    } else {
                        log.warn("Asynchronous auto-commit of offsets {} failed: {}", allConsumedOffsets, t.getMessage());
                    }
                    return null;
                });
    }

    private class OffsetCommitRequestState {
        private final Map<TopicPartition, OffsetAndMetadata> offsets;
        private final String groupId;
        private final GroupState.Generation generation;
        private final String groupInstanceId;
        private final NetworkClientDelegate.FutureCompletionHandler future;

        public OffsetCommitRequestState(final Map<TopicPartition, OffsetAndMetadata> offsets,
                                        final String groupId,
                                        final String groupInstanceId,
                                        final GroupState.Generation generation) {
            this.offsets = offsets;
            this.future = new NetworkClientDelegate.FutureCompletionHandler();
            this.groupId = groupId;
            this.generation = generation;
            this.groupInstanceId = groupInstanceId;
        }

        public CompletableFuture<ClientResponse> future() {
            return future.future();
        }

        public NetworkClientDelegate.UnsentRequest toUnsentRequest() {
            Map<String, OffsetCommitRequestData.OffsetCommitRequestTopic> requestTopicDataMap = new HashMap<>();
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                TopicPartition topicPartition = entry.getKey();
                OffsetAndMetadata offsetAndMetadata = entry.getValue();

                OffsetCommitRequestData.OffsetCommitRequestTopic topic = requestTopicDataMap
                        .getOrDefault(topicPartition.topic(),
                                new OffsetCommitRequestData.OffsetCommitRequestTopic()
                                        .setName(topicPartition.topic())
                        );

                topic.partitions().add(new OffsetCommitRequestData.OffsetCommitRequestPartition()
                        .setPartitionIndex(topicPartition.partition())
                        .setCommittedOffset(offsetAndMetadata.offset())
                        .setCommittedLeaderEpoch(offsetAndMetadata.leaderEpoch().orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH))
                        .setCommittedMetadata(offsetAndMetadata.metadata())
                );
                requestTopicDataMap.put(topicPartition.topic(), topic);
            }

            OffsetCommitRequest.Builder builder = new OffsetCommitRequest.Builder(
                    new OffsetCommitRequestData()
                            .setGroupId(this.groupId)
                            .setGenerationId(generation.generationId)
                            .setMemberId(generation.memberId)
                            .setGroupInstanceId(groupInstanceId)
                            .setTopics(new ArrayList<>(requestTopicDataMap.values())));
            return new NetworkClientDelegate.UnsentRequest(
                    builder,
                    coordinatorRequestManager.coordinator(),
                    future);
        }
    }

    private class OffsetFetchRequestState extends RequestState {
        public final Set<TopicPartition> requestedPartitions;
        public final GroupState.Generation requestedGeneration;
        public CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future;

        public OffsetFetchRequestState(final Set<TopicPartition> partitions,
                                       final GroupState.Generation generation,
                                       final long retryBackoffMs) {
            super(retryBackoffMs);
            this.requestedPartitions = partitions;
            this.requestedGeneration = generation;
            this.future = new CompletableFuture<>();
        }

        public boolean sameRequest(final OffsetFetchRequestState request) {
            return Objects.equals(requestedGeneration, request.requestedGeneration) && requestedPartitions.equals(request.requestedPartitions);
        }

        public NetworkClientDelegate.UnsentRequest toUnsentRequest(final long currentTimeMs) {
            OffsetFetchRequest.Builder builder = new OffsetFetchRequest.Builder(
                    groupState.groupId,
                    true,
                    new ArrayList<>(this.requestedPartitions),
                    throwOnFetchStableOffsetUnsupported);
            NetworkClientDelegate.UnsentRequest unsentRequest = new NetworkClientDelegate.UnsentRequest(
                    builder,
                    coordinatorRequestManager.coordinator());
            unsentRequest.future().whenComplete((r, t) -> {
                onResponse(currentTimeMs, (OffsetFetchResponse) r.responseBody());
            });
            return unsentRequest;
        }

        public void onResponse(
                final long currentTimeMs,
                final OffsetFetchResponse response) {
            Errors responseError = response.groupLevelError(groupState.groupId);
            if (responseError != Errors.NONE) {
                onFailure(currentTimeMs, responseError);
                return;
            }
            onSuccess(currentTimeMs, response);
        }

        private void onFailure(final long currentTimeMs,
                               final Errors responseError) {
            log.debug("Offset fetch failed: {}", responseError.message());

            // TODO: should we retry on COORDINATOR_NOT_AVAILABLE as well ?
            if (responseError == Errors.COORDINATOR_LOAD_IN_PROGRESS) {
                retry(currentTimeMs);
            } else if (responseError == Errors.NOT_COORDINATOR) {
                // re-discover the coordinator and retry
                coordinatorRequestManager.markCoordinatorUnknown(responseError.message(), Time.SYSTEM.milliseconds());
                retry(currentTimeMs);
            } else if (responseError == Errors.GROUP_AUTHORIZATION_FAILED) {
                future.completeExceptionally(GroupAuthorizationException.forGroupId(groupState.groupId));
            } else {
                future.completeExceptionally(new KafkaException("Unexpected error in fetch offset response: " + responseError.message()));
            }
        }

        private void retry(final long currentTimeMs) {
            onFailedAttempt(currentTimeMs);
            onSendAttempt(currentTimeMs);
            pendingRequests.addOffsetFetchRequest(this);
        }

        private void onSuccess(final long currentTimeMs,
                               final OffsetFetchResponse response) {
            Set<String> unauthorizedTopics = null;
            Map<TopicPartition, OffsetFetchResponse.PartitionData> responseData =
                    response.partitionDataMap(groupState.groupId);
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(responseData.size());
            Set<TopicPartition> unstableTxnOffsetTopicPartitions = new HashSet<>();
            for (Map.Entry<TopicPartition, OffsetFetchResponse.PartitionData> entry : responseData.entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetFetchResponse.PartitionData partitionData = entry.getValue();
                if (partitionData.hasError()) {
                    Errors error = partitionData.error;
                    log.debug("Failed to fetch offset for partition {}: {}", tp, error.message());

                    if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                        future.completeExceptionally(new KafkaException("Topic or Partition " + tp + " does " +
                                "not " +
                                "exist"));
                        return;
                    } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                        if (unauthorizedTopics == null) {
                            unauthorizedTopics = new HashSet<>();
                        }
                        unauthorizedTopics.add(tp.topic());
                    } else if (error == Errors.UNSTABLE_OFFSET_COMMIT) {
                        unstableTxnOffsetTopicPartitions.add(tp);
                    } else {
                        future.completeExceptionally(new KafkaException("Unexpected error in fetch offset " +
                                "response for partition " + tp + ": " + error.message()));
                        return;
                    }
                } else if (partitionData.offset >= 0) {
                    // record the position with the offset (-1 indicates no committed offset to fetch);
                    // if there's no committed offset, record as null
                    offsets.put(tp, new OffsetAndMetadata(partitionData.offset, partitionData.leaderEpoch, partitionData.metadata));
                } else {
                    log.info("Found no committed offset for partition {}", tp);
                    offsets.put(tp, null);
                }
            }

            if (unauthorizedTopics != null) {
                future.completeExceptionally(new TopicAuthorizationException(unauthorizedTopics));
            } else if (!unstableTxnOffsetTopicPartitions.isEmpty()) {
                // TODO: Optimization question: Do we need to retry all partitions upon a single partition error?
                log.info("The following partitions still have unstable offsets " +
                        "which are not cleared on the broker side: {}" +
                        ", this could be either " +
                        "transactional offsets waiting for completion, or " +
                        "normal offsets waiting for replication after appending to local log", unstableTxnOffsetTopicPartitions);
                retry(currentTimeMs);
            } else {
                future.complete(offsets);
            }
        }

        private CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> chainFuture(final CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future) {
            return this.future.whenComplete((r, t) -> {
                if (t != null) {
                    future.completeExceptionally(t);
                } else {
                    future.complete(r);
                }
            });
        }
    }

    /**
     * <p>This is used to stage the unsent {@link OffsetCommitRequestState} and {@link OffsetFetchRequestState}.
     * <li>unsentOffsetCommits holds the offset commit requests that have not been sent out</>
     * <li>unsentOffsetFetches holds the offset fetch requests that have not been sent out</li>
     * <li>inflightOffsetFetches holds the offset fetch requests that have been sent out but incompleted</>.
     *
     * {@code addOffsetFetchRequest} dedupes the requests to avoid sending the same requests.
     */

    class PendingRequests {
        // Queue is used to ensure the sequence of commit
        Queue<OffsetCommitRequestState> unsentOffsetCommits = new LinkedList<>();
        List<OffsetFetchRequestState> unsentOffsetFetches = new ArrayList<>();
        List<OffsetFetchRequestState> inflightOffsetFetches = new ArrayList<>();

        public boolean hasUnsentRequests() {
            return !unsentOffsetCommits.isEmpty() || !unsentOffsetFetches.isEmpty();
        }

        public CompletableFuture<ClientResponse> addOffsetCommitRequest(final Map<TopicPartition, OffsetAndMetadata> offsets) {
            // TODO: Dedupe committing the same offsets to the same partitions
            OffsetCommitRequestState request = new OffsetCommitRequestState(
                    offsets,
                    groupState.groupId,
                    groupState.groupInstanceId.orElse(null),
                    groupState.generation);
            unsentOffsetCommits.add(request);
            return request.future();
        }

        /**
         *  <p>Adding an offset fetch request to the outgoing buffer.  If the same request was made, we chain the future
         *  to the existing one.
         *
         *  <p>If the request is new, it invokes a callback to remove itself from the {@code inflightOffsetFetches}
         *  upon completion.</>
         */
        private CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> addOffsetFetchRequest(final OffsetFetchRequestState request) {
            Optional<OffsetFetchRequestState> dupe =
                    unsentOffsetFetches.stream().filter(r -> r.sameRequest(request)).findAny();
            Optional<OffsetFetchRequestState> inflight =
                    inflightOffsetFetches.stream().filter(r -> r.sameRequest(request)).findAny();

            if (dupe.isPresent() || inflight.isPresent()) {
                log.info("Duplicated OffsetFetchRequest: " + request.requestedPartitions);
                dupe.orElseGet(() -> inflight.get()).chainFuture(request.future);
            } else {
                // remove the request from the outbound buffer: inflightOffsetFetches
                request.future.whenComplete((r, t) -> {
                    if (!inflightOffsetFetches.remove(request)) {
                        log.warn("A duplicated, inflight, request was identified, but unable to find it in the " +
                                "outbound buffer:" + request);
                    }
                });
                this.unsentOffsetFetches.add(request);
            }
            return request.future;
        }

        private CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> addOffsetFetchRequest(final Set<TopicPartition> partitions) {
            OffsetFetchRequestState request = new OffsetFetchRequestState(
                    partitions,
                    groupState.generation,
                    retryBackoffMs);
            return addOffsetFetchRequest(request);
        }

        /**
         * Clear {@code unsentOffsetCommits} and moves all the sendable request in {@code unsentOffsetFetches} to the
         * {@code inflightOffsetFetches} to bookkeep all of the inflight requests.
         *
         * Note: Sendable requests are determined by their timer as we are expecting backoff on failed attempt. See
         * {@link RequestState}.
         **/
        public List<NetworkClientDelegate.UnsentRequest> drain(final long currentTimeMs) {
            List<NetworkClientDelegate.UnsentRequest> unsentRequests = new ArrayList<>();

            // Add all unsent offset commit requests to the unsentRequests list
            unsentRequests.addAll(
                    unsentOffsetCommits.stream()
                            .map(OffsetCommitRequestState::toUnsentRequest)
                            .collect(Collectors.toList()));

            // Partition the unsent offset fetch requests into sendable and non-sendable lists
            Map<Boolean, List<OffsetFetchRequestState>> partitionedBySendability =
                    unsentOffsetFetches.stream()
                            .collect(Collectors.partitioningBy(request -> request.canSendRequest(currentTimeMs)));

            // Add all sendable offset fetch requests to the unsentRequests list and to the inflightOffsetFetches list
            for (OffsetFetchRequestState request : partitionedBySendability.get(true)) {
                request.onSendAttempt(currentTimeMs);
                unsentRequests.add(request.toUnsentRequest(currentTimeMs));
                inflightOffsetFetches.add(request);
            }

            // Clear the unsent offset commit and fetch lists and add all non-sendable offset fetch requests to the unsentOffsetFetches list
            unsentOffsetCommits.clear();
            unsentOffsetFetches.clear();
            unsentOffsetFetches.addAll(partitionedBySendability.get(false));

            return Collections.unmodifiableList(unsentRequests);
        }
    }

    /**
     * Encapsulates the state of auto-committing and manages the auto-commit timer.
     */
    private static class AutoCommitState {
        private final Timer timer;
        private final long autoCommitInterval;
        private boolean hasInflightCommit;

        public AutoCommitState(
                final Time time,
                final long autoCommitInterval) {
            this.autoCommitInterval = autoCommitInterval;
            this.timer = time.timer(autoCommitInterval);
            this.hasInflightCommit = false;
        }

        public boolean canSendAutocommit() {
            return !this.hasInflightCommit && this.timer.isExpired();
        }

        public void resetTimer() {
            this.timer.reset(autoCommitInterval);
        }

        public void ack(final long currentTimeMs) {
            this.timer.update(currentTimeMs);
        }

        public void setInflightCommitStatus(final boolean inflightCommitStatus) {
            this.hasInflightCommit = inflightCommitStatus;
        }
    }
}
