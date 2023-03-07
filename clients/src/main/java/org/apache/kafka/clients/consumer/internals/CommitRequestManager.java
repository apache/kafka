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
import org.apache.kafka.common.errors.UnstableOffsetCommitException;
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
import java.util.Iterator;
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
    private final Queue<StagedCommit> stagedCommits;
    // TODO: We will need to refactor the subscriptionState
    private final SubscriptionState subscriptionState;
    private final Logger log;
    private final Optional<AutoCommitState> autoCommitState;
    private final OffsetFetchRequestState offsetFetchRequestState;
    private final CoordinatorRequestManager coordinatorRequestManager;
    private final GroupState groupState;
    private final long retryBackoffMs;
    private final boolean throwOnFetchStableOffsetUnsupported;

    public CommitRequestManager(
            final Time time,
            final LogContext logContext,
            final SubscriptionState subscriptionState,
            final ConsumerConfig config,
            final CoordinatorRequestManager coordinatorRequestManager,
            final GroupState groupState) {
        Objects.requireNonNull(coordinatorRequestManager, "Coordinator is needed upon committing offsets");
        this.log = logContext.logger(getClass());
        this.stagedCommits = new LinkedList<>();
        if (config.getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)) {
            final long autoCommitInterval =
                    Integer.toUnsignedLong(config.getInt(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG));
            this.autoCommitState = Optional.of(new AutoCommitState(time, autoCommitInterval));
        } else {
            this.autoCommitState = Optional.empty();
        }
        this.offsetFetchRequestState = new OffsetFetchRequestState();
        this.coordinatorRequestManager = coordinatorRequestManager;
        this.groupState = groupState;
        this.subscriptionState = subscriptionState;
        this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        this.throwOnFetchStableOffsetUnsupported = config.getBoolean(THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED);
    }

    /**
     * Poll for the commit request if there's any. The function will also try to autocommit, if enabled.
     *
     * @param currentTimeMs
     * @return
     */
    @Override
    public NetworkClientDelegate.PollResult poll(final long currentTimeMs) {
        maybeAutoCommit();
        List<NetworkClientDelegate.UnsentRequest> unsentRequests = new ArrayList<>();

        if (!stagedCommits.isEmpty()) {
            unsentRequests = stagedCommits.stream().map(StagedCommit::toUnsentRequest).collect(Collectors.toList());
            stagedCommits.clear();
        }

        if (offsetFetchRequestState.canSendOffsetFetch()) {
            unsentRequests.addAll(sendFetchCommittedOffsetRequests(currentTimeMs));
        }
        return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.unmodifiableList(unsentRequests));
    }

    public CompletableFuture<ClientResponse> add(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        StagedCommit commit = new StagedCommit(
                offsets,
                groupState.groupId,
                groupState.groupInstanceId.orElse(null),
                groupState.generation);
        this.stagedCommits.add(commit);
        return commit.future();
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
        log.debug("Auto-committing offsets {}", allConsumedOffsets);
        sendAutoCommit(allConsumedOffsets);
        autocommit.resetTimer();
    }

    // Visible for testing
    CompletableFuture<ClientResponse> sendAutoCommit(final Map<TopicPartition, OffsetAndMetadata> allConsumedOffsets) {
        CompletableFuture<ClientResponse> future = this.add(allConsumedOffsets)
                .whenComplete((response, throwable) -> {
                    if (throwable == null) {
                        log.debug("Completed asynchronous auto-commit of offsets {}", allConsumedOffsets);
                    }
                    // setting inflight commit to false upon completion
                    autoCommitState.get().setInflightCommitStatus(false);
                })
                .exceptionally(t -> {
                    if (t instanceof RetriableCommitFailedException) {
                        log.debug("Asynchronous auto-commit of offsets {} failed due to retriable error: {}", allConsumedOffsets, t);
                    } else {
                        log.warn("Asynchronous auto-commit of offsets {} failed: {}", allConsumedOffsets, t.getMessage());
                    }
                    return null;
                });
        return future;
    }

    /**
     * Get all of the sendable requests, and create a list of UnsentRequest.
     * @param currentTimeMs
     * @return
     */
    private List<NetworkClientDelegate.UnsentRequest> sendFetchCommittedOffsetRequests(final long currentTimeMs) {
        List<InflightOffsetFetchRequest> requests = offsetFetchRequestState.sendableRequests(currentTimeMs);
        List<NetworkClientDelegate.UnsentRequest> pollResults = new ArrayList<>();
        requests.forEach(req -> {
            OffsetFetchRequest.Builder builder = new OffsetFetchRequest.Builder(
                    groupState.groupId, true,
                    new ArrayList<>(req.requestedPartitions),
                    throwOnFetchStableOffsetUnsupported);
            NetworkClientDelegate.UnsentRequest unsentRequest = new NetworkClientDelegate.UnsentRequest(
                    builder,
                    coordinatorRequestManager.coordinator());
            FetchCommittedOffsetResponseHandler cb = new FetchCommittedOffsetResponseHandler(req);
            unsentRequest.future().whenComplete((r, t) -> {
                cb.onResponse(currentTimeMs, (OffsetFetchResponse) r.responseBody());
            });
            pollResults.add(unsentRequest);
        });
        return pollResults;
    }

    public void sendFetchCommittedOffsetRequest(final Set<TopicPartition> partitions,
                                                final CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future) {
        offsetFetchRequestState.enqueue(new InflightOffsetFetchRequest(
                partitions,
                groupState.generation,
                future,
                retryBackoffMs));
    }


    public void clientPoll(final long currentTimeMs) {
        this.autoCommitState.ifPresent(t -> t.ack(currentTimeMs));
    }

    // Visible for testing
    Queue<StagedCommit> stagedCommits() {
        return this.stagedCommits;
    }

    private class StagedCommit {
        private final Map<TopicPartition, OffsetAndMetadata> offsets;
        private final String groupId;
        private final GroupState.Generation generation;
        private final String groupInstanceId;
        private final NetworkClientDelegate.FutureCompletionHandler future;

        public StagedCommit(final Map<TopicPartition, OffsetAndMetadata> offsets,
                            final String groupId,
                            final String groupInstanceId,
                            final GroupState.Generation generation) {
            this.offsets = offsets;
            // if no callback is provided, DefaultOffsetCommitCallback will be used.
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

    static class InflightOffsetFetchRequest extends RequestState {
        public final Set<TopicPartition> requestedPartitions;
        public final GroupState.Generation requestedGeneration;
        public CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future;

        public InflightOffsetFetchRequest(final Set<TopicPartition> partitions,
                                          final GroupState.Generation generation,
                                          final CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future,
                                          final long retryBackoffMs) {
            super(retryBackoffMs);
            this.requestedPartitions = partitions;
            this.requestedGeneration = generation;
            this.future = future;
        }

        public boolean sameRequest(final InflightOffsetFetchRequest request) {
            return Objects.equals(requestedGeneration, request.requestedGeneration) && requestedPartitions.equals(request.requestedPartitions);
        }
    }

    /**
     * This is used to support the committed() API. Here we hold two Java Collections. {@code sentRequest} holds the
     * request that have been sent but haven't yet completed. {@code unsentRequests} holds request that have not been
     * sent.
     *
     * When committed is invoked. We check if the same request has been sent in the {@code sentRequest}, or waiting
     * to be send in the {@code unsentRequest}.
     * If the same request was previously enqueued, we chain the future to the existing one.
     * If neither, then we add it to the {@code unsentRequests}.
     * TODO: There's an optimization of not resend the sent request. Current code only check ones that haven't been
     * sent to the broker.
     */
    class OffsetFetchRequestState {
        private List<InflightOffsetFetchRequest> unsentRequests = new ArrayList<>();

        public boolean canSendOffsetFetch() {
            return !unsentRequests.isEmpty();
        }

        public void enqueue(final InflightOffsetFetchRequest req) {
            Optional<InflightOffsetFetchRequest> dupe = unsentRequests.stream().filter(r -> r.sameRequest(req)).findAny();
            if (!dupe.isPresent()) {
                this.unsentRequests.add(req);
                return;
            }

            log.info("Duplicated OffsetFetchRequest: " + req.requestedPartitions);
            dupe.get().future.whenComplete((r, t) -> {
                if (t != null) {
                    req.future.completeExceptionally(t);
                } else {
                    req.future.complete(r);
                }
            });
        }

        public List<InflightOffsetFetchRequest> sendableRequests(final long currentTimeMs) {
            Iterator<InflightOffsetFetchRequest> iterator = unsentRequests.iterator();
            List<InflightOffsetFetchRequest> staged = new ArrayList<>();
            while (iterator.hasNext()) {
                InflightOffsetFetchRequest unsent = iterator.next();
                if (unsent.canSendRequest(currentTimeMs)) {
                    unsent.onSendAttempt(currentTimeMs);
                    iterator.remove();
                    staged.add(unsent);
                }
            }
            return staged;
        }
    }

    static class AutoCommitState {
        private final Timer timer;
        private final long autoCommitInterval;
        private boolean hasInflightCommit;

        public AutoCommitState(
                final Time time,
                final long autoCommitInterval) {
            this.autoCommitInterval = autoCommitInterval;
            this.timer = time.timer(autoCommitInterval);
        }

        public boolean canSendAutocommit() {
            return !hasInflightCommit && this.timer.isExpired();
        }

        public void resetTimer() {
            this.timer.reset(autoCommitInterval);
        }

        public void setInflightCommitStatus(final boolean hasInflightCommit) {
            this.hasInflightCommit = hasInflightCommit;
        }

        public void ack(final long currentTimeMs) {
            this.timer.update(currentTimeMs);
        }
    }


    private class FetchCommittedOffsetResponseHandler {
        private final InflightOffsetFetchRequest request;

        private FetchCommittedOffsetResponseHandler(final InflightOffsetFetchRequest request) {
            this.request = request;
        }

        public void onResponse(
                final long currentTimeMs,
                final OffsetFetchResponse response) {
            Errors responseError = response.groupLevelError(groupState.groupId);
            if (responseError != Errors.NONE) {
                onFailure(currentTimeMs, responseError);
                return;
            }

            onSuccess(response);
        }
        private void onFailure(final long currentTimeMs, final Errors responseError) {
            log.debug("Offset fetch failed: {}", responseError.message());

            if (responseError == Errors.COORDINATOR_LOAD_IN_PROGRESS) {
                retry(currentTimeMs);
            } else if (responseError == Errors.NOT_COORDINATOR) {
                // re-discover the coordinator and retry
                coordinatorRequestManager.markCoordinatorUnknown(responseError.message(), Time.SYSTEM.milliseconds());
                retry(currentTimeMs);
            } else if (responseError == Errors.GROUP_AUTHORIZATION_FAILED) {
                // TODO: I'm not sure if we should retry here.  Sounds like we should propagate the error to let the
                //  user to fix the permission
                request.future.completeExceptionally(GroupAuthorizationException.forGroupId(groupState.groupId));
            } else {
                request.future.completeExceptionally(new KafkaException("Unexpected error in fetch offset response: " + responseError.message()));
            }
            return;
        }

        private void retry(long currentTimeMs) {
            this.request.onFailedAttempt(currentTimeMs);
            offsetFetchRequestState.enqueue(this.request);
        }

        private void onSuccess(final OffsetFetchResponse response) {
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
                        request.future.completeExceptionally(new KafkaException("Topic or Partition " + tp + " does not " +
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
                        request.future.completeExceptionally(new KafkaException("Unexpected error in fetch offset " +
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
                request.future.completeExceptionally(new TopicAuthorizationException(unauthorizedTopics));
            } else if (!unstableTxnOffsetTopicPartitions.isEmpty()) {
                // just retry
                log.info("The following partitions still have unstable offsets " +
                        "which are not cleared on the broker side: {}" +
                        ", this could be either " +
                        "transactional offsets waiting for completion, or " +
                        "normal offsets waiting for replication after appending to local log", unstableTxnOffsetTopicPartitions);
                request.future.completeExceptionally(new UnstableOffsetCommitException("There are unstable offsets for " +
                        "the requested topic partitions"));
            } else {
                request.future.complete(offsets);
            }
        }
    }
}
