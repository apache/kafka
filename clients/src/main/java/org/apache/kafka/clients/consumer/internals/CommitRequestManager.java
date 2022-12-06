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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class CommitRequestManager implements RequestManager {
    private final Queue<StagedCommit> stagedCommits;
    // TODO: We will need to refactor the subscriptionState
    private final SubscriptionState subscriptionState;
    private final Logger log;
    private final Optional<AutoCommitState> autoCommitState;
    private final CoordinatorRequestManager coordinatorRequestManager;
    private final GroupStateManager groupState;

    public CommitRequestManager(
            final Time time,
            final LogContext logContext,
            final SubscriptionState subscriptionState,
            final ConsumerConfig config,
            final CoordinatorRequestManager coordinatorRequestManager,
            final GroupStateManager groupState) {
        this.log = logContext.logger(getClass());
        this.stagedCommits = new LinkedList<>();
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
    }

    // Visible for testing
    CommitRequestManager(
            final LogContext logContext,
            final SubscriptionState subscriptionState,
            final CoordinatorRequestManager coordinatorRequestManager,
            final GroupStateManager groupState,
            final AutoCommitState autoCommitState) {
        this.log = logContext.logger(getClass());
        this.subscriptionState = subscriptionState;
        this.coordinatorRequestManager = coordinatorRequestManager;
        this.groupState = groupState;
        this.autoCommitState = Optional.ofNullable(autoCommitState);
        this.stagedCommits = new LinkedList<>();
    }

    /**
     * Poll for the commit request if there's any. The function will also try to autocommit, if enabled.
     *
     * @param currentTimeMs
     * @return
     */
    @Override
    public NetworkClientDelegate.PollResult poll(final long currentTimeMs) {
        if (coordinatorRequestManager == null) {
            return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, new ArrayList<>());
        }

        maybeAutoCommit(currentTimeMs);

        if (stagedCommits.isEmpty()) {
            return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, new ArrayList<>());
        }

        List<NetworkClientDelegate.UnsentRequest> unsentCommitRequests =
                stagedCommits.stream().map(StagedCommit::toUnsentRequest).collect(Collectors.toList());
        return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.unmodifiableList(unsentCommitRequests));
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

    private void maybeAutoCommit(final long currentTimeMs) {
        if (!autoCommitState.isPresent()) {
            return;
        }

        AutoCommitState autocommit = autoCommitState.get();
        if (!autocommit.canSendAutocommit(currentTimeMs)) {
            return;
        }

        Map<TopicPartition, OffsetAndMetadata> allConsumedOffsets = subscriptionState.allConsumed();
        log.debug("Auto-committing offsets {}", allConsumedOffsets);
        this.add(allConsumedOffsets).whenComplete((response, throwable) -> {
            if (throwable == null) {
                log.debug("Completed asynchronous auto-commit of offsets {}", allConsumedOffsets);
            }

            if (throwable instanceof RetriableCommitFailedException) {
                log.debug("Asynchronous auto-commit of offsets {} failed due to retriable error: {}", allConsumedOffsets,
                        throwable);
                autoCommitState.get().reset();
            } else {
                log.warn("Asynchronous auto-commit of offsets {} failed: {}", allConsumedOffsets,
                        throwable.getMessage());
            }
        });
        autocommit.reset();
    }

    public void clientPoll(final long currentTimeMs) {
        this.autoCommitState.ifPresent(t -> t.ack(currentTimeMs));
    }

    private static class OffsetCommitRequestHandler extends NetworkClientDelegate.FutureCompletionHandler {
        protected final Map<TopicPartition, OffsetAndMetadata> offsets;

        public OffsetCommitRequestHandler(final Map<TopicPartition, OffsetAndMetadata> offsets) {
            this.offsets = offsets;
        }
    }

    private class StagedCommit {
        private final Map<TopicPartition, OffsetAndMetadata> offsets;
        private final String groupId;
        private final GroupStateManager.Generation generation;
        private final String groupInstanceId;
        private final NetworkClientDelegate.FutureCompletionHandler callback;

        public StagedCommit(final Map<TopicPartition, OffsetAndMetadata> offsets,
                            final String groupId,
                            final String groupInstanceId,
                            final GroupStateManager.Generation generation) {
            this.offsets = offsets;
            // if no callback is provided, DefaultOffsetCommitCallback will be used.
            this.callback = new NetworkClientDelegate.FutureCompletionHandler();
            this.groupId = groupId;
            this.generation = generation;
            this.groupInstanceId = groupInstanceId;
        }

        public CompletableFuture<ClientResponse> future() {
            return callback.future();
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
                    callback);
        }
    }

    static class AutoCommitState {
        private final Timer timer;
        private final long autoCommitInterval;

        public AutoCommitState(
                final Time time,
                final long autoCommitInterval) {
            this.autoCommitInterval = autoCommitInterval;
            this.timer = time.timer(autoCommitInterval);
        }

        public boolean canSendAutocommit(final long currentTimeMs) {
            return this.timer.isExpired();
        }

        public void reset() {
            this.timer.reset(autoCommitInterval);
        }

        public void ack(final long currentTimeMs) {
            this.timer.update(currentTimeMs);
        }
    }
}
