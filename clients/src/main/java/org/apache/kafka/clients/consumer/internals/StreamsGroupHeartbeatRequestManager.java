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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.StreamsAssignmentInterface.Assignment;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.clients.consumer.internals.metrics.HeartbeatMetricsManager;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData.TopicPartitions;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData.TaskIds;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData.Endpoint;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatRequest;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StreamsGroupHeartbeatRequestManager implements RequestManager {

    private final Logger logger;

    private final int maxPollIntervalMs;

    private final CoordinatorRequestManager coordinatorRequestManager;

    private final StreamsGroupHeartbeatRequestManager.HeartbeatRequestState heartbeatRequestState;

    private final StreamsGroupHeartbeatRequestManager.HeartbeatState heartbeatState;

    private final ConsumerMembershipManager membershipManager;

    private final StreamsGroupInitializeRequestManager streamsGroupInitializeRequestManager;

    private final BackgroundEventHandler backgroundEventHandler;

    private final Timer pollTimer;

    private final HeartbeatMetricsManager metricsManager;

    private StreamsAssignmentInterface streamsInterface;

    private final Map<String, Uuid> assignedTopicIdCache;

    private final ConsumerMetadata metadata;

    public StreamsGroupHeartbeatRequestManager(
        final LogContext logContext,
        final Time time,
        final ConsumerConfig config,
        final CoordinatorRequestManager coordinatorRequestManager,
        final StreamsGroupInitializeRequestManager streamsGroupInitializeRequestManager,
        final ConsumerMembershipManager membershipManager,
        final BackgroundEventHandler backgroundEventHandler,
        final Metrics metrics,
        final StreamsAssignmentInterface streamsAssignmentInterface,
        final ConsumerMetadata metadata
    ) {
        this.coordinatorRequestManager = coordinatorRequestManager;
        this.logger = logContext.logger(getClass());
        this.membershipManager = membershipManager;
        this.streamsGroupInitializeRequestManager = streamsGroupInitializeRequestManager;
        this.backgroundEventHandler = backgroundEventHandler;
        this.maxPollIntervalMs = config.getInt(CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG);
        long retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        long retryBackoffMaxMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG);
        this.heartbeatState = new StreamsGroupHeartbeatRequestManager.HeartbeatState(streamsAssignmentInterface, membershipManager,
            maxPollIntervalMs);
        this.heartbeatRequestState = new StreamsGroupHeartbeatRequestManager.HeartbeatRequestState(logContext, time, 0, retryBackoffMs,
            retryBackoffMaxMs, maxPollIntervalMs);
        this.pollTimer = time.timer(maxPollIntervalMs);
        this.metricsManager = new HeartbeatMetricsManager(metrics);
        this.streamsInterface = streamsAssignmentInterface;
        this.assignedTopicIdCache = new HashMap<>();
        this.metadata = metadata;
    }

    @Override
    public NetworkClientDelegate.PollResult poll(long currentTimeMs) {
        if (!coordinatorRequestManager.coordinator().isPresent() || membershipManager.shouldSkipHeartbeat()) {
            membershipManager.onHeartbeatRequestSkipped();
            return NetworkClientDelegate.PollResult.EMPTY;
        }
        pollTimer.update(currentTimeMs);
        if (pollTimer.isExpired() && !membershipManager.isLeavingGroup()) {
            logger.warn("Consumer poll timeout has expired. This means the time between " +
                "subsequent calls to poll() was longer than the configured max.poll.interval.ms, " +
                "which typically implies that the poll loop is spending too much time processing " +
                "messages. You can address this either by increasing max.poll.interval.ms or by " +
                "reducing the maximum size of batches returned in poll() with max.poll.records.");

            membershipManager.transitionToSendingLeaveGroup(true);
            NetworkClientDelegate.UnsentRequest leaveHeartbeat = makeHeartbeatRequest(currentTimeMs, true);

            // We can ignore the leave response because we can join before or after receiving the response.
            heartbeatRequestState.reset();
            heartbeatState.reset();
            return new NetworkClientDelegate.PollResult(heartbeatRequestState.heartbeatIntervalMs,
                Collections.singletonList(leaveHeartbeat));
        }

        boolean heartbeatNow = membershipManager.shouldHeartbeatNow() && !heartbeatRequestState.requestInFlight();
        if (!heartbeatRequestState.canSendRequest(currentTimeMs) && !heartbeatNow) {
            return new NetworkClientDelegate.PollResult(heartbeatRequestState.nextHeartbeatMs(currentTimeMs));
        }

        NetworkClientDelegate.UnsentRequest request = makeHeartbeatRequest(currentTimeMs, false);
        return new NetworkClientDelegate.PollResult(heartbeatRequestState.heartbeatIntervalMs, Collections.singletonList(request));
    }

    public ConsumerMembershipManager membershipManager() {
        return membershipManager;
    }

    @Override
    public long maximumTimeToWait(long currentTimeMs) {
        pollTimer.update(currentTimeMs);
        if (
            pollTimer.isExpired() ||
                (membershipManager.shouldHeartbeatNow() && !heartbeatRequestState.requestInFlight())
        ) {
            return 0L;
        }
        return Math.min(pollTimer.remainingMs() / 2, heartbeatRequestState.nextHeartbeatMs(currentTimeMs));
    }

    public void resetPollTimer(final long pollMs) {
        pollTimer.update(pollMs);
        if (pollTimer.isExpired()) {
            logger.warn("Time between subsequent calls to poll() was longer than the configured " +
                    "max.poll.interval.ms, exceeded approximately by {} ms. Member {} will rejoin the group now.",
                pollTimer.isExpiredBy(), membershipManager().memberId());
            membershipManager().maybeRejoinStaleMember();
        }
        pollTimer.reset(maxPollIntervalMs);
    }

    private NetworkClientDelegate.UnsentRequest makeHeartbeatRequest(final long currentTimeMs,
                                                                     final boolean ignoreResponse) {
        NetworkClientDelegate.UnsentRequest request = makeHeartbeatRequest(ignoreResponse);
        heartbeatRequestState.onSendAttempt(currentTimeMs);
        membershipManager.onHeartbeatRequestGenerated();
        metricsManager.recordHeartbeatSentMs(currentTimeMs);
        return request;
    }

    private NetworkClientDelegate.UnsentRequest makeHeartbeatRequest(final boolean ignoreResponse) {
        NetworkClientDelegate.UnsentRequest request = new NetworkClientDelegate.UnsentRequest(
            new StreamsGroupHeartbeatRequest.Builder(this.heartbeatState.buildRequestData()),
            coordinatorRequestManager.coordinator());
        if (ignoreResponse) {
            return logResponse(request);
        } else {
            return request.whenComplete((response, exception) -> {
                long completionTimeMs = request.handler().completionTimeMs();
                if (response != null) {
                    metricsManager.recordRequestLatency(response.requestLatencyMs());
                    onResponse((StreamsGroupHeartbeatResponse) response.responseBody(), completionTimeMs);
                } else {
                    onFailure(exception, completionTimeMs);
                }
            });
        }
    }

    private NetworkClientDelegate.UnsentRequest logResponse(final NetworkClientDelegate.UnsentRequest request) {
        return request.whenComplete((response, exception) -> {
            if (response != null) {
                metricsManager.recordRequestLatency(response.requestLatencyMs());
                Errors error =
                    Errors.forCode(((StreamsGroupHeartbeatResponse) response.responseBody()).data().errorCode());
                if (error == Errors.NONE) {
                    logger.debug("StreamsGroupHeartbeat responded successfully: {}", response);
                } else {
                    logger.error("StreamsGroupHeartbeat failed because of {}: {}", error, response);
                }
            } else {
                logger.error("StreamsGroupHeartbeat failed because of unexpected exception.", exception);
            }
        });
    }

    private void onFailure(final Throwable exception, final long responseTimeMs) {
        this.heartbeatRequestState.onFailedAttempt(responseTimeMs);
        this.heartbeatState.reset();
        membershipManager.onHeartbeatFailure(exception instanceof RetriableException);
        if (exception instanceof RetriableException) {
            String message = String.format("StreamsGroupHeartbeatRequest failed because of the retriable exception. " +
                    "Will retry in %s ms: %s",
                heartbeatRequestState.remainingBackoffMs(responseTimeMs),
                exception.getMessage());
            logger.debug(message);
        } else {
            logger.error("StreamsGroupHeartbeatRequest failed due to fatal error", exception);
            handleFatalFailure(exception);
        }
    }

    private void onResponse(final StreamsGroupHeartbeatResponse response, long currentTimeMs) {
        if (Errors.forCode(response.data().errorCode()) == Errors.NONE) {
            onSuccessResponse(response, currentTimeMs);
        } else {
            onErrorResponse(response, currentTimeMs);
        }
    }

    private void onSuccessResponse(final StreamsGroupHeartbeatResponse response, final long currentTimeMs) {
        final StreamsGroupHeartbeatResponseData data = response.data();

        heartbeatRequestState.updateHeartbeatIntervalMs(data.heartbeatIntervalMs());
        heartbeatRequestState.onSuccessfulAttempt(currentTimeMs);
        heartbeatRequestState.resetTimer();

        if (data.shouldInitializeTopology()) {
            streamsGroupInitializeRequestManager.initialize();
        }
        if (data.partitionsByUserEndpoint() != null) {
            streamsInterface.partitionsByHost.set(convertHostInfoMap(data));
        }

        ConsumerGroupHeartbeatResponseData cgData = new ConsumerGroupHeartbeatResponseData();
        cgData.setMemberId(data.memberId());
        cgData.setMemberEpoch(data.memberEpoch());
        cgData.setErrorCode(data.errorCode());
        cgData.setErrorMessage(data.errorMessage());
        cgData.setThrottleTimeMs(data.throttleTimeMs());
        cgData.setHeartbeatIntervalMs(data.heartbeatIntervalMs());

        List<StreamsGroupHeartbeatResponseData.Status> statuses = data.status();

        if (statuses != null && !statuses.isEmpty()) {
            String statusDetails = statuses.stream()
                .map(status -> "(" + status.statusCode() + ") " + status.statusDetail())
                .collect(Collectors.joining(", "));
            logger.warn("Membership is in the following statuses: {}.", statusDetails);
        }

        if (data.activeTasks() != null && data.standbyTasks() != null && data.warmupTasks() != null) {

            setTargetAssignment(data);
            setTargetAssignmentForConsumerGroup(data, cgData);

        } else {
            if (data.activeTasks() != null || data.standbyTasks() != null || data.warmupTasks() != null) {
                throw new IllegalStateException("Invalid response data, task collections must be all null or all non-null: " + data);
            }
        }

        membershipManager.onHeartbeatSuccess(new ConsumerGroupHeartbeatResponse(cgData));
    }

    private void setTargetAssignmentForConsumerGroup(final StreamsGroupHeartbeatResponseData data, final ConsumerGroupHeartbeatResponseData cgData) {
        Map<String, TopicPartitions> tps = new HashMap<>();
        data.activeTasks().forEach(taskId -> Stream.concat(
                streamsInterface.subtopologyMap().get(taskId.subtopologyId()).sourceTopics.stream(),
                streamsInterface.subtopologyMap().get(taskId.subtopologyId()).repartitionSourceTopics.keySet().stream()
            )
            .forEach(topic -> {
                final TopicPartitions toInsert = tps.computeIfAbsent(topic, k -> {
                    final Optional<Uuid> uuid = findTopicIdInGlobalOrLocalCache(topic);
                    if (uuid.isPresent()) {
                        TopicPartitions t =
                            new TopicPartitions();
                        t.setTopicId(uuid.get());
                        return t;
                    } else {
                        return null;
                    }
                });
                if (toInsert != null) {
                    toInsert.partitions().addAll(taskId.partitions());
                }
            }));
        ConsumerGroupHeartbeatResponseData.Assignment cgAssignment = new ConsumerGroupHeartbeatResponseData.Assignment();
        cgAssignment.setTopicPartitions(new ArrayList<>(tps.values()));
        cgData.setAssignment(cgAssignment);
    }

    private void setTargetAssignment(final StreamsGroupHeartbeatResponseData data) {
        Assignment targetAssignment = new Assignment();
        updateTaskIdCollection(data.activeTasks(), targetAssignment.activeTasks);
        updateTaskIdCollection(data.standbyTasks(), targetAssignment.standbyTasks);
        updateTaskIdCollection(data.warmupTasks(), targetAssignment.warmupTasks);
        streamsInterface.targetAssignment.set(targetAssignment);
    }

    private static Map<StreamsAssignmentInterface.HostInfo, List<TopicPartition>> convertHostInfoMap(final StreamsGroupHeartbeatResponseData data) {
        Map<StreamsAssignmentInterface.HostInfo, List<TopicPartition>> partitionsByHost = new HashMap<>();
        data.partitionsByUserEndpoint().forEach(endpoint -> {
            List<TopicPartition> topicPartitions = endpoint.partitions().stream()
                .flatMap(partition ->
                    partition.partitions().stream().map(partitionId -> new TopicPartition(partition.topic(), partitionId)))
                .collect(Collectors.toList());
            Endpoint userEndpoint = endpoint.userEndpoint();
            partitionsByHost.put(new StreamsAssignmentInterface.HostInfo(userEndpoint.host(), userEndpoint.port()), topicPartitions);
        });
        return partitionsByHost;
    }

    private void updateTaskIdCollection(
        final List<StreamsGroupHeartbeatResponseData.TaskIds> source,
        final Set<StreamsAssignmentInterface.TaskId> target
    ) {
        target.clear();
        source.forEach(taskId -> {
            taskId.partitions().forEach(partition -> {
                target.add(new StreamsAssignmentInterface.TaskId(taskId.subtopologyId(), partition));
            });
        });
    }

    private void onErrorResponse(final StreamsGroupHeartbeatResponse response,
                                 final long currentTimeMs) {
        Errors error = Errors.forCode(response.data().errorCode());
        String errorMessage = response.data().errorMessage();
        String message;

        this.heartbeatState.reset();
        this.heartbeatRequestState.onFailedAttempt(currentTimeMs);
        membershipManager.onHeartbeatFailure(false);

        switch (error) {
            case NOT_COORDINATOR:
                // the manager should retry immediately when the coordinator node becomes available again
                message = String.format("StreamsGroupHeartbeatRequest failed because the group coordinator %s is incorrect. " +
                        "Will attempt to find the coordinator again and retry",
                    coordinatorRequestManager.coordinator());
                logInfo(message, response, currentTimeMs);
                coordinatorRequestManager.markCoordinatorUnknown(errorMessage, currentTimeMs);
                // Skip backoff so that the next HB is sent as soon as the new coordinator is discovered
                heartbeatRequestState.reset();
                break;

            case COORDINATOR_NOT_AVAILABLE:
                message = String.format("StreamsGroupHeartbeatRequest failed because the group coordinator %s is not available. " +
                        "Will attempt to find the coordinator again and retry",
                    coordinatorRequestManager.coordinator());
                logInfo(message, response, currentTimeMs);
                coordinatorRequestManager.markCoordinatorUnknown(errorMessage, currentTimeMs);
                // Skip backoff so that the next HB is sent as soon as the new coordinator is discovered
                heartbeatRequestState.reset();
                break;

            case COORDINATOR_LOAD_IN_PROGRESS:
                // the manager will backoff and retry
                message = String.format("StreamsGroupHeartbeatRequest failed because the group coordinator %s is still loading." +
                        "Will retry",
                    coordinatorRequestManager.coordinator());
                logInfo(message, response, currentTimeMs);
                break;

            case GROUP_AUTHORIZATION_FAILED:
                GroupAuthorizationException exception =
                    GroupAuthorizationException.forGroupId(membershipManager.groupId());
                logger.error("StreamsGroupHeartbeatRequest failed due to group authorization failure: {}", exception.getMessage());
                handleFatalFailure(error.exception(exception.getMessage()));
                break;

            case UNRELEASED_INSTANCE_ID:
                logger.error("StreamsGroupHeartbeatRequest failed due to the instance id {} was not released: {}",
                    membershipManager.groupInstanceId().orElse("null"), errorMessage);
                handleFatalFailure(Errors.UNRELEASED_INSTANCE_ID.exception(errorMessage));
                break;

            case INVALID_REQUEST:
            case GROUP_MAX_SIZE_REACHED:
            case UNSUPPORTED_ASSIGNOR:
            case UNSUPPORTED_VERSION:
                logger.error("StreamsGroupHeartbeatRequest failed due to error: {}", error);
                handleFatalFailure(error.exception(errorMessage));
                break;

            case FENCED_MEMBER_EPOCH:
                message = String.format("StreamsGroupHeartbeatRequest failed for member %s because epoch %s is fenced.",
                    membershipManager.memberId(), membershipManager.memberEpoch());
                logInfo(message, response, currentTimeMs);
                membershipManager.transitionToFenced();
                // Skip backoff so that a next HB to rejoin is sent as soon as the fenced member releases its assignment
                heartbeatRequestState.reset();
                break;

            case UNKNOWN_MEMBER_ID:
                message = String.format("StreamsGroupHeartbeatRequest failed because member %s is unknown.",
                    membershipManager.memberId());
                logInfo(message, response, currentTimeMs);
                membershipManager.transitionToFenced();
                // Skip backoff so that a next HB to rejoin is sent as soon as the fenced member releases its assignment
                heartbeatRequestState.reset();
                break;

            default:
                // If the manager receives an unknown error - there could be a bug in the code or a new error code
                logger.error("StreamsGroupHeartbeatRequest failed due to unexpected error: {}", error);
                handleFatalFailure(error.exception(errorMessage));
                break;
        }
    }

    private void logInfo(final String message,
                         final StreamsGroupHeartbeatResponse response,
                         final long currentTimeMs) {
        logger.info("{} in {}ms: {}",
            message,
            heartbeatRequestState.remainingBackoffMs(currentTimeMs),
            response.data().errorMessage());
    }

    private void handleFatalFailure(Throwable error) {
        backgroundEventHandler.add(new ErrorEvent(error));
        membershipManager.transitionToFatal();
    }

    /**
     * Represents the state of a heartbeat request, including logic for timing, retries, and exponential backoff. The object extends
     * {@link RequestState} to enable exponential backoff and duplicated request handling. The two fields that it holds are:
     */
    static class HeartbeatRequestState extends RequestState {

        /**
         * heartbeatTimer tracks the time since the last heartbeat was sent
         */
        private final Timer heartbeatTimer;

        /**
         * The heartbeat interval which is acquired/updated through the heartbeat request
         */
        private long heartbeatIntervalMs;

        public HeartbeatRequestState(
            final LogContext logContext,
            final Time time,
            final long heartbeatIntervalMs,
            final long retryBackoffMs,
            final long retryBackoffMaxMs,
            final double jitter) {
            super(logContext, StreamsGroupHeartbeatRequestManager.HeartbeatRequestState.class.getName(), retryBackoffMs, 2, retryBackoffMaxMs,
                jitter);
            this.heartbeatIntervalMs = heartbeatIntervalMs;
            this.heartbeatTimer = time.timer(heartbeatIntervalMs);
        }

        private void update(final long currentTimeMs) {
            this.heartbeatTimer.update(currentTimeMs);
        }

        public void resetTimer() {
            this.heartbeatTimer.reset(heartbeatIntervalMs);
        }

        @Override
        public boolean canSendRequest(final long currentTimeMs) {
            update(currentTimeMs);
            return heartbeatTimer.isExpired() && super.canSendRequest(currentTimeMs);
        }

        public long nextHeartbeatMs(final long currentTimeMs) {
            if (heartbeatTimer.remainingMs() == 0) {
                return this.remainingBackoffMs(currentTimeMs);
            }
            return heartbeatTimer.remainingMs();
        }

        private void updateHeartbeatIntervalMs(final long heartbeatIntervalMs) {
            if (this.heartbeatIntervalMs == heartbeatIntervalMs) {
                // no need to update the timer if the interval hasn't changed
                return;
            }
            this.heartbeatIntervalMs = heartbeatIntervalMs;
            this.heartbeatTimer.updateAndReset(heartbeatIntervalMs);
        }
    }

    private Optional<Uuid> findTopicIdInGlobalOrLocalCache(String topicName) {
        Uuid idFromMetadataCache = metadata.topicIds().getOrDefault(topicName, null);
        if (idFromMetadataCache != null) {
            // Add topic name to local cache, so it can be reused if included in a next target
            // assignment if metadata cache not available.
            assignedTopicIdCache.put(topicName, idFromMetadataCache);
            return Optional.of(idFromMetadataCache);
        } else {
            Uuid idFromLocalCache = assignedTopicIdCache.getOrDefault(topicName, null);
            return Optional.ofNullable(idFromLocalCache);
        }
    }

    static class HeartbeatState {

        private final ConsumerMembershipManager membershipManager;
        private final int rebalanceTimeoutMs;
        private final StreamsGroupHeartbeatRequestManager.HeartbeatState.SentFields sentFields;

        /*
         * StreamsGroupMetadata holds the metadata for the streams group
         */
        private final StreamsAssignmentInterface streamsInterface;

        public HeartbeatState(
            final StreamsAssignmentInterface streamsInterface,
            final ConsumerMembershipManager membershipManager,
            final int rebalanceTimeoutMs) {

            this.membershipManager = membershipManager;
            this.rebalanceTimeoutMs = rebalanceTimeoutMs;
            this.sentFields = new StreamsGroupHeartbeatRequestManager.HeartbeatState.SentFields();
            this.streamsInterface = streamsInterface;
        }

        public void reset() {
            sentFields.reset();
        }

        public StreamsGroupHeartbeatRequestData buildRequestData() {
            StreamsGroupHeartbeatRequestData data = new StreamsGroupHeartbeatRequestData();

            // GroupId - always sent
            data.setGroupId(membershipManager.groupId());

            // TopologyId - always sent
            data.setTopologyId(streamsInterface.topologyId());

            // MemberId - always sent, empty until it has been received from the coordinator
            data.setMemberId(membershipManager.memberId());

            // MemberEpoch - always sent
            data.setMemberEpoch(membershipManager.memberEpoch());

            // InstanceId - set if present
            membershipManager.groupInstanceId().ifPresent(data::setInstanceId);

            boolean joining = membershipManager.state() == MemberState.JOINING;

            // RebalanceTimeoutMs - only sent when joining or if it has changed since the last heartbeat
            if (joining || sentFields.rebalanceTimeoutMs != rebalanceTimeoutMs) {
                data.setRebalanceTimeoutMs(rebalanceTimeoutMs);
                sentFields.rebalanceTimeoutMs = rebalanceTimeoutMs;
            }

            // Immutable -- only sent when joining
            if (joining) {
                data.setProcessId(streamsInterface.processId().toString());
                data.setActiveTasks(Collections.emptyList());
                data.setStandbyTasks(Collections.emptyList());
                data.setWarmupTasks(Collections.emptyList());
                streamsInterface.endpoint().ifPresent(streamsEndpoint -> {
                    data.setUserEndpoint(new StreamsGroupHeartbeatRequestData.Endpoint()
                        .setHost(streamsEndpoint.host)
                        .setPort(streamsEndpoint.port)
                    );
                });
                data.setClientTags(streamsInterface.clientTags().entrySet().stream()
                    .map(entry -> new StreamsGroupHeartbeatRequestData.KeyValue()
                        .setKey(entry.getKey())
                        .setValue(entry.getValue())
                    )
                    .collect(Collectors.toList()));
            }

            if (streamsInterface.shutdownRequested()) {
                data.setShutdownApplication(true);
            }

            Assignment reconciledAssignment = streamsInterface.reconciledAssignment.get();

            if (reconciledAssignment != null) {
                if (!reconciledAssignment.equals(sentFields.assignment)) {
                    data.setActiveTasks(convertTaskIdCollection(reconciledAssignment.activeTasks));
                    data.setStandbyTasks(convertTaskIdCollection(reconciledAssignment.standbyTasks));
                    data.setWarmupTasks(convertTaskIdCollection(reconciledAssignment.warmupTasks));
                    sentFields.assignment = reconciledAssignment;
                }
            }

            return data;
        }

        private List<TaskIds> convertTaskIdCollection(final Set<StreamsAssignmentInterface.TaskId> tasks) {
            return tasks.stream()
                .collect(
                    Collectors.groupingBy(StreamsAssignmentInterface.TaskId::subtopologyId,
                        Collectors.mapping(StreamsAssignmentInterface.TaskId::partitionId, Collectors.toList()))
                )
                .entrySet()
                .stream()
                .map(entry -> {
                    TaskIds ids = new TaskIds();
                    ids.setSubtopologyId(entry.getKey());
                    ids.setPartitions(entry.getValue());
                    return ids;
                })
                .collect(Collectors.toList());
        }

        // Fields of StreamsGroupHeartbeatRequest sent in the most recent request
        static class SentFields {

            private int rebalanceTimeoutMs = -1;
            private Assignment assignment = null;

            SentFields() {
            }

            void reset() {
                rebalanceTimeoutMs = -1;
                assignment = null;
            }
        }
    }

}
