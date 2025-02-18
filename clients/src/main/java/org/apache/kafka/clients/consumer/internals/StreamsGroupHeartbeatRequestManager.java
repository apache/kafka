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
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData.CopartitionGroup;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData.KeyValue;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData.TaskIds;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData.Topology;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData.Endpoint;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatRequest;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class StreamsGroupHeartbeatRequestManager implements RequestManager {

    private final Logger logger;

    private final int maxPollIntervalMs;

    private final CoordinatorRequestManager coordinatorRequestManager;

    private final StreamsGroupHeartbeatRequestManager.HeartbeatRequestState heartbeatRequestState;

    private final StreamsGroupHeartbeatRequestManager.HeartbeatState heartbeatState;

    private final StreamsMembershipManager membershipManager;

    private final BackgroundEventHandler backgroundEventHandler;

    private final Timer pollTimer;

    private final HeartbeatMetricsManager metricsManager;

    private StreamsAssignmentInterface streamsInterface;

    public StreamsGroupHeartbeatRequestManager(
        final LogContext logContext,
        final Time time,
        final ConsumerConfig config,
        final CoordinatorRequestManager coordinatorRequestManager,
        final StreamsMembershipManager membershipManager,
        final BackgroundEventHandler backgroundEventHandler,
        final Metrics metrics,
        final StreamsAssignmentInterface streamsAssignmentInterface
    ) {
        this.coordinatorRequestManager = coordinatorRequestManager;
        this.logger = logContext.logger(getClass());
        this.membershipManager = membershipManager;
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
    }

    @Override
    public NetworkClientDelegate.PollResult poll(long currentTimeMs) {
        if (coordinatorRequestManager.coordinator().isEmpty() || membershipManager.shouldSkipHeartbeat()) {
            membershipManager.transitionToUnsubscribeIfLeaving();
            return NetworkClientDelegate.PollResult.EMPTY;
        }
        pollTimer.update(currentTimeMs);
        if (pollTimer.isExpired() && !membershipManager.isLeavingGroup()) {
            logger.warn("Consumer poll timeout has expired. This means the time between " +
                "subsequent calls to poll() was longer than the configured max.poll.interval.ms, " +
                "which typically implies that the poll loop is spending too much time processing " +
                "messages. You can address this either by increasing max.poll.interval.ms or by " +
                "reducing the maximum size of batches returned in poll() with max.poll.records.");

            membershipManager.onPollTimerExpired();
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

    public StreamsMembershipManager membershipManager() {
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

        if (data.partitionsByUserEndpoint() != null) {
            streamsInterface.partitionsByHost.set(convertHostInfoMap(data));
        }

        List<StreamsGroupHeartbeatResponseData.Status> statuses = data.status();

        if (statuses != null && !statuses.isEmpty()) {
            String statusDetails = statuses.stream()
                .map(status -> "(" + status.statusCode() + ") " + status.statusDetail())
                .collect(Collectors.joining(", "));
            logger.error("Membership is in the following statuses: {}.", statusDetails);
        }

        membershipManager.onHeartbeatSuccess(response);
    }

    private static Map<StreamsAssignmentInterface.HostInfo, StreamsAssignmentInterface.EndpointPartitions> convertHostInfoMap(
        final StreamsGroupHeartbeatResponseData data) {
        Map<StreamsAssignmentInterface.HostInfo, StreamsAssignmentInterface.EndpointPartitions> partitionsByHost = new HashMap<>();
        data.partitionsByUserEndpoint().forEach(endpoint -> {
            List<TopicPartition> activeTopicPartitions = getTopicPartitionList(endpoint.activePartitions());
            List<TopicPartition> standbyTopicPartitions = getTopicPartitionList(endpoint.standbyPartitions());
            Endpoint userEndpoint = endpoint.userEndpoint();
            StreamsAssignmentInterface.EndpointPartitions endpointPartitions = new StreamsAssignmentInterface.EndpointPartitions(activeTopicPartitions, standbyTopicPartitions);
            partitionsByHost.put(new StreamsAssignmentInterface.HostInfo(userEndpoint.host(), userEndpoint.port()), endpointPartitions);
        });
        return partitionsByHost;
    }

    static List<TopicPartition> getTopicPartitionList(List<StreamsGroupHeartbeatResponseData.TopicPartition> topicPartitions) {
        return topicPartitions.stream()
            .flatMap(partition ->
                partition.partitions().stream().map(partitionId -> new TopicPartition(partition.topic(), partitionId)))
            .collect(Collectors.toList());
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
                membershipManager.onFenced();
                // Skip backoff so that a next HB to rejoin is sent as soon as the fenced member releases its assignment
                heartbeatRequestState.reset();
                break;

            case UNKNOWN_MEMBER_ID:
                message = String.format("StreamsGroupHeartbeatRequest failed because member %s is unknown.",
                    membershipManager.memberId());
                logInfo(message, response, currentTimeMs);
                membershipManager.onFenced();
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
            super(logContext, StreamsGroupHeartbeatRequestManager.HeartbeatRequestState.class.getName(), retryBackoffMs, 2,
                retryBackoffMaxMs,
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

    static class HeartbeatState {

        private final StreamsMembershipManager membershipManager;
        private final int rebalanceTimeoutMs;
        private final StreamsGroupHeartbeatRequestManager.HeartbeatState.SentFields sentFields;

        /*
         * StreamsGroupMetadata holds the metadata for the streams group
         */
        private final StreamsAssignmentInterface streamsInterface;

        public HeartbeatState(
            final StreamsAssignmentInterface streamsInterface,
            final StreamsMembershipManager membershipManager,
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

            // MemberId - always sent, it will be generated at Streams Consumer startup
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

                // Topology -- sent when joining
                Topology topology = new Topology();
                topology.setSubtopologies(getTopologyFromStreams());
                topology.setEpoch(streamsInterface.topologyEpoch());

                data.setTopology(topology);
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

        private List<StreamsGroupHeartbeatRequestData.Subtopology> getTopologyFromStreams() {
            final Map<String, StreamsAssignmentInterface.Subtopology> subTopologyMap = streamsInterface.subtopologyMap();
            final List<StreamsGroupHeartbeatRequestData.Subtopology> subtopologies = new ArrayList<>(subTopologyMap.size());
            for (final Map.Entry<String, StreamsAssignmentInterface.Subtopology> subtopology : subTopologyMap.entrySet()) {
                subtopologies.add(getSubtopologyFromStreams(subtopology.getKey(), subtopology.getValue()));
            }
            subtopologies.sort(Comparator.comparing(StreamsGroupHeartbeatRequestData.Subtopology::subtopologyId));
            return subtopologies;
        }

        private static StreamsGroupHeartbeatRequestData.Subtopology getSubtopologyFromStreams(final String subtopologyName,
                                                                                              final StreamsAssignmentInterface.Subtopology subtopology) {
            final StreamsGroupHeartbeatRequestData.Subtopology subtopologyData = new StreamsGroupHeartbeatRequestData.Subtopology();
            subtopologyData.setSubtopologyId(subtopologyName);
            ArrayList<String> sortedSourceTopics = new ArrayList<>(subtopology.sourceTopics);
            Collections.sort(sortedSourceTopics);
            subtopologyData.setSourceTopics(sortedSourceTopics);
            ArrayList<String> sortedSinkTopics = new ArrayList<>(subtopology.repartitionSinkTopics);
            Collections.sort(sortedSinkTopics);
            subtopologyData.setRepartitionSinkTopics(sortedSinkTopics);
            subtopologyData.setRepartitionSourceTopics(getRepartitionTopicsInfoFromStreams(subtopology));
            subtopologyData.setStateChangelogTopics(getChangelogTopicsInfoFromStreams(subtopology));
            subtopologyData.setCopartitionGroups(
                getCopartitionGroupsFromStreams(subtopology.copartitionGroups, subtopologyData));
            return subtopologyData;
        }

        private static List<CopartitionGroup> getCopartitionGroupsFromStreams(final Collection<Set<String>> copartitionGroups,
                                                                              final StreamsGroupHeartbeatRequestData.Subtopology subtopologyData) {

            final Map<String, Short> sourceTopicsMap =
                IntStream.range(0, subtopologyData.sourceTopics().size())
                    .boxed()
                    .collect(Collectors.toMap(subtopologyData.sourceTopics()::get, Integer::shortValue));

            final Map<String, Short> repartitionSourceTopics =
                IntStream.range(0, subtopologyData.repartitionSourceTopics().size())
                    .boxed()
                    .collect(
                        Collectors.toMap(x -> subtopologyData.repartitionSourceTopics().get(x).name(),
                            Integer::shortValue));

            return copartitionGroups.stream()
                .map(x -> getCopartitionGroupFromStreams(x, sourceTopicsMap, repartitionSourceTopics))
                .collect(Collectors.toList());
        }

        private static CopartitionGroup getCopartitionGroupFromStreams(final Set<String> topicNames,
                                                                       final Map<String, Short> sourceTopicsMap,
                                                                       final Map<String, Short> repartitionSourceTopics) {
            CopartitionGroup copartitionGroup = new CopartitionGroup();

            topicNames.forEach(topicName -> {
                if (sourceTopicsMap.containsKey(topicName)) {
                    copartitionGroup.sourceTopics().add(sourceTopicsMap.get(topicName));
                } else if (repartitionSourceTopics.containsKey(topicName)) {
                    copartitionGroup.repartitionSourceTopics()
                        .add(repartitionSourceTopics.get(topicName));
                } else {
                    throw new IllegalStateException(
                        "Source topic not found in subtopology: " + topicName);
                }
            });

            return copartitionGroup;
        }

        private static List<StreamsGroupHeartbeatRequestData.TopicInfo> getRepartitionTopicsInfoFromStreams(
            final StreamsAssignmentInterface.Subtopology subtopologyDataFromStreams) {
            final List<StreamsGroupHeartbeatRequestData.TopicInfo> repartitionTopicsInfo = new ArrayList<>();
            for (final Map.Entry<String, StreamsAssignmentInterface.TopicInfo> repartitionTopic : subtopologyDataFromStreams.repartitionSourceTopics.entrySet()) {
                final StreamsGroupHeartbeatRequestData.TopicInfo repartitionTopicInfo = new StreamsGroupHeartbeatRequestData.TopicInfo();
                repartitionTopicInfo.setName(repartitionTopic.getKey());
                repartitionTopic.getValue().numPartitions.ifPresent(repartitionTopicInfo::setPartitions);
                repartitionTopic.getValue().replicationFactor.ifPresent(repartitionTopicInfo::setReplicationFactor);
                repartitionTopic.getValue().topicConfigs.forEach((k, v) ->
                    repartitionTopicInfo.topicConfigs().add(new KeyValue().setKey(k).setValue(v))
                );
                repartitionTopicsInfo.add(repartitionTopicInfo);
            }
            repartitionTopicsInfo.sort(Comparator.comparing(StreamsGroupHeartbeatRequestData.TopicInfo::name));
            return repartitionTopicsInfo;
        }

        private static List<StreamsGroupHeartbeatRequestData.TopicInfo> getChangelogTopicsInfoFromStreams(
            final StreamsAssignmentInterface.Subtopology subtopologyDataFromStreams) {
            final List<StreamsGroupHeartbeatRequestData.TopicInfo> changelogTopicsInfo = new ArrayList<>();
            for (final Map.Entry<String, StreamsAssignmentInterface.TopicInfo> changelogTopic : subtopologyDataFromStreams.stateChangelogTopics.entrySet()) {
                final StreamsGroupHeartbeatRequestData.TopicInfo changelogTopicInfo = new StreamsGroupHeartbeatRequestData.TopicInfo();
                changelogTopicInfo.setName(changelogTopic.getKey());
                changelogTopic.getValue().replicationFactor.ifPresent(changelogTopicInfo::setReplicationFactor);
                changelogTopic.getValue().topicConfigs.forEach((k, v) ->
                    changelogTopicInfo.topicConfigs().add(new KeyValue().setKey(k).setValue(v))
                );
                changelogTopicsInfo.add(changelogTopicInfo);
            }
            changelogTopicsInfo.sort(Comparator.comparing(StreamsGroupHeartbeatRequestData.TopicInfo::name));
            return changelogTopicsInfo;
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
