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
import org.apache.kafka.clients.consumer.internals.events.AsyncPollEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.clients.consumer.internals.metrics.HeartbeatMetricsManager;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
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
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult.EMPTY;
import static org.apache.kafka.clients.consumer.internals.RequestState.RETRY_BACKOFF_JITTER;

/**
 * <p>Manages the request creation and response handling for the streams group heartbeat. The class creates a
 * heartbeat request using the state stored in the membership manager. The requests can be retrieved
 * by calling {@link StreamsGroupHeartbeatRequestManager#poll(long)}. Once the response is received, it updates the
 * state in the membership manager and handles any errors.
 *
 * <p>The heartbeat manager generates heartbeat requests based on the member state. It's also responsible
 * for the timing of the heartbeat requests to ensure they are sent according to the heartbeat interval
 * (while the member state is stable) or on demand (while the member is acknowledging an assignment or
 * leaving the group).
 */
public class StreamsGroupHeartbeatRequestManager implements RequestManager {

    private static final String UNSUPPORTED_VERSION_ERROR_MESSAGE = "The cluster does not support the STREAMS group " +
        "protocol or does not support the versions of the STREAMS group protocol used by this client " +
        "(used versions: " + StreamsGroupHeartbeatRequestData.LOWEST_SUPPORTED_VERSION + " to " +
        StreamsGroupHeartbeatRequestData.HIGHEST_SUPPORTED_VERSION + ").";

    static class HeartbeatState {

        // Fields of StreamsGroupHeartbeatRequest sent in the most recent request
        static class LastSentFields {

            private StreamsRebalanceData.Assignment assignment = null;

            LastSentFields() {
            }

            void reset() {
                assignment = null;
            }
        }

        private final StreamsMembershipManager membershipManager;
        private final int rebalanceTimeoutMs;
        private final StreamsRebalanceData streamsRebalanceData;
        private final LastSentFields lastSentFields = new LastSentFields();
        private int endpointInformationEpoch = -1;


        public HeartbeatState(final StreamsRebalanceData streamsRebalanceData,
                              final StreamsMembershipManager membershipManager,
                              final int rebalanceTimeoutMs) {
            this.membershipManager = membershipManager;
            this.streamsRebalanceData = streamsRebalanceData;
            this.rebalanceTimeoutMs = rebalanceTimeoutMs;
        }

        public void reset() {
            lastSentFields.reset();
        }

        public int endpointInformationEpoch() {
            return endpointInformationEpoch;
        }

        public void setEndpointInformationEpoch(int endpointInformationEpoch) {
            this.endpointInformationEpoch = endpointInformationEpoch;
        }

        public StreamsGroupHeartbeatRequestData buildRequestData() {
            StreamsGroupHeartbeatRequestData data = new StreamsGroupHeartbeatRequestData();
            data.setGroupId(membershipManager.groupId());
            data.setMemberId(membershipManager.memberId());
            data.setMemberEpoch(membershipManager.memberEpoch());
            data.setEndpointInformationEpoch(endpointInformationEpoch);
            membershipManager.groupInstanceId().ifPresent(data::setInstanceId);

            boolean joining = membershipManager.state() == MemberState.JOINING;

            if (joining) {
                StreamsGroupHeartbeatRequestData.Topology topology = new StreamsGroupHeartbeatRequestData.Topology();
                topology.setSubtopologies(fromStreamsToHeartbeatRequest(streamsRebalanceData.subtopologies()));
                topology.setEpoch(streamsRebalanceData.topologyEpoch());
                data.setTopology(topology);
                data.setRebalanceTimeoutMs(rebalanceTimeoutMs);
                data.setProcessId(streamsRebalanceData.processId().toString());
                streamsRebalanceData.endpoint().ifPresent(userEndpoint -> {
                    data.setUserEndpoint(new StreamsGroupHeartbeatRequestData.Endpoint()
                        .setHost(userEndpoint.host())
                        .setPort(userEndpoint.port())
                    );
                });
                data.setClientTags(streamsRebalanceData.clientTags().entrySet().stream()
                    .map(entry -> new StreamsGroupHeartbeatRequestData.KeyValue()
                        .setKey(entry.getKey())
                        .setValue(entry.getValue())
                    )
                    .collect(Collectors.toList()));
                data.setActiveTasks(fromStreamsToHeartbeatRequest(Set.of()));
                data.setStandbyTasks(fromStreamsToHeartbeatRequest(Set.of()));
                data.setWarmupTasks(fromStreamsToHeartbeatRequest(Set.of()));
            } else {
                StreamsRebalanceData.Assignment reconciledAssignment = streamsRebalanceData.reconciledAssignment();
                if (!reconciledAssignment.equals(lastSentFields.assignment)) {
                    data.setActiveTasks(fromStreamsToHeartbeatRequest(reconciledAssignment.activeTasks()));
                    data.setStandbyTasks(fromStreamsToHeartbeatRequest(reconciledAssignment.standbyTasks()));
                    data.setWarmupTasks(fromStreamsToHeartbeatRequest(reconciledAssignment.warmupTasks()));
                    lastSentFields.assignment = reconciledAssignment;
                }
            }
            data.setShutdownApplication(streamsRebalanceData.shutdownRequested());
            return data;
        }

        private static List<StreamsGroupHeartbeatRequestData.TaskIds> fromStreamsToHeartbeatRequest(final Set<StreamsRebalanceData.TaskId> tasks) {
            return tasks.stream()
                .collect(
                    Collectors.groupingBy(StreamsRebalanceData.TaskId::subtopologyId,
                        Collectors.mapping(StreamsRebalanceData.TaskId::partitionId, Collectors.toList()))
                )
                .entrySet()
                .stream()
                .map(entry -> {
                    return new StreamsGroupHeartbeatRequestData.TaskIds()
                        .setSubtopologyId(entry.getKey())
                        .setPartitions(entry.getValue());
                })
                .collect(Collectors.toList());
        }

        private static List<StreamsGroupHeartbeatRequestData.Subtopology> fromStreamsToHeartbeatRequest(final Map<String, StreamsRebalanceData.Subtopology> subtopologies) {
            final List<StreamsGroupHeartbeatRequestData.Subtopology> subtopologiesForRequest = new ArrayList<>(subtopologies.size());
            for (final Map.Entry<String, StreamsRebalanceData.Subtopology> subtopology : subtopologies.entrySet()) {
                subtopologiesForRequest.add(fromStreamsToHeartbeatRequest(subtopology.getKey(), subtopology.getValue()));
            }
            subtopologiesForRequest.sort(Comparator.comparing(StreamsGroupHeartbeatRequestData.Subtopology::subtopologyId));
            return subtopologiesForRequest;
        }

        private static StreamsGroupHeartbeatRequestData.Subtopology fromStreamsToHeartbeatRequest(final String subtopologyId,
                                                                                                  final StreamsRebalanceData.Subtopology subtopology) {
            final StreamsGroupHeartbeatRequestData.Subtopology subtopologyData = new StreamsGroupHeartbeatRequestData.Subtopology();
            subtopologyData.setSubtopologyId(subtopologyId);
            ArrayList<String> sortedSourceTopics = new ArrayList<>(subtopology.sourceTopics());
            Collections.sort(sortedSourceTopics);
            subtopologyData.setSourceTopics(sortedSourceTopics);
            ArrayList<String> sortedSinkTopics = new ArrayList<>(subtopology.repartitionSinkTopics());
            Collections.sort(sortedSinkTopics);
            subtopologyData.setRepartitionSinkTopics(sortedSinkTopics);
            subtopologyData.setRepartitionSourceTopics(getRepartitionTopicsInfoFromStreams(subtopology));
            subtopologyData.setStateChangelogTopics(getChangelogTopicsInfoFromStreams(subtopology));
            subtopologyData.setCopartitionGroups(
                getCopartitionGroupsFromStreams(subtopology.copartitionGroups(), subtopologyData));
            return subtopologyData;
        }

        private static List<StreamsGroupHeartbeatRequestData.CopartitionGroup> getCopartitionGroupsFromStreams(final Collection<Set<String>> copartitionGroups,
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

        private static StreamsGroupHeartbeatRequestData.CopartitionGroup getCopartitionGroupFromStreams(final Set<String> topicNames,
                                                                                                        final Map<String, Short> sourceTopicsMap,
                                                                                                        final Map<String, Short> repartitionSourceTopics) {
            StreamsGroupHeartbeatRequestData.CopartitionGroup copartitionGroup = new StreamsGroupHeartbeatRequestData.CopartitionGroup();

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

        private static List<StreamsGroupHeartbeatRequestData.TopicInfo> getRepartitionTopicsInfoFromStreams(final StreamsRebalanceData.Subtopology subtopologyDataFromStreams) {
            final List<StreamsGroupHeartbeatRequestData.TopicInfo> repartitionTopicsInfo = new ArrayList<>();
            for (final Map.Entry<String, StreamsRebalanceData.TopicInfo> repartitionTopic : subtopologyDataFromStreams.repartitionSourceTopics().entrySet()) {
                final StreamsGroupHeartbeatRequestData.TopicInfo repartitionTopicInfo = new StreamsGroupHeartbeatRequestData.TopicInfo();
                repartitionTopicInfo.setName(repartitionTopic.getKey());
                repartitionTopic.getValue().numPartitions().ifPresent(repartitionTopicInfo::setPartitions);
                repartitionTopic.getValue().replicationFactor().ifPresent(repartitionTopicInfo::setReplicationFactor);
                repartitionTopic.getValue().topicConfigs().forEach((k, v) ->
                    repartitionTopicInfo.topicConfigs().add(new StreamsGroupHeartbeatRequestData.KeyValue().setKey(k).setValue(v))
                );
                repartitionTopicsInfo.add(repartitionTopicInfo);
                repartitionTopicInfo.topicConfigs().sort(Comparator.comparing(StreamsGroupHeartbeatRequestData.KeyValue::key));
            }
            repartitionTopicsInfo.sort(Comparator.comparing(StreamsGroupHeartbeatRequestData.TopicInfo::name));
            return repartitionTopicsInfo;
        }

        private static List<StreamsGroupHeartbeatRequestData.TopicInfo> getChangelogTopicsInfoFromStreams(final StreamsRebalanceData.Subtopology subtopologyDataFromStreams) {
            final List<StreamsGroupHeartbeatRequestData.TopicInfo> changelogTopicsInfo = new ArrayList<>();
            for (final Map.Entry<String, StreamsRebalanceData.TopicInfo> changelogTopic : subtopologyDataFromStreams.stateChangelogTopics().entrySet()) {
                final StreamsGroupHeartbeatRequestData.TopicInfo changelogTopicInfo = new StreamsGroupHeartbeatRequestData.TopicInfo();
                changelogTopicInfo.setName(changelogTopic.getKey());
                changelogTopic.getValue().replicationFactor().ifPresent(changelogTopicInfo::setReplicationFactor);
                changelogTopic.getValue().topicConfigs().forEach((k, v) ->
                    changelogTopicInfo.topicConfigs().add(new StreamsGroupHeartbeatRequestData.KeyValue().setKey(k).setValue(v))
                );
                changelogTopicInfo.topicConfigs().sort(Comparator.comparing(StreamsGroupHeartbeatRequestData.KeyValue::key));
                changelogTopicsInfo.add(changelogTopicInfo);
            }
            changelogTopicsInfo.sort(Comparator.comparing(StreamsGroupHeartbeatRequestData.TopicInfo::name));
            return changelogTopicsInfo;
        }
    }

    private final Logger logger;

    private final int maxPollIntervalMs;

    private final CoordinatorRequestManager coordinatorRequestManager;

    private final HeartbeatRequestState heartbeatRequestState;

    private final HeartbeatState heartbeatState;

    private final StreamsMembershipManager membershipManager;

    private final BackgroundEventHandler backgroundEventHandler;

    private final HeartbeatMetricsManager metricsManager;

    private final StreamsRebalanceData streamsRebalanceData;

    /**
     * Timer for tracking the time since the last consumer poll.  If the timer expires, the consumer will stop
     * sending heartbeat until the next poll.
     */
    private final Timer pollTimer;

    public StreamsGroupHeartbeatRequestManager(final LogContext logContext,
                                               final Time time,
                                               final ConsumerConfig config,
                                               final CoordinatorRequestManager coordinatorRequestManager,
                                               final StreamsMembershipManager membershipManager,
                                               final BackgroundEventHandler backgroundEventHandler,
                                               final Metrics metrics,
                                               final StreamsRebalanceData streamsRebalanceData) {
        this.logger = logContext.logger(getClass());
        this.coordinatorRequestManager = Objects.requireNonNull(
            coordinatorRequestManager,
            "Coordinator request manager cannot be null"
        );
        this.membershipManager = Objects.requireNonNull(
            membershipManager,
            "Streams membership manager cannot be null"
        );
        this.backgroundEventHandler = Objects.requireNonNull(
            backgroundEventHandler,
            "Background event handler cannot be null"
        );
        this.metricsManager = new HeartbeatMetricsManager(
            Objects.requireNonNull(metrics, "Metrics cannot be null")
        );
        this.streamsRebalanceData = Objects.requireNonNull(streamsRebalanceData, "Streams rebalance data cannot be null");
        this.maxPollIntervalMs = config.getInt(CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG);
        long retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        long retryBackoffMaxMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG);
        this.heartbeatState = new HeartbeatState(streamsRebalanceData, membershipManager, maxPollIntervalMs);
        this.heartbeatRequestState = new HeartbeatRequestState(
            logContext,
            time,
            0,
            retryBackoffMs,
            retryBackoffMaxMs,
            RETRY_BACKOFF_JITTER
        );
        this.pollTimer = time.timer(maxPollIntervalMs);
    }

    /**
     * This will build a heartbeat request if one must be sent, determined based on the member
     * state. A heartbeat is sent when all of the following applies:
     * <ol>
     *     <li>Member is part of the consumer group or wants to join it.</li>
     *     <li>The heartbeat interval has expired, or the member is in a state that indicates
     *     that it should heartbeat without waiting for the interval.</li>
     * </ol>
     * This will also determine the maximum wait time until the next poll based on the member's
     * state.
     * <ol>
     *     <li>If the member is without a coordinator or is in a failed state, the timer is set
     *     to Long.MAX_VALUE, as there's no need to send a heartbeat.</li>
     *     <li>If the member cannot send a heartbeat due to either exponential backoff, it will
     *     return the remaining time left on the backoff timer.</li>
     *     <li>If the member's heartbeat timer has not expired, It will return the remaining time
     *     left on the heartbeat timer.</li>
     *     <li>If the member can send a heartbeat, the timer is set to the current heartbeat interval.</li>
     * </ol>
     *
     * @return {@link org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult} that includes a
     *         heartbeat request if one must be sent, and the time to wait until the next poll.
     */
    @Override
    public NetworkClientDelegate.PollResult poll(long currentTimeMs) {
        if (coordinatorRequestManager.coordinator().isEmpty() || membershipManager.shouldSkipHeartbeat()) {
            membershipManager.onHeartbeatRequestSkipped();
            maybePropagateCoordinatorFatalErrorEvent();
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
            NetworkClientDelegate.UnsentRequest leaveHeartbeat = makeHeartbeatRequestAndLogResponse(currentTimeMs);

            // We can ignore the leave response because we can join before or after receiving the response.
            heartbeatRequestState.reset();
            heartbeatState.reset();
            return new NetworkClientDelegate.PollResult(heartbeatRequestState.heartbeatIntervalMs(), Collections.singletonList(leaveHeartbeat));
        }
        if (shouldHeartbeatBeforeIntervalExpires() || heartbeatRequestState.canSendRequest(currentTimeMs)) {
            NetworkClientDelegate.UnsentRequest request = makeHeartbeatRequestAndHandleResponse(currentTimeMs);
            return new NetworkClientDelegate.PollResult(heartbeatRequestState.heartbeatIntervalMs(), Collections.singletonList(request));
        } else {
            return new NetworkClientDelegate.PollResult(heartbeatRequestState.timeToNextHeartbeatMs(currentTimeMs));
        }
    }

    /**
     * Generate a heartbeat request to leave the group if the state is still LEAVING when this is
     * called to close the consumer.
     * <p/>
     * Note that when closing the consumer, even though an event to Unsubscribe is generated
     * (triggers callbacks and sends leave group), it could be the case that the Unsubscribe event
     * processing does not complete in time and moves on to close the managers (ex. calls to
     * close with zero timeout). So we could end up on this pollOnClose with the member in
     * {@link MemberState#PREPARE_LEAVING} (ex. app thread did not have the time to process the
     * event to execute callbacks), or {@link MemberState#LEAVING} (ex. the leave request could
     * not be sent due to coordinator not available at that time). In all cases, the pollOnClose
     * will be triggered right before sending the final requests, so we ensure that we generate
     * the request to leave if needed.
     *
     * @param currentTimeMs The current system time in milliseconds at which the method was called
     * @return PollResult containing the request to send
     */
    @Override
    public NetworkClientDelegate.PollResult pollOnClose(long currentTimeMs) {
        if (membershipManager.isLeavingGroup()) {
            NetworkClientDelegate.UnsentRequest request = makeHeartbeatRequestAndLogResponse(currentTimeMs);
            return new NetworkClientDelegate.PollResult(heartbeatRequestState.heartbeatIntervalMs(), List.of(request));
        }
        return EMPTY;
    }

    public StreamsMembershipManager membershipManager() {
        return membershipManager;
    }

    /**
     * Returns the delay for which the application thread can safely wait before it should be responsive
     * to results from the request managers. For example, the subscription state can change when heartbeats
     * are sent, so blocking for longer than the heartbeat interval might mean the application thread is not
     * responsive to changes.
     *
     * <p>Similarly, we may have to unblock the application thread to send a {@link AsyncPollEvent} to make sure
     * our poll timer will not expire while we are polling.
     *
     * <p>In the event that heartbeats are currently being skipped, this still returns the next heartbeat
     * delay rather than {@code Long.MAX_VALUE} so that the application thread remains responsive.
     */
    @Override
    public long maximumTimeToWait(long currentTimeMs) {
        pollTimer.update(currentTimeMs);
        if (pollTimer.isExpired() ||
            membershipManager.shouldNotWaitForHeartbeatInterval() && !heartbeatRequestState.requestInFlight()) {

            return 0L;
        }
        return Math.min(pollTimer.remainingMs() / 2, heartbeatRequestState.timeToNextHeartbeatMs(currentTimeMs));
    }

    public void resetPollTimer(final long pollMs) {
        pollTimer.update(pollMs);
        if (pollTimer.isExpired()) {
            logger.warn("Time between subsequent calls to poll() was longer than the configured " +
                    "max.poll.interval.ms, exceeded approximately by {} ms. Member {} will rejoin the group now.",
                pollTimer.isExpiredBy(), membershipManager.memberId());
            membershipManager.maybeRejoinStaleMember();
        }
        pollTimer.reset(maxPollIntervalMs);
    }

    /**
     * A heartbeat should be sent without waiting for the heartbeat interval to expire if:
     * - the member is leaving the group
     * or
     * - the member is joining the group or acknowledging the assignment and for both cases there is no heartbeat request
     *   in flight.
     *
     * @return true if a heartbeat should be sent before the interval expires, false otherwise
     */
    private boolean shouldHeartbeatBeforeIntervalExpires() {
        return membershipManager.state() == MemberState.LEAVING
            ||
            (membershipManager.state() == MemberState.JOINING || membershipManager.state() == MemberState.ACKNOWLEDGING)
                && !heartbeatRequestState.requestInFlight();
    }

    private void maybePropagateCoordinatorFatalErrorEvent() {
        coordinatorRequestManager.getAndClearFatalError()
            .ifPresent(fatalError -> backgroundEventHandler.add(new ErrorEvent(fatalError)));
    }

    private NetworkClientDelegate.UnsentRequest makeHeartbeatRequestAndLogResponse(final long currentTimeMs) {
        return makeHeartbeatRequest(currentTimeMs).whenComplete((response, exception) -> {
            if (response != null) {
                metricsManager.recordRequestLatency(response.requestLatencyMs());
                Errors error = Errors.forCode(((StreamsGroupHeartbeatResponse) response.responseBody()).data().errorCode());
                if (error == Errors.NONE)
                    logger.debug("StreamsGroupHeartbeatRequest responded successfully: {}", response);
                else
                    logger.error("StreamsGroupHeartbeatRequest failed because of {}: {}", error, response);
            } else {
                logger.error("StreamsGroupHeartbeatRequest failed because of unexpected exception.", exception);
            }
        });
    }

    private NetworkClientDelegate.UnsentRequest makeHeartbeatRequestAndHandleResponse(final long currentTimeMs) {
        NetworkClientDelegate.UnsentRequest request = makeHeartbeatRequest(currentTimeMs);
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

    private NetworkClientDelegate.UnsentRequest makeHeartbeatRequest(final long currentTimeMs) {
        NetworkClientDelegate.UnsentRequest request = new NetworkClientDelegate.UnsentRequest(
            new StreamsGroupHeartbeatRequest.Builder(this.heartbeatState.buildRequestData()),
            coordinatorRequestManager.coordinator()
        );
        heartbeatRequestState.onSendAttempt(currentTimeMs);
        membershipManager.onHeartbeatRequestGenerated();
        metricsManager.recordHeartbeatSentMs(currentTimeMs);
        heartbeatRequestState.resetTimer();
        return request;
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
        heartbeatState.setEndpointInformationEpoch(data.endpointInformationEpoch());
        streamsRebalanceData.setHeartbeatIntervalMs(data.heartbeatIntervalMs());

        if (data.partitionsByUserEndpoint() != null) {
            streamsRebalanceData.setPartitionsByHost(convertHostInfoMap(data));
        }

        List<StreamsGroupHeartbeatResponseData.Status> statuses = data.status();
        if (statuses != null) {
            streamsRebalanceData.setStatuses(statuses);
            if (!statuses.isEmpty()) {
                String statusDetails = statuses.stream()
                    .map(status -> "(" + status.statusCode() + ") " + status.statusDetail())
                    .collect(Collectors.joining(", "));
                logger.warn("Membership is in the following statuses: {}", statusDetails);
            }
        }

        membershipManager.onHeartbeatSuccess(response);
    }

    private void onErrorResponse(final StreamsGroupHeartbeatResponse response, final long currentTimeMs) {
        final Errors error = Errors.forCode(response.data().errorCode());
        final String errorMessage = response.data().errorMessage();

        heartbeatState.reset();
        this.heartbeatRequestState.onFailedAttempt(currentTimeMs);

        switch (error) {
            case NOT_COORDINATOR:
                logInfo(
                    String.format("StreamsGroupHeartbeatRequest failed because the group coordinator %s is incorrect. " +
                        "Will attempt to find the coordinator again and retry", coordinatorRequestManager.coordinator()),
                    response,
                    currentTimeMs
                );
                coordinatorRequestManager.markCoordinatorUnknown(errorMessage, currentTimeMs);
                // Skip backoff so that the next HB is sent as soon as the new coordinator is discovered
                heartbeatRequestState.reset();
                break;

            case COORDINATOR_NOT_AVAILABLE:
                logInfo(
                    String.format("StreamsGroupHeartbeatRequest failed because the group coordinator %s is not available. " +
                        "Will attempt to find the coordinator again and retry", coordinatorRequestManager.coordinator()),
                    response,
                    currentTimeMs
                );
                coordinatorRequestManager.markCoordinatorUnknown(errorMessage, currentTimeMs);
                // Skip backoff so that the next HB is sent as soon as the new coordinator is discovered
                heartbeatRequestState.reset();
                break;

            case COORDINATOR_LOAD_IN_PROGRESS:
                logInfo(
                    String.format("StreamsGroupHeartbeatRequest failed because the group coordinator %s is still loading. " +
                    "Will retry", coordinatorRequestManager.coordinator()),
                    response,
                    currentTimeMs
                );
                break;

            case GROUP_AUTHORIZATION_FAILED:
                GroupAuthorizationException exception =
                    GroupAuthorizationException.forGroupId(membershipManager.groupId());
                logger.error("StreamsGroupHeartbeatRequest failed due to group authorization failure: {}",
                    exception.getMessage());
                handleFatalFailure(error.exception(exception.getMessage()));
                break;

            case TOPIC_AUTHORIZATION_FAILED:
                logger.error("StreamsGroupHeartbeatRequest failed for member {} with state {} due to {}: {}",
                    membershipManager.memberId(), membershipManager.state(), error, errorMessage);
                // Propagate auth error received in HB so that it's returned on poll.
                // Member should stay in its current state so it can recover if ever the missing ACLs are added.
                backgroundEventHandler.add(new ErrorEvent(error.exception()));
                break;

            case INVALID_REQUEST:
            case GROUP_MAX_SIZE_REACHED:
            case STREAMS_INVALID_TOPOLOGY:
            case STREAMS_INVALID_TOPOLOGY_EPOCH:
            case STREAMS_TOPOLOGY_FENCED:
                logger.error("StreamsGroupHeartbeatRequest failed due to {}: {}", error, errorMessage);
                handleFatalFailure(error.exception(errorMessage));
                break;

            case FENCED_MEMBER_EPOCH:
                logInfo(
                    String.format("StreamsGroupHeartbeatRequest failed for member %s because epoch %s is fenced.",
                        membershipManager.memberId(), membershipManager.memberEpoch()),
                    response,
                    currentTimeMs
                );
                membershipManager.onFenced();
                // Skip backoff so that a next HB to rejoin is sent as soon as the fenced member releases its assignment
                heartbeatRequestState.reset();
                break;

            case UNKNOWN_MEMBER_ID:
                logInfo(
                    String.format("StreamsGroupHeartbeatRequest failed because member %s is unknown.",
                        membershipManager.memberId()),
                    response,
                    currentTimeMs
                );
                membershipManager.onFenced();
                // Skip backoff so that a next HB to rejoin is sent as soon as the fenced member releases its assignment
                heartbeatRequestState.reset();
                break;

            case UNSUPPORTED_VERSION:
                logger.error("StreamsGroupHeartbeatRequest failed due to {}: {}", error, UNSUPPORTED_VERSION_ERROR_MESSAGE);
                handleFatalFailure(error.exception(UNSUPPORTED_VERSION_ERROR_MESSAGE));
                break;

            default:
                logger.error("StreamsGroupHeartbeatRequest failed due to unexpected error {}: {}", error, errorMessage);
                handleFatalFailure(error.exception(errorMessage));
        }
        membershipManager.onFatalHeartbeatFailure();
    }

    private void logInfo(final String message,
                         final StreamsGroupHeartbeatResponse response,
                         final long currentTimeMs) {
        logger.info("{} in {}ms: {}",
            message,
            heartbeatRequestState.remainingBackoffMs(currentTimeMs),
            response.data().errorMessage());
    }

    private void onFailure(final Throwable exception, final long responseTimeMs) {
        heartbeatRequestState.onFailedAttempt(responseTimeMs);
        heartbeatState.reset();
        if (exception instanceof RetriableException) {
            coordinatorRequestManager.handleCoordinatorDisconnect(exception, responseTimeMs);
            String message = String.format("StreamsGroupHeartbeatRequest failed because of a retriable exception. Will retry in %s ms: %s",
                heartbeatRequestState.remainingBackoffMs(responseTimeMs),
                exception.getMessage());
            logger.debug(message);
            membershipManager.onRetriableHeartbeatFailure();
        } else {
            if (exception instanceof UnsupportedVersionException) {
                logger.error("StreamsGroupHeartbeatRequest failed because of an unsupported version exception: {}",
                    exception.getMessage());
                handleFatalFailure(new UnsupportedVersionException(UNSUPPORTED_VERSION_ERROR_MESSAGE));
            } else {
                logger.error("StreamsGroupHeartbeatRequest failed because of a fatal exception while sending request: {}",
                    exception.getMessage());
                handleFatalFailure(exception);
            }
            membershipManager.onFatalHeartbeatFailure();
        }
    }

    private void handleFatalFailure(Throwable error) {
        backgroundEventHandler.add(new ErrorEvent(error));
        membershipManager.transitionToFatal();
    }

    private static Map<StreamsRebalanceData.HostInfo, StreamsRebalanceData.EndpointPartitions> convertHostInfoMap(
            final StreamsGroupHeartbeatResponseData data) {
        Map<StreamsRebalanceData.HostInfo, StreamsRebalanceData.EndpointPartitions> partitionsByHost = new HashMap<>();
        data.partitionsByUserEndpoint().forEach(endpoint -> {
            List<TopicPartition> activeTopicPartitions = getTopicPartitionList(endpoint.activePartitions());
            List<TopicPartition> standbyTopicPartitions = getTopicPartitionList(endpoint.standbyPartitions());
            StreamsGroupHeartbeatResponseData.Endpoint userEndpoint = endpoint.userEndpoint();
            StreamsRebalanceData.EndpointPartitions endpointPartitions = new StreamsRebalanceData.EndpointPartitions(activeTopicPartitions, standbyTopicPartitions);
            partitionsByHost.put(new StreamsRebalanceData.HostInfo(userEndpoint.host(), userEndpoint.port()), endpointPartitions);
        });
        return partitionsByHost;
    }

    static List<TopicPartition> getTopicPartitionList(List<StreamsGroupHeartbeatResponseData.TopicPartition> topicPartitions) {
        return topicPartitions.stream()
                .flatMap(partition ->
                        partition.partitions().stream().map(partitionId -> new TopicPartition(partition.topic(), partitionId)))
                .collect(Collectors.toList());
    }

}
