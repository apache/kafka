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
import org.apache.kafka.clients.consumer.internals.metrics.HeartbeatMetricsManager;
import org.apache.kafka.common.TopicPartition;
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

public class StreamsGroupHeartbeatRequestManager implements RequestManager {

    static class HeartbeatState {

        private final StreamsMembershipManager membershipManager;
        private final int rebalanceTimeoutMs;
        private final StreamsRebalanceData streamsRebalanceData;

        public HeartbeatState(final StreamsRebalanceData streamsRebalanceData,
                              final StreamsMembershipManager membershipManager,
                              final int rebalanceTimeoutMs) {
            this.membershipManager = membershipManager;
            this.streamsRebalanceData = streamsRebalanceData;
            this.rebalanceTimeoutMs = rebalanceTimeoutMs;
        }

        public StreamsGroupHeartbeatRequestData buildRequestData() {
            StreamsGroupHeartbeatRequestData data = new StreamsGroupHeartbeatRequestData();
            data.setGroupId(membershipManager.groupId());
            data.setMemberId(membershipManager.memberId());
            data.setMemberEpoch(membershipManager.memberEpoch());
            membershipManager.groupInstanceId().ifPresent(data::setInstanceId);
            StreamsGroupHeartbeatRequestData.Topology topology = new StreamsGroupHeartbeatRequestData.Topology();
            topology.setSubtopologies(getTopologyFromStreams(streamsRebalanceData.subtopologies()));
            topology.setEpoch(streamsRebalanceData.topologyEpoch());
            data.setRebalanceTimeoutMs(rebalanceTimeoutMs);
            data.setTopology(topology);
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
            data.setShutdownApplication(streamsRebalanceData.shutdownRequested());
            StreamsRebalanceData.Assignment reconciledAssignment = streamsRebalanceData.reconciledAssignment();
            data.setActiveTasks(convertTaskIdCollection(reconciledAssignment.activeTasks()));
            data.setStandbyTasks(convertTaskIdCollection(reconciledAssignment.standbyTasks()));
            data.setWarmupTasks(convertTaskIdCollection(reconciledAssignment.warmupTasks()));
            return data;
        }

        private static List<StreamsGroupHeartbeatRequestData.TaskIds> convertTaskIdCollection(final Set<StreamsRebalanceData.TaskId> tasks) {
            return tasks.stream()
                .collect(
                    Collectors.groupingBy(StreamsRebalanceData.TaskId::subtopologyId,
                        Collectors.mapping(StreamsRebalanceData.TaskId::partitionId, Collectors.toList()))
                )
                .entrySet()
                .stream()
                .map(entry -> {
                    StreamsGroupHeartbeatRequestData.TaskIds ids = new StreamsGroupHeartbeatRequestData.TaskIds();
                    ids.setSubtopologyId(entry.getKey());
                    ids.setPartitions(entry.getValue());
                    return ids;
                })
                .collect(Collectors.toList());
        }

        private static List<StreamsGroupHeartbeatRequestData.Subtopology> getTopologyFromStreams(final Map<String, StreamsRebalanceData.Subtopology> subtopologies) {
            final List<StreamsGroupHeartbeatRequestData.Subtopology> subtopologiesForRequest = new ArrayList<>(subtopologies.size());
            for (final Map.Entry<String, StreamsRebalanceData.Subtopology> subtopology : subtopologies.entrySet()) {
                subtopologiesForRequest.add(getSubtopologyFromStreams(subtopology.getKey(), subtopology.getValue()));
            }
            subtopologiesForRequest.sort(Comparator.comparing(StreamsGroupHeartbeatRequestData.Subtopology::subtopologyId));
            return subtopologiesForRequest;
        }

        private static StreamsGroupHeartbeatRequestData.Subtopology getSubtopologyFromStreams(final String subtopologyName,
                                                                                              final StreamsRebalanceData.Subtopology subtopology) {
            final StreamsGroupHeartbeatRequestData.Subtopology subtopologyData = new StreamsGroupHeartbeatRequestData.Subtopology();
            subtopologyData.setSubtopologyId(subtopologyName);
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
                changelogTopicsInfo.add(changelogTopicInfo);
            }
            changelogTopicsInfo.sort(Comparator.comparing(StreamsGroupHeartbeatRequestData.TopicInfo::name));
            return changelogTopicsInfo;
        }
    }


    /**
     * Represents the state of a heartbeat request, including logic for timing, retries, and exponential backoff. The object extends
     * {@link RequestState} to enable exponential backoff and duplicated request handling. The two fields that it holds are:
     */
    static class HeartbeatRequestState extends RequestState {

        /**
         * The heartbeat timer tracks the time since the last heartbeat was sent
         */
        private final Timer heartbeatTimer;

        /**
         * The heartbeat interval which is acquired/updated through the heartbeat request
         */
        private long heartbeatIntervalMs;

        public HeartbeatRequestState(final LogContext logContext,
                                     final Time time,
                                     final long heartbeatIntervalMs,
                                     final long retryBackoffMs,
                                     final long retryBackoffMaxMs,
                                     final double jitter) {
            super(
                logContext,
                StreamsGroupHeartbeatRequestManager.HeartbeatRequestState.class.getName(),
                retryBackoffMs,
                2,
                retryBackoffMaxMs,
                jitter
            );
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

        private void updateHeartbeatIntervalMs(final long heartbeatIntervalMs) {
            if (this.heartbeatIntervalMs == heartbeatIntervalMs) {
                // no need to update the timer if the interval hasn't changed
                return;
            }
            this.heartbeatIntervalMs = heartbeatIntervalMs;
            this.heartbeatTimer.updateAndReset(heartbeatIntervalMs);
        }
    }

    private final Logger logger;

    private final int maxPollIntervalMs;

    private final CoordinatorRequestManager coordinatorRequestManager;

    private final HeartbeatRequestState heartbeatRequestState;

    private final HeartbeatState heartbeatState;

    private final StreamsMembershipManager membershipManager;

    private final HeartbeatMetricsManager metricsManager;

    private StreamsRebalanceData streamsRebalanceData;

    public StreamsGroupHeartbeatRequestManager(final LogContext logContext,
                                               final Time time,
                                               final ConsumerConfig config,
                                               final CoordinatorRequestManager coordinatorRequestManager,
                                               final StreamsMembershipManager membershipManager,
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
            maxPollIntervalMs
        );
    }

    @Override
    public NetworkClientDelegate.PollResult poll(long currentTimeMs) {
        return new NetworkClientDelegate.PollResult(
            heartbeatRequestState.heartbeatIntervalMs,
            Collections.singletonList(makeHeartbeatRequest(currentTimeMs))
        );
    }

    private NetworkClientDelegate.UnsentRequest makeHeartbeatRequest(final long currentTimeMs) {
        NetworkClientDelegate.UnsentRequest request = new NetworkClientDelegate.UnsentRequest(
            new StreamsGroupHeartbeatRequest.Builder(this.heartbeatState.buildRequestData()),
            coordinatorRequestManager.coordinator()
        );
        request.whenComplete((response, exception) -> {
            long completionTimeMs = request.handler().completionTimeMs();
            if (response != null) {
                metricsManager.recordRequestLatency(response.requestLatencyMs());
                onResponse((StreamsGroupHeartbeatResponse) response.responseBody(), completionTimeMs);
            }
        });
        heartbeatRequestState.onSendAttempt(currentTimeMs);
        membershipManager.onHeartbeatRequestGenerated();
        metricsManager.recordHeartbeatSentMs(currentTimeMs);
        return request;
    }

    private void onResponse(final StreamsGroupHeartbeatResponse response, long currentTimeMs) {
        if (Errors.forCode(response.data().errorCode()) == Errors.NONE) {
            onSuccessResponse(response, currentTimeMs);
        }
    }

    private void onSuccessResponse(final StreamsGroupHeartbeatResponse response, final long currentTimeMs) {
        final StreamsGroupHeartbeatResponseData data = response.data();

        heartbeatRequestState.updateHeartbeatIntervalMs(data.heartbeatIntervalMs());
        heartbeatRequestState.onSuccessfulAttempt(currentTimeMs);
        heartbeatRequestState.resetTimer();

        if (data.partitionsByUserEndpoint() != null) {
            streamsRebalanceData.setPartitionsByHost(convertHostInfoMap(data));
        }

        List<StreamsGroupHeartbeatResponseData.Status> statuses = data.status();

        if (statuses != null && !statuses.isEmpty()) {
            String statusDetails = statuses.stream()
                .map(status -> "(" + status.statusCode() + ") " + status.statusDetail())
                .collect(Collectors.joining(", "));
            logger.warn("Membership is in the following statuses: {}.", statusDetails);
        }

        membershipManager.onHeartbeatSuccess(response);
    }

    private static Map<StreamsRebalanceData.HostInfo, List<TopicPartition>> convertHostInfoMap(final StreamsGroupHeartbeatResponseData data) {
        Map<StreamsRebalanceData.HostInfo, List<TopicPartition>> partitionsByHost = new HashMap<>();
        data.partitionsByUserEndpoint().forEach(endpoint -> {
            List<TopicPartition> topicPartitions = endpoint.partitions().stream()
                .flatMap(partition ->
                    partition.partitions().stream().map(partitionId -> new TopicPartition(partition.topic(), partitionId)))
                .collect(Collectors.toList());
            StreamsGroupHeartbeatResponseData.Endpoint userEndpoint = endpoint.userEndpoint();
            partitionsByHost.put(new StreamsRebalanceData.HostInfo(userEndpoint.host(), userEndpoint.port()), topicPartitions);
        });
        return partitionsByHost;
    }
}
