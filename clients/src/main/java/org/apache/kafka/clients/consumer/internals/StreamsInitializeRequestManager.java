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
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult;
import org.apache.kafka.common.message.StreamsGroupInitializeRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.StreamsGroupInitializeRequest;
import org.apache.kafka.common.requests.StreamsGroupInitializeResponse;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class StreamsInitializeRequestManager implements RequestManager {

    private final Logger logger;
    private final String groupId;
    private final StreamsAssignmentInterface streamsAssignmentInterface;
    private final CoordinatorRequestManager coordinatorRequestManager;

    private Optional<NetworkClientDelegate.UnsentRequest> unsentRequest = Optional.empty();


    StreamsInitializeRequestManager(final LogContext logContext,
                                    final String groupId,
                                    final StreamsAssignmentInterface streamsAssignmentInterface,
                                    final CoordinatorRequestManager coordinatorRequestManager) {
        this.logger = logContext.logger(getClass());
        this.groupId = groupId;
        this.streamsAssignmentInterface = streamsAssignmentInterface;
        this.coordinatorRequestManager = coordinatorRequestManager;
    }

    @Override
    public PollResult poll(final long currentTimeMs) {
        final PollResult pollResult = unsentRequest.map(PollResult::new).orElse(PollResult.EMPTY);
        unsentRequest = Optional.empty();
        return pollResult;
    }

    public void initialize() {
        final NetworkClientDelegate.UnsentRequest unsentRequest = makeRequest();

        unsentRequest.whenComplete(this::onResponse);

        this.unsentRequest = Optional.of(unsentRequest);
    }

    private NetworkClientDelegate.UnsentRequest makeRequest() {
        final StreamsGroupInitializeRequestData streamsGroupInitializeRequestData = new StreamsGroupInitializeRequestData();
        streamsGroupInitializeRequestData.setGroupId(groupId);
        final List<StreamsGroupInitializeRequestData.Subtopology> topology = getTopologyFromStreams();
        streamsGroupInitializeRequestData.setTopology(topology);
        final StreamsGroupInitializeRequest.Builder streamsInitializeRequestBuilder = new StreamsGroupInitializeRequest.Builder(
            streamsGroupInitializeRequestData
        );
        return new NetworkClientDelegate.UnsentRequest(
            streamsInitializeRequestBuilder,
            coordinatorRequestManager.coordinator()
        );
    }

    private List<StreamsGroupInitializeRequestData.Subtopology> getTopologyFromStreams() {
        final Map<String, StreamsAssignmentInterface.Subtopology> subTopologyMap = streamsAssignmentInterface.subtopologyMap();
        final List<StreamsGroupInitializeRequestData.Subtopology> topology = new ArrayList<>(subTopologyMap.size());
        for (final Map.Entry<String, StreamsAssignmentInterface.Subtopology> subtopology : subTopologyMap.entrySet()) {
            topology.add(getSubtopologyFromStreams(subtopology.getKey(), subtopology.getValue()));
        }
        return topology;
    }

    private static StreamsGroupInitializeRequestData.Subtopology getSubtopologyFromStreams(final String subtopologyName,
                                                                                           final StreamsAssignmentInterface.Subtopology subtopology) {
        final StreamsGroupInitializeRequestData.Subtopology subtopologyData = new StreamsGroupInitializeRequestData.Subtopology();
        subtopologyData.setSubtopology(subtopologyName);
        subtopologyData.setSourceTopics(new ArrayList<>(subtopology.sourceTopics));
        subtopologyData.setRepartitionSinkTopics(new ArrayList<>(subtopology.sinkTopics));
        subtopologyData.setRepartitionSourceTopics(getRepartitionTopicsInfoFromStreams(subtopology));
        subtopologyData.setStateChangelogTopics(getChangelogTopicsInfoFromStreams(subtopology));
        return subtopologyData;
    }

    private static List<StreamsGroupInitializeRequestData.TopicInfo> getRepartitionTopicsInfoFromStreams(final StreamsAssignmentInterface.Subtopology subtopologyDataFromStreams) {
        final List<StreamsGroupInitializeRequestData.TopicInfo> repartitionTopicsInfo = new ArrayList<>();
        for (final Map.Entry<String, StreamsAssignmentInterface.TopicInfo> repartitionTopic : subtopologyDataFromStreams.repartitionSourceTopics.entrySet()) {
            final StreamsGroupInitializeRequestData.TopicInfo repartitionTopicInfo = new StreamsGroupInitializeRequestData.TopicInfo();
            repartitionTopicInfo.setName(repartitionTopic.getKey());
            repartitionTopic.getValue().numPartitions.ifPresent(repartitionTopicInfo::setPartitions);
            repartitionTopicsInfo.add(repartitionTopicInfo);
        }
        return repartitionTopicsInfo;
    }

    private static List<StreamsGroupInitializeRequestData.TopicInfo> getChangelogTopicsInfoFromStreams(final StreamsAssignmentInterface.Subtopology subtopologyDataFromStreams) {
        final List<StreamsGroupInitializeRequestData.TopicInfo> changelogTopicsInfo = new ArrayList<>();
        for (final Map.Entry<String, StreamsAssignmentInterface.TopicInfo> changelogTopic : subtopologyDataFromStreams.stateChangelogTopics.entrySet()) {
            final StreamsGroupInitializeRequestData.TopicInfo changelogTopicInfo = new StreamsGroupInitializeRequestData.TopicInfo();
            changelogTopicInfo.setName(changelogTopic.getKey());
            changelogTopicsInfo.add(changelogTopicInfo);
        }
        return changelogTopicsInfo;
    }
    
    private void onResponse(final ClientResponse response, final Throwable exception) {
        if (exception != null) {
            // todo: handle error
            logger.error("Error during Streams initialization: ", exception);
        } else {
            onResponse((StreamsGroupInitializeResponse) response.responseBody());
        }
    }

    private void onResponse(final StreamsGroupInitializeResponse response) {
        if (Errors.forCode(response.data().errorCode()) == Errors.NONE) {
            onSuccessResponse(response);
        } else {
            onErrorResponse(response);
        }
    }

    private void onErrorResponse(final StreamsGroupInitializeResponse response) {
        // todo: handle error
        logger.error("Error during Streams initialization: {}", response);
    }

    private void onSuccessResponse(final StreamsGroupInitializeResponse response) {
        // todo: handle success
        logger.info("Streams initialization successful {}", response);
    }
}
