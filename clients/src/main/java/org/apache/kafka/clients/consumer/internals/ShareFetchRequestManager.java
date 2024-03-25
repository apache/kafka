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
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.IdempotentCloser;
import org.apache.kafka.common.message.ShareFetchRequestData;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ShareFetchRequest;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult.EMPTY;

/**
 * {@code ShareFetchRequestManager} is responsible for generating {@link ShareFetchRequest} that
 * represent the {@link SubscriptionState#fetchablePartitions(Predicate)} based on the share group
 * consumer's assignment.
 */
public class ShareFetchRequestManager implements RequestManager {

    private final Logger log;
    private final LogContext logContext;
    private final String groupId;
    private final ConsumerMetadata metadata;
    private final SubscriptionState subscriptions;
    private final FetchConfig fetchConfig;
    protected final ShareFetchBuffer shareFetchBuffer;
    private final Map<Integer, ShareSessionHandler> sessionHandlers;
    private final Set<Integer> nodesWithPendingRequests;
    private final FetchMetricsManager metricsManager;
    private final IdempotentCloser idempotentCloser = new IdempotentCloser();

    // Temporary - to be fixed in AKCORE-51
    private final Uuid shareSessionUuid;

    ShareFetchRequestManager(final LogContext logContext,
                             final String groupId,
                             final ConsumerMetadata metadata,
                             final SubscriptionState subscriptions,
                             final FetchConfig fetchConfig,
                             final ShareFetchBuffer shareFetchBuffer,
                             final FetchMetricsManager metricsManager) {
        this.log = logContext.logger(AbstractFetch.class);
        this.logContext = logContext;
        this.groupId = groupId;
        this.metadata = metadata;
        this.subscriptions = subscriptions;
        this.fetchConfig = fetchConfig;
        this.shareFetchBuffer = shareFetchBuffer;
        this.metricsManager = metricsManager;
        this.sessionHandlers = new HashMap<>();
        this.nodesWithPendingRequests = new HashSet<>();
        this.shareSessionUuid = Uuid.randomUuid();
    }

    @Override
    public NetworkClientDelegate.PollResult poll(long currentTimeMs) {
        List<NetworkClientDelegate.UnsentRequest> requests;
        Map<Node, ShareSessionHandler.ShareFetchRequestData> shareFetchRequests = prepareShareFetchRequests();

        requests = shareFetchRequests.entrySet().stream().map(entry -> {
            final Node target = entry.getKey();
            final ShareSessionHandler.ShareFetchRequestData data = entry.getValue();
            final ShareFetchRequest.Builder request = createShareFetchRequest(target, data);
            final BiConsumer<ClientResponse, Throwable> responseHandler = (clientResponse, error) -> {
                if (error != null) {
                    handleShareFetchFailure(target, data, error);
                } else {
                    handleShareFetchSuccess(target, data, clientResponse);
                }
            };
            return new NetworkClientDelegate.UnsentRequest(request, Optional.of(target)).whenComplete(responseHandler);
        }).collect(Collectors.toList());

        return new NetworkClientDelegate.PollResult(requests);
    }

    private Map<Node, ShareSessionHandler.ShareFetchRequestData> prepareShareFetchRequests() {
        Map<Node, ShareSessionHandler.Builder> partitionsToFetch = new HashMap<>();
        Map<String, Uuid> topicIds = metadata.topicIds();

        for (TopicPartition partition : partitionsToFetch()) {
            Optional<Node> leaderOpt = metadata.currentLeader(partition).leader;

            if (!leaderOpt.isPresent()) {
                log.debug("Requesting metadata update for partition {} since current leader node is missing", partition);
                metadata.requestUpdate(false);
                continue;
            }

            Node node = leaderOpt.get();
            if (nodesWithPendingRequests.contains(node.id())) {
                log.trace("Skipping fetch for partition {} because previous fetch request to {} has not been processed", partition, node);
            } else {
                // if there is a leader and no in-flight requests, issue a new fetch
                ShareSessionHandler.Builder builder = partitionsToFetch.computeIfAbsent(node, k -> {
                    ShareSessionHandler shareSessionHandler =
                            sessionHandlers.computeIfAbsent(node.id(), n -> new ShareSessionHandler(logContext, n, shareSessionUuid));
                    return shareSessionHandler.newBuilder();
                });

                Uuid topicId = topicIds.getOrDefault(partition.topic(), Uuid.ZERO_UUID);
                TopicIdPartition tip = new TopicIdPartition(topicId, partition);
                builder.add(tip, shareFetchBuffer.getAcknowledgementsToSend(tip));

                log.debug("Added fetch request for partition {} to node {}", partition, node);
            }
        }

        Map<Node, ShareSessionHandler.ShareFetchRequestData> requests = new LinkedHashMap<>();
        for (Map.Entry<Node, ShareSessionHandler.Builder> entry : partitionsToFetch.entrySet()) {
            requests.put(entry.getKey(), entry.getValue().build());
        }

        return requests;
    }

    private ShareFetchRequest.Builder createShareFetchRequest(Node fetchTarget, ShareSessionHandler.ShareFetchRequestData requestData) {
        final ShareFetchRequest.Builder request = ShareFetchRequest.Builder
                .forConsumer(fetchConfig.maxWaitMs, fetchConfig.minBytes, fetchConfig.maxBytes, fetchConfig.fetchSize,
                        requestData.toSend(), acknowledgementBatches(requestData.acknowledgements()))
                .forShareSession(groupId, requestData.metadata());

        nodesWithPendingRequests.add(fetchTarget.id());

        return request;
    }

    private void handleShareFetchSuccess(Node fetchTarget, ShareSessionHandler.ShareFetchRequestData data, ClientResponse resp) {
        try {
            final ShareFetchResponse response = (ShareFetchResponse) resp.responseBody();
            final ShareSessionHandler handler = sessionHandler(fetchTarget.id());

            if (handler == null) {
                log.error("Unable to find ShareSessionHandler for node {}. Ignoring share fetch response.",
                        fetchTarget.id());
                return;
            }

            final short requestVersion = resp.requestHeader().apiVersion();

            if (!handler.handleResponse(response, requestVersion)) {
                if (response.error() == Errors.UNKNOWN_TOPIC_ID) {
                    metadata.requestUpdate(false);
                }

                return;
            }

            final Map<TopicIdPartition, ShareFetchResponseData.PartitionData> responseData = new LinkedHashMap<>();
            Map<Uuid, String> topicNames = handler.sessionTopicNames();

            response.data().responses().forEach(topicResponse -> {
                String name = topicNames.get(topicResponse.topicId());
                if (name != null) {
                    topicResponse.partitions().forEach(partition ->
                            responseData.put(new TopicIdPartition(topicResponse.topicId(), partition.partitionIndex(), name), partition));
                }
            });

            for (Map.Entry<TopicIdPartition, ShareFetchResponseData.PartitionData> entry : responseData.entrySet()) {
                TopicIdPartition partition = entry.getKey();

                ShareFetchResponseData.PartitionData partitionData = entry.getValue();

                log.debug("Share fetch for partition {} returned fetch data {}", partition, partitionData);

                ShareCompletedFetch completedFetch = new ShareCompletedFetch(
                        logContext,
                        BufferSupplier.create(),
                        partition,
                        partitionData,
                        requestVersion);
                shareFetchBuffer.add(completedFetch);
                shareFetchBuffer.handleAcknowledgementResponses(partition, Errors.forCode(partitionData.acknowledgeErrorCode()));
            }

            metricsManager.recordLatency(resp.requestLatencyMs());
        } finally {
            log.debug("Removing pending request for node {} - success", fetchTarget);
            nodesWithPendingRequests.remove(fetchTarget.id());
        }
    }

    private void handleShareFetchFailure(Node fetchTarget,
                                         ShareSessionHandler.ShareFetchRequestData data,
                                         Throwable error) {
        try {
            final ShareSessionHandler handler = sessionHandler(fetchTarget.id());

            if (handler != null) {
                handler.handleError(error);
            }
        } finally {
            log.debug("Removing pending request for node {} - failed", fetchTarget);
            nodesWithPendingRequests.remove(fetchTarget.id());
        }
    }

    private Map<TopicIdPartition, List<ShareFetchRequestData.AcknowledgementBatch>> acknowledgementBatches(Map<TopicIdPartition, Acknowledgements> acknowledgementsMap) {
        Map<TopicIdPartition, List<ShareFetchRequestData.AcknowledgementBatch>> acknowledgementBatches = new HashMap<>();
        acknowledgementsMap.forEach((partition, acknowledgements) -> {
            acknowledgementBatches.put(partition, acknowledgements.getAcknowledgmentBatches());
        });
        return acknowledgementBatches;
    }

    private List<TopicPartition> partitionsToFetch() {
        return subscriptions.fetchablePartitions(tp -> true);
    }

    public ShareSessionHandler sessionHandler(int node) {
        return sessionHandlers.get(node);
    }

    @Override
    public NetworkClientDelegate.PollResult pollOnClose() {
        // This is where the final ShareFetch or ShareAcknowledge goes
        return EMPTY;
    }

    boolean hasCompletedFetches() {
        return !shareFetchBuffer.isEmpty();
    }

    protected void closeInternal() {
        Utils.closeQuietly(shareFetchBuffer, "shareFetchBuffer");
    }

    public void close() {
        idempotentCloser.close(this::closeInternal);
    }
}