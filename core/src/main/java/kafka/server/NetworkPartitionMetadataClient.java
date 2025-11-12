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
package kafka.server;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsTopic;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.PartitionMetadataClient;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.util.InterBrokerSendThread;
import org.apache.kafka.server.util.RequestAndCompletionHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

public class NetworkPartitionMetadataClient implements PartitionMetadataClient {

    private static final Logger log = LoggerFactory.getLogger(NetworkPartitionMetadataClient.class);

    private final MetadataCache metadataCache;
    private final SendThread sendThread;
    private final Time time;
    private final ListenerName listenerName;

    public NetworkPartitionMetadataClient(
        MetadataCache metadataCache,
        KafkaClient networkClient,
        Time time,
        ListenerName listenerName
    ) {
        this.metadataCache = metadataCache;
        this.time = time;
        this.listenerName = listenerName;
        this.sendThread = new SendThread(
            "NetworkPartitionMetadataClientSendThread",
            networkClient,
            Math.toIntExact(CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS),  //30 seconds
            this.time
        );
        this.sendThread.start();
    }

    @Override
    public Map<TopicPartition, CompletableFuture<OffsetResponse>> listLatestOffsets(
        Set<TopicPartition> topicPartitions
    ) {
        if (topicPartitions == null || topicPartitions.isEmpty()) {
            return Map.of();
        }

        // Map to store futures for each TopicPartition
        Map<TopicPartition, CompletableFuture<OffsetResponse>> futures = new HashMap<>();

        // Group TopicPartitions by leader node
        Map<Node, List<TopicPartition>> partitionsByNode = new HashMap<>();

        for (TopicPartition tp : topicPartitions) {
            CompletableFuture<OffsetResponse> future = new CompletableFuture<>();
            futures.put(tp, future);

            // Get leader node for this partition
            Optional<Node> leaderNodeOpt = metadataCache.getPartitionLeaderEndpoint(
                tp.topic(),
                tp.partition(),
                listenerName
            );

            if (leaderNodeOpt.isEmpty() || leaderNodeOpt.get().isEmpty()) {
                // No leader available - complete with error

                future.complete(new OffsetResponse(-1, Errors.LEADER_NOT_AVAILABLE));
                continue;
            }

            Node leaderNode = leaderNodeOpt.get();
            partitionsByNode.computeIfAbsent(leaderNode, k -> new ArrayList<>()).add(tp);
        }

        // Create and enqueue requests for each node
        for (Map.Entry<Node, List<TopicPartition>> entry : partitionsByNode.entrySet()) {
            Node node = entry.getKey();
            List<TopicPartition> partitions = entry.getValue();

            // Create a map of futures only for partitions in this request
            Map<TopicPartition, CompletableFuture<OffsetResponse>> partitionFutures = new HashMap<>();
            for (TopicPartition tp : partitions) {
                partitionFutures.put(tp, futures.get(tp));
            }

            // Create ListOffsetsRequest for this node
            ListOffsetsRequest.Builder requestBuilder = createListOffsetsRequest(partitions);

            // Create pending request to track this request
            PendingRequest pendingRequest = new PendingRequest(node, partitions, partitionFutures, requestBuilder);

            // Enqueue to send thread
            sendThread.enqueue(pendingRequest);
        }

        return futures;
    }

    /**
     * Creates a ListOffsetsRequest Builder for the given partitions requesting latest offsets.
     */
    private ListOffsetsRequest.Builder createListOffsetsRequest(List<TopicPartition> partitions) {
        // Group partitions by topic name
        Map<String, List<TopicPartition>> partitionsByTopic = partitions.stream()
            .collect(Collectors.groupingBy(TopicPartition::topic));

        List<ListOffsetsTopic> topics = new ArrayList<>();
        for (Map.Entry<String, List<TopicPartition>> entry : partitionsByTopic.entrySet()) {
            String topicName = entry.getKey();
            ListOffsetsTopic topic = new ListOffsetsTopic().setName(topicName);

            for (TopicPartition tp : entry.getValue()) {
                topic.partitions().add(
                    new ListOffsetsPartition()
                        .setPartitionIndex(tp.partition())
                        .setTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP)
                        .setCurrentLeaderEpoch(-1) // Will be set by broker if needed
                );
            }
            topics.add(topic);
        }

        // Isolation level will always be READ_UNCOMMITTED when finding the partition end offset.
        return ListOffsetsRequest.Builder.forConsumer(
            true,
            IsolationLevel.READ_UNCOMMITTED
        ).setTargetTimes(topics);
    }

    @Override
    public void close() {
        try {
            sendThread.shutdown();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while shutting down NetworkPartitionMetadataClient", e);
        }
    }

    /**
     * Tracks a pending ListOffsets request and its associated futures.
     */
    private record PendingRequest(
        Node node,
        List<TopicPartition> partitions,
        Map<TopicPartition, CompletableFuture<OffsetResponse>> futures,
        ListOffsetsRequest.Builder requestBuilder
    ) { }

    private class SendThread extends InterBrokerSendThread {
        private final ConcurrentLinkedQueue<PendingRequest> pendingRequests = new ConcurrentLinkedQueue<>();

        protected SendThread(String name, KafkaClient networkClient, int requestTimeoutMs, Time time) {
            super(name, networkClient, requestTimeoutMs, time);
        }

        /**
         * Enqueues a pending request to be sent.
         */
        public void enqueue(PendingRequest pendingRequest) {
            pendingRequests.add(pendingRequest);
            wakeup();
        }

        @Override
        public Collection<RequestAndCompletionHandler> generateRequests() {
            List<RequestAndCompletionHandler> requests = new ArrayList<>();
            
            // Process all pending requests
            PendingRequest pending;
            while ((pending = pendingRequests.poll()) != null) {
                final PendingRequest current = pending;
                ListOffsetsRequest.Builder requestBuilder = current.requestBuilder;

                // Create completion handler
                RequestAndCompletionHandler requestHandler = new RequestAndCompletionHandler(
                    time.milliseconds(),
                    current.node,
                    requestBuilder,
                    response -> handleResponse(current, response)
                );

                requests.add(requestHandler);
            }

            return requests;
        }

        /**
         * Handles the response from a ListOffsets request.
         */
        private void handleResponse(PendingRequest pendingRequest, ClientResponse clientResponse) {
            if (clientResponse == null || !clientResponse.hasResponse()) {
                handleErrorResponse(pendingRequest, clientResponse);
                return;
            }

            log.debug("ListOffsets response received - {}", clientResponse);

            // Parse the response
            ListOffsetsResponse response = (ListOffsetsResponse) clientResponse.responseBody();
            
            for (ListOffsetsTopicResponse topicResponse : response.topics()) {
                String topicName = topicResponse.name();
                for (ListOffsetsPartitionResponse partitionResponse : topicResponse.partitions()) {
                    TopicPartition tp = new TopicPartition(topicName, partitionResponse.partitionIndex());
                    // Remove the corresponding future from the map and complete it.
                    CompletableFuture<OffsetResponse> future = pendingRequest.futures.remove(tp);
                    if (future != null) {
                        future.complete(new OffsetResponse(partitionResponse.offset(), Errors.forCode(partitionResponse.errorCode())));
                    }
                }
            }

            pendingRequest.futures.forEach((tp, future) -> {
                // If any partition was not included in the response, complete with error
                future.complete(new OffsetResponse(-1, Errors.UNKNOWN_TOPIC_OR_PARTITION));
            });
        }

        /**
         * Handles error scenarios for ListOffsets responses.
         */
        private void handleErrorResponse(PendingRequest pendingRequest, ClientResponse clientResponse) {
            Errors error = Errors.UNKNOWN_SERVER_ERROR;
            if (clientResponse == null) {
                log.debug("Response for ListOffsets for topicPartitions: {} is null", pendingRequest.partitions);
            } else if (!clientResponse.hasResponse()) {
                log.debug("Response for ListOffsets for topicPartitions: {} is invalid - {}", pendingRequest.partitions, clientResponse);
                if (clientResponse.wasDisconnected()) {
                    log.error("ListOffsets for TopicPartitions: {} was disconnected - {}.", pendingRequest.partitions, clientResponse);
                    error = Errors.NETWORK_EXCEPTION;
                } else if (clientResponse.wasTimedOut()) {
                    log.error("Response for ListOffsets for TopicPartitions: {} timed out - {}.", pendingRequest.partitions, clientResponse);
                    error = Errors.REQUEST_TIMED_OUT;
                }
            }
            for (TopicPartition tp : pendingRequest.partitions) {
                CompletableFuture<OffsetResponse> future = pendingRequest.futures.get(tp);
                if (future != null) {
                    future.complete(new OffsetResponse(-1, error));
                }
            }
        }
    }
}
