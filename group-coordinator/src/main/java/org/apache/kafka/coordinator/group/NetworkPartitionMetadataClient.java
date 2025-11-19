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
package org.apache.kafka.coordinator.group;

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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public class NetworkPartitionMetadataClient implements PartitionMetadataClient {

    private static final Logger log = LoggerFactory.getLogger(NetworkPartitionMetadataClient.class);

    private final MetadataCache metadataCache;
    private final Supplier<KafkaClient> networkClientSupplier;
    private final Time time;
    private final ListenerName listenerName;
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private volatile SendThread sendThread;

    public NetworkPartitionMetadataClient(MetadataCache metadataCache,
                                          Supplier<KafkaClient> networkClientSupplier,
                                          Time time, ListenerName listenerName) {
        this.metadataCache = metadataCache;
        this.networkClientSupplier = networkClientSupplier;
        this.time = time;
        this.listenerName = listenerName;
    }

    @Override
    public Map<TopicPartition, CompletableFuture<OffsetResponse>> listLatestOffsets(Set<TopicPartition> topicPartitions) {
        if (topicPartitions == null || topicPartitions.isEmpty()) {
            return Map.of();
        }

        // Initialize sendThread lazily on first call
        ensureSendThreadInitialized();

        // Map to store futures for each TopicPartition
        Map<TopicPartition, CompletableFuture<OffsetResponse>> futures = new HashMap<>();
        // Group TopicPartitions by leader node
        Map<Node, List<TopicPartition>> partitionsByNode = new HashMap<>();
        for (TopicPartition tp : topicPartitions) {
            // Get leader node for this partition
            Optional<Node> leaderNodeOpt = metadataCache.getPartitionLeaderEndpoint(
                tp.topic(),
                tp.partition(),
                listenerName
            );

            if (leaderNodeOpt.isEmpty() || leaderNodeOpt.get().isEmpty()) {
                // No leader available - complete with error
                futures.put(tp, CompletableFuture.completedFuture(new OffsetResponse(-1, Errors.LEADER_NOT_AVAILABLE)));
                continue;
            }

            partitionsByNode.computeIfAbsent(leaderNodeOpt.get(), k -> new ArrayList<>()).add(tp);
        }

        // Create and enqueue requests for each node
        partitionsByNode.forEach((node, partitionsByLeader) -> {
            // All partitions with the same leader node will be included in the same ListOffsetsRequest.
            Map<TopicPartition, CompletableFuture<OffsetResponse>> partitionFuturesByLeader = new HashMap<>();
            for (TopicPartition tp : partitionsByLeader) {
                CompletableFuture<OffsetResponse> future = new CompletableFuture<>();
                futures.put(tp, future);
                partitionFuturesByLeader.put(tp, future);
            }

            // Create ListOffsetsRequest for this node
            ListOffsetsRequest.Builder requestBuilder = createListOffsetsRequest(partitionsByLeader);
            // Create pending request to track this request
            PendingRequest pendingRequest = new PendingRequest(node, partitionFuturesByLeader, requestBuilder);
            // Enqueue to send thread
            sendThread.enqueue(pendingRequest);
        });

        return futures;
    }

    @Override
    public void close() {
        // Only close sendThread if it was initialized. Note, close is called only during broker shutdown, so need
        // for further synchronization here.
        if (!initialized.get()) {
            return;
        }
        if (sendThread != null) {
            try {
                sendThread.shutdown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Interrupted while shutting down NetworkPartitionMetadataClient", e);
            }
        }
    }

    /**
     * Ensures that the sendThread is initialized. This method is thread-safe and will only
     * initialize the sendThread once, even if called concurrently.
     */
    // Visible for testing.
    void ensureSendThreadInitialized() {
        if (initialized.compareAndSet(false, true)) {
            KafkaClient networkClient = networkClientSupplier.get();
            sendThread = new SendThread(
                "NetworkPartitionMetadataClientSendThread",
                networkClient,
                Math.toIntExact(CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS),  //30 seconds
                this.time
            );
            sendThread.start();
            log.info("NetworkPartitionMetadataClient sendThread initialized and started");
        }
    }

    /**
     * Creates a ListOffsetsRequest Builder for the given partitions requesting latest offsets.
     */
    private ListOffsetsRequest.Builder createListOffsetsRequest(List<TopicPartition> partitions) {
        Map<String, ListOffsetsTopic> topicsMap = new HashMap<>();
        partitions.forEach(tp -> {
            if (!topicsMap.containsKey(tp.topic())) {
                ListOffsetsTopic topic = new ListOffsetsTopic().setName(tp.topic());
                topicsMap.put(tp.topic(), topic);
            }
            ListOffsetsTopic topic = topicsMap.get(tp.topic());
            topic.partitions().add(
                new ListOffsetsPartition()
                    .setPartitionIndex(tp.partition())
                    .setTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP)
                    .setCurrentLeaderEpoch(-1) // Will be set by broker if needed
            );
        });
        // Isolation level will always be READ_UNCOMMITTED when finding the partition end offset.
        return ListOffsetsRequest.Builder.forConsumer(
            true,
            IsolationLevel.READ_UNCOMMITTED
        ).setTargetTimes(List.copyOf(topicsMap.values()));
    }

    /**
     * Handles the response from a ListOffsets request.
     */
    // Visible for Testing.
    void handleResponse(Map<TopicPartition, CompletableFuture<OffsetResponse>> partitionFutures, ClientResponse clientResponse) {
        // Handle error responses first
        if (maybeHandleErrorResponse(partitionFutures, clientResponse)) {
            return;
        }

        log.debug("ListOffsets response received successfully - {}", clientResponse);
        ListOffsetsResponse response = (ListOffsetsResponse) clientResponse.responseBody();

        for (ListOffsetsTopicResponse topicResponse : response.topics()) {
            String topicName = topicResponse.name();
            for (ListOffsetsPartitionResponse partitionResponse : topicResponse.partitions()) {
                TopicPartition tp = new TopicPartition(topicName, partitionResponse.partitionIndex());
                // Get the corresponding future from the map and complete it.
                CompletableFuture<OffsetResponse> future = partitionFutures.get(tp);
                if (future != null) {
                    future.complete(new OffsetResponse(partitionResponse.offset(), Errors.forCode(partitionResponse.errorCode())));
                }
            }
        }

        partitionFutures.forEach((tp, future) -> {
            // If future is not completed yet hence topic-partition was not included in the response, complete with error
            if (!future.isDone()) {
                future.complete(new OffsetResponse(-1, Errors.UNKNOWN_TOPIC_OR_PARTITION));
            }
        });
    }

    /**
     * Handles error responses by completing all associated futures with an error. Returns true if an error was
     * handled. Otherwise, returns false.
     */
    private boolean maybeHandleErrorResponse(Map<TopicPartition, CompletableFuture<OffsetResponse>> partitionFutures, ClientResponse clientResponse) {
        Errors error;
        if (clientResponse == null) {
            log.error("Response for ListOffsets for topicPartitions: {} is null", partitionFutures.keySet());
            error = Errors.UNKNOWN_SERVER_ERROR;
        } else if (clientResponse.authenticationException() != null) {
            log.error("Authentication exception", clientResponse.authenticationException());
            error = Errors.UNKNOWN_SERVER_ERROR;
        } else if (clientResponse.versionMismatch() != null) {
            log.error("Version mismatch exception", clientResponse.versionMismatch());
            error = Errors.UNKNOWN_SERVER_ERROR;
        } else if (clientResponse.wasDisconnected()) {
            log.error("Response for ListOffsets for TopicPartitions: {} was disconnected - {}.", partitionFutures.keySet(), clientResponse);
            error = Errors.NETWORK_EXCEPTION;
        } else if (clientResponse.wasTimedOut()) {
            log.error("Response for ListOffsets for TopicPartitions: {} timed out - {}.", partitionFutures.keySet(), clientResponse);
            error = Errors.REQUEST_TIMED_OUT;
        } else if (!clientResponse.hasResponse()) {
            log.error("Response for ListOffsets for TopicPartitions: {} has no response - {}.", partitionFutures.keySet(), clientResponse);
            error = Errors.UNKNOWN_SERVER_ERROR;
        } else {
            // No error to handle, returning false instantly.
            return false;
        }

        partitionFutures.forEach((tp, future) -> future.complete(new OffsetResponse(-1, error)));
        return true;
    }

    /**
     * Tracks a pending ListOffsets request and its associated futures.
     */
    private record PendingRequest(Node node,
                                  Map<TopicPartition, CompletableFuture<OffsetResponse>> futures,
                                  ListOffsetsRequest.Builder requestBuilder) {
    }

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
                    time.hiResClockMs(),
                    current.node,
                    requestBuilder,
                    response -> handleResponse(current.futures, response)
                );

                requests.add(requestHandler);
            }

            return requests;
        }
    }
}
