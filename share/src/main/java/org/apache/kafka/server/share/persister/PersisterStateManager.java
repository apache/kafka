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

package org.apache.kafka.server.share.persister;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.ReadShareGroupStateRequestData;
import org.apache.kafka.common.message.ReadShareGroupStateResponseData;
import org.apache.kafka.common.message.WriteShareGroupStateRequestData;
import org.apache.kafka.common.message.WriteShareGroupStateResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.ReadShareGroupStateRequest;
import org.apache.kafka.common.requests.ReadShareGroupStateResponse;
import org.apache.kafka.common.requests.WriteShareGroupStateRequest;
import org.apache.kafka.common.requests.WriteShareGroupStateResponse;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.util.InterBrokerSendThread;
import org.apache.kafka.server.util.RequestAndCompletionHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * This class encapsulates various handler classes corresponding to share
 * state RPCs. It also holds an {@link InterBrokerSendThread} specialization
 * which manages the sending the RPC requests over the network.
 * This class is for the exclusive purpose of being used with {@link DefaultStatePersister}
 * but can be extended for other {@link Persister} implementations as well.
 */
public class PersisterStateManager {

    private SendThread sender;
    private final AtomicBoolean isStarted = new AtomicBoolean(false);
    private final ShareCoordinatorMetadataCacheHelper cacheHelper;
    public static final long REQUEST_BACKOFF_MS = 1_000L;
    public static final long REQUEST_BACKOFF_MAX_MS = 30_000L;
    private static final int MAX_FIND_COORD_ATTEMPTS = 5;
    private final Time time;
    // holds the set of share coord nodes for each RPC type which is currently sent but not completed
    private final Map<RPCType, Set<Node>> inFlight = new HashMap<>();

    // Mapping for batchable RPCs. The top level grouping is based on destination share coordinator node.
    // Since kafkaApis for each RPC type are separate, we cannot batch different types of RPCs. Hence, we need
    // RPCType'd key inner map.
    // The RPC schemas defined in kip-932 have a single group id per request. Hence, we cannot batch RPCs
    // with different groupIds and therefore, another inner map keyed on groupId is needed.
    // Finally, the value is a list of handlers
    private final Map<Node, Map<RPCType, Map<String, List<PersisterStateManagerHandler>>>> nodeRPCMap = new HashMap<>();

    // Final object to serve synchronization needs.
    private final Object nodeMapLock = new Object();

    // Called when the generateRequests method is executed by InterBrokerSendThread, returning requests.
    // Mainly for testing and introspection purpose to inspect the state of the nodeRPC map
    // when generateRequests is called.
    private Runnable generateCallback;

    public enum RPCType {
        READ,
        WRITE,
        DELETE,
        SUMMARY,
        UNKNOWN
    }

    public PersisterStateManager(KafkaClient client, Time time, ShareCoordinatorMetadataCacheHelper cacheHelper) {
        if (client == null) {
            throw new IllegalArgumentException("Kafkaclient must not be null.");
        }
        if (cacheHelper == null) {
            throw new IllegalArgumentException("ShareCoordinatorMetadataCacheHelper must not be null.");
        }
        this.cacheHelper = cacheHelper;
        this.time = time == null ? Time.SYSTEM : time;
        this.sender = new SendThread(
            "PersisterStateManager",
            client,
            30_000,  //30 seconds
            this.time,
            true,
            new Random(this.time.milliseconds()));
    }

    public void enqueue(PersisterStateManagerHandler handler) {
        this.sender.enqueue(handler);
    }

    public void start() {
        if (!isStarted.get()) {
            this.sender.start();
            isStarted.set(true);
        }
    }

    public void stop() throws InterruptedException {
        this.sender.shutdown();
    }

    // test visibility
    Map<Node, Map<RPCType, Map<String, List<PersisterStateManagerHandler>>>> nodeRPCMap() {
        return nodeRPCMap;
    }

    public void setGenerateCallback(Runnable generateCallback) {
        this.generateCallback = generateCallback;
    }

    /**
     * Parent class of all RPCs. Uses template pattern to implement core methods.
     * Various child classes can extend this class to define how to handle RPC specific
     * responses, retries, batching etc.
     * <p>
     * Since the find coordinator RPC/lookup is a necessary pre-condition for all
     * share state RPCs, the infra code for it is encapsulated in this class itself.
     */
    public abstract class PersisterStateManagerHandler implements RequestCompletionHandler {
        protected Node coordinatorNode;
        protected final String groupId;
        protected final Uuid topicId;
        protected final int partition;
        private final ExponentialBackoff findCoordBackoff;
        private int findCoordAttempts = 0;
        private final int maxFindCoordAttempts;
        protected final Logger log = LoggerFactory.getLogger(getClass());
        private Consumer<ClientResponse> onCompleteCallback;

        public PersisterStateManagerHandler(
            String groupId,
            Uuid topicId,
            int partition,
            long backoffMs,
            long backoffMaxMs,
            int maxFindCoordAttempts
        ) {
            this.groupId = groupId;
            this.topicId = topicId;
            this.partition = partition;
            this.findCoordBackoff = new ExponentialBackoff(
                backoffMs,
                CommonClientConfigs.RETRY_BACKOFF_EXP_BASE,
                backoffMaxMs,
                CommonClientConfigs.RETRY_BACKOFF_JITTER);
            this.maxFindCoordAttempts = maxFindCoordAttempts;
        }

        /**
         * Child class must create appropriate builder object for the handled RPC
         *
         * @return builder for the request
         */
        protected abstract AbstractRequest.Builder<? extends AbstractRequest> requestBuilder();

        /**
         * Handles the response for an RPC.
         *
         * @param response - Client response
         */
        protected abstract void handleRequestResponse(ClientResponse response);

        /**
         * Returns true if the response if valid for the respective child class.
         *
         * @param response - Client response
         * @return - boolean
         */
        protected abstract boolean isRequestResponse(ClientResponse response);

        /**
         * Handle invalid find coordinator response. If error is UNKNOWN_SERVER_ERROR. Look at the
         * exception details to figure out the problem.
         *
         * @param error
         * @param exception
         */
        protected abstract void findCoordinatorErrorResponse(Errors error, Exception exception);

        /**
         * Child class must provide a descriptive name for the implementation.
         *
         * @return String
         */
        protected abstract String name();

        /**
         * Child class must return appropriate type of RPC here
         *
         * @return String
         */
        protected abstract RPCType rpcType();

        /**
         * Returns builder for share coordinator
         *
         * @return builder for find coordinator
         */
        protected AbstractRequest.Builder<FindCoordinatorRequest> findShareCoordinatorBuilder() {
            return new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData()
                .setKeyType(FindCoordinatorRequest.CoordinatorType.SHARE.id())
                .setKey(coordinatorKey()));
        }

        public void addRequestToNodeMap(Node node, PersisterStateManagerHandler handler) {
            if (!handler.isBatchable()) {
                return;
            }
            synchronized (nodeMapLock) {
                nodeRPCMap.computeIfAbsent(node, k -> new HashMap<>())
                    .computeIfAbsent(handler.rpcType(), k -> new HashMap<>())
                    .computeIfAbsent(handler.groupId, k -> new LinkedList<>())
                    .add(handler);
            }
            sender.wakeup();
        }

        /**
         * Returns true is coordinator node is not yet set
         *
         * @return boolean
         */
        protected boolean lookupNeeded() {
            if (coordinatorNode != null) {
                return false;
            }
            if (cacheHelper.containsTopic(Topic.SHARE_GROUP_STATE_TOPIC_NAME)) {
                log.debug("{} internal topic already exists.", Topic.SHARE_GROUP_STATE_TOPIC_NAME);
                Node node = cacheHelper.getShareCoordinator(coordinatorKey(), Topic.SHARE_GROUP_STATE_TOPIC_NAME);
                if (node != Node.noNode()) {
                    log.debug("Found coordinator node in cache: {}", node);
                    coordinatorNode = node;
                    addRequestToNodeMap(node, this);
                    return false;
                }
            }
            return true;
        }

        /**
         * Returns the String key to be used as share coordinator key
         *
         * @return String
         */
        protected String coordinatorKey() {
            return SharePartitionKey.asCoordinatorKey(groupId, topicId, partition);
        }

        /**
         * Returns true if the RPC response if for Find Coordinator RPC.
         *
         * @param response - Client response object
         * @return boolean
         */
        protected boolean isFindCoordinatorResponse(ClientResponse response) {
            return response != null && response.requestHeader().apiKey() == ApiKeys.FIND_COORDINATOR;
        }

        @Override
        public void onComplete(ClientResponse response) {
            if (onCompleteCallback != null) {
                onCompleteCallback.accept(response);
            }
            if (response != null && response.hasResponse()) {
                if (isFindCoordinatorResponse(response)) {
                    handleFindCoordinatorResponse(response);
                } else if (isRequestResponse(response)) {
                    handleRequestResponse(response);
                }
            }
            sender.wakeup();
        }

        private void resetAttempts() {
            findCoordAttempts = 0;
        }

        private void resetCoordinatorNode() {
            coordinatorNode = null;
        }

        /**
         * Handles the response for find coordinator RPC and sets appropriate state.
         *
         * @param response - Client response for find coordinator RPC
         */
        protected void handleFindCoordinatorResponse(ClientResponse response) {
            log.debug("Find coordinator response received - {}", response);

            // Incrementing the number of find coordinator attempts
            findCoordAttempts++;
            List<FindCoordinatorResponseData.Coordinator> coordinators = ((FindCoordinatorResponse) response.responseBody()).coordinators();
            if (coordinators.size() != 1) {
                log.error("Find coordinator response for {} is invalid", coordinatorKey());
                findCoordinatorErrorResponse(Errors.UNKNOWN_SERVER_ERROR, new IllegalStateException("Invalid response with multiple coordinators."));
                return;
            }

            FindCoordinatorResponseData.Coordinator coordinatorData = coordinators.get(0);
            Errors error = Errors.forCode(coordinatorData.errorCode());

            switch (error) {
                case NONE:
                    log.debug("Find coordinator response valid. Enqueuing actual request.");
                    resetAttempts();
                    coordinatorNode = new Node(coordinatorData.nodeId(), coordinatorData.host(), coordinatorData.port());
                    // now we want the actual share state RPC call to happen
                    if (this.isBatchable()) {
                        addRequestToNodeMap(coordinatorNode, this);
                    } else {
                        enqueue(this);
                    }
                    break;

                case COORDINATOR_NOT_AVAILABLE: // retriable error codes
                case COORDINATOR_LOAD_IN_PROGRESS:
                    log.warn("Received retriable error in find coordinator: {}", error.message());
                    if (findCoordAttempts >= this.maxFindCoordAttempts) {
                        log.error("Exhausted max retries to find coordinator without success.");
                        findCoordinatorErrorResponse(error, new Exception("Exhausted max retries to find coordinator without success."));
                        break;
                    }
                    log.debug("Waiting before retrying find coordinator RPC.");
                    try {
                        TimeUnit.MILLISECONDS.sleep(findCoordBackoff.backoff(findCoordAttempts));
                    } catch (InterruptedException e) {
                        log.warn("Interrupted waiting before retrying find coordinator request", e);
                    }
                    resetCoordinatorNode();
                    enqueue(this);
                    break;

                default:
                    log.error("Unable to find coordinator.");
                    findCoordinatorErrorResponse(error, null);
            }
        }

        // Visible for testing
        public Node getCoordinatorNode() {
            return coordinatorNode;
        }

        protected abstract boolean isBatchable();

        /**
         * This method can be called by child class objects to register a callback
         * which will be called when the onComplete cb is called on request completion.
         * @param callback
         */
        protected void setOnCompleteCallback(Consumer<ClientResponse> callback) {
            this.onCompleteCallback = callback;
        }
    }

    public class WriteStateHandler extends PersisterStateManagerHandler {
        private final int stateEpoch;
        private final int leaderEpoch;
        private final long startOffset;
        private final List<PersisterStateBatch> batches;
        private final CompletableFuture<WriteShareGroupStateResponse> result;

        public WriteStateHandler(
            String groupId,
            Uuid topicId,
            int partition,
            int stateEpoch,
            int leaderEpoch,
            long startOffset,
            List<PersisterStateBatch> batches,
            CompletableFuture<WriteShareGroupStateResponse> result,
            long backoffMs,
            long backoffMaxMs,
            int maxFindCoordAttempts
        ) {
            super(groupId, topicId, partition, backoffMs, backoffMaxMs, maxFindCoordAttempts);
            this.stateEpoch = stateEpoch;
            this.leaderEpoch = leaderEpoch;
            this.startOffset = startOffset;
            this.batches = batches;
            this.result = result;
        }

        public WriteStateHandler(
            String groupId,
            Uuid topicId,
            int partition,
            int stateEpoch,
            int leaderEpoch,
            long startOffset,
            List<PersisterStateBatch> batches,
            CompletableFuture<WriteShareGroupStateResponse> result,
            Consumer<ClientResponse> onCompleteCallback
        ) {
            this(
                groupId,
                topicId,
                partition,
                stateEpoch,
                leaderEpoch,
                startOffset,
                batches,
                result,
                REQUEST_BACKOFF_MS,
                REQUEST_BACKOFF_MAX_MS,
                MAX_FIND_COORD_ATTEMPTS
            );
        }

        @Override
        protected String name() {
            return "WriteStateHandler";
        }

        @Override
        protected AbstractRequest.Builder<? extends AbstractRequest> requestBuilder() {
            throw new RuntimeException("Write requests are batchable, hence individual requests not needed.");
        }

        @Override
        protected boolean isRequestResponse(ClientResponse response) {
            return response.requestHeader().apiKey() == ApiKeys.WRITE_SHARE_GROUP_STATE;
        }

        @Override
        protected void handleRequestResponse(ClientResponse response) {
            log.debug("Write state response received - {}", response);
            // response can be a combined one for large number of requests
            // we need to deconstruct it
            WriteShareGroupStateResponse combinedResponse = (WriteShareGroupStateResponse) response.responseBody();
            for (WriteShareGroupStateResponseData.WriteStateResult writeStateResult : combinedResponse.data().results()) {
                if (writeStateResult.topicId().equals(topicId)) {
                    Optional<WriteShareGroupStateResponseData.PartitionResult> partitionStateData =
                        writeStateResult.partitions().stream().filter(partitionResult -> partitionResult.partition() == partition)
                            .findFirst();
                    if (partitionStateData.isPresent()) {
                        WriteShareGroupStateResponseData.WriteStateResult result = WriteShareGroupStateResponse.toResponseWriteStateResult(topicId, Collections.singletonList(partitionStateData.get()));
                        this.result.complete(new WriteShareGroupStateResponse(
                            new WriteShareGroupStateResponseData().setResults(Collections.singletonList(result))));
                        return;
                    }
                }
            }
        }

        @Override
        protected void findCoordinatorErrorResponse(Errors error, Exception exception) {
            this.result.complete(new WriteShareGroupStateResponse(
                WriteShareGroupStateResponse.toErrorResponseData(topicId, partition, error, "Error in find coordinator. " +
                    (exception == null ? error.message() : exception.getMessage()))));
        }

        // Visible for testing
        public CompletableFuture<WriteShareGroupStateResponse> getResult() {
            return result;
        }

        @Override
        protected boolean isBatchable() {
            return true;
        }

        @Override
        protected RPCType rpcType() {
            return RPCType.WRITE;
        }
    }

    public class ReadStateHandler extends PersisterStateManagerHandler {
        private final int leaderEpoch;
        private final String coordinatorKey;
        private final CompletableFuture<ReadShareGroupStateResponse> result;

        public ReadStateHandler(
            String groupId,
            Uuid topicId,
            int partition,
            int leaderEpoch,
            CompletableFuture<ReadShareGroupStateResponse> result,
            long backoffMs,
            long backoffMaxMs,
            int maxFindCoordAttempts,
            Consumer<ClientResponse> onCompleteCallback
        ) {
            super(groupId, topicId, partition, backoffMs, backoffMaxMs, maxFindCoordAttempts);
            this.leaderEpoch = leaderEpoch;
            this.coordinatorKey = SharePartitionKey.asCoordinatorKey(groupId, topicId, partition);
            this.result = result;
        }

        public ReadStateHandler(
            String groupId,
            Uuid topicId,
            int partition,
            int leaderEpoch,
            CompletableFuture<ReadShareGroupStateResponse> result,
            Consumer<ClientResponse> onCompleteCallback
        ) {
            this(
                groupId,
                topicId,
                partition,
                leaderEpoch,
                result,
                REQUEST_BACKOFF_MS,
                REQUEST_BACKOFF_MAX_MS,
                MAX_FIND_COORD_ATTEMPTS,
                onCompleteCallback
            );
        }

        @Override
        protected String name() {
            return "ReadStateHandler";
        }

        @Override
        protected AbstractRequest.Builder<ReadShareGroupStateRequest> requestBuilder() {
            throw new RuntimeException("Read requests are batchable, hence individual requests not needed.");
        }

        @Override
        protected boolean isRequestResponse(ClientResponse response) {
            return response.requestHeader().apiKey() == ApiKeys.READ_SHARE_GROUP_STATE;
        }

        @Override
        protected void handleRequestResponse(ClientResponse response) {
            log.debug("Read state response received - {}", response);

            ReadShareGroupStateResponse combinedResponse = (ReadShareGroupStateResponse) response.responseBody();
            ReadShareGroupStateResponseData readShareGroupStateResponseData = new ReadShareGroupStateResponseData();
            for (ReadShareGroupStateResponseData.ReadStateResult readStateResult : combinedResponse.data().results()) {
                if (readStateResult.topicId().equals(topicId)) {
                    Optional<ReadShareGroupStateResponseData.PartitionResult> partitionStateData =
                        readStateResult.partitions().stream().filter(partitionResult -> partitionResult.partition() == partition)
                            .findFirst();

                    if (partitionStateData.isPresent()) {
                        ReadShareGroupStateResponseData.ReadStateResult result = ReadShareGroupStateResponse.toResponseReadStateResult(topicId, Collections.singletonList(partitionStateData.get()));
                        readShareGroupStateResponseData.setResults(Collections.singletonList(result));
                        break;
                    }
                }
            }

            String errorMessage = "Failed to read state for partition " + partition + " in topic " + topicId + " for group " + groupId;
            if (readShareGroupStateResponseData.results().size() != 1) {
                log.error("ReadState response for {} is invalid", coordinatorKey);
                this.result.complete(new ReadShareGroupStateResponse(
                    ReadShareGroupStateResponse.toErrorResponseData(topicId, partition, Errors.forException(new IllegalStateException(errorMessage)), errorMessage)));
            }
            ReadShareGroupStateResponseData.ReadStateResult topicData = readShareGroupStateResponseData.results().get(0);
            if (!topicData.topicId().equals(topicId)) {
                log.error("ReadState response for {} is invalid", coordinatorKey);
                this.result.complete(new ReadShareGroupStateResponse(
                    ReadShareGroupStateResponse.toErrorResponseData(topicId, partition, Errors.forException(new IllegalStateException(errorMessage)), errorMessage)));
            }
            if (topicData.partitions().size() != 1) {
                log.error("ReadState response for {} is invalid", coordinatorKey);
                this.result.complete(new ReadShareGroupStateResponse(
                    ReadShareGroupStateResponse.toErrorResponseData(topicId, partition, Errors.forException(new IllegalStateException(errorMessage)), errorMessage)));
            }
            ReadShareGroupStateResponseData.PartitionResult partitionResponse = topicData.partitions().get(0);
            if (partitionResponse.partition() != partition) {
                log.error("ReadState response for {} is invalid", coordinatorKey);
                this.result.complete(new ReadShareGroupStateResponse(
                    ReadShareGroupStateResponse.toErrorResponseData(topicId, partition, Errors.forException(new IllegalStateException(errorMessage)), errorMessage)));
            }
            result.complete(new ReadShareGroupStateResponse(readShareGroupStateResponseData));
        }

        @Override
        protected void findCoordinatorErrorResponse(Errors error, Exception exception) {
            this.result.complete(new ReadShareGroupStateResponse(
                ReadShareGroupStateResponse.toErrorResponseData(topicId, partition, error, "Error in find coordinator. " +
                    (exception == null ? error.message() : exception.getMessage()))));
        }

        // Visible for testing
        public CompletableFuture<ReadShareGroupStateResponse> getResult() {
            return result;
        }

        @Override
        protected boolean isBatchable() {
            return true;
        }

        @Override
        protected RPCType rpcType() {
            return RPCType.READ;
        }
    }

    private class SendThread extends InterBrokerSendThread {
        private final ConcurrentLinkedQueue<PersisterStateManagerHandler> queue = new ConcurrentLinkedQueue<>();
        private final Random random;

        public SendThread(String name, KafkaClient networkClient, int requestTimeoutMs, Time time, boolean isInterruptible, Random random) {
            super(name, networkClient, requestTimeoutMs, time, isInterruptible);
            this.random = random;
        }

        private Node randomNode() {
            List<Node> nodes = cacheHelper.getClusterNodes();
            if (nodes == null || nodes.isEmpty()) {
                return Node.noNode();
            }
            return nodes.get(random.nextInt(nodes.size()));
        }

        /**
         * The incoming requests will have the keys in the following format
         * groupId: [
         * topidId1: [part1, part2, part3],
         * topicId2: [part1, part2, part3]
         * ...
         * ]
         * Hence, the total number of keys would be 1 x m x n (1 is for the groupId) where m is number of topicIds
         * and n is number of partitions specified per topicId.
         * <p>
         * For each RPC, we need to identify the coordinator node first.
         * If the internal share state topic is not found in the metadata cache, when RPC is received
         * we will need to make a FIND_COORDINATOR RPC which will have the side effect of creating the internal
         * topic as well. If the node is found in the cache, we will use it directly.
         *
         * @return list of requests to send
         */
        @Override
        public Collection<RequestAndCompletionHandler> generateRequests() {
            // There are two sources for requests here:
            // 1. A queue which will contain FIND_CORD RPCs and other non-batchable RPCs.
            // 2. A hashMap keyed on the share coordinator nodes which may contain batched requests.

            if (generateCallback != null) {
                generateCallback.run();
            }
            List<RequestAndCompletionHandler> requests = new ArrayList<>();

            // honor queue first as find coordinator
            // is mandatory for batching and sending the
            // request to correct destination node
            if (!queue.isEmpty()) {
                PersisterStateManagerHandler handler = queue.peek();
                queue.poll();
                if (handler.lookupNeeded()) {
                    // we need to find the coordinator node
                    Node randomNode = randomNode();
                    if (randomNode == Node.noNode()) {
                        log.error("Unable to find node to use for coordinator lookup.");
                        // fatal failure, cannot retry or progress
                        // fail the RPC
                        handler.findCoordinatorErrorResponse(Errors.COORDINATOR_NOT_AVAILABLE, Errors.COORDINATOR_NOT_AVAILABLE.exception());
                        return Collections.emptyList();
                    }
                    log.debug("Sending find coordinator RPC");
                    return Collections.singletonList(new RequestAndCompletionHandler(
                        time.milliseconds(),
                        randomNode,
                        handler.findShareCoordinatorBuilder(),
                        handler
                    ));
                } else {
                    // useful for tests and
                    // other RPCs which might not be batchable
                    if (!handler.isBatchable()) {
                        requests.add(new RequestAndCompletionHandler(
                            time.milliseconds(),
                            handler.coordinatorNode,
                            handler.requestBuilder(),
                            handler
                        ));
                    }
                }
            }

            // node1: {
            //   group1: {
            //      write: [w1, w2],
            //      read: [r1, r2],
            //      delete: [d1],
            //      summary: [s1]
            //   }
            //   group2: {
            //      write: [w3, w4]
            //   }
            // }
            // For a sequence of writes, the flow would be:
            // 1. 1st write request arrives
            // 2. it is enqueued in the send thread
            // 3. wakeup event causes the generate requests to find the coordinator
            // 4. it will cause either RPC or cache lookup
            // 5. once complete, the write handler is added to the nodeMap for batching and not the queue
            // 6. wakeup event causes generate requests to iterate over the map and send the write request (W1) and
            // remove node from the nodeMap and add it to inFlight
            // 7. until W1 completes, more write requests (W2, W3, ...) could come in and get added to the nodeMap as per point 3, 4, 5.
            // 8. if these belong to same node as W1. They will not be sent as the membership test with inFlight will pass.
            // 9. when W1 completes, it will clear inFlight and raise wakeup event.
            // 10. at this point W2, W3, etc. could be sent as a combined request thus achieving batching.
            final Map<RPCType, Set<Node>> sending = new HashMap<>();
            synchronized (nodeMapLock) {
                nodeRPCMap.forEach((coordNode, rpcTypesPerNode) ->
                    rpcTypesPerNode.forEach((rpcType, groupsPerRpcType) ->
                        groupsPerRpcType.forEach((groupId, handlersPerGroup) -> {
                            // this condition causes requests of same type and same destination node
                            // to not be sent immediately but get batched
                            if (!inFlight.containsKey(rpcType) || !inFlight.get(rpcType).contains(coordNode)) {
                                AbstractRequest.Builder<? extends AbstractRequest> combinedRequestPerTypePerGroup =
                                    RequestCoalescerHelper.coalesceRequests(groupId, rpcType, handlersPerGroup);
                                requests.add(new RequestAndCompletionHandler(
                                    time.milliseconds(),
                                    coordNode,
                                    combinedRequestPerTypePerGroup,
                                    response -> {
                                        inFlight.computeIfPresent(rpcType, (key, oldVal) -> {
                                            oldVal.remove(coordNode);
                                            return oldVal;
                                        });
                                        // now the combined request has completed
                                        // we need to create responses for individual
                                        // requests which composed the combined request
                                        handlersPerGroup.forEach(handler1 -> handler1.onComplete(response));
                                        wakeup();
                                    }));
                                sending.computeIfAbsent(rpcType, key -> new HashSet<>()).add(coordNode);
                            }
                        })));

                sending.forEach((rpcType, nodeSet) -> {
                    // we need to add these nodes to inFlight
                    inFlight.computeIfAbsent(rpcType, key -> new HashSet<>()).addAll(nodeSet);

                    // remove from nodeMap
                    nodeSet.forEach(node -> nodeRPCMap.computeIfPresent(node, (nodeKey, oldRPCTypeSet) -> {
                        oldRPCTypeSet.remove(rpcType);
                        return oldRPCTypeSet;
                    }));
                });
            } // close of synchronized context

            return requests;
        }

        public void enqueue(PersisterStateManagerHandler handler) {
            queue.add(handler);
            wakeup();
        }
    }

    /**
     * Util class which takes in builders of requests of the same type
     * and returns a combined request of the same type. This is required for
     * batching requests.
     */
    private static class RequestCoalescerHelper {
        public static AbstractRequest.Builder<? extends AbstractRequest> coalesceRequests(String groupId, RPCType rpcType, List<? extends PersisterStateManagerHandler> handlers) {
            switch (rpcType) {
                case WRITE:
                    return coalesceWrites(groupId, handlers);
                case READ:
                    return coalesceReads(groupId, handlers);
                default:
                    throw new RuntimeException("Unknown rpc type: " + rpcType);
            }
        }

        private static AbstractRequest.Builder<? extends AbstractRequest> coalesceWrites(String groupId, List<? extends PersisterStateManagerHandler> handlers) {
            Map<Uuid, List<WriteShareGroupStateRequestData.PartitionData>> partitionData = new HashMap<>();
            handlers.forEach(persHandler -> {
                assert persHandler instanceof WriteStateHandler;
                WriteStateHandler handler = (WriteStateHandler) persHandler;
                partitionData.computeIfAbsent(handler.topicId, topicId -> new LinkedList<>());
                partitionData.get(handler.topicId).add(
                    new WriteShareGroupStateRequestData.PartitionData()
                        .setPartition(handler.partition)
                        .setStateEpoch(handler.stateEpoch)
                        .setLeaderEpoch(handler.leaderEpoch)
                        .setStartOffset(handler.startOffset)
                        .setStateBatches(handler.batches.stream()
                            .map(batch -> new WriteShareGroupStateRequestData.StateBatch()
                                .setFirstOffset(batch.firstOffset())
                                .setLastOffset(batch.lastOffset())
                                .setDeliveryState(batch.deliveryState())
                                .setDeliveryCount(batch.deliveryCount()))
                            .collect(Collectors.toList())));
            });

            return new WriteShareGroupStateRequest.Builder(new WriteShareGroupStateRequestData()
                .setGroupId(groupId)
                .setTopics(partitionData.entrySet().stream()
                    .map(entry -> new WriteShareGroupStateRequestData.WriteStateData()
                        .setTopicId(entry.getKey())
                        .setPartitions(entry.getValue()))
                    .collect(Collectors.toList())));
        }

        private static AbstractRequest.Builder<? extends AbstractRequest> coalesceReads(String groupId, List<? extends PersisterStateManagerHandler> handlers) {
            Map<Uuid, List<ReadShareGroupStateRequestData.PartitionData>> partitionData = new HashMap<>();
            handlers.forEach(persHandler -> {
                assert persHandler instanceof ReadStateHandler;
                ReadStateHandler handler = (ReadStateHandler) persHandler;
                partitionData.computeIfAbsent(handler.topicId, topicId -> new LinkedList<>());
                partitionData.get(handler.topicId).add(
                    new ReadShareGroupStateRequestData.PartitionData()
                        .setPartition(handler.partition)
                        .setLeaderEpoch(handler.leaderEpoch)
                );
            });

            return new ReadShareGroupStateRequest.Builder(new ReadShareGroupStateRequestData()
                .setGroupId(groupId)
                .setTopics(partitionData.entrySet().stream()
                    .map(entry -> new ReadShareGroupStateRequestData.ReadStateData()
                        .setTopicId(entry.getKey())
                        .setPartitions(entry.getValue()))
                    .collect(Collectors.toList())));
        }
    }
}
