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
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.ReadShareGroupStateRequest;
import org.apache.kafka.common.requests.ReadShareGroupStateResponse;
import org.apache.kafka.common.requests.WriteShareGroupStateRequest;
import org.apache.kafka.common.requests.WriteShareGroupStateResponse;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.util.InterBrokerSendThread;
import org.apache.kafka.server.util.RequestAndCompletionHandler;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.server.util.timer.TimerTask;

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
    public static final long REQUEST_BACKOFF_MS = 1_000L;
    public static final long REQUEST_BACKOFF_MAX_MS = 30_000L;
    private static final int MAX_FIND_COORD_ATTEMPTS = 5;
    private final Time time;
    private final Timer timer;
    private final ShareCoordinatorMetadataCacheHelper cacheHelper;
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

    private static class BackoffManager {
        private final int maxAttempts;
        private int attempts;
        private final ExponentialBackoff backoff;

        BackoffManager(int maxAttempts, long initialBackoffMs, long maxBackoffMs) {
            this.maxAttempts = maxAttempts;
            this.backoff = new ExponentialBackoff(
                initialBackoffMs,
                CommonClientConfigs.RETRY_BACKOFF_EXP_BASE,
                maxBackoffMs,
                CommonClientConfigs.RETRY_BACKOFF_JITTER
            );
        }

        void incrementAttempt() {
            attempts++;
        }

        void resetAttempts() {
            attempts = 0;
        }

        boolean canAttempt() {
            return attempts < maxAttempts;
        }

        long backOff() {
            return this.backoff.backoff(attempts);
        }
    }

    public enum RPCType {
        READ,
        WRITE,
        DELETE,
        SUMMARY,
        UNKNOWN
    }

    public PersisterStateManager(KafkaClient client, ShareCoordinatorMetadataCacheHelper cacheHelper, Time time, Timer timer) {
        if (client == null) {
            throw new IllegalArgumentException("Kafkaclient must not be null.");
        }
        if (time == null) {
            throw new IllegalArgumentException("Time must not be null.");
        }
        if (timer == null) {
            throw new IllegalArgumentException("Timer must not be null.");
        }
        if (cacheHelper == null) {
            throw new IllegalArgumentException("CacheHelper must not be null.");
        }
        this.time = time;
        this.timer = timer;
        this.cacheHelper = cacheHelper;
        this.sender = new SendThread(
            "PersisterStateManager",
            client,
            Math.toIntExact(CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS),  //30 seconds
            this.time,
            true,
            new Random(this.time.milliseconds()));
    }

    public void enqueue(PersisterStateManagerHandler handler) {
        this.sender.enqueue(handler);
    }

    public void start() {
        if (isStarted.compareAndSet(false, true)) {
            this.sender.start();
            isStarted.set(true);
        }
    }

    public void stop() throws Exception {
        if (isStarted.compareAndSet(true, false)) {
            this.sender.shutdown();
            Utils.closeQuietly(this.timer, "PersisterStateManager timer");
        }
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
        private final BackoffManager findCoordBackoff;
        protected final Logger log = LoggerFactory.getLogger(getClass());
        private Consumer<ClientResponse> onCompleteCallback;
        protected final SharePartitionKey partitionKey;

        public PersisterStateManagerHandler(
            String groupId,
            Uuid topicId,
            int partition,
            long backoffMs,
            long backoffMaxMs,
            int maxRPCRetryAttempts
        ) {
            this.findCoordBackoff = new BackoffManager(maxRPCRetryAttempts, backoffMs, backoffMaxMs);
            this.onCompleteCallback = response -> {
            }; // noop
            partitionKey = SharePartitionKey.getInstance(groupId, topicId, partition);
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
         * Returns true if the response is valid for the respective child class.
         *
         * @param response - Client response
         * @return - boolean
         */
        protected abstract boolean isResponseForRequest(ClientResponse response);

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
         * Child class should return the appropriate completable future encapsulating
         * the response for the RPC.
         *
         * @return A completable future of RPC response
         */
        protected abstract CompletableFuture<? extends AbstractResponse> result();

        /**
         * Returns builder for share coordinator
         *
         * @return builder for find coordinator
         */
        protected AbstractRequest.Builder<FindCoordinatorRequest> findShareCoordinatorBuilder() {
            return new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData()
                .setKeyType(FindCoordinatorRequest.CoordinatorType.SHARE.id())
                .setKey(partitionKey().asCoordinatorKey()));
        }

        public void addRequestToNodeMap(Node node, PersisterStateManagerHandler handler) {
            if (!handler.isBatchable()) {
                return;
            }
            synchronized (nodeMapLock) {
                nodeRPCMap.computeIfAbsent(node, k -> new HashMap<>())
                    .computeIfAbsent(handler.rpcType(), k -> new HashMap<>())
                    .computeIfAbsent(partitionKey().groupId(), k -> new LinkedList<>())
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
                Node node = cacheHelper.getShareCoordinator(partitionKey(), Topic.SHARE_GROUP_STATE_TOPIC_NAME);
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
        protected SharePartitionKey partitionKey() {
            return partitionKey;
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
                } else if (isResponseForRequest(response)) {
                    handleRequestResponse(response);
                }
            }
            sender.wakeup();
        }

        protected void resetCoordinatorNode() {
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
            findCoordBackoff.incrementAttempt();
            List<FindCoordinatorResponseData.Coordinator> coordinators = ((FindCoordinatorResponse) response.responseBody()).coordinators();
            if (coordinators.size() != 1) {
                log.error("Find coordinator response for {} is invalid", partitionKey());
                findCoordinatorErrorResponse(Errors.UNKNOWN_SERVER_ERROR, new IllegalStateException("Invalid response with multiple coordinators."));
                return;
            }

            FindCoordinatorResponseData.Coordinator coordinatorData = coordinators.get(0);
            Errors error = Errors.forCode(coordinatorData.errorCode());

            switch (error) {
                case NONE:
                    log.debug("Find coordinator response valid. Enqueuing actual request.");
                    findCoordBackoff.resetAttempts();
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
                case NOT_COORDINATOR:
                    log.warn("Received retriable error in find coordinator for {} using key {}: {}", name(), partitionKey(), error.message());
                    if (!findCoordBackoff.canAttempt()) {
                        log.error("Exhausted max retries to find coordinator for {} using key {} without success.", name(), partitionKey());
                        findCoordinatorErrorResponse(error, new Exception("Exhausted max retries to find coordinator without success."));
                        break;
                    }
                    resetCoordinatorNode();
                    timer.add(new PersisterTimerTask(findCoordBackoff.backOff(), this));
                    break;

                default:
                    log.error("Unable to find coordinator for {} using key {}.", name(), partitionKey());
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
         *
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
        private final BackoffManager writeStateBackoff;

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
            int maxRPCRetryAttempts
        ) {
            super(groupId, topicId, partition, backoffMs, backoffMaxMs, maxRPCRetryAttempts);
            this.stateEpoch = stateEpoch;
            this.leaderEpoch = leaderEpoch;
            this.startOffset = startOffset;
            this.batches = batches;
            this.result = result;
            this.writeStateBackoff = new BackoffManager(maxRPCRetryAttempts, backoffMs, backoffMaxMs);
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
        protected boolean isResponseForRequest(ClientResponse response) {
            return response.requestHeader().apiKey() == ApiKeys.WRITE_SHARE_GROUP_STATE;
        }

        @Override
        protected void handleRequestResponse(ClientResponse response) {
            log.debug("Write state response received - {}", response);
            writeStateBackoff.incrementAttempt();

            // response can be a combined one for large number of requests
            // we need to deconstruct it
            WriteShareGroupStateResponse combinedResponse = (WriteShareGroupStateResponse) response.responseBody();

            for (WriteShareGroupStateResponseData.WriteStateResult writeStateResult : combinedResponse.data().results()) {
                if (writeStateResult.topicId().equals(partitionKey().topicId())) {
                    Optional<WriteShareGroupStateResponseData.PartitionResult> partitionStateData =
                        writeStateResult.partitions().stream().filter(partitionResult -> partitionResult.partition() == partitionKey().partition())
                            .findFirst();

                    if (partitionStateData.isPresent()) {
                        Errors error = Errors.forCode(partitionStateData.get().errorCode());
                        switch (error) {
                            case NONE:
                                writeStateBackoff.resetAttempts();
                                WriteShareGroupStateResponseData.WriteStateResult result = WriteShareGroupStateResponse.toResponseWriteStateResult(
                                    partitionKey().topicId(),
                                    Collections.singletonList(partitionStateData.get())
                                );
                                this.result.complete(new WriteShareGroupStateResponse(
                                    new WriteShareGroupStateResponseData().setResults(Collections.singletonList(result))));
                                return;

                            // check retriable errors
                            case COORDINATOR_NOT_AVAILABLE:
                            case COORDINATOR_LOAD_IN_PROGRESS:
                            case NOT_COORDINATOR:
                                log.warn("Received retriable error in write state RPC for key {}: {}", partitionKey(), error.message());
                                if (!writeStateBackoff.canAttempt()) {
                                    log.error("Exhausted max retries for write state RPC for key {} without success.", partitionKey());
                                    writeStateErrorResponse(error, new Exception("Exhausted max retries to complete write state RPC without success."));
                                    return;
                                }
                                super.resetCoordinatorNode();
                                timer.add(new PersisterTimerTask(writeStateBackoff.backOff(), this));
                                return;

                            default:
                                log.error("Unable to perform write state RPC for key {}: {}", partitionKey(), error.message());
                                writeStateErrorResponse(error, null);
                                return;
                        }
                    }
                }
            }

            // no response found specific topic partition
            IllegalStateException exception = new IllegalStateException(
                "Failed to write state for share partition: " + partitionKey()
            );
            writeStateErrorResponse(Errors.forException(exception), exception);
        }

        private void writeStateErrorResponse(Errors error, Exception exception) {
            this.result.complete(new WriteShareGroupStateResponse(
                WriteShareGroupStateResponse.toErrorResponseData(partitionKey().topicId(), partitionKey().partition(), error, "Error in write state RPC. " +
                    (exception == null ? error.message() : exception.getMessage()))));
        }

        @Override
        protected void findCoordinatorErrorResponse(Errors error, Exception exception) {
            this.result.complete(new WriteShareGroupStateResponse(
                WriteShareGroupStateResponse.toErrorResponseData(partitionKey().topicId(), partitionKey().partition(), error, "Error in find coordinator. " +
                    (exception == null ? error.message() : exception.getMessage()))));
        }

        protected CompletableFuture<WriteShareGroupStateResponse> result() {
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
        private final CompletableFuture<ReadShareGroupStateResponse> result;
        private final BackoffManager readStateBackoff;

        public ReadStateHandler(
            String groupId,
            Uuid topicId,
            int partition,
            int leaderEpoch,
            CompletableFuture<ReadShareGroupStateResponse> result,
            long backoffMs,
            long backoffMaxMs,
            int maxRPCRetryAttempts,
            Consumer<ClientResponse> onCompleteCallback
        ) {
            super(groupId, topicId, partition, backoffMs, backoffMaxMs, maxRPCRetryAttempts);
            this.leaderEpoch = leaderEpoch;
            this.result = result;
            this.readStateBackoff = new BackoffManager(maxRPCRetryAttempts, backoffMs, backoffMaxMs);
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
        protected boolean isResponseForRequest(ClientResponse response) {
            return response.requestHeader().apiKey() == ApiKeys.READ_SHARE_GROUP_STATE;
        }

        @Override
        protected void handleRequestResponse(ClientResponse response) {
            log.debug("Read state response received - {}", response);
            readStateBackoff.incrementAttempt();

            ReadShareGroupStateResponse combinedResponse = (ReadShareGroupStateResponse) response.responseBody();
            for (ReadShareGroupStateResponseData.ReadStateResult readStateResult : combinedResponse.data().results()) {
                if (readStateResult.topicId().equals(partitionKey().topicId())) {
                    Optional<ReadShareGroupStateResponseData.PartitionResult> partitionStateData =
                        readStateResult.partitions().stream().filter(partitionResult -> partitionResult.partition() == partitionKey().partition())
                            .findFirst();

                    if (partitionStateData.isPresent()) {
                        Errors error = Errors.forCode(partitionStateData.get().errorCode());
                        switch (error) {
                            case NONE:
                                readStateBackoff.resetAttempts();
                                ReadShareGroupStateResponseData.ReadStateResult result = ReadShareGroupStateResponse.toResponseReadStateResult(
                                    partitionKey().topicId(),
                                    Collections.singletonList(partitionStateData.get())
                                );
                                this.result.complete(new ReadShareGroupStateResponse(new ReadShareGroupStateResponseData()
                                    .setResults(Collections.singletonList(result))));
                                return;

                            // check retriable errors
                            case COORDINATOR_NOT_AVAILABLE:
                            case COORDINATOR_LOAD_IN_PROGRESS:
                            case NOT_COORDINATOR:
                                log.warn("Received retriable error in read state RPC for key {}: {}", partitionKey(), error.message());
                                if (!readStateBackoff.canAttempt()) {
                                    log.error("Exhausted max retries for read state RPC for key {} without success.", partitionKey());
                                    readStateErrorReponse(error, new Exception("Exhausted max retries to complete read state RPC without success."));
                                    return;
                                }
                                super.resetCoordinatorNode();
                                timer.add(new PersisterTimerTask(readStateBackoff.backOff(), this));
                                return;

                            default:
                                log.error("Unable to perform read state RPC for key {}: {}", partitionKey(), error.message());
                                readStateErrorReponse(error, null);
                                return;
                        }
                    }
                }
            }

            // no response found specific topic partition
            IllegalStateException exception = new IllegalStateException(
                "Failed to read state for share partition " + partitionKey()
            );
            readStateErrorReponse(Errors.forException(exception), exception);
        }

        protected void readStateErrorReponse(Errors error, Exception exception) {
            this.result.complete(new ReadShareGroupStateResponse(
                ReadShareGroupStateResponse.toErrorResponseData(partitionKey().topicId(), partitionKey().partition(), error, "Error in find coordinator. " +
                    (exception == null ? error.message() : exception.getMessage()))));
        }

        @Override
        protected void findCoordinatorErrorResponse(Errors error, Exception exception) {
            this.result.complete(new ReadShareGroupStateResponse(
                ReadShareGroupStateResponse.toErrorResponseData(partitionKey().topicId(), partitionKey().partition(), error, "Error in read state RPC. " +
                    (exception == null ? error.message() : exception.getMessage()))));
        }

        protected CompletableFuture<ReadShareGroupStateResponse> result() {
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

    private final class PersisterTimerTask extends TimerTask {
        private final PersisterStateManagerHandler handler;

        PersisterTimerTask(long delayMs, PersisterStateManagerHandler handler) {
            super(delayMs);
            this.handler = handler;
        }

        @Override
        public void run() {
            enqueue(handler);
            sender.wakeup();
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
                partitionData.computeIfAbsent(handler.partitionKey().topicId(), topicId -> new LinkedList<>())
                    .add(
                        new WriteShareGroupStateRequestData.PartitionData()
                            .setPartition(handler.partitionKey().partition())
                            .setStateEpoch(handler.stateEpoch)
                            .setLeaderEpoch(handler.leaderEpoch)
                            .setStartOffset(handler.startOffset)
                            .setStateBatches(handler.batches.stream()
                                .map(batch -> new WriteShareGroupStateRequestData.StateBatch()
                                    .setFirstOffset(batch.firstOffset())
                                    .setLastOffset(batch.lastOffset())
                                    .setDeliveryState(batch.deliveryState())
                                    .setDeliveryCount(batch.deliveryCount()))
                                .collect(Collectors.toList()))
                    );
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
                partitionData.computeIfAbsent(handler.partitionKey().topicId(), topicId -> new LinkedList<>())
                    .add(
                        new ReadShareGroupStateRequestData.PartitionData()
                            .setPartition(handler.partitionKey().partition())
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
