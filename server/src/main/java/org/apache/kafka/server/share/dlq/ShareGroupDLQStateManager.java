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

package org.apache.kafka.server.share.dlq;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.ExponentialBackoffManager;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.share.LogReader;
import org.apache.kafka.server.share.metrics.ShareGroupMetrics;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.util.InterBrokerSendThread;
import org.apache.kafka.server.util.RequestAndCompletionHandler;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.server.util.timer.TimerTask;
import org.apache.kafka.storage.internals.log.LogReadResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Core implementation of RPC send logic for the dlq manager.
 * This class allows for enqueuing records meant to be DLQ'ed
 * and manages various RPC which are to be sent to the KafkaApis.
 * These RPCs include PRODUCE, CREATE_TOPIC.
 */
public class ShareGroupDLQStateManager {
    private final AtomicBoolean isStarted = new AtomicBoolean(false);
    private final SendThread sender;
    private final Time time;
    private final Timer timer;
    private final ShareGroupDLQMetadataCacheHelper cacheHelper;
    private final LogReader logReader;
    private final ShareGroupMetrics shareGroupMetrics;
    public static final long REQUEST_BACKOFF_MS = 1_000L;
    public static final long REQUEST_BACKOFF_MAX_MS = 30_000L;
    private static final int MAX_REQUEST_ATTEMPTS = 5;
    private static final int RETRY_BACKOFF_EXP_BASE = CommonClientConfigs.RETRY_BACKOFF_EXP_BASE;
    private static final double RETRY_BACKOFF_JITTER = CommonClientConfigs.RETRY_BACKOFF_JITTER;

    /**
     * In most cases we expect the records getting DLQ'ed will be single offsets and
     * not complete batches. Hence, using a large upper limit while reading from the log
     * would be fruitless in most cases. Therefore, the value of 1 MB has been chosen
     * for the DLQ related log reads.
     */
    private static final int DLQ_MAX_FETCH_BYTES = 1024 * 1024;
    private static final Logger log = LoggerFactory.getLogger(ShareGroupDLQStateManager.class);

    private final Set<Node> inFlight = new HashSet<>();
    private final Map<Node, List<ProduceRequestHandler>> nodeRPCMap = new HashMap<>();
    private final Object nodeMapLock = new Object();

    public ShareGroupDLQStateManager(
        KafkaClient client,
        ShareGroupDLQMetadataCacheHelper cacheHelper,
        Time time,
        Timer timer,
        ShareGroupMetrics shareGroupMetrics,
        LogReader logReader
    ) {
        if (client == null) {
            throw new IllegalArgumentException("Kafkaclient must not be null.");
        }

        if (cacheHelper == null) {
            throw new IllegalArgumentException("Cache helper must not be null.");
        }

        if (time == null) {
            throw new IllegalArgumentException("Time must not be null.");
        }

        if (timer == null) {
            throw new IllegalArgumentException("Timer must not be null.");
        }

        if (shareGroupMetrics == null) {
            throw new IllegalArgumentException("ShareGroupMetrics must not be null.");
        }

        if (logReader == null) {
            throw new IllegalArgumentException("LogReader must not be null.");
        }

        this.time = time;
        this.timer = timer;
        this.cacheHelper = cacheHelper;
        this.shareGroupMetrics = shareGroupMetrics;
        this.logReader = logReader;
        this.sender = new SendThread(
            "ShareGroupDLQSendThread",
            client,
            Math.toIntExact(CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS),  //30 seconds
            this.time,
            true,
            new Random(this.time.milliseconds())
        );
    }

    public void start() {
        if (isStarted.compareAndSet(false, true)) {
            log.info("Starting ShareGroupDLQStateManager");
            this.sender.start();
        }
    }

    public void stop() throws Exception {
        if (isStarted.compareAndSet(true, false)) {
            this.sender.shutdown();
        }
    }

    /**
     * Enqueues a {@link ShareGroupDLQRecordParameter} based on which records will be DLQ'ed.
     * The actual record written to the DLQ topic will be built by fetching information from this argument.
     *
     * @param param Reference comprising offset information
     * @return A future completing normally on successful DLQ, exceptionally otherwise.
     */
    public CompletableFuture<Void> dlq(ShareGroupDLQRecordParameter param) {
        return dlq(param, REQUEST_BACKOFF_MS, REQUEST_BACKOFF_MAX_MS, MAX_REQUEST_ATTEMPTS);
    }

    // Visibility for tests
    CompletableFuture<Void> dlq(ShareGroupDLQRecordParameter param, long requestBackoffMs, long requestBackoffMaxMs, int maxRequestAttempts) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        ProduceRequestHandler requestHandler = new ProduceRequestHandler(param, future, requestBackoffMs, requestBackoffMaxMs, maxRequestAttempts);
        enqueue(requestHandler);
        return future;
    }

    // Visibility for tests
    Map<Node, List<ShareGroupDLQStateManager.ProduceRequestHandler>> nodeRPCMap() {
        // Using Collections.unmodifiableMap and not Map.copyOf as we are looking for a quick
        // immutable view of the map in the tests. The tests will invoke the
        // method repeatedly to check the state of the map. Map.copyOf will create
        // a deep copy of the map on every call and changes will might get missed resulting
        // in flakiness.
        return Collections.unmodifiableMap(nodeRPCMap);
    }

    private void enqueue(ProduceRequestHandler requestHandler) {
        sender.enqueue(requestHandler);
    }

    /**
     * Add a produce request handler after determining that the DLQ topic exists
     * or has been created by he CREATE_TOPIC RPC. The map is used to collect all PRODUCE
     * requests which are destined for a specific destination node. The Sender class
     * then performs coalescing on all the handlers to create one single PRODUCE instead
     * of sending multiple RPCs. This method is currently called when a DLQ topic already
     * exists and there is no need to send a CREATE_TOPIC RPC and if it does not, post
     * successful DLQ topic creation.
     *
     * @param node    The destination node where the produce request needs to be sent.
     * @param handler The handler instance to add to the node map.
     */
    private void addRequestToNodeMap(Node node, ProduceRequestHandler handler) {
        if (!handler.isBatchable()) {
            return;
        }
        synchronized (nodeMapLock) {
            nodeRPCMap.computeIfAbsent(node, k -> new LinkedList<>())
                .add(handler);
        }
        sender.wakeup();
    }

    // Visibility for tests
    class ProduceRequestHandler implements RequestCompletionHandler {
        private final CompletableFuture<Void> result;
        private final ShareGroupDLQRecordParameter param;
        private static final Logger LOG = LoggerFactory.getLogger(ShareGroupDLQStateManager.ProduceRequestHandler.class);
        private final ExponentialBackoffManager createTopicsBackoff;
        private final ExponentialBackoffManager produceRequestBackoff;
        private Node dlqPartitionLeaderNode;
        private int dlqDestinationPartition;
        private ShareGroupDLQMetadataCacheHelper.TopicPartitionData dlqTopicPartitionData;

        public static final String HEADER_DLQ_ERRORS_TOPIC = "__dlq.errors.topic";
        public static final String HEADER_DLQ_ERRORS_PARTITION = "__dlq.errors.partition";
        public static final String HEADER_DLQ_ERRORS_OFFSET = "__dlq.errors.offset";
        public static final String HEADER_DLQ_ERRORS_GROUP = "__dlq.errors.group";
        public static final String HEADER_DLQ_ERRORS_DELIVERY_COUNT = "__dlq.errors.delivery.count";
        public static final String HEADER_DLQ_ERRORS_MESSAGE = "__dlq.errors.message";

        public ProduceRequestHandler(
            ShareGroupDLQRecordParameter param,
            CompletableFuture<Void> result,
            long backoffMs,
            long backoffMaxMs,
            int maxRPCRetryAttempts
        ) {
            this.param = param;
            this.result = result;
            this.createTopicsBackoff = new ExponentialBackoffManager(
                maxRPCRetryAttempts,
                backoffMs,
                RETRY_BACKOFF_EXP_BASE,
                backoffMaxMs,
                RETRY_BACKOFF_JITTER
            );
            this.produceRequestBackoff = new ExponentialBackoffManager(
                maxRPCRetryAttempts,
                backoffMs,
                RETRY_BACKOFF_EXP_BASE,
                backoffMaxMs,
                RETRY_BACKOFF_JITTER
            );
        }

        @Override
        public void onComplete(ClientResponse response) {
            // We don't know if FIND_COORD or actual REQUEST. Let's err on side of request.
            if (response == null) {
                result.completeExceptionally(Errors.UNKNOWN_SERVER_ERROR.exception());
                sender.wakeup();
                return;
            }

            if (response.requestHeader().apiKey() == ApiKeys.CREATE_TOPICS) {
                handleCreateTopicsResponse(response);
            } else if (response.requestHeader().apiKey() == ApiKeys.PRODUCE) {
                handleProduceResponse(response);
            }

            sender.wakeup();
        }

        public String name() {
            return "ProduceRequestHandler";
        }

        /**
         * This method helps determine if the handler could
         * participate in batching (added to nodeMap). This will
         * be helpful if the RPCs which cannot be batched are included in
         * this class as well.
         *
         * @return Boolean indicating whether this handler can be coalesced with others
         * to reduce number of RPCs sent.
         */
        boolean isBatchable() {
            return true;
        }

        public void requestErrorResponse(Throwable exception) {
            this.result.completeExceptionally(exception);
        }

        public AbstractRequest.Builder<CreateTopicsRequest> createTopicBuilder() throws ConfigException {
            // Since the configs are dynamic - something might have changed, so revalidate.
            Optional<String> dlqTopic = cacheHelper.shareGroupDlqTopic(param.groupId());
            if (dlqTopic.isEmpty()) {
                throw new ConfigException(String.format("DLQ topic is not configured for share group %s.", param.groupId()));
            }

            CreateTopicsRequestData.CreatableTopicConfigCollection topicConfigs = new CreateTopicsRequestData.CreatableTopicConfigCollection();
            CreateTopicsRequestData.CreatableTopicConfig enableDLQConfig = new CreateTopicsRequestData.CreatableTopicConfig()
                .setName(TopicConfig.ERRORS_DEADLETTERQUEUE_GROUP_ENABLE_CONFIG)
                .setValue("true");
            topicConfigs.add(enableDLQConfig);

            CreateTopicsRequestData.CreatableTopicCollection topicCollection = new CreateTopicsRequestData.CreatableTopicCollection();
            topicCollection.add(new CreateTopicsRequestData.CreatableTopic()
                .setName(dlqTopic.get())
                .setReplicationFactor((short) -1)
                .setNumPartitions((short) -1)
                .setConfigs(topicConfigs));

            return new CreateTopicsRequest.Builder(new CreateTopicsRequestData()
                .setTopics(topicCollection));
        }

        public AbstractRequest.Builder<? extends AbstractRequest> requestBuilder() {
            throw new RuntimeException("Produce requests are batchable, hence individual requests not needed.");
        }

        public void populateDLQTopicData() throws ConfigException {
            Optional<String> dlqTopic = cacheHelper.shareGroupDlqTopic(param.groupId());
            if (dlqTopic.isEmpty()) {
                throw new ConfigException(String.format("DLQ topic is not configured for share group %s.", param.groupId()));
            }

            ShareGroupDLQMetadataCacheHelper.TopicPartitionData tpData = cacheHelper.topicPartitionData(dlqTopic.get());

            if (tpData.topicId().isEmpty()) {
                throw new ConfigException(String.format("DLQ topic id could not be found for share group %s with DLQ topic %s.", param.groupId(), dlqTopic.get()));
            }

            if (tpData.numPartitions().isEmpty()) {
                throw new ConfigException(String.format("DLQ topic partition count could not be found for share group %s with DLQ topic %s.", param.groupId(), dlqTopic.get()));
            }

            if (tpData.partitionLeaderNodes().isEmpty() || tpData.partitionLeaderNodes().size() != tpData.numPartitions().get()) {
                throw new ConfigException(String.format("DLQ topic partition leaders for share group %s with DLQ topic %s could not be found.", param.groupId(), dlqTopic.get()));
            }

            this.dlqDestinationPartition = param.topicIdPartition().partition() % tpData.numPartitions().get();
            this.dlqPartitionLeaderNode = tpData.partitionLeaderNodes().get(dlqDestinationPartition);

            if (this.dlqPartitionLeaderNode == null || this.dlqPartitionLeaderNode.equals(Node.noNode())) {
                throw new ConfigException(String.format("DLQ topic partition leader node for share group %s with DLQ topic %s and partition %d could not be found.", param.groupId(), dlqTopic.get(), dlqDestinationPartition));
            }

            this.dlqTopicPartitionData = tpData;
        }

        public ProduceRequestData.TopicProduceData topicProduceData() {
            Map<Long, Record> originalRecordData = maybeFetchRecordData();

            List<SimpleRecord> simpleRecords = new ArrayList<>();
            for (long i = param.firstOffset(); i <= param.lastOffset(); i++) {
                long timestamp = time.hiResClockMs();
                ByteBuffer key = null;
                ByteBuffer value = null;
                Record record = originalRecordData.get(i);
                if (record != null) {
                    key = record.hasKey() ? record.key() : null;
                    value = record.hasValue() ? record.value() : null;
                }
                simpleRecords.add(new SimpleRecord(timestamp, key, value, headers(i)));
            }

            MemoryRecords records = MemoryRecords.withRecords(
                Compression.NONE,
                simpleRecords.toArray(new SimpleRecord[]{})
            );

            // Update the metric to say a new request is created to se sent. This might not be the
            // actual RPC count as we coalesce the requests before sending.
            shareGroupMetrics.recordDLQProduce(param.groupId());

            return new ProduceRequestData.TopicProduceData()
                .setName(dlqTopicPartitionData.topicName())
                .setTopicId(dlqTopicPartitionData.topicId().get())
                .setPartitionData(List.of(
                    new ProduceRequestData.PartitionProduceData()
                        .setIndex(dlqDestinationPartition)  // partition
                        .setRecords(records)
                ));
        }

        public Node dlqPartitionLeaderNode() {
            return this.dlqPartitionLeaderNode;
        }

        public Optional<Throwable> validateDlqTopic() {
            Optional<String> topicNameOpt = cacheHelper.shareGroupDlqTopic(param.groupId());
            Optional<String> topicPrefix = cacheHelper.shareGroupDlqTopicPrefix();

            // Verify that DLQ topic for the share group is set and is correctly named.
            if (topicNameOpt.isEmpty()) {
                return Optional.of(new ConfigException(String.format("Configured DLQ topic name in share group: %s is empty.", param.groupId())));
            } else if (topicNameOpt.get().startsWith("__")) {
                return Optional.of(new ConfigException(String.format("Configured DLQ topic name in share group: %s cannot start with __, topic: %s.", param.groupId(), topicNameOpt.get())));
            }

            String topicName = topicNameOpt.get();

            // Verify that DLQ is enabled on a correctly named topic, configured on a share group.
            if (cacheHelper.containsTopic(topicName) && !cacheHelper.isDlqEnabledOnTopic(topicName)) {
                return Optional.of(new ConfigException(String.format("DLQ is not enabled on configured DLQ topic for share group: %s, topic: %s.", param.groupId(), topicName)));
            }

            // Verify that for a non-existent correctly named DLQ topic, auto create should be enabled.
            if (!cacheHelper.containsTopic(topicName) && !cacheHelper.isDlqAutoTopicCreateEnabled()) {
                return Optional.of(new ConfigException(String.format("DLQ topic does not exist and auto create is disabled on cluster for share group: %s, topic: %s.", param.groupId(), topicName)));
            }

            // Verify that if configured, the DLQ topic name prefix aligns with the topic name.
            return topicPrefix.map(prefix -> {
                if (!prefix.isEmpty() && !topicName.startsWith(prefix)) {
                    return new ConfigException(String.format("Configured DLQ topic name does not comply with the DLQ topic prefix in share group: %s, topic: %s, prefix: %s.", param.groupId(), topicName, prefix));
                }
                return null;
            });
        }

        public boolean dlqTopicExists() {
            Optional<String> shareGroupDlqTopic = cacheHelper.shareGroupDlqTopic(param.groupId());
            boolean isDlqTopicPresent = shareGroupDlqTopic.filter(cacheHelper::containsTopic).isPresent();
            if (isDlqTopicPresent) {
                try {
                    populateDLQTopicData();
                } catch (ConfigException e) {
                    return false;
                }
                addRequestToNodeMap(dlqPartitionLeaderNode, this);
            }
            return isDlqTopicPresent;
        }

        @Override
        public String toString() {
            return "ProduceRequestHandler(" +
                "param: " + param + "\n" +
                "dlqTopicData: " + dlqTopicPartitionData + "\n" +
                ")";
        }

        private Header[] headers(long offset) {
            List<Header> headers = new ArrayList<>();
            headers.add(new RecordHeader(HEADER_DLQ_ERRORS_TOPIC, recordTopic().getBytes(StandardCharsets.UTF_8)));
            headers.add(new RecordHeader(HEADER_DLQ_ERRORS_PARTITION, Integer.toString(param.topicIdPartition().partition()).getBytes(StandardCharsets.UTF_8)));
            headers.add(new RecordHeader(HEADER_DLQ_ERRORS_OFFSET, Long.toString(offset).getBytes(StandardCharsets.UTF_8)));
            headers.add(new RecordHeader(HEADER_DLQ_ERRORS_GROUP, param.groupId().getBytes(StandardCharsets.UTF_8)));
            param.deliveryCount().ifPresent(deliveryCount -> headers.add(
                new RecordHeader(HEADER_DLQ_ERRORS_DELIVERY_COUNT, Short.toString(deliveryCount).getBytes(StandardCharsets.UTF_8))));
            param.cause().ifPresent(cause -> {
                if (cause.getMessage() != null) {
                    headers.add(new RecordHeader(HEADER_DLQ_ERRORS_MESSAGE, cause.getMessage().getBytes(StandardCharsets.UTF_8)));
                }
            });

            return headers.toArray(new Header[0]);
        }

        private String recordTopic() {
            TopicIdPartition topicIdPartition = param.topicIdPartition();
            String recordTopicName = param.topicIdPartition().topic();
            if (recordTopicName == null || recordTopicName.isEmpty()) {
                // If topic name lookup fails, use topic id as a String in the header.
                recordTopicName = cacheHelper.topicName(param.topicIdPartition().topicId()).orElse(topicIdPartition.topicId().toString());
            }
            return recordTopicName;
        }

        // Visibility for testing
        Optional<Errors> checkResponseError(ClientResponse response) {
            if (response.hasResponse()) {
                return Optional.empty();
            }

            String dlqTopicName = cacheHelper.shareGroupDlqTopic(param.groupId()).orElse("<UNKNOWN>");

            LOG.debug("Response for RPC for handler {} with DLQ topic {} is invalid - {}.", this, dlqTopicName, response);

            if (response.authenticationException() != null) {
                LOG.error("Authentication exception.", response.authenticationException());
                Errors error = Errors.forException(response.authenticationException());
                return Optional.of(error);
            } else if (response.versionMismatch() != null) {
                LOG.error("Version mismatch exception.", response.versionMismatch());
                Errors error = Errors.forException(response.versionMismatch());
                return Optional.of(error);
            } else if (response.wasDisconnected()) {    // Retriable
                return Optional.of(Errors.NETWORK_EXCEPTION);
            } else if (response.wasTimedOut()) {    // Retriable
                LOG.debug("Response for RPC for handler {} with DLQ topic {} timed out - {}.", this, dlqTopicName, response);
                return Optional.of(Errors.REQUEST_TIMED_OUT);
            } else {
                return Optional.of(Errors.UNKNOWN_SERVER_ERROR);
            }
        }

        private void handleCreateTopicsResponse(ClientResponse response) {
            LOG.debug("Received CreateTopicsResponse {}.", response);
            createTopicsBackoff.incrementAttempt();
            Errors clientResponseError = checkResponseError(response).orElse(Errors.NONE);
            String clientResponseErrorMessage = clientResponseError.message();
            String dlqTopicName = cacheHelper.shareGroupDlqTopic(param.groupId()).orElse("<UNKNOWN>");

            switch (clientResponseError) {
                case NONE:
                    // Topic has been created
                    CreateTopicsResponse createTopicsResponse = ((CreateTopicsResponse) response.responseBody());
                    Optional<CreateTopicsResponseData.CreatableTopicResult> topicResultOpt = createTopicsResponse.data().topics().stream().findFirst();
                    if (topicResultOpt.isEmpty()) {
                        LOG.error("DLQ topic not found in create topic response {}.", dlqTopicName);
                        requestErrorResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION.exception());
                        break;
                    }

                    CreateTopicsResponseData.CreatableTopicResult topicResult = topicResultOpt.get();
                    Errors error = Errors.forCode(topicResult.errorCode());
                    String errorMessage = topicResult.errorMessage();
                    switch (error) {
                        case NONE:
                            try {
                                populateDLQTopicData();
                                createTopicsBackoff.resetAttempts();
                                if (this.isBatchable()) {
                                    addRequestToNodeMap(this.dlqPartitionLeaderNode, this);
                                } else {
                                    enqueue(this);
                                }
                            } catch (ConfigException e) {
                                LOG.error("Error enqueueing after DLQ create topic response {}.", this, e);
                                if (!createTopicsBackoff.canAttempt()) {
                                    LOG.error("Exhausted max retries while populating DLQ topic for {} using DLQ topic {} without success.", name(), dlqTopicName);
                                    requestErrorResponse(new Exception("Exhausted max retries while populating DLQ topic without success."));
                                    break;
                                }
                                timer.add(new ShareGroupDLQTimerTask(createTopicsBackoff.backOff(), this));
                            }
                            break;

                        case TOPIC_ALREADY_EXISTS:
                            // When topic creation request was sent, it could be that it a previous request
                            // was in-flight. As such this request might get TOPIC_ALREADY_EXISTS error, which is acceptable
                            // let it try again and sender logic will take care of it.
                        case THROTTLING_QUOTA_EXCEEDED:
                            LOG.debug("Received retriable error in create DLQ topic response for {} using DLQ topic {}: {}.", name(), dlqTopicName, errorMessage);
                            if (!createTopicsBackoff.canAttempt()) {
                                LOG.error("Exhausted max retries to create DLQ topic for {} using DLQ topic {} without success.", name(), dlqTopicName);
                                requestErrorResponse(new Exception("Exhausted max retries to create DLQ topic without success."));
                                break;
                            }
                            timer.add(new ShareGroupDLQTimerTask(createTopicsBackoff.backOff(), this));
                            break;

                        default:
                            LOG.error("Unable to create DLQ topic for {} using DLQ topic {}: {}.", name(), dlqTopicName, errorMessage);
                            requestErrorResponse(error.exception());
                    }
                    break;

                case NETWORK_EXCEPTION: // Retriable client response error codes.
                case REQUEST_TIMED_OUT:
                    LOG.debug("Received retriable error in create topics client response for {} using DLQ topic {} due to {}.", name(), dlqTopicName, clientResponseErrorMessage);
                    if (!createTopicsBackoff.canAttempt()) {
                        LOG.error("Exhausted max retries to create DLQ topic due to error in client response for {} using DLQ topic {}.", name(), dlqTopicName);
                        requestErrorResponse(clientResponseError.exception());
                        break;
                    }
                    timer.add(new ShareGroupDLQTimerTask(createTopicsBackoff.backOff(), this));
                    break;

                default:
                    LOG.error("Unable to create DLQ topic due to error in client response for {} using DLQ topic {}: {}.", name(), dlqTopicName, clientResponseError.code());
                    requestErrorResponse(clientResponseError.exception());
            }
        }

        private void handleProduceResponse(ClientResponse response) {
            LOG.debug("Received ProduceRequestResponse {}.", response);
            produceRequestBackoff.incrementAttempt();
            Errors clientResponseError = checkResponseError(response).orElse(Errors.NONE);
            String clientResponseErrorMessage = clientResponseError.message();

            switch (clientResponseError) {
                case NONE:
                    // Produce response received
                    ProduceResponse produceResponse = ((ProduceResponse) response.responseBody());
                    ProduceResponseData.TopicProduceResponseCollection produceResponseCollection = produceResponse.data().responses();
                    if (produceResponseCollection.isEmpty()) {
                        LOG.error("Received empty produce response for {} to dlq topic node {}.", this, dlqPartitionLeaderNode());
                        requestErrorResponse(Errors.UNKNOWN_SERVER_ERROR.exception());
                        break;
                    }

                    ProduceResponseData.TopicProduceResponse topicProduceResponse = produceResponseCollection.find(
                        new ProduceResponseData.TopicProduceResponse()
                            .setTopicId(dlqTopicPartitionData.topicId().get())
                    );
                    if (topicProduceResponse == null ||
                        topicProduceResponse.partitionResponses().isEmpty()
                    ) {
                        LOG.error("Received empty topic produce response {} to dlq topic node {}.", this, dlqPartitionLeaderNode());
                        requestErrorResponse(Errors.UNKNOWN_SERVER_ERROR.exception());
                        break;
                    }

                    List<ProduceResponseData.PartitionProduceResponse> partitionResponses = topicProduceResponse.partitionResponses();
                    ProduceResponseData.PartitionProduceResponse partitionResponse = partitionResponses.stream().filter(res -> res.index() == dlqDestinationPartition)
                        .findFirst()
                        .orElse(null);

                    if (partitionResponse == null) {
                        LOG.error("Received empty partition produce response {} to dlq topic node {}.", this, dlqPartitionLeaderNode());
                        requestErrorResponse(Errors.UNKNOWN_SERVER_ERROR.exception());
                        break;
                    }

                    Errors error = Errors.forCode(partitionResponse.errorCode());
                    String errorMessage = partitionResponse.errorMessage();
                    switch (error) {
                        case NONE:
                            LOG.debug("Successfully produced records {} to dlq topic node {}.", this, dlqPartitionLeaderNode());
                            shareGroupMetrics.recordDLQRecordWrite(param.groupId(), (int) (param.lastOffset() - param.firstOffset() + 1));
                            produceRequestBackoff.resetAttempts();
                            this.result.complete(null);
                            break;

                        case NOT_LEADER_OR_FOLLOWER:
                            LOG.debug("Received retriable error produce response for {} to dlq topic node {} - {}.", this, dlqPartitionLeaderNode(), errorMessage);
                            if (!produceRequestBackoff.canAttempt()) {
                                LOG.error("Exhausted max retries to produce {} to  DLQ topic node {}.", this, dlqPartitionLeaderNode());
                                shareGroupMetrics.recordDLQProduceFailed(param.groupId());
                                requestErrorResponse(new Exception("Exhausted max retries to produce to DLQ topic without success."));
                                break;
                            }
                            timer.add(new ShareGroupDLQTimerTask(produceRequestBackoff.backOff(), this));
                            break;

                        default:
                            LOG.error("Unable to produce {} to DLQ topic node {} - {}.", this, dlqPartitionLeaderNode(), errorMessage);
                            partitionResponse.recordErrors().forEach(recordError ->
                                LOG.error("Records with errors {} - {}.", recordError.batchIndex(), recordError.batchIndexErrorMessage()));
                            shareGroupMetrics.recordDLQProduceFailed(param.groupId());
                            requestErrorResponse(error.exception());
                    }
                    break;

                case NETWORK_EXCEPTION: // Retriable client response error codes.
                case REQUEST_TIMED_OUT:
                    LOG.debug("Received retriable error produce client response for {} for DLQ node {} due to {}.",
                        param, dlqPartitionLeaderNode(), clientResponseErrorMessage);
                    if (!produceRequestBackoff.canAttempt()) {
                        LOG.error("Exhausted max retries to produce {} to  DLQ topic node {} due to client response error {}.",
                            param, dlqPartitionLeaderNode(), clientResponseErrorMessage);
                        shareGroupMetrics.recordDLQProduceFailed(param.groupId());
                        requestErrorResponse(clientResponseError.exception());
                        break;
                    }
                    timer.add(new ShareGroupDLQTimerTask(produceRequestBackoff.backOff(), this));
                    break;

                default:
                    LOG.error("Unable to produce {} to DLQ topic node {} due to client response error {}.",
                        param, dlqPartitionLeaderNode(), clientResponseErrorMessage);
                    shareGroupMetrics.recordDLQProduceFailed(param.groupId());
                    requestErrorResponse(clientResponseError.exception());
            }
        }

        private Map<Long, Record> maybeFetchRecordData() {
            if (!cacheHelper.isShareGroupDlqCopyRecordEnabled(param.groupId())) {
                return Map.of();
            }
            long startTime = time.hiResClockMs();
            TopicIdPartition tp = param.topicIdPartition();

            FetchParams fetchParams = new FetchParams(
                FetchRequest.CONSUMER_REPLICA_ID,           // -1, reading as a consumer
                -1,                                         // replicaEpoch
                0L,                                         // maxWaitMs - don't block
                1,                                          // minBytes
                DLQ_MAX_FETCH_BYTES,                        // maxBytes
                FetchIsolation.HIGH_WATERMARK,              // committed only
                Optional.empty()                            // clientMetadata
            );

            long nextOffset = param.firstOffset();
            long endOffset = param.lastOffset();
            int recordCount = (int) (param.lastOffset() - param.firstOffset() + 1);

            Map<Long, Record> recordMap = new HashMap<>(recordCount);
            LinkedHashMap<TopicIdPartition, Long> offsets = new LinkedHashMap<>();
            LinkedHashMap<TopicIdPartition, Integer> maxBytesMap = new LinkedHashMap<>();
            maxBytesMap.put(tp, DLQ_MAX_FETCH_BYTES);

            // We are fetching data for one TopicIdPartition only. Hence, there
            // is no need to keep recreating the maxBytes map, and we can re-use a
            // single copy. In similar vein, we needn't clear the offsets map
            // either and just update the value corresponding to the TopicIdPartition
            // key in offsets map within the while loop.
            while (nextOffset <= endOffset) {
                long readFrom = nextOffset; // offset requested for this iteration
                offsets.put(tp, readFrom);

                LinkedHashMap<TopicIdPartition, LogReadResult> result =
                    logReader.read(fetchParams, Set.of(tp), offsets, maxBytesMap);

                LogReadResult res = result.get(param.topicIdPartition());
                if (res == null || res.error().code() != Errors.NONE.code()) {
                    log.warn("Unable to fetch actual record at offset {} for handler {}.", readFrom, this);
                    return Map.of();
                }

                res.info().delayedRemoteStorageFetch.ifPresent(data -> log.info(
                    "Some offset data in is in remote storage. Skipping it."));

                for (RecordBatch batch : res.info().records.batches()) {
                    for (Record record : batch) {
                        // A fetch can return a batch whose base offset is below the requested
                        // offset, so skip any record at or before the read position to avoid
                        // re-processing and dragging nextOffset backwards.
                        if (record.offset() < readFrom) continue;
                        if (record.offset() > param.lastOffset()) {
                            log.trace("Preempted log fetch took {} ms for {} records starting at {} for {}", time.hiResClockMs() - startTime,
                                recordCount, param.firstOffset(), this);
                            return Map.copyOf(recordMap);
                        }
                        recordMap.put(record.offset(), record);
                        nextOffset = Math.max(nextOffset, record.offset() + 1); // never moves backwards
                    }
                }

                // If the read position did not advance this iteration we have made no progress
                // (reached HWM/LEO or only stale records were returned). Bail out to guarantee
                // termination rather than re-fetching the same offset forever.
                if (nextOffset <= readFrom) break;
            }
            log.trace("Full log fetch took {} ms for {} records starting at {} for {}", time.hiResClockMs() - startTime,
                recordCount, param.firstOffset(), this);
            log.info("Total offsets fetched: {}, Records found: {}", recordCount, recordMap.size());
            return Map.copyOf(recordMap);
        }
    }

    private class SendThread extends InterBrokerSendThread {
        private final ConcurrentLinkedQueue<ShareGroupDLQStateManager.ProduceRequestHandler> queue = new ConcurrentLinkedQueue<>();
        private final Random random;

        SendThread(String name, KafkaClient client, int requestTimeoutMs, Time time, boolean isInterruptible, Random random) {
            super(name, client, requestTimeoutMs, time, isInterruptible);
            this.random = random;
        }

        @Override
        public Collection<RequestAndCompletionHandler> generateRequests() {
            List<RequestAndCompletionHandler> requests = new ArrayList<>();

            if (!queue.isEmpty()) {
                ShareGroupDLQStateManager.ProduceRequestHandler handler = queue.poll();
                // At this point either a correctly named and configured DLQ topic exists or
                // one is configured but does non-exist. We have already validated that the
                // auto create should be enabled, in that case.
                if (!handler.dlqTopicExists()) {
                    // We need to send RPC to create the topic
                    Node randomNode = randomNode();
                    if (randomNode == Node.noNode()) {
                        log.error("Unable to find node to send create topic request for handler {}.", handler);
                        // fatal failure, cannot retry or progress
                        // fail the RPC
                        handler.requestErrorResponse(Errors.BROKER_NOT_AVAILABLE.exception());
                        return List.of();
                    }

                    try {
                        AbstractRequest.Builder<CreateTopicsRequest> builder = handler.createTopicBuilder();
                        return List.of(new RequestAndCompletionHandler(
                            time.milliseconds(),
                            randomNode,
                            builder,
                            handler
                        ));
                    } catch (ConfigException exp) {
                        log.error("Unable to create topic request for handler {}.", handler, exp);
                        handler.requestErrorResponse(Errors.INVALID_CONFIG.exception());
                    }
                } else {
                    if (!handler.isBatchable()) {
                        requests.add(new RequestAndCompletionHandler(
                            time.milliseconds(),
                            handler.dlqPartitionLeaderNode(),
                            handler.requestBuilder(),
                            handler
                        ));
                    }
                }
            }

            // {
            //  node1: {
            //      [P1, P2, P3]
            //  },
            //  node2: {
            //.     [P4, P5]
            //  }, ...
            // }
            // For a sequence of produce RPCs, the flow would be:
            // 1. 1st produce request arrives.
            // 2. it is enqueued in the send thread.
            // 3. wakeup event causes the generate requests to create the DLQ topic if required.
            // 4. it will cause either RPC or cache lookup.
            // 5. once complete, the produce handler is added to the nodeMap for batching and not the queue.
            // 6. wakeup event causes generateRequests to iterate over the map and send the produce request (P1) and
            // remove node from the nodeMap and add it to inFlight.
            // 7. until P1 completes, more produce requests (P2, P3, ...) could come in and get added to the nodeMap as per point 3, 4, 5.
            // 8. if these belong to same node as P1. They will not be sent as the membership test with inFlight will pass.
            // 9. when P1 completes, it will clear inFlight and raise wakeup event.
            // 10. at this point P2, P3, etc. could be sent as a combined request thus achieving batching.
            final Set<Node> sending = new HashSet<>();
            final Set<Node> emptyNodes = new HashSet<>();   // Nodes for which no coalesced handler was found.
            synchronized (nodeMapLock) {
                nodeRPCMap.forEach((destNode, handlers) -> {
                    // this condition causes requests of same type and same destination node
                    // to not be sent immediately but get batched
                    if (!inFlight.contains(destNode)) {
                        CoalesceResults results = coalesceProduceRequests(handlers);
                        if (results.liveHandlers.isEmpty()) {
                            emptyNodes.add(destNode);
                            return;
                        }
                        requests.add(new RequestAndCompletionHandler(
                            time.milliseconds(),
                            destNode,
                            results.request,
                            response -> {
                                inFlight.remove(destNode);

                                // now the combined request has completed
                                // we need to create responses for individual
                                // requests which composed the combined request
                                results.liveHandlers.forEach(handler -> handler.onComplete(response));
                                wakeup();
                            }));
                        sending.add(destNode);
                    }
                });

                emptyNodes.forEach(nodeRPCMap::remove);
                sending.forEach(node -> {
                    // we need to add these nodes to inFlight
                    inFlight.add(node);

                    // remove from nodeMap
                    nodeRPCMap.remove(node);
                });
            } // close of synchronized context

            return requests;
        }

        public void enqueue(ShareGroupDLQStateManager.ProduceRequestHandler handler) {
            Optional<Throwable> exp = handler.validateDlqTopic();
            if (exp.isPresent()) {
                handler.requestErrorResponse(exp.get());
                return;
            }
            queue.add(handler);
            wakeup();
        }

        private Node randomNode() {
            List<Node> nodes = cacheHelper.getClusterNodes();
            if (nodes == null || nodes.isEmpty()) {
                return Node.noNode();
            }
            return nodes.get(random.nextInt(nodes.size()));
        }
    }

    private final class ShareGroupDLQTimerTask extends TimerTask {
        private final ProduceRequestHandler handler;

        ShareGroupDLQTimerTask(long delayMs, ProduceRequestHandler handler) {
            super(delayMs);
            this.handler = handler;
        }

        @Override
        public void run() {
            sender.enqueue(handler);
            sender.wakeup();
        }
    }

    // Visibility for tests
    record CoalesceResults(
        AbstractRequest.Builder<? extends AbstractRequest> request,
        List<ProduceRequestHandler> liveHandlers
    ) {
    }

    // Visibility for tests
    static CoalesceResults coalesceProduceRequests(List<ProduceRequestHandler> handlers) {
        // Above handlers are destined for the same broker node - it could be for different DLQ topics and partitions
        // but the same broker node. Now the produce request requires each topic data request to be
        // scoped to a specific topic/topicId and the partition data could have all the record information
        // and the destination DLQ partition. To accomplish this, we will map handlers by DLQ topic id.
        Map<Uuid, ProduceRequestData.TopicProduceData> produceHandlerMap = new HashMap<>();
        List<ProduceRequestHandler> liveHandlers = new ArrayList<>(handlers.size());
        handlers.forEach(handler -> {
            try {
                ProduceRequestData.TopicProduceData topicProduceData = handler.topicProduceData();
                produceHandlerMap.computeIfAbsent(topicProduceData.topicId(), topicId ->
                    new ProduceRequestData.TopicProduceData()
                        .setName(topicProduceData.name())
                        .setTopicId(topicId)
                ).partitionData().addAll(topicProduceData.partitionData());
                liveHandlers.add(handler);
            } catch (Exception exception) {
                log.error("Unable to coalesce ProduceRequestData for handler {}. It will be skipped from DLQ.", handler, exception);
                handler.requestErrorResponse(exception);
            }
        });

        ProduceRequestData data = new ProduceRequestData()
            .setTopicData(new ProduceRequestData.TopicProduceDataCollection(produceHandlerMap.values().iterator()))
            .setAcks((short) -1)  // all replicas
            .setTimeoutMs(ServerConfigs.REQUEST_TIMEOUT_MS_DEFAULT);

        return new CoalesceResults(
            new ProduceRequest.Builder(ApiKeys.PRODUCE.latestVersion(), ApiKeys.PRODUCE.latestVersion(), data),
            liveHandlers
        );
    }
}
