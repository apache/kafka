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
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.ExponentialBackoffManager;
import org.apache.kafka.server.util.InterBrokerSendThread;
import org.apache.kafka.server.util.RequestAndCompletionHandler;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.server.util.timer.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Random;
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
    public static final long REQUEST_BACKOFF_MS = 1_000L;
    public static final long REQUEST_BACKOFF_MAX_MS = 30_000L;
    private static final int MAX_REQUEST_ATTEMPTS = 5;
    private static final int RETRY_BACKOFF_EXP_BASE = CommonClientConfigs.RETRY_BACKOFF_EXP_BASE;
    private static final double RETRY_BACKOFF_JITTER = CommonClientConfigs.RETRY_BACKOFF_JITTER;
    private static final Logger log = LoggerFactory.getLogger(ShareGroupDLQStateManager.class);

    public ShareGroupDLQStateManager(KafkaClient client, ShareGroupDLQMetadataCacheHelper cacheHelper, Time time, Timer timer) {
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

        this.time = time;
        this.timer = timer;
        this.cacheHelper = cacheHelper;
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
            isStarted.set(true);
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
        CompletableFuture<Void> future = new CompletableFuture<>();
        ProduceRequestHandler requestHandler = new ProduceRequestHandler(param, future, REQUEST_BACKOFF_MS, REQUEST_BACKOFF_MAX_MS, MAX_REQUEST_ATTEMPTS);
        enqueue(requestHandler);
        return future;
    }

    private void enqueue(ProduceRequestHandler requestHandler) {
        sender.enqueue(requestHandler);
    }

    private class ProduceRequestHandler implements RequestCompletionHandler {
        private final CompletableFuture<Void> result;
        private final ShareGroupDLQRecordParameter param;
        private static final Logger LOG = LoggerFactory.getLogger(ShareGroupDLQStateManager.ProduceRequestHandler.class);
        private final ExponentialBackoffManager createTopicsBackoff;

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
            } else {
                // handle the response
            }

            sender.wakeup();
        }

        public String name() {
            return "ProduceRequestHandler";
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
            return shareGroupDlqTopic.filter(cacheHelper::containsTopic).isPresent();
        }

        // Visibility for testing
        Optional<Errors> checkResponseError(ClientResponse response) {
            if (response.hasResponse()) {
                return Optional.empty();
            }

            String dlqTopicName = cacheHelper.shareGroupDlqTopic(param.groupId()).orElse("UNKNOWN");

            LOG.debug("Response for RPC {} with DLQ topic {} is invalid - {}", name(), dlqTopicName, response);

            if (response.authenticationException() != null) {
                LOG.error("Authentication exception", response.authenticationException());
                Errors error = Errors.forException(response.authenticationException());
                return Optional.of(error);
            } else if (response.versionMismatch() != null) {
                LOG.error("Version mismatch exception", response.versionMismatch());
                Errors error = Errors.forException(response.versionMismatch());
                return Optional.of(error);
            } else if (response.wasDisconnected()) {    // Retriable
                return Optional.of(Errors.NETWORK_EXCEPTION);
            } else if (response.wasTimedOut()) {    // Retriable
                LOG.debug("Response for RPC {} with DLQ topic {} timed out - {}.", name(), dlqTopicName, response);
                return Optional.of(Errors.REQUEST_TIMED_OUT);
            } else {
                return Optional.of(Errors.UNKNOWN_SERVER_ERROR);
            }
        }

        private void handleCreateTopicsResponse(ClientResponse response) {
            LOG.debug("Received CreateTopicsResponse {}", response);
            createTopicsBackoff.incrementAttempt();
            Errors clientResponseError = checkResponseError(response).orElse(Errors.NONE);
            String clientResponseErrorMessage = clientResponseError.message();
            String dlqTopicName = cacheHelper.shareGroupDlqTopic(param.groupId()).orElse("UNKNOWN");

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
                            // Replace with enqueue post PRODUCE implementation
                            this.result.complete(null);
                            break;

                        case TOPIC_ALREADY_EXISTS:
                            // When topic creation request was sent, it could be that it a previous request
                            // was in-flight. As such this request might get TOPIC_ALREADY_EXISTS error, which is acceptable
                            // let it try again and sender logic will take care of it.
                        case THROTTLING_QUOTA_EXCEEDED:
                            LOG.debug("Received retriable error in create DLQ topic response for {} using DLQ topic {}: {}", name(), dlqTopicName, errorMessage);
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
                    LOG.error("Unable to create DLQ topic due to error in client response for {} using DLQ topic {}: {}", name(), dlqTopicName, clientResponseError.code());
                    requestErrorResponse(clientResponseError.exception());
            }
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
            if (!queue.isEmpty()) {
                ShareGroupDLQStateManager.ProduceRequestHandler handler = queue.poll();
                // At this point either a correctly named and configured DLQ topic exists or
                // one is configured but does non-exist. We have already validated that the
                // auto create should be enabled, in that case.
                if (!handler.dlqTopicExists()) {
                    // We need to send RPC to create the topic
                    Node randomNode = randomNode();
                    if (randomNode == Node.noNode()) {
                        log.error("Unable to find node to send create topic request.");
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
                        log.error("Unable to create topic request.", exp);
                        handler.requestErrorResponse(Errors.INVALID_CONFIG.exception());
                    }
                }
            }
            return List.of();
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
}
