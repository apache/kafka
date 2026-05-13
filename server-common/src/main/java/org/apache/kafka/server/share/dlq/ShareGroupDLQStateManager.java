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
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.server.util.InterBrokerSendThread;
import org.apache.kafka.server.util.RequestAndCompletionHandler;

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
        ProduceRequestHandler requestHandler = new ProduceRequestHandler(param, future);
        sender.enqueue(requestHandler);
        return future;
    }

    private abstract class ShareGroupDLQStateManagerHandler implements RequestCompletionHandler {
        private final ShareGroupDLQRecordParameter param;

        ShareGroupDLQStateManagerHandler(ShareGroupDLQRecordParameter param) {
            this.param = param;
        }

        protected abstract AbstractRequest.Builder<? extends AbstractRequest> requestBuilder();

        protected abstract CompletableFuture<? extends AbstractResponse> result();

        protected abstract String name();

        protected abstract void createTopicErrorResponse(Exception exception);

        protected AbstractRequest.Builder<CreateTopicsRequest> createTopicBuilder() {
            return new CreateTopicsRequest.Builder(new CreateTopicsRequestData());
        }

        public Optional<Throwable> validateDlqTopic() {
            Optional<String> topicNameOpt = cacheHelper.shareGroupDlqTopic(param.groupId());
            Optional<String> topicPrefix = cacheHelper.shareGroupDlqTopicPrefix();

            // Verify that DLQ topic for the share group is set and is correctly named.
            if (topicNameOpt.isEmpty()) {
                return Optional.of(new ConfigException(String.format("Configured DLQ topic name in share group: %s is empty.", param.groupId())));
            } else if (!topicNameOpt.get().startsWith("__")) {
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

        public ShareGroupDLQRecordParameter recordParam() {
            return param;
        }

        public boolean dlqTopicExists() {
            Optional<String> shareGroupDlqTopic = cacheHelper.shareGroupDlqTopic(param.groupId());
            return shareGroupDlqTopic.filter(cacheHelper::containsTopic).isPresent();
        }
    }

    private class ProduceRequestHandler extends ShareGroupDLQStateManagerHandler {
        private final CompletableFuture<Void> result;
        private static final Logger LOG = LoggerFactory.getLogger(ShareGroupDLQStateManager.ProduceRequestHandler.class);

        public ProduceRequestHandler(ShareGroupDLQRecordParameter param, CompletableFuture<Void> result) {
            super(param);
            this.result = result;
        }

        @Override
        protected AbstractRequest.Builder<? extends AbstractRequest> requestBuilder() {
            return null;
        }

        @Override
        protected CompletableFuture<? extends AbstractResponse> result() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        protected String name() {
            return "ProduceRequestHandler";
        }

        @Override
        protected void createTopicErrorResponse(Exception exception) {
            this.result.completeExceptionally(exception);
        }

        @Override
        public void onComplete(ClientResponse response) {

        }
    }

    private class SendThread extends InterBrokerSendThread {
        private final ConcurrentLinkedQueue<ShareGroupDLQStateManager.ShareGroupDLQStateManagerHandler> queue = new ConcurrentLinkedQueue<>();
        private final Random random;

        SendThread(String name, KafkaClient client, int requestTimeoutMs, Time time, boolean isInterruptible, Random random) {
            super(name, client, requestTimeoutMs, time, isInterruptible);
            this.random = random;
        }

        @Override
        public Collection<RequestAndCompletionHandler> generateRequests() {
            if (!queue.isEmpty()) {
                ShareGroupDLQStateManager.ShareGroupDLQStateManagerHandler handler = queue.poll();
                // At this point either a correctly named and configured DLQ topic exists or
                // one is configured but does non-exist. We have already validated that the
                // auto create should be enabled, in that case.
                if (!handler.dlqTopicExists()) {
                    // We need to send RPC to create the topic
                    Node randomNode = randomNode();
                    if (randomNode == Node.noNode()) {
                        log.error("Unable to find node to use for coordinator lookup.");
                        // fatal failure, cannot retry or progress
                        // fail the RPC
                        handler.createTopicErrorResponse(Errors.BROKER_NOT_AVAILABLE.exception());
                        return List.of();
                    }
                    return List.of(new RequestAndCompletionHandler(
                        time.milliseconds(),
                        randomNode,
                        handler.createTopicBuilder(),
                        handler
                    ));
                }
            }
            return List.of();
        }

        public void enqueue(ShareGroupDLQStateManager.ShareGroupDLQStateManagerHandler handler) {
            Optional<Throwable> exp = handler.validateDlqTopic();
            if (exp.isPresent()) {
                handler.result().completeExceptionally(exp.get());
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
}
