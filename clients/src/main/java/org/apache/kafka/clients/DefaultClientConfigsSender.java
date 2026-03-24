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
package org.apache.kafka.clients;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.message.GetConfigSubscriptionRequestData;
import org.apache.kafka.common.message.GetConfigSubscriptionResponseData;
import org.apache.kafka.common.message.PushConfigRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.GetConfigSubscriptionRequest;
import org.apache.kafka.common.requests.GetConfigSubscriptionResponse;
import org.apache.kafka.common.requests.PushConfigRequest;
import org.apache.kafka.common.requests.PushConfigResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Default implementation of ClientConfigsSender that manages the config push handshake.
 * <p>
 * This implementation follows a simple state machine:
 * <pre>
 *   NOT_STARTED → SUBSCRIPTION_IN_PROGRESS → PUSH_IN_PROGRESS → COMPLETED/FAILED
 * </pre>
 */
public class DefaultClientConfigsSender implements ClientConfigsSender {

    private static final Logger log = LoggerFactory.getLogger(DefaultClientConfigsSender.class);

    @Override
    public void close() throws Exception {

    }

    private enum State {
        NOT_STARTED,               // Initial state, need to send GetConfigSubscription
        SUBSCRIPTION_IN_PROGRESS,  // Waiting for GetConfigSubscription response
        PUSH_IN_PROGRESS,          // Waiting for PushConfig response
        COMPLETED,                 // Successfully pushed config
        FAILED                     // Failed (but client continues)
    }

    private final AbstractConfig clientConfig;
    private volatile Uuid clientInstanceId = Uuid.ZERO_UUID;
    private volatile State state = State.NOT_STARTED;
    private volatile int configSubscriptionId = -1;
    private volatile int configMaxBytes = 0;
    private volatile List<String> requestedConfigKeys = new ArrayList<>();

    public DefaultClientConfigsSender(AbstractConfig clientConfig) {
        this.clientConfig = clientConfig;
    }

    @Override
    public boolean shouldAttemptHandshake() {
        return state != State.COMPLETED && state != State.FAILED;
    }

    @Override
    public synchronized Optional<AbstractRequest.Builder<?>> createRequest() {
        switch (state) {
            case NOT_STARTED:
                log.debug("Creating GetConfigSubscription request");
                state = State.SUBSCRIPTION_IN_PROGRESS;
                return Optional.of(createGetConfigSubscriptionRequest());

            case SUBSCRIPTION_IN_PROGRESS:
                // Waiting for subscription response, no new request to send
                return Optional.empty();

            case PUSH_IN_PROGRESS:
                // Check if we have subscription details and need to send push request
                if (needsPushRequest()) {
                    PushConfigRequest.Builder builder = createPushConfigRequest();
                    if (builder != null) {
                        // Mark that we've created the push request
                        requestedConfigKeys.clear();  // Clear to avoid creating duplicate requests
                        return Optional.of(builder);
                    }
                }
                return Optional.empty();

            default:
                // Terminal states (COMPLETED, FAILED)
                return Optional.empty();
        }
    }

    @Override
    public synchronized void handleResponse(GetConfigSubscriptionResponse response) {
        if (state != State.SUBSCRIPTION_IN_PROGRESS) {
            log.warn("Received GetConfigSubscription response in unexpected state: {}", state);
            return;
        }

        Errors error = response.error();
        if (error != Errors.NONE) {
            log.warn("GetConfigSubscription request failed with error: {}", error);
            state = State.FAILED;
            return;
        }

        GetConfigSubscriptionResponseData data = response.data();

        // Store client instance ID if this was the first request
        Uuid receivedInstanceId = data.clientInstanceId();
        if (!receivedInstanceId.equals(Uuid.ZERO_UUID)) {
            clientInstanceId = receivedInstanceId;
            log.debug("Received client instance ID: {}", clientInstanceId);
        }

        // Store subscription details
        configSubscriptionId = data.subscriptionId();
        configMaxBytes = data.configMaxBytes();

        // Extract requested keys
        requestedConfigKeys = data.requestedKeys()
            .stream()
            .map(key -> key.name())
            .collect(Collectors.toList());

        log.debug("Config subscription received: subscriptionId={}, maxBytes={}, keys={}",
            configSubscriptionId, configMaxBytes, requestedConfigKeys.size());

        // Transition to next state - PushConfig will be created on next createRequest() call
        state = State.PUSH_IN_PROGRESS;
    }

    @Override
    public synchronized void handleResponse(PushConfigResponse response) {
        if (state != State.PUSH_IN_PROGRESS) {
            log.warn("Received PushConfig response in unexpected state: {}", state);
            return;
        }

        Errors error = response.error();

        if (error == Errors.NONE) {
            log.info("Configuration push completed successfully");
            state = State.COMPLETED;

        } else if (error == Errors.UNKNOWN_CONFIG_SUBSCRIPTION_ID) {
            log.warn("Subscription changed, retrying GetConfigSubscription");
            // Reset to retry once
            state = State.NOT_STARTED;
            configSubscriptionId = -1;
            requestedConfigKeys.clear();

        } else if (error == Errors.CONFIG_TOO_LARGE) {
            log.error("Config payload too large, cannot retry");
            state = State.FAILED;

        } else {
            log.warn("PushConfig failed with error: {}", error);
            state = State.FAILED;
        }
    }

    @Override
    public void handleFailedGetConfigsSubscriptionRequest(KafkaException kafkaException) {

    }

    @Override
    public void handleFailedPushConfigsRequest(KafkaException kafkaException) {

    }

    @Override
    public synchronized void handleDisconnect() {
        if (state == State.SUBSCRIPTION_IN_PROGRESS || state == State.PUSH_IN_PROGRESS) {
            log.debug("Disconnected during config push handshake");
            state = State.FAILED;
        }
    }

    @Override
    public Uuid clientInstanceId() {
        return clientInstanceId;
    }

    private GetConfigSubscriptionRequest.Builder createGetConfigSubscriptionRequest() {
        GetConfigSubscriptionRequestData requestData = new GetConfigSubscriptionRequestData()
            .setClientInstanceId(clientInstanceId);  // ZERO_UUID on first call

        return new GetConfigSubscriptionRequest.Builder(requestData);
    }

    /**
     * Creates a PushConfig request with collected configuration.
     * This should only be called after receiving a successful GetConfigSubscription response.
     */
    private PushConfigRequest.Builder createPushConfigRequest() {
        log.debug("Collecting and preparing config push");

//        // Collect configs using ConfigCollector
        List<PushConfigRequestData.ClientConfig> configs;
        try {
            configs = ConfigCollector.collectConfigs(
                clientConfig,
                requestedConfigKeys,
                configMaxBytes
            );
        } catch (Exception e) {
            log.error("Failed to collect configs for push", e);
            state = State.FAILED;
            return null;
        }

        // Build request
        PushConfigRequestData requestData = new PushConfigRequestData()
            .setClientInstanceId(clientInstanceId)
            .setSubscriptionId(configSubscriptionId)
            .setConfigs(configs);

        return new PushConfigRequest.Builder(requestData);
    }

    /**
     * Checks if we need to send a PushConfig request.
     * This is true when we've received a subscription but haven't pushed yet.
     */
    synchronized boolean needsPushRequest() {
        return state == State.PUSH_IN_PROGRESS &&
            configSubscriptionId != -1 &&
            !requestedConfigKeys.isEmpty();
    }
}
