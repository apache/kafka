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
import org.apache.kafka.common.message.GetConfigProfileKeysRequestData;
import org.apache.kafka.common.message.GetConfigProfileKeysResponseData;
import org.apache.kafka.common.message.PushConfigRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.GetConfigProfileKeysRequest;
import org.apache.kafka.common.requests.GetConfigProfileKeysResponse;
import org.apache.kafka.common.requests.PushConfigRequest;
import org.apache.kafka.common.requests.PushConfigResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Default implementation of ClientConfigsSender that manages the config push handshake.
 * <p>
 * This implementation follows a simple state machine:
 * <pre>
 *   NOT_STARTED → PROFILE_KEYS_IN_PROGRESS → PUSH_IN_PROGRESS → COMPLETED/FAILED
 * </pre>
 */
public class DefaultClientConfigsSender implements ClientConfigsSender {

    private static final Logger log = LoggerFactory.getLogger(DefaultClientConfigsSender.class);

    @Override
    public void close() throws Exception {

    }

    private enum State {
        NOT_STARTED,               // Initial state, need to send GetConfigProfileKeys
        PROFILE_KEYS_IN_PROGRESS,  // Waiting for GetConfigProfileKeys response
        PUSH_IN_PROGRESS,          // Waiting for PushConfig response
        COMPLETED,                 // Successfully pushed config
        FAILED                     // Failed (but client continues)
    }

    private final AbstractConfig clientConfig;
    private volatile Uuid clientInstanceId = Uuid.ZERO_UUID;
    private volatile State state = State.NOT_STARTED;
    private volatile long configurationProfileCrc = -1L;
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
                log.debug("Creating GetConfigProfileKeys request");
                state = State.PROFILE_KEYS_IN_PROGRESS;
                return Optional.of(createGetConfigProfileKeysRequest());

            case PROFILE_KEYS_IN_PROGRESS:
                // Waiting for profile keys response, no new request to send
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
    public synchronized void handleResponse(GetConfigProfileKeysResponse response) {
        if (state != State.PROFILE_KEYS_IN_PROGRESS) {
            log.warn("Received GetConfigProfileKeys response in unexpected state: {}", state);
            return;
        }

        Errors error = response.error();
        if (error != Errors.NONE) {
            log.warn("GetConfigProfileKeys request failed with error: {} - {}",
                error, response.data().errorMessage());
            state = State.FAILED;
            return;
        }

        GetConfigProfileKeysResponseData data = response.data();

        // Store configuration profile CRC
        configurationProfileCrc = data.configurationProfileCrc();
        configMaxBytes = data.configMaxBytes();

        // Extract requested keys (now simple string array, not nested structure)
        requestedConfigKeys = new ArrayList<>(data.configKeys());

        log.debug("Config profile received: crc={}, maxBytes={}, keys={}",
            configurationProfileCrc, configMaxBytes, requestedConfigKeys.size());

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

        } else if (error == Errors.INVALID_CONFIG) {
            // Log error message from the response
            String errorMessage = response.data().errorMessage();
            if (errorMessage != null && !errorMessage.isEmpty()) {
                log.error("Configuration push failed: INVALID_CONFIG - {}", errorMessage);
            } else {
                log.error("Configuration push failed: INVALID_CONFIG (no details provided)");
            }
            state = State.FAILED;

        } else if (error == Errors.UNKNOWN_CONFIG_PROFILE) {
            log.warn("Configuration profile changed, retrying GetConfigProfileKeys");
            // Reset to retry once
            state = State.NOT_STARTED;
            configurationProfileCrc = -1L;
            requestedConfigKeys.clear();

        } else if (error == Errors.CONFIG_TOO_LARGE) {
            String errorMessage = response.data().errorMessage();
            log.error("Config payload too large, cannot retry: {}",
                errorMessage != null ? errorMessage : "");
            state = State.FAILED;

        } else {
            String errorMessage = response.data().errorMessage();
            log.warn("PushConfig failed with error: {} - {}",
                error, errorMessage != null ? errorMessage : "");
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
        if (state == State.PROFILE_KEYS_IN_PROGRESS || state == State.PUSH_IN_PROGRESS) {
            log.debug("Disconnected during config push handshake");
            state = State.FAILED;
        }
    }

    @Override
    public Uuid clientInstanceId() {
        return clientInstanceId;
    }

    private GetConfigProfileKeysRequest.Builder createGetConfigProfileKeysRequest() {
        // No fields in GetConfigProfileKeysRequest - client profile comes from ApiVersionsRequest context
        GetConfigProfileKeysRequestData requestData = new GetConfigProfileKeysRequestData();

        return new GetConfigProfileKeysRequest.Builder(requestData);
    }

    /**
     * Creates a PushConfig request with collected configuration.
     * This should only be called after receiving a successful GetConfigProfileKeys response.
     */
    private PushConfigRequest.Builder createPushConfigRequest() {
        log.debug("Collecting and preparing config push");

        // Collect configs using ConfigCollector
        List<PushConfigRequestData.Config> configs;
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
            .setConfigurationProfileCrc(configurationProfileCrc)
            .setConfigs(configs);

        return new PushConfigRequest.Builder(requestData);
    }

    /**
     * Checks if we need to send a PushConfig request.
     * This is true when we've received profile keys but haven't pushed yet.
     */
    synchronized boolean needsPushRequest() {
        return state == State.PUSH_IN_PROGRESS &&
            configurationProfileCrc != -1L &&
            !requestedConfigKeys.isEmpty();
    }
}
