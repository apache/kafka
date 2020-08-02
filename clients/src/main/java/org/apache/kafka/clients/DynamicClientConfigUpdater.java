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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.message.DescribeConfigsRequestData;
import org.apache.kafka.common.message.DescribeConfigsRequestData.DescribeConfigsResource;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.DescribeConfigsRequest;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.apache.kafka.common.requests.DescribeConfigsResponse;

/**
 * Handles the interval for which a dynamic client configuration request should be sent 
 */
public abstract class DynamicClientConfigUpdater {
    /* Interval to wait before requesting dynamic configs again */
    private final long dynamicConfigurationExpireMs;

    /* Atomic variables used for multithreaded access in the consumer */
    /* Last time dynamic configs were updated */
    private AtomicLong lastSuccessfulUpdate;

    /* Still waiting for a response */
    private AtomicBoolean updateInProgress;
    
    /* Clock to use */
    private Time time;

    /* If we recieve an error code when attempting to fetch configs this will get set */
    private boolean disable;

    public DynamicClientConfigUpdater(Time time) {
        this.dynamicConfigurationExpireMs = 30000;
        this.lastSuccessfulUpdate = new AtomicLong();
        this.updateInProgress = new AtomicBoolean();
        this.time = time;
        this.disable = false;
    }

    public void disable() {
        this.disable = true;
    }

    public boolean shouldDisable() {
        return this.disable;
    }

    /**
     * Send a {@link DescribeConfigsRequest} to the least loaded node requesting dynamic client configurations
     *
     * @param now  Current time in milliseconds
     * @return true if configs were fetched
     */ 
    public abstract boolean maybeFetchConfigs(long now);

    /**
     * Check if the current configs have expired
     * @param now current time in milliseconds
     * @return true if update is needed
     */
    protected boolean shouldUpdateConfigs(long now) {
        if (now > dynamicConfigurationExpireMs + lastSuccessfulUpdate.get() && !updateInProgress.get()) {
            return true;
        }
        return false;
    }

    protected void updateInProgress() {
        this.updateInProgress.set(true);
    }

    /**
     * To be called in {@link RequestCompletionHandler} on successful update
     * @param now current time in milliseconds
     */
    protected void update() {
        updateInProgress.set(false);
        lastSuccessfulUpdate.set(time.milliseconds());
    }

    /**
     * To be called in {@link RequestCompletionHandler} on failed update
     */
    protected void retry() {
        updateInProgress.set(false);
    }

    /**
     * @return {@link DescribeConfigsRequest.Builder}
     */
    protected AbstractRequest.Builder<?> newRequestBuilder(String clientId) {
        DescribeConfigsResource resource = new DescribeConfigsResource()
            .setResourceName(clientId)
            .setResourceType(ConfigResource.Type.CLIENT.id())
            .setConfigurationKeys(null);
        DescribeConfigsRequestData data = new DescribeConfigsRequestData();
        List<DescribeConfigsResource> resources = new ArrayList<>();
        resources.add(resource);
        data.setResources(resources);
        return new DescribeConfigsRequest.Builder(data);
    }

    /**
     * Create the map of dynamic configs or set the feature to be disabled if we 
     * recieve a {@link org.apache.kafka.common.errors.InvalidRequestException} error code from the broker
     * @param configsResponse
     * @param log
     * @return map of dynamic configs
     */
    protected Map<String, String> createResultMapAndHandleErrors(DescribeConfigsResponse configsResponse, Logger log) {
        Map<String, String> dynamicConfigs = new HashMap<>();
        configsResponse.resultMap().entrySet().forEach(entry -> {
            if (entry.getValue().errorCode() == Errors.INVALID_REQUEST.code()) {
                log.info("DynamicConfiguration not compatible with the broker, this feature will be disabled");
                disable();
                return;
            }
            entry.getValue().configs().forEach(config -> dynamicConfigs.put(config.name(), config.value()));
        });

        return dynamicConfigs;
    }
}
