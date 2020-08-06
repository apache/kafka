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
public class DynamicClientConfigUpdater {
    /* Interval to wait before requesting dynamic configs again */
    private final long dynamicConfigurationExpireMs;

    /* Last time dynamic configs were updated */
    private long lastSuccessfulUpdate;

    /* Still waiting for a response */
    private boolean updateInProgress;
    
    /* Clock to use */
    private Time time;

    /* Set to disable the dynamic client config feature */ 
    private boolean enable;

    /* Have initial dynamic configs been discovered */
    private boolean initialConfigsFetched;

    public DynamicClientConfigUpdater(long dynamicConfigurationExpireMs, boolean enable, Time time) {
        this.dynamicConfigurationExpireMs = dynamicConfigurationExpireMs;
        this.lastSuccessfulUpdate = 0;
        this.updateInProgress = false;
        this.time = time;
        this.enable = enable;
        this.initialConfigsFetched = false;
    }

    /**
     * Check if initial configs need to be fetched
     *
     * @param now
     * @return
     */
    public boolean shouldFetchInitialConfigs() {
        return enable && !initialConfigsFetched;
    }

    /**
     * Check if the current configs have expired, the feature is still enabled, and no config update is in progress
     * @param now current time in milliseconds
     * @return true if update is needed
     */
    public boolean shouldUpdateConfigs(long now) {
        return enable 
                && !updateInProgress
                && now > (dynamicConfigurationExpireMs + lastSuccessfulUpdate);
    }

    public void sentConfigsRequest() {
        this.updateInProgress = true;
    }

    public void receiveInitialConfigs() {
        initialConfigsFetched = true;
    }

    /**
     * To be called in {@link RequestCompletionHandler} on successful update
     * @param now current time in milliseconds
     */
    public void receiveConfigs() {
        updateInProgress = false;
        lastSuccessfulUpdate = time.milliseconds();
    }

    /**
     * To be called in {@link RequestCompletionHandler} on failed update
     */
    public void retry() {
        updateInProgress = false;
    }

    /**
     * @return {@link DescribeConfigsRequest.Builder}
     */
    public AbstractRequest.Builder<?> newRequestBuilder(String clientId) {
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
    public Map<String, String> createResultMapAndHandleErrors(DescribeConfigsResponse configsResponse, Logger log) {
        Map<String, String> dynamicConfigs = new HashMap<>();
        configsResponse.resultMap().entrySet().forEach(entry -> {
            if (entry.getValue().errorCode() == Errors.INVALID_REQUEST.code()) {
                log.info("DynamicConfiguration not compatible with the broker, this feature will be disabled");
                enable = false;
                return;
            }
            entry.getValue().configs().forEach(config -> dynamicConfigs.put(config.name(), config.value()));
        });

        return dynamicConfigs;
    }
}
