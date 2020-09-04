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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.DescribeClientConfigsRequest;
import org.apache.kafka.common.requests.DescribeClientConfigsResponse;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

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

    /* Set to enable the dynamic client config feature */ 
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
     * @return {@link DescribeClientConfigsRequest.Builder}
     */
    public AbstractRequest.Builder<?> newRequestBuilder(String clientId, List<String> supportedConfigs) {
        ClientQuotaFilterComponent clientIdComponent = ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.CLIENT_ID, clientId);
        ClientQuotaFilter filter = ClientQuotaFilter.containsOnly(Collections.singleton(clientIdComponent));
        supportedConfigs = initialConfigsFetched ? null : supportedConfigs;
        return new DescribeClientConfigsRequest.Builder(filter, supportedConfigs, true);
    }

    /**
     * Create the map of dynamic configs or set the feature to be disabled if
     * a {@link org.apache.kafka.common.errors.InvalidRequestException} error code is received from the broker
     * @param configsResponse
     * @param log
     * @return map of dynamic configs
     */
    public Map<String, String> createResultMapAndHandleErrors(DescribeClientConfigsResponse configsResponse, Logger log) {
        Map<String, String> dynamicConfigs = new HashMap<>();
        if (configsResponse.errorCode() == Errors.INVALID_REQUEST.code()) {
            log.info("The broker does not support dynamic client config requests.");
            enable = false;
        } else {
            dynamicConfigs = configsResponse.resultMap(log);
        }

        return dynamicConfigs;
    }
}
