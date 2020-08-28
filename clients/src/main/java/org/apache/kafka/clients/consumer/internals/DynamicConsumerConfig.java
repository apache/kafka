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
package org.apache.kafka.clients.consumer.internals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.DynamicClientConfigUpdater;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.requests.DescribeClientConfigsResponse;
import org.apache.kafka.common.requests.DescribeConfigsRequest;
import org.apache.kafka.common.requests.DescribeConfigsResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

/**
 * Handles the request and response of a dynamic client configuration update for the consumer
 */
public class DynamicConsumerConfig {
    /* Client to use */
    private ConsumerNetworkClient client;

    /* Configs to update */
    private GroupRebalanceConfig rebalanceConfig;

    /* Logger to use */
    private Logger log;

    /* The resource name to use when constructing a DescribeConfigsRequest */
    private final String clientId;

    /* Dynamic Configs recieved from the previous DescribeConfigsResponse */
    private Map<String, String> previousDynamicConfigs;

    private final DynamicClientConfigUpdater updater;

    private final List<String> supportedConfigs;

    public DynamicConsumerConfig(ConsumerNetworkClient client, GroupRebalanceConfig config, Time time, LogContext logContext) {
        this.rebalanceConfig = config;
        this.log = logContext.logger(DynamicConsumerConfig.class);
        this.client = client;
        this.clientId = rebalanceConfig.clientId;
        this.previousDynamicConfigs = new HashMap<>();
        this.supportedConfigs = new ArrayList<>();
        supportedConfigs.add(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG);
        supportedConfigs.add(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG);
        this.updater = new DynamicClientConfigUpdater(
            rebalanceConfig.dynamicConfigExpireMs, rebalanceConfig.dynamicConfigEnabled, time
        );
    }
    
    /**
     * Send a {@link DescribeConfigsRequest} to a node specifically for dynamic client configurations and
     * block for a {@link DescribeConfigsResponse}. Used to fetch the initial dynamic configurations synchronously 
     * before sending the initial {@link org.apache.kafka.common.requests.JoinGroupRequest}. 
     * Since this join RPC sends the group member's session timeout to the group coordinator, 
     * it's best to check if a dynamic configuration for session timeout is set before joining.
     * If this initial fetch is not done synchronously, then an unnecessary group rebalance operation could be triggered by 
     * sending a second join request after the dynamic configs are recieved asynchronously.
     *
     * @return true if the {@link DescribeConfigsResponse} was recieved and processed
     */ 
    public void maybeFetchInitialConfigs(long now) {
        if (updater.shouldFetchInitialConfigs()) {
            Node node = client.leastLoadedNode();
            if (node != null && client.ready(node, now)) {
                log.info("Trying to fetch initial dynamic configs before join group request");
                RequestFuture<ClientResponse> configsFuture = client.send(node, updater.newRequestBuilder(this.clientId, this.supportedConfigs));
                updater.sentConfigsRequest();
                client.poll(configsFuture);
                if (configsFuture.isDone()) {
                    DescribeClientConfigsResponse configsResponse = (DescribeClientConfigsResponse) configsFuture.value().responseBody();
                    updater.receiveInitialConfigs();
                    handleConfigsResponse(configsResponse);
                }
            }
        }
    }

    /**
     * Maybe send a {@link DescribeConfigsRequest} to a node specifically for dynamic client configurations and 
     * don't block waiting for a response. This will be used by the HeartbeatThread to periodically fetch dynamic configurations
     *
     * @param node Node to send request to
     * @param now  Current time in milliseconds
     */ 
    public RequestFuture<ClientResponse> maybeFetchConfigs(long now) {
        if (updater.shouldUpdateConfigs(now)) {
            Node node = client.leastLoadedNode();
            if (node != null && client.ready(node, now)) {
                log.info("Sending periodic describe configs request for dynamic config update");
                RequestFuture<ClientResponse> configsFuture = client.send(node, updater.newRequestBuilder(this.clientId, this.supportedConfigs));
                updater.sentConfigsRequest();
                return configsFuture;
            }
        }
        return null;
    }

    public void handleFailedConfigsResponse() {
        updater.retry();
    }

    /**
     * Handle the {@link DescribeConfigsResponse} by processing the dynamic configs and resetting the RPC timer,
     * or by disabling this feature if the broker is incompatible.
     * @param resp {@link DescribeConfigsResponse}
     */
    public void handleConfigsResponse(DescribeClientConfigsResponse configsResponse) {
        Map<String, String> dynamicConfigs = updater.createResultMapAndHandleErrors(configsResponse, log);
        log.info("DescribeConfigsResponse received");
        updater.receiveConfigs();

        // We only want to process them if they have changed since the last time they were fetched.
        if (!dynamicConfigs.equals(previousDynamicConfigs)) {
            previousDynamicConfigs = dynamicConfigs;
            try {
                rebalanceConfig.setDynamicConfigs(dynamicConfigs, log);
            } catch (IllegalArgumentException e) {
                log.info("Rejecting dynamic configs: {}", e.getMessage());
            }
        }
    }
}
