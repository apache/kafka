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

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.DynamicClientConfigUpdater;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.requests.DescribeConfigsRequest;
import org.apache.kafka.common.requests.DescribeConfigsResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

/**
 * Handles the request and response of a dynamic client configuration update for the consumer
 */
public class DynamicConsumerConfig extends DynamicClientConfigUpdater {
    /* Client to use */
    private ConsumerNetworkClient client;

    /* Configs to update */
    private GroupRebalanceConfig rebalanceConfig;

    /* Object to synchronize on when response is recieved */
    Object lock;

    /* Logger to use */
    private Logger log;

    /* The resource name to use when constructing a DescribeConfigsRequest */
    private final String clientId;

    /* Dynamic Configs recieved from the previous DescribeConfigsResponse */
    private Map<String, String> previousDynamicConfigs;

    /* Indicates if we have recieved the initial dynamic configurations */
    private boolean initialConfigsFetched;

    public DynamicConsumerConfig(ConsumerNetworkClient client, Object lock, GroupRebalanceConfig config, Time time, LogContext logContext) {
        super(time);
        this.rebalanceConfig = config;
        this.log = logContext.logger(DynamicConsumerConfig.class);
        this.client = client;
        this.lock = lock;
        this.clientId = rebalanceConfig.clientId;
        this.previousDynamicConfigs = new HashMap<>();
        this.initialConfigsFetched = false;
    }
    
    /**
     * Send a {@link DescribeConfigsRequest} to a node specifically for dynamic client configurations
     *
     * @return {@link RequestFuture} 
     */ 
    public RequestFuture<ClientResponse> maybeFetchInitialConfigs() {
        if (!initialConfigsFetched) {
            Node node = null;
            while (node == null) {
                node = client.leastLoadedNode();
            }
            log.info("Trying to fetch initial dynamic configs before join group request");
            RequestFuture<ClientResponse> configsFuture = client.send(node, newRequestBuilder(this.clientId));
            return configsFuture;
        }
        return null;
    }

    /**
     * Block for a {@link DescribeConfigsResponse} and process it. Used to fetch the initial dynamic configurations synchronously before sending the initial
     * {@link org.apache.kafka.common.requests.JoinGroupRequest}. Since this join RPC sends the group member's session timeout
     * to the group coordinator, we should check if a dynamic configuration for session timeout is set before joining.
     * If we do not do this initial fetch synchronously, then we could possibly trigger an unnecessary group rebalance operation by 
     * sending a second join request after the dynamic configs are recieved asynchronously.
     *
     * @param responseFuture - future to block on
     * @return true if responseFuture was blocked on and a response was recieved
     */
    public boolean maybeWaitForInitialConfigs(RequestFuture<ClientResponse> responseFuture) {
        if (responseFuture != null) {
            client.poll(responseFuture);
            if (responseFuture.isDone()) {
                DescribeConfigsResponse configsResponse = (DescribeConfigsResponse) responseFuture.value().responseBody();
                handleSuccessfulResponse(configsResponse);
                this.initialConfigsFetched = true;
                return true;
            }
        }
        return false;
    }

    /**
     * Maybe send a {@link DescribeConfigsRequest} to a node specifically for dynamic client configurations and 
     * don't block waiting for a response. This will be used by the HeartbeatThread to periodically fetch dynamic configurations
     *
     * @param node Node to send request to
     * @param now  Current time in milliseconds
     */ 
    @Override
    public boolean maybeFetchConfigs(long now) {
        if (shouldUpdateConfigs(now)) {
            Node node = client.leastLoadedNode();
            // Order matters, if the node is null we should not set updateInProgress to true.
            // This is lazily evaluated so it is ok as long as order is kept
            if (node != null && client.ready(node, now)) {
                updateInProgress();
                log.info("Sending periodic describe configs request for dynamic config update");
                RequestFuture<ClientResponse> configsFuture = client.send(node, newRequestBuilder(this.clientId));
                configsFuture.addListener(new RequestFutureListener<ClientResponse>() {
                    @Override
                    public void onSuccess(ClientResponse resp) {
                        synchronized (lock) {
                            DescribeConfigsResponse configsResponse = (DescribeConfigsResponse) resp.responseBody();
                            handleSuccessfulResponse(configsResponse);
                            update();
                        }
                    }
                    @Override
                    public void onFailure(RuntimeException e) {
                        synchronized (lock) {
                            retry();
                        }
                    }
                });
                return true;
            }
        }
        return false;
    }

    /**
     * Handle the {@link DescribeConfigsResponse} by processing the dynamic configs and resetting the RPC timer,
     * or by disabling this feature if the broker is incompatible.
     * @param resp {@link DescribeConfigsResponse}
     */
    private void handleSuccessfulResponse(DescribeConfigsResponse configsResponse) {
        Map<String, String> dynamicConfigs = createResultMapAndHandleErrors(configsResponse, log);
        log.info("DescribeConfigsResponse received");

        // We only want to process them if they have changed since the last time they were fetched.
        if (!dynamicConfigs.equals(previousDynamicConfigs)) {
            previousDynamicConfigs = dynamicConfigs;
            try {
                rebalanceConfig.setDynamicConfigs(dynamicConfigs);
            } catch (IllegalArgumentException e) {
                log.info("Rejecting dynamic configs: {}", e.getMessage());
            }
        }
        update();
    }
}
