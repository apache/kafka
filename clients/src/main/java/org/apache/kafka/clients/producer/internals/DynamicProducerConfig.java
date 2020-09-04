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
package org.apache.kafka.clients.producer.internals;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.DynamicClientConfigUpdater;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.requests.DescribeClientConfigsResponse;
import org.apache.kafka.common.requests.DescribeConfigsRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

/**
 * Handles the request and response of a dynamic client configuration update for the producer
 */
public class DynamicProducerConfig {
    /* CLient to use */
    private KafkaClient client;

    /* Timeout to use */
    private int requestTimeoutMs;

    /* Logger to use */
    private Logger log;

    /* User provided configs */
    private final Map<String, Object> originals;

    /* The configs recieved from the last DescribeConfigsRequest */
    private Map<String, String> previousDynamicConfigs;

    /* User provided configs overlayed with the current dynamic configs */
    private ProducerConfig updatedConfigs;

    /* Resource name to use when constructing a DescribeConfigsRequest */
    private final String clientId;

    private final DynamicClientConfigUpdater updater;

    private final List<String> supportedConfigs;

    public DynamicProducerConfig(KafkaClient client, ProducerConfig config, Time time, LogContext logContext, int requestTimeoutMs) {
        this.client = client;
        this.originals = config.originals();
        this.requestTimeoutMs = requestTimeoutMs;
        this.log = logContext.logger(DynamicProducerConfig.class);
        this.updatedConfigs = config;
        this.clientId = config.getString(ProducerConfig.CLIENT_ID_CONFIG);
        this.previousDynamicConfigs = new HashMap<>();
        this.supportedConfigs = new ArrayList<>();
        supportedConfigs.add(ProducerConfig.ACKS_CONFIG);
        this.updater = new DynamicClientConfigUpdater(
            config.getLong(CommonClientConfigs.METADATA_MAX_AGE_CONFIG), 
            config.getBoolean(CommonClientConfigs.ENABLE_DYNAMIC_CONFIG_CONFIG), 
            time
        );
    }


    /**
     * Send a {@link DescribeConfigsRequest} to a node specifically for dynamic client configurations
     *
     * @param node Node to send request to
     * @param now  Current time in milliseconds
     */ 
    public void maybeFetchConfigs(long now) {
        if (updater.shouldUpdateConfigs(now)) {
            Node node = client.leastLoadedNode(now);
            if (node != null && client.ready(node, now)) {
                updater.sentConfigsRequest();
                RequestCompletionHandler callback = response -> handleConfigsResponse(response);
                ClientRequest clientRequest = client
                    .newClientRequest(node.idString(), updater.newRequestBuilder(this.clientId, this.supportedConfigs), now, true, requestTimeoutMs, callback);
                this.client.send(clientRequest, now);
                log.info("Sent DescribeClientConfigsRequest");
            }
        }
    }

    /**
     * Handler for the {@link org.apache.kafka.common.requests.DescribeConfigsResponse}
     * @param response
     */
    private void handleConfigsResponse(ClientResponse response) {
        if (response.hasResponse()) {
            log.info("Recieved DescribeClientConfigResponse");
            updater.receiveConfigs();
            updater.receiveInitialConfigs();

            DescribeClientConfigsResponse configsResponse = (DescribeClientConfigsResponse) response.responseBody();
            Map<String, String> dynamicConfigs = updater.createResultMapAndHandleErrors(configsResponse, log);

            // Only parse and validate dynamic configs if they have changed since the last time they were fetched
            if (!dynamicConfigs.equals(previousDynamicConfigs)) {
                try {
                    // We want dynamic configs to take priority over user provided configs
                    Map<String, Object> overlayed = new HashMap<>();
                    overlayed.putAll(originals);
                    overlayed.putAll(dynamicConfigs);
                    previousDynamicConfigs = dynamicConfigs;

                    // Only update configs if the parse in ProducerConfig is successful
                    ProducerConfig parsedConfigs = new ProducerConfig(overlayed, false);
                    updatedConfigs = parsedConfigs;
                    log.info("Updated dynamic configurations {}", dynamicConfigs);
                    log.info("Using acks={}", this.getAcks().toString()); 
                } catch (ConfigException ce) {
                    log.info("Rejecting new dynamic configs {}", dynamicConfigs);
                }
            }
        } else {
            log.info("Did not recieve DescribeConfigResponse");
            updater.retry();
        }
    }

    /**
     * Gets the dynamically updated acks configuration
     * @return current acks configuration
     */
    public Short getAcks() {
        return Short.parseShort(updatedConfigs.getString(ProducerConfig.ACKS_CONFIG));
    }
}
