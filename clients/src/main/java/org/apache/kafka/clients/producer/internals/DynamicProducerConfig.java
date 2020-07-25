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
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.DynamicClientConfigUpdater;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.requests.DescribeConfigsRequest;
import org.apache.kafka.common.requests.DescribeConfigsResponse;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

/**
 * Handles the request and response of a dynamic client configuration update for the producer
 */
public class DynamicProducerConfig extends DynamicClientConfigUpdater {
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

    public DynamicProducerConfig(KafkaClient client, ProducerConfig config, Time time, LogContext logContext, int requestTimeoutMs) {
        super(time);
        this.client = client;
        this.originals = config.originals();
        this.requestTimeoutMs = requestTimeoutMs;
        this.log = logContext.logger(DynamicProducerConfig.class);
        this.updatedConfigs = config;
        this.clientId = config.getString(ProducerConfig.CLIENT_ID_CONFIG);
        this.previousDynamicConfigs = new HashMap<>();
    }


    /**
     * Send a {@link DescribeConfigsRequest} to a node specifically for dynamic client configurations
     *
     * @param node Node to send request to
     * @param now  Current time in milliseconds
     */ 
    @Override
    public boolean maybeFetchConfigs(long now) {
        if (shouldUpdateConfigs(now)) {
            System.out.println(now);
            Node node = client.leastLoadedNode(now);
            if (node != null && client.ready(node, now)) {
                updateInProgress();
                RequestCompletionHandler callback = response -> handleDescribeConfigsResponse(response);
                ClientRequest clientRequest = client
                    .newClientRequest(node.idString(), newRequestBuilder(this.clientId), now, true, requestTimeoutMs, callback);
                this.client.send(clientRequest, now);
                log.info("Sent DescribeConfigsRequest");
                return true;
            }
        }
        return false;
    }

    /**
     * Handler for the {@link org.apache.kafka.common.requests.DescribeConfigsResponse}
     * @param response
     */
    private void handleDescribeConfigsResponse(ClientResponse response) {
        if (response.hasResponse()) {
            log.info("Recieved DescribeConfigResponse");

            DescribeConfigsResponse configsResponse = (DescribeConfigsResponse) response.responseBody();
            Map<String, String> dynamicConfigs = createResultMapAndHandleErrors(configsResponse, log);

            // Only parse and validate dynamic configs if they have changed since the last time they were fetched
            if (!dynamicConfigs.equals(previousDynamicConfigs)) {
                try {
                    Map<String, Object> overlayed = new HashMap<>();
                    overlayed.putAll(originals);
                    overlayed.putAll(dynamicConfigs);
                    previousDynamicConfigs = dynamicConfigs;
                    ProducerConfig parsedConfigs = new ProducerConfig(overlayed, false);
                    updatedConfigs = parsedConfigs;
                    log.info("Updated dynamic configurations {}", dynamicConfigs);
                    log.info("Using acks=", getAcks());
                } catch (ConfigException ce) {
                    log.info("Rejecting new dynamic configs");
                }
            }
            update();
        } else {
            log.info("Did not recieve DescribeConfigResponse");
            retry();
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
