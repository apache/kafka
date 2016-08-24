/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.protocol.ApiKeys;

import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.RequestSend;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.DeleteTopicsRequest;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Arrays;


public class InternalTopicManager {

    private static final Logger log = LoggerFactory.getLogger(InternalTopicManager.class);

    private final int replicationFactor;
    private final KafkaClient kafkaClient;
    StreamsConfig config;

    public InternalTopicManager() {
        this.config = null;
        this.kafkaClient = null;
        this.replicationFactor = 0;
    }

    public InternalTopicManager(StreamsConfig config) {
        this.config = config;
        this.kafkaClient = new DefaultKafkaClientSupplier().getKafkaClient(config);
        this.replicationFactor = 0;
    }

    public InternalTopicManager(StreamsConfig config, int replicationFactor) {
        this.config = config;
        this.kafkaClient = new DefaultKafkaClientSupplier().getKafkaClient(config);
        this.replicationFactor = replicationFactor;
    }


    public void makeReady(String topic, int numPartitions, boolean compactTopic) {

        Node brokerNode = kafkaClient.leastLoadedNode(new SystemTime().milliseconds());

        // Delete the topic if it exists before.
        boolean topicExists = topicExists(topic, brokerNode);
        if (topicExists) {
            deleteTopic(topic, brokerNode);
            log.debug("Deleted: " + topic);
        }
        // Create a new topic
        createTopic(topic, numPartitions, replicationFactor, compactTopic, brokerNode);
        log.debug("Created: " + topic);
    }


    /**
     * Check if the topic exists already
     *
     * @param topic
     * @param brokerNode
     * @return
     */
    private boolean topicExists(String topic, Node brokerNode) {
        MetadataRequest metadataRequest = new MetadataRequest(Arrays.asList(topic));
        String brokerId = Integer.toString(brokerNode.id());
        RequestSend send = new RequestSend(brokerId,
                kafkaClient.nextRequestHeader(ApiKeys.METADATA),
                metadataRequest.toStruct());

        RequestCompletionHandler callback = new RequestCompletionHandler() {
            public void onComplete(ClientResponse response) {

            }
        };

        // Send the async request to check the topic existance
        ClientRequest clientRequest = new ClientRequest(new SystemTime().milliseconds(), true, send, callback);
        SystemTime systemTime = new SystemTime();
        int iterationCount = 0;
        while (iterationCount < 5) { // TODO: Set this later
            if (kafkaClient.ready(brokerNode, systemTime.milliseconds())) {
                kafkaClient.send(clientRequest, systemTime.milliseconds());
                break;
            } else {
                // If the client is not ready call poll to make the client ready
                kafkaClient.poll(config.getLong(StreamsConfig.POLL_MS_CONFIG), new SystemTime().milliseconds());
            }
        }

        // Process the response
        iterationCount = 0;
        while (iterationCount < 5) { // TODO: Set this later
            List<ClientResponse> responseList = kafkaClient.poll(config.getLong(StreamsConfig.POLL_MS_CONFIG), new SystemTime().milliseconds());
            for (ClientResponse clientResponse: responseList) {
                if (clientResponse.request().request().body().equals(metadataRequest.toStruct())) {
                    MetadataResponse metadataResponse = new MetadataResponse(clientResponse.responseBody());
                    if (metadataResponse.errors().isEmpty()) {
                        return true;
                    }
                }
            }
            iterationCount++;
        }
        return false;
    }


    /**
     * Create a new topic.
     *
     * @param topic
     * @param numPartitions
     * @param replicationFactor
     * @param compactTopic
     * @param brokerNode
     */
    private void createTopic(String topic, int numPartitions, int replicationFactor, boolean compactTopic, Node brokerNode)  {

        CreateTopicsRequest.TopicDetails topicDetails = new CreateTopicsRequest.TopicDetails(numPartitions, (short) replicationFactor);
        Map<String, CreateTopicsRequest.TopicDetails> topics = new HashMap<>();
        topics.put(topic, topicDetails);

        CreateTopicsRequest createTopicsRequest = new CreateTopicsRequest(topics, config.getInt(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG));

        String brokerId = Integer.toString(brokerNode.id());
        RequestSend send = new RequestSend(brokerId,
                kafkaClient.nextRequestHeader(ApiKeys.CREATE_TOPICS),
                createTopicsRequest.toStruct());

        sendRequest(send, brokerNode);
    }


    /**
     * Delere an existing topic
     *
     * @param topic
     * @param brokerNode
     */
    private void deleteTopic(String topic, Node brokerNode) {
        log.debug("Deleting topic {} from ZK in partition assignor.", topic);
        Set<String> topics = new HashSet();
        topics.add(topic);

        DeleteTopicsRequest deleteTopicsRequest = new DeleteTopicsRequest(topics, config.getInt(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG));

        String brokerId = Integer.toString(brokerNode.id());
        RequestSend send = new RequestSend(brokerId,
                kafkaClient.nextRequestHeader(ApiKeys.DELETE_TOPICS),
                deleteTopicsRequest.toStruct());

        sendRequest(send, brokerNode);
    }

    /**
     * Send a requet to a node.
     *
     * @param send
     * @param brokerNode
     */
    private void  sendRequest(RequestSend send, Node brokerNode) {
        RequestCompletionHandler callback = new RequestCompletionHandler() {
            public void onComplete(ClientResponse response) {
                handleResponse(response);
            }
        };

        ClientRequest clientRequest = new ClientRequest(new SystemTime().milliseconds(), true, send, callback);
        SystemTime systemTime = new SystemTime();
        int iterationCount = 0;
        while (iterationCount < 5) { // TODO: Set this later
            if (kafkaClient.ready(brokerNode, systemTime.milliseconds())) {
                kafkaClient.send(clientRequest, systemTime.milliseconds());
                break;
            } else {
                kafkaClient.poll(config.getLong(StreamsConfig.POLL_MS_CONFIG), systemTime.milliseconds());
            }
        }

    }


    /**
     * Handle a produce response
     */
    private void handleResponse(ClientResponse response) {
        // TODO: figure out what to do with the response!
    }

}