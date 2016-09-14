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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.protocol.ApiKeys;

import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.DeleteTopicsRequest;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;


public class InternalTopicManager {

    public static final String CLEANUP_POLICY_PROP = "cleanup.policy";
    private static final Set<String> CLEANUP_POLICIES = Utils.mkSet("compact", "delete");
    public static final String RETENTION_MS = "retention.ms";
    public static final Long WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_DEFAULT = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);

    private static final Logger log = LoggerFactory.getLogger(InternalTopicManager.class);

    private final int replicationFactor;
    private StreamsKafkaClient streamsKafkaClient;
    StreamsConfig config;

    private static final String COMPACT = "compact";

    public InternalTopicManager() {
        this.config = null;
        this.replicationFactor = 0;
    }

    public InternalTopicManager(StreamsConfig config) {
        this.config = config;
        this.streamsKafkaClient = new StreamsKafkaClient(config);
        this.replicationFactor = 0;
    }

    public InternalTopicManager(StreamsConfig config, int replicationFactor) {
        this.config = config;
        this.streamsKafkaClient = new StreamsKafkaClient(config);
        this.replicationFactor = replicationFactor;
    }


    public void shutDown() throws IOException {
        this.streamsKafkaClient.shutdown();
    }

    public void makeReady(String topic, int numPartitions) {
        makeReady(topic, numPartitions, false);
    }
    public void makeReady(String topic, int numPartitions, boolean compactTopic) {

        // Delete the topic if it exists before.
        boolean topicExists = topicExists(topic);
        if (topicExists) {
            deleteTopic(topic);
            log.debug("Deleted: " + topic);
        }
        // Create a new topic
        createTopic(topic, numPartitions, replicationFactor, compactTopic);
        log.debug("Created: " + topic);
    }


    /**
     * Check if the topic exists already
     *
     * @param topic
     * @return
     */
    private boolean topicExists(String topic) {
        return streamsKafkaClient.topicExists(topic);
    }


    /**
     * Create a new topic.
     *
     * @param topic
     * @param numPartitions
     * @param replicationFactor
     * @param compactTopic
     */
    private void createTopic(String topic, int numPartitions, int replicationFactor, boolean compactTopic)  {

        CreateTopicsRequest.TopicDetails topicDetails;
        if (compactTopic) {
            Map<String, String> topicConfig = new HashMap<>();
            topicConfig.put(CLEANUP_POLICY_PROP, COMPACT);
            topicDetails = new CreateTopicsRequest.TopicDetails(numPartitions, (short) replicationFactor, topicConfig);
        } else {
            topicDetails = new CreateTopicsRequest.TopicDetails(numPartitions, (short) replicationFactor);
        }

        Map<String, CreateTopicsRequest.TopicDetails> topics = new HashMap<>();
        topics.put(topic, topicDetails);

        CreateTopicsRequest createTopicsRequest = new CreateTopicsRequest(topics, config.getInt(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG));

        RequestCompletionHandler callback = new RequestCompletionHandler() {
            public void onComplete(ClientResponse response) {
                handleResponse(response);
            }
        };
        streamsKafkaClient.sendRequest(createTopicsRequest.toStruct(), ApiKeys.CREATE_TOPICS, callback);
    }


    /**
     * Delere an existing topic
     *
     * @param topic
     */
    private void deleteTopic(String topic) {
        log.debug("Deleting topic {} from ZK in partition assignor.", topic);
        Set<String> topics = new HashSet();
        topics.add(topic);

        DeleteTopicsRequest deleteTopicsRequest = new DeleteTopicsRequest(topics, config.getInt(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG));

        RequestCompletionHandler callback = new RequestCompletionHandler() {
            public void onComplete(ClientResponse response) {
                handleResponse(response);
            }
        };
        streamsKafkaClient.sendRequest(deleteTopicsRequest.toStruct(), ApiKeys.DELETE_TOPICS, callback);

    }

    /**
     * Handle a produce response
     */
    private void handleResponse(ClientResponse response) {
        // Do nothing!!!
    }

}