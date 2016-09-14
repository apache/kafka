/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.integration;


import kafka.utils.MockTime;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.DeleteTopicsRequest;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.processor.internals.StreamsKafkaClient;
import org.apache.kafka.test.TestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

/**
 * Tests related to internal topics in streams
 */
public class InternalTopicIntegrationTest {

    private static final int NUM_BROKERS = 1;

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private static final String DEFAULT_INPUT_TOPIC = "inputTopic";
    private static final String DEFAULT_OUTPUT_TOPIC = "outputTopic";
    private final MockTime mockTime = CLUSTER.time;


    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(DEFAULT_INPUT_TOPIC);
        CLUSTER.createTopic(DEFAULT_OUTPUT_TOPIC);
    }

    /**
     * Check if the given topic exists.
     *
     * @param topic
     * @return
     */
    private boolean topicExists(String topic, StreamsKafkaClient streamsKafkaClient) {
        return streamsKafkaClient.topicExists(topic);
    }

    private void deleteTopic(String topic, StreamsConfig config, StreamsKafkaClient streamsKafkaClient) {

        Set<String> topics = new HashSet<>();
        topics.add(topic);

        DeleteTopicsRequest deleteTopicsRequest = new DeleteTopicsRequest(topics, config.getInt(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG));

        RequestCompletionHandler callback = new RequestCompletionHandler() {
            public void onComplete(ClientResponse response) {
                // Do nothing!
                System.out.print("===");
            }
        };
        streamsKafkaClient.sendRequest(deleteTopicsRequest.toStruct(), ApiKeys.DELETE_TOPICS, callback);
    }

    private void createTopic(String topic, StreamsConfig config, int numPartitions, int replicationFactor, boolean compactTopic, StreamsKafkaClient streamsKafkaClient)  {

        String cleanupPolicyProp = "cleanup.policy";

        CreateTopicsRequest.TopicDetails topicDetails;
        if (compactTopic) {
            Map<String, String> topicConfig = new HashMap<>();
            topicConfig.put(cleanupPolicyProp, "compact");
            topicDetails = new CreateTopicsRequest.TopicDetails(numPartitions, (short) replicationFactor, topicConfig);
        } else {
            topicDetails = new CreateTopicsRequest.TopicDetails(numPartitions, (short) replicationFactor);
        }

        Map<String, CreateTopicsRequest.TopicDetails> topics = new HashMap<>();
        topics.put(topic, topicDetails);

        CreateTopicsRequest createTopicsRequest = new CreateTopicsRequest(topics, config.getInt(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG));

        RequestCompletionHandler callback = new RequestCompletionHandler() {
            public void onComplete(ClientResponse response) {
                // Do nothing!
            }
        };
        streamsKafkaClient.sendRequest(createTopicsRequest.toStruct(), ApiKeys.CREATE_TOPICS, callback);
    }

    @Test
    public void testCreateTopic() throws Exception {

        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "internal-topics-integration-test");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsConfig streamsConfig = new StreamsConfig(streamsConfiguration);
        StreamsKafkaClient streamsKafkaClient = new StreamsKafkaClient(streamsConfig);

        String topicName = "testTopic" + mockTime.milliseconds();
        createTopic(topicName, streamsConfig, 1, (short) 1, true, streamsKafkaClient);
        boolean topicExists = topicExists(topicName, streamsKafkaClient);
        assertTrue(topicExists);

    }

    @Test
    public void testDeleteTopic() throws Exception {

        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "internal-topics-integration-test");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsConfig streamsConfig = new StreamsConfig(streamsConfiguration);
        StreamsKafkaClient streamsKafkaClient = new StreamsKafkaClient(streamsConfig);

        String topicName = "testTopic" + mockTime.milliseconds();
        createTopic(topicName, streamsConfig, 1, (short) 1, true, streamsKafkaClient);
        Thread.sleep(5000);
        deleteTopic(topicName, streamsConfig, streamsKafkaClient);
        Thread.sleep(5000);
        boolean topicExists = topicExists(topicName, streamsKafkaClient);
        assertTrue(!topicExists);
    }

}