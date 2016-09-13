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
package org.apache.kafka.streams.integration;


import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestSend;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedSingleNodeKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.internals.StreamsKafkaClient;
import org.apache.kafka.test.MockKeyValueMapper;
import org.apache.kafka.test.TestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

/**
 * Tests related to internal topics in streams
 */
public class InternalTopicIntegrationTest {
    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();
    private static final String DEFAULT_INPUT_TOPIC = "inputTopic";
    private static final String DEFAULT_OUTPUT_TOPIC = "outputTopic";

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(DEFAULT_INPUT_TOPIC);
        CLUSTER.createTopic(DEFAULT_OUTPUT_TOPIC);
    }

    /**
     * Check if the given topic exists.
     *
     * @param topic
     * @param config
     * @return
     */
    private boolean topicExists(String topic, StreamsConfig config) {
        StreamsKafkaClient streamsKafkaClient = new StreamsKafkaClient(config);
        KafkaClient kafkaClient = streamsKafkaClient.getKafkaClient();
        Node brokerNode = kafkaClient.leastLoadedNode(new SystemTime().milliseconds());
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

    @Test
    public void shouldCompactTopicsForStateChangelogs() throws Exception {
        List<String> inputValues = Arrays.asList("hello", "world", "world", "hello world");

        //
        // Step 1: Configure and start a simple word count topology
        //
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "compact-topics-integration-test");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> textLines = builder.stream(DEFAULT_INPUT_TOPIC);

        KStream<String, Long> wordCounts = textLines
            .flatMapValues(new ValueMapper<String, Iterable<String>>() {
                @Override
                public Iterable<String> apply(String value) {
                    return Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+"));
                }
            }).groupBy(MockKeyValueMapper.<String, String>SelectValueMapper())
                .count("Counts").toStream();

        wordCounts.to(stringSerde, longSerde, DEFAULT_OUTPUT_TOPIC);

        // Remove any state from previous test runs
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);

        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();

        //
        // Step 2: Produce some input data to the input topic.
        //
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        IntegrationTestUtils.produceValuesSynchronously(DEFAULT_INPUT_TOPIC, inputValues, producerConfig);

        //
        // Step 3: Verify the state changelog topics are compact
        //
        streams.close();

        // Check if the topics were created correctly.
        boolean topicExists = topicExists("compact-topics-integration-test-Counts-changelog", new StreamsConfig(streamsConfiguration));
        assertTrue(topicExists);
    }
}
