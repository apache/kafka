/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.DeleteTopicsRequest;
import org.apache.kafka.common.requests.RequestSend;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;

import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;


public class StreamsKafkaClient {

    private static final Logger log = LoggerFactory.getLogger(StreamsKafkaClient.class);
    private Metadata metadata;
    private Metrics metrics;
    private Selector selector;
    private ChannelBuilder channelBuilder;
    private KafkaClient kafkaClient;
    private StreamsConfig streamsConfig;
    private Node brokerNode;

    public static final String CLEANUP_POLICY_PROP = "cleanup.policy";
    private static final String COMPACT = "compact";

    private int maxIterations = 10;

    public StreamsKafkaClient(StreamsConfig streamsConfig) {

        this.streamsConfig = streamsConfig;
        Time time = new SystemTime();

        Map<String, String> metricTags = new LinkedHashMap<>();
        metricTags.put("client-id", StreamsConfig.CLIENT_ID_CONFIG);

        this.metadata = new Metadata(streamsConfig.getLong(streamsConfig.RETRY_BACKOFF_MS_CONFIG), streamsConfig.getLong(streamsConfig.METADATA_MAX_AGE_CONFIG));
        List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(streamsConfig.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        metadata.update(Cluster.bootstrap(addresses), 0);

        MetricConfig metricConfig = new MetricConfig().samples(streamsConfig.getInt(CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG))
                .timeWindow(streamsConfig.getLong(CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                .tags(metricTags);
        List<MetricsReporter> reporters = streamsConfig.getConfiguredInstances(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG,
                MetricsReporter.class);
        // TODO: This should come from the KafkaStream
        reporters.add(new JmxReporter("kafka.streams"));
        this.metrics = new Metrics(metricConfig, reporters, time);

        this.channelBuilder = ClientUtils.createChannelBuilder(streamsConfig.values());

        selector = new Selector(streamsConfig.getLong(streamsConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG), this.metrics, time, "kafka-client", this.channelBuilder);

        kafkaClient = new NetworkClient(
                selector,
                metadata,
                streamsConfig.getString(StreamsConfig.CLIENT_ID_CONFIG),
                100, // a fixed large enough value will suffice
                streamsConfig.getLong(streamsConfig.RECONNECT_BACKOFF_MS_CONFIG),
                streamsConfig.getInt(streamsConfig.SEND_BUFFER_CONFIG),
                streamsConfig.getInt(streamsConfig.RECEIVE_BUFFER_CONFIG),
                streamsConfig.getInt(streamsConfig.REQUEST_TIMEOUT_MS_CONFIG), time);

        brokerNode = kafkaClient.leastLoadedNode(new SystemTime().milliseconds());
    }

    public KafkaClient getKafkaClient() {
        return kafkaClient;
    }

    public void shutdown() {
        log.info("Closing the StreamsKafkaClient.");
        // this will keep track of the first encountered exception
        AtomicReference<Throwable> firstException = new AtomicReference<Throwable>();
        ClientUtils.closeQuietly(metrics, "producer metrics", firstException);

        log.debug("The StreamsKafkaClient has closed.");
        if (firstException.get() != null)
            throw new KafkaException("Failed to close kafka producer", firstException.get());
    }

    /**
     * Cretes a new topic with the given number of partitions and replication factor.
     *
     * @param internalTopicConfig
     * @param numPartitions
     * @param replicationFactor
     */
    public void createTopic(InternalTopicConfig internalTopicConfig, int numPartitions, int replicationFactor, long windowChangeLogAdditionalRetention) {
        CreateTopicsRequest.TopicDetails topicDetails;
        Properties topicProperties = internalTopicConfig.toProperties(windowChangeLogAdditionalRetention);
        Map<String, String> topicConfig = new HashMap<>();
        for (String key : topicProperties.stringPropertyNames()) {
            topicConfig.put(key, topicProperties.getProperty(key));
        }
        topicDetails = new CreateTopicsRequest.TopicDetails(numPartitions, (short) replicationFactor, topicConfig);
        Map<String, CreateTopicsRequest.TopicDetails> topics = new HashMap<>();
        topics.put(internalTopicConfig.name(), topicDetails);

        CreateTopicsRequest createTopicsRequest = new CreateTopicsRequest(topics, streamsConfig.getInt(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG));

        RequestCompletionHandler callback = new RequestCompletionHandler() {
            public void onComplete(ClientResponse response) {
                // Do nothing!
            }
        };
        ClientResponse clientResponse = sendRequest(createTopicsRequest.toStruct(), ApiKeys.CREATE_TOPICS, callback);
        CreateTopicsResponse createTopicsResponse = new CreateTopicsResponse(clientResponse.responseBody());
        if (createTopicsResponse.errors().get(internalTopicConfig.name()).code() > 0) {
            throw new StreamsException("Could not create topic: " + internalTopicConfig.name());
        }
    }

    /**
     * Delete a topic.
     *
     * @param topic
     */
    public void deleteTopic(String topic) {
        log.debug("Deleting topic {} from ZK in partition assignor.", topic);
        Set<String> topics = new HashSet();
        topics.add(topic);

        DeleteTopicsRequest deleteTopicsRequest = new DeleteTopicsRequest(topics, streamsConfig.getInt(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG));

        RequestCompletionHandler callback = new RequestCompletionHandler() {
            public void onComplete(ClientResponse response) {
                // Do nothing!
            }
        };
        ClientResponse clientResponse = sendRequest(deleteTopicsRequest.toStruct(), ApiKeys.DELETE_TOPICS, callback);
        CreateTopicsResponse createTopicsResponse = new CreateTopicsResponse(clientResponse.responseBody());
        if (createTopicsResponse.errors().get(topic).code() > 0) {
            throw new StreamsException("Could not delete topic: " + topic);
        }

    }

    /**
     * Send a request to kafka broker of this client. Polls the request for a given number of iterations to receive the response.
     *
     * @param request
     * @param apiKeys
     * @param callback
     */
    public ClientResponse sendRequest(Struct request, ApiKeys apiKeys, RequestCompletionHandler callback) {

        String brokerId = Integer.toString(brokerNode.id());

        RequestSend send = new RequestSend(brokerId,
                kafkaClient.nextRequestHeader(apiKeys),
                request);

        ClientRequest clientRequest = new ClientRequest(new SystemTime().milliseconds(), true, send, callback);
        SystemTime systemTime = new SystemTime();
        int iterationCount = 0;
        while (iterationCount < maxIterations) {
            if (kafkaClient.ready(brokerNode, systemTime.milliseconds())) {
                kafkaClient.send(clientRequest, systemTime.milliseconds());
                break;
            } else {
                kafkaClient.poll(streamsConfig.getLong(StreamsConfig.POLL_MS_CONFIG), systemTime.milliseconds());
            }
            iterationCount++;
        }

        iterationCount = 0;
        // Poll for the response.
        while (iterationCount < maxIterations) {
            List<ClientResponse> responseList = kafkaClient.poll(streamsConfig.getLong(StreamsConfig.POLL_MS_CONFIG), new SystemTime().milliseconds());
            for (ClientResponse clientResponse: responseList) {
                if (clientResponse.request().equals(clientRequest)) {
                    return clientResponse;
                }
            }
            iterationCount++;
        }
        throw new StreamsException("StreamsKafkaClient failed to send the request: " + apiKeys.name());
    }


    /**
     * Get the metadata for a topic.
     * @param topic
     * @return
     */
    public MetadataResponse.TopicMetadata getTopicMetadata(String topic) {
        MetadataRequest metadataRequest = new MetadataRequest(Arrays.asList(topic));

        RequestCompletionHandler callback = new RequestCompletionHandler() {
            public void onComplete(ClientResponse response) {
                // Do nothing!
            }
        };

        ClientResponse clientResponse = sendRequest(metadataRequest.toStruct(), ApiKeys.METADATA, callback);
        MetadataResponse metadataResponse = new MetadataResponse(clientResponse.responseBody());

        for (MetadataResponse.TopicMetadata topicMetadata: metadataResponse.topicMetadata()) {
            if (topicMetadata.topic().equalsIgnoreCase(topic)) {
                if (topicMetadata.error().code() == 0) {
                    return topicMetadata;
                }
                return null;
            }
        }

        return null;
    }

    /**
     * Check to see if a topic exists.
     * @param topicName
     * @return
     */
    public boolean topicExists(String topicName) {
        Map<String, List<PartitionInfo>> topics;

        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, streamsConfig.getList(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        topics = consumer.listTopics();
        for (String topicNameInList:topics.keySet()) {
            if (topicNameInList.equalsIgnoreCase(topicName)) {
                return true;
            }
        }
        return false;
    }

}
