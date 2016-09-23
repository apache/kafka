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
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;
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
import org.apache.kafka.common.requests.RequestSend;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;

import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class StreamsKafkaClient {

    private final KafkaClient kafkaClient;
    private final StreamsConfig streamsConfig;

    private static final int MAX_INFLIGHT_REQUESTS = 100;
    private static final long MAX_WAIT_TIME_MS = 30000;

    public StreamsKafkaClient(final StreamsConfig streamsConfig) {

        this.streamsConfig = streamsConfig;
        final Time time = new SystemTime();

        final Map<String, String> metricTags = new LinkedHashMap<>();
        metricTags.put("client-id", StreamsConfig.CLIENT_ID_CONFIG);

        final Metadata metadata = new Metadata(streamsConfig.getLong(StreamsConfig.RETRY_BACKOFF_MS_CONFIG), streamsConfig.getLong(StreamsConfig.METADATA_MAX_AGE_CONFIG));
        final List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(streamsConfig.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        metadata.update(Cluster.bootstrap(addresses), time.milliseconds());

        final MetricConfig metricConfig = new MetricConfig().samples(streamsConfig.getInt(CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG))
                .timeWindow(streamsConfig.getLong(CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                .tags(metricTags);
        final List<MetricsReporter> reporters = streamsConfig.getConfiguredInstances(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG,
                MetricsReporter.class);
        // TODO: This should come from the KafkaStream
        reporters.add(new JmxReporter("kafka.streams"));
        final Metrics metrics = new Metrics(metricConfig, reporters, time);

        final ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(streamsConfig.values());

        final Selector selector = new Selector(streamsConfig.getLong(StreamsConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG), metrics, time, "kafka-client", channelBuilder);

        kafkaClient = new NetworkClient(
                selector,
                metadata,
                streamsConfig.getString(StreamsConfig.CLIENT_ID_CONFIG),
                MAX_INFLIGHT_REQUESTS, // a fixed large enough value will suffice
                streamsConfig.getLong(StreamsConfig.RECONNECT_BACKOFF_MS_CONFIG),
                streamsConfig.getInt(StreamsConfig.SEND_BUFFER_CONFIG),
                streamsConfig.getInt(StreamsConfig.RECEIVE_BUFFER_CONFIG),
                streamsConfig.getInt(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG), time);

    }


    /**
     * Cretes a new topic with the given number of partitions and replication factor.
     *
     * @param internalTopicConfig
     * @param numPartitions
     * @param replicationFactor
     */
    public void createTopic(final InternalTopicConfig internalTopicConfig, final int numPartitions, final int replicationFactor, final long windowChangeLogAdditionalRetention) {
        final Properties topicProperties = internalTopicConfig.toProperties(windowChangeLogAdditionalRetention);
        final Map<String, String> topicConfig = new HashMap<>();
        for (String key : topicProperties.stringPropertyNames()) {
            topicConfig.put(key, topicProperties.getProperty(key));
        }
        final CreateTopicsRequest.TopicDetails topicDetails = new CreateTopicsRequest.TopicDetails(numPartitions, (short) replicationFactor, topicConfig);
        final Map<String, CreateTopicsRequest.TopicDetails> topics = new HashMap<>();
        topics.put(internalTopicConfig.name(), topicDetails);

        final CreateTopicsRequest createTopicsRequest = new CreateTopicsRequest(topics, streamsConfig.getInt(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG));

        final ClientResponse clientResponse = sendRequest(createTopicsRequest.toStruct(), ApiKeys.CREATE_TOPICS);
        final CreateTopicsResponse createTopicsResponse = new CreateTopicsResponse(clientResponse.responseBody());
        if (createTopicsResponse.errors().get(internalTopicConfig.name()).code() > 0) {
            throw new StreamsException("Could not create topic: " + internalTopicConfig.name() + ". " + createTopicsResponse.errors().get(internalTopicConfig.name()).name());
        }
    }


    /**
     * Send a request to kafka broker of this client. Polls the request for a given number of iterations to receive the response.
     *
     * @param request
     * @param apiKeys
     */
    private ClientResponse sendRequest(final Struct request, final ApiKeys apiKeys) {

        final Node brokerNode = kafkaClient.leastLoadedNode(new SystemTime().milliseconds());
        final String brokerId = Integer.toString(brokerNode.id());

        final SystemTime systemTime = new SystemTime();

        final RequestSend send = new RequestSend(brokerId,
                kafkaClient.nextRequestHeader(apiKeys),
                request);

        final ClientRequest clientRequest = new ClientRequest(systemTime.milliseconds(), true, send, null);


        final long readyTimeout = systemTime.milliseconds() + MAX_WAIT_TIME_MS;
        while (!kafkaClient.ready(brokerNode, systemTime.milliseconds()) && systemTime.milliseconds() < readyTimeout) {
            kafkaClient.poll(streamsConfig.getLong(StreamsConfig.POLL_MS_CONFIG), systemTime.milliseconds());
        }
        if (!kafkaClient.ready(brokerNode, systemTime.milliseconds())) {
            throw new StreamsException("Timed out waiting for node=" + brokerNode + " to become available");
        }
        kafkaClient.send(clientRequest, systemTime.milliseconds());

        final long responseTimeout = systemTime.milliseconds() + MAX_WAIT_TIME_MS;
        // Poll for the response.
        while (systemTime.milliseconds() < responseTimeout) {
            List<ClientResponse> responseList = kafkaClient.poll(streamsConfig.getLong(StreamsConfig.POLL_MS_CONFIG), systemTime.milliseconds());
            for (ClientResponse clientResponse: responseList) {
                if (clientResponse.request().equals(clientRequest)) {
                    return clientResponse;
                }
            }
        }
        throw new StreamsException("Failed to get response from node=" + brokerNode + " within timeout");
    }


    /**
     * Get the metadata for a topic.
     * @param topic
     * @return
     */
    public MetadataResponse.TopicMetadata getTopicMetadata(final String topic) {
        final MetadataRequest metadataRequest = new MetadataRequest(Collections.singletonList(topic));

        final ClientResponse clientResponse = sendRequest(metadataRequest.toStruct(), ApiKeys.METADATA);
        final MetadataResponse metadataResponse = new MetadataResponse(clientResponse.responseBody());

        for (MetadataResponse.TopicMetadata topicMetadata: metadataResponse.topicMetadata()) {
            if (topicMetadata.topic().equalsIgnoreCase(topic)) {
                return topicMetadata;
            }
        }

        throw new StreamsException("Topic " + topic + " was not found.");
    }

    /**
     * Check to see if a topic exists.
     * @param topicName
     * @return
     */
    public boolean topicExists(final String topicName) {

        final Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, streamsConfig.getList(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        final Map<String, List<PartitionInfo>> topics = consumer.listTopics();
        for (String topicNameInList : topics.keySet()) {
            if (topicNameInList.equalsIgnoreCase(topicName)) {
                return true;
            }
        }
        return false;
    }

}
