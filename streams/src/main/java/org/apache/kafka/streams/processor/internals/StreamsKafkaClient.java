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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.DeleteTopicsRequest;
import org.apache.kafka.common.requests.DeleteTopicsResponse;
import org.apache.kafka.common.requests.Req;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class StreamsKafkaClient {

    private final KafkaClient kafkaClient;
    private final List<MetricsReporter> reporters;
    private final StreamsConfig streamsConfig;

    private static final int MAX_INFLIGHT_REQUESTS = 100;

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
        reporters = streamsConfig.getConfiguredInstances(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG,
                MetricsReporter.class);
        // TODO: This should come from the KafkaStream
        reporters.add(new JmxReporter("kafka.admin"));
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

    public void close() throws IOException {
        for (MetricsReporter metricsReporter: this.reporters) {
            metricsReporter.close();
        }
    }

    /**
     * Creates a set of new topics using batch request.
     *
     * @param topicsMap
     * @param replicationFactor
     * @param windowChangeLogAdditionalRetention
     */
    public void createTopics(final Map<InternalTopicConfig, Integer> topicsMap, final int replicationFactor, final long windowChangeLogAdditionalRetention) {

        final Map<String, CreateTopicsRequest.TopicDetails> topicRequestDetails = new HashMap<>();
        for (InternalTopicConfig internalTopicConfig:topicsMap.keySet()) {
            final Properties topicProperties = internalTopicConfig.toProperties(windowChangeLogAdditionalRetention);
            final Map<String, String> topicConfig = new HashMap<>();
            for (String key : topicProperties.stringPropertyNames()) {
                topicConfig.put(key, topicProperties.getProperty(key));
            }
            final CreateTopicsRequest.TopicDetails topicDetails = new CreateTopicsRequest.TopicDetails(topicsMap.get(internalTopicConfig), (short) replicationFactor, topicConfig);

            topicRequestDetails.put(internalTopicConfig.name(), topicDetails);
        }
        final CreateTopicsRequest createTopicsRequest = new CreateTopicsRequest(topicRequestDetails, streamsConfig.getInt(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG));
        final ClientResponse clientResponse = sendRequest(createTopicsRequest.toStruct(), ApiKeys.CREATE_TOPICS);
        final CreateTopicsResponse createTopicsResponse = new CreateTopicsResponse(clientResponse.responseBody());
        for (InternalTopicConfig internalTopicConfig:topicsMap.keySet()) {
            short errorCode = createTopicsResponse.errors().get(internalTopicConfig.name()).code();
            if (errorCode > 0) {
                if (errorCode == Errors.TOPIC_ALREADY_EXISTS.code()) {
                    continue;
                } else {
                    throw new StreamsException("Could not create topic: " + internalTopicConfig.name() + ". " + createTopicsResponse.errors().get(internalTopicConfig.name()).name());
                }
            }
        }
    }

    /**
     * Delets a set of topics.
     *
     * @param topics
     */
    public void deleteTopics(final Map<InternalTopicConfig, Integer> topics) {

        final Set<String> topicNames = new HashSet<>();
        for (InternalTopicConfig internalTopicConfig: topics.keySet()) {
            topicNames.add(internalTopicConfig.name());
        }
        deleteTopics(topicNames);
    }

    /**
     * Delete a set of topics in one request.
     *
     * @param topics
     */
    private void deleteTopics(final Set<String> topics) {

        final DeleteTopicsRequest deleteTopicsRequest = new DeleteTopicsRequest(topics, streamsConfig.getInt(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG));
        final ClientResponse clientResponse = sendRequest(deleteTopicsRequest.toStruct(), ApiKeys.DELETE_TOPICS);
        final DeleteTopicsResponse deleteTopicsResponse = new DeleteTopicsResponse(clientResponse.responseBody());
        for (String topicName: deleteTopicsResponse.errors().keySet()) {
            if (deleteTopicsResponse.errors().get(topicName).code() > 0) {
                throw new StreamsException("Could not delete topic: " + topicName);
            }
        }

    }

    /**
     * Send a request to kafka broker of this client. Keep polling until the corresponding response is received.
     *
     * @param request
     * @param apiKeys
     */
    private ClientResponse sendRequest(final Struct request, final ApiKeys apiKeys) {

        String brokerId = null;
        final SystemTime systemTime = new SystemTime();

        final Metadata metadata = new Metadata(streamsConfig.getLong(StreamsConfig.RETRY_BACKOFF_MS_CONFIG), streamsConfig.getLong(StreamsConfig.METADATA_MAX_AGE_CONFIG));
        final List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(streamsConfig.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        metadata.update(Cluster.bootstrap(addresses), systemTime.milliseconds());

        final List<Node> nodes = metadata.fetch().nodes();
        final long readyTimeout = systemTime.milliseconds() + streamsConfig.getInt(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG);
        boolean foundNode = false;
        while (!foundNode && (systemTime.milliseconds() < readyTimeout)) {
            for (Node node: nodes) {
                if (kafkaClient.ready(node, systemTime.milliseconds())) {
                    brokerId = Integer.toString(node.id());
                    foundNode = true;
                    break;
                }
            }
            kafkaClient.poll(streamsConfig.getLong(StreamsConfig.POLL_MS_CONFIG), systemTime.milliseconds());
        }
        if (brokerId == null) {
            throw new StreamsException("Could not find any available broker.");
        }

        final RequestSend send = new RequestSend(brokerId,
                kafkaClient.nextRequestHeader(apiKeys),
                request);

        final ClientRequest clientRequest = new ClientRequest(systemTime.milliseconds(), true, send, null);

        kafkaClient.send(clientRequest, systemTime.milliseconds());

        final long responseTimeout = systemTime.milliseconds() + streamsConfig.getInt(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG);
        // Poll for the response.
        while (systemTime.milliseconds() < responseTimeout) {
            List<ClientResponse> responseList = kafkaClient.poll(streamsConfig.getLong(StreamsConfig.POLL_MS_CONFIG), systemTime.milliseconds());
            if (!responseList.isEmpty()) {
                if (responseList.size() > 1) {
                    throw new StreamsException("Sent one request but received multiple or no responses.");
                }
                if (responseList.get(0).request().equals(clientRequest)) {
                    return responseList.get(0);
                } else {
                    throw new StreamsException("Inconsistent response received.");
                }
            }
        }
        throw new StreamsException("Failed to get response from broker within timeout");
    }


    /**
     * Get the metadata for a topic.
     * @param topic
     * @return
     */
    public MetadataResponse.TopicMetadata getTopicMetadata(final String topic) {

        final ClientResponse clientResponse = sendRequest(MetadataRequest.allTopics().toStruct(), ApiKeys.METADATA);
        final MetadataResponse metadataResponse = new MetadataResponse(clientResponse.responseBody());
        for (MetadataResponse.TopicMetadata topicMetadata: metadataResponse.topicMetadata()) {
            if (topicMetadata.topic().equalsIgnoreCase(topic)) {
                return topicMetadata;
            }
        }
        return null;
    }


    public Collection<MetadataResponse.TopicMetadata> fetchTopicMetadata() {
        final ClientResponse clientResponse = sendRequest(MetadataRequest.allTopics().toStruct(), ApiKeys.METADATA);
        final MetadataResponse metadataResponse = new MetadataResponse(clientResponse.responseBody());
        return metadataResponse.topicMetadata();
    }

}
