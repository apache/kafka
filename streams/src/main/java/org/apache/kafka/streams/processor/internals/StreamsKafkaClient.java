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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestSend;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by hojjat on 8/29/16.
 */
public class StreamsKafkaClient {

    private static final Logger log = LoggerFactory.getLogger(StreamsKafkaClient.class);
    private Metadata metadata;
    private Metrics metrics;
    private Selector selector;
    private ChannelBuilder channelBuilder;
    private KafkaClient kafkaClient;
    private StreamsConfig config;
    private Node brokerNode;

    private int maxIterations = 5;

    public StreamsKafkaClient(StreamsConfig config) {

        this.config = config;
        Time time = new SystemTime();

        Map<String, String> metricTags = new LinkedHashMap<String, String>();
        metricTags.put("client-id", StreamsConfig.CLIENT_ID_CONFIG);

        this.metadata = new Metadata(config.getLong(config.RETRY_BACKOFF_MS_CONFIG), config.getLong(config.METADATA_MAX_AGE_CONFIG), true);
        List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        metadata.update(Cluster.bootstrap(addresses), 0);

        MetricConfig metricConfig = new MetricConfig().samples(config.getInt(CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG))
                .timeWindow(config.getLong(CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                .tags(metricTags);
        List<MetricsReporter> reporters = config.getConfiguredInstances(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG,
                MetricsReporter.class);
        // TODO: This should come from the KafkaStream
        reporters.add(new JmxReporter("kafka.streams"));
        this.metrics = new Metrics(metricConfig, reporters, time);

        this.channelBuilder = ClientUtils.createChannelBuilder(config.values());

        selector = new Selector(config.getLong(config.CONNECTIONS_MAX_IDLE_MS_CONFIG), this.metrics, time, "kafka-client", this.channelBuilder);

        kafkaClient = new NetworkClient(
                selector,
                metadata,
                config.getString(StreamsConfig.CLIENT_ID_CONFIG),
                100, // a fixed large enough value will suffice
                config.getLong(config.RECONNECT_BACKOFF_MS_CONFIG),
                config.getInt(config.SEND_BUFFER_CONFIG),
                config.getInt(config.RECEIVE_BUFFER_CONFIG),
                config.getInt(config.REQUEST_TIMEOUT_MS_CONFIG), time);

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
     * Send a requet to a node.
     *
     * @param request
     * @param apiKeys
     * @param callback
     */
    public void  sendRequest(Struct request, ApiKeys apiKeys, RequestCompletionHandler callback) {

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
                kafkaClient.poll(config.getLong(StreamsConfig.POLL_MS_CONFIG), systemTime.milliseconds());
            }
            iterationCount++;
        }

    }

    /**
     * Check if the topic exists.
     *
     * @param topic
     * @return
     */
    public boolean topicExists(String topic) {

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
        while (iterationCount < maxIterations) {
            if (kafkaClient.ready(brokerNode, systemTime.milliseconds())) {
                kafkaClient.send(clientRequest, systemTime.milliseconds());
                break;
            } else {
                // If the client is not ready call poll to make the client ready
                kafkaClient.poll(config.getLong(StreamsConfig.POLL_MS_CONFIG), new SystemTime().milliseconds());
            }
            iterationCount++;
        }

        // Process the response
        iterationCount = 0;
        while (iterationCount < maxIterations) {
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

}
