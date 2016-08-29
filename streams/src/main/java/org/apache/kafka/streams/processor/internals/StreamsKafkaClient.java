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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
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

    public StreamsKafkaClient(StreamsConfig config) {

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

}
