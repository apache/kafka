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

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsConfig;

import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DefaultKafkaClientSupplier implements KafkaClientSupplier {
    @Override
    public Producer<byte[], byte[]> getProducer(Map<String, Object> config) {
        return new KafkaProducer<>(config, new ByteArraySerializer(), new ByteArraySerializer());
    }

    @Override
    public Consumer<byte[], byte[]> getConsumer(Map<String, Object> config) {
        return new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    @Override
    public Consumer<byte[], byte[]> getRestoreConsumer(Map<String, Object> config) {
        return new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    /**
     * Creates a default KafkaClient. For now there is no need to expose this in the KafkaClientSupplier interface.
     * @param config
     * @return
     */
    public KafkaClient getKafkaClient(StreamsConfig config) {

        Time time = new SystemTime();

        Map<String, String> metricTags = new LinkedHashMap<String, String>();
        metricTags.put("client-id", StreamsConfig.CLIENT_ID_CONFIG);

        Metadata metadata = new Metadata(config.getLong(config.RETRY_BACKOFF_MS_CONFIG), config.getLong(config.METADATA_MAX_AGE_CONFIG), true);
        List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        metadata.update(Cluster.bootstrap(addresses), 0);

        MetricConfig metricConfig = new MetricConfig().samples(config.getInt(CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG))
                .timeWindow(config.getLong(CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                .tags(metricTags);
        List<MetricsReporter> reporters = config.getConfiguredInstances(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG,
                MetricsReporter.class);
        // TODO: This should come from the KafkaStream
        reporters.add(new JmxReporter("kafka.streams"));
        Metrics metrics = new Metrics(metricConfig, reporters, time);

        ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(config.values());
        return new NetworkClient(
                new Selector(config.getLong(config.CONNECTIONS_MAX_IDLE_MS_CONFIG), metrics, time, "kafka-client", channelBuilder),
                metadata,
                config.getString(StreamsConfig.CLIENT_ID_CONFIG),
                100, // a fixed large enough value will suffice
                config.getLong(config.RECONNECT_BACKOFF_MS_CONFIG),
                config.getInt(config.SEND_BUFFER_CONFIG),
                config.getInt(config.RECEIVE_BUFFER_CONFIG),
                config.getInt(config.REQUEST_TIMEOUT_MS_CONFIG), time);
    }
}
