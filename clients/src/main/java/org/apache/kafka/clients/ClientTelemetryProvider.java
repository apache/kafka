/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.ConsumerUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.metrics.MetricsContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClientTelemetryProvider implements Configurable {

    public static final String DOMAIN = "org.apache.kafka";
    // Client metrics tags
    public static final String CLIENT_RACK = "client_rack";
    public static final String GROUP_ID = "group_id";
    public static final String GROUP_INSTANCE_ID = "group_instance_id";
    public static final String TRANSACTIONAL_ID = "transactional_id";

    private static final Map<String, String> PRODUCER_CONFIG_MAPPING = new HashMap<>();
    private static final Map<String, String> CONSUMER_CONFIG_MAPPING = new HashMap<>();

    private Resource resource = null;
    private Map<String, ?> config = null;

    static {
        PRODUCER_CONFIG_MAPPING.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, ClientTelemetryProvider.TRANSACTIONAL_ID);

        CONSUMER_CONFIG_MAPPING.put(ConsumerConfig.GROUP_ID_CONFIG, ClientTelemetryProvider.GROUP_ID);
        CONSUMER_CONFIG_MAPPING.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, ClientTelemetryProvider.GROUP_INSTANCE_ID);
    }

    @Override
    public synchronized void configure(Map<String, ?> configs) {
        this.config = configs;
    }

    /**
     * Validate that all the data required for generating correct metrics is present. The provider
     * will be disabled if validation fails.
     *
     * @param metricsContext {@link MetricsContext}
     * @return false if all the data required for generating correct metrics is missing, true
     * otherwise.
     */
    public boolean validate(MetricsContext metricsContext, Map<String, ?> config) {
        // metric collection will be disabled for clients without a client id (e.g. transient admin clients)
        return ClientTelemetryUtils.validateResourceLabel(config, CommonClientConfigs.CLIENT_ID_CONFIG) &&
            ClientTelemetryUtils.validateRequiredResourceLabels(metricsContext.contextLabels());
    }

    /**
     * Sets the metrics tags for the service or library exposing metrics. This will be called before
     * {@link org.apache.kafka.common.metrics.MetricsReporter#init(List)} and may be called anytime
     * after that.
     *
     * @param metricsContext {@link MetricsContext}
     */
    public void contextChange(MetricsContext metricsContext) {
        final Resource.Builder resourceBuilder = Resource.newBuilder();

        final String namespace = metricsContext.contextLabels().get(MetricsContext.NAMESPACE);
        if (KafkaProducer.JMX_PREFIX.equals(namespace)) {
            // Add producer resource labels.
            PRODUCER_CONFIG_MAPPING.forEach((configKey, telemetryKey) -> {
                if (config.containsKey(configKey)) {
                    addAttribute(resourceBuilder, telemetryKey, String.valueOf(config.get(configKey)));
                }
            });
        } else if (ConsumerUtils.CONSUMER_JMX_PREFIX.equals(namespace)) {
            // Add consumer resource labels.
            CONSUMER_CONFIG_MAPPING.forEach((configKey, telemetryKey) -> {
                if (config.containsKey(configKey)) {
                    addAttribute(resourceBuilder, telemetryKey, String.valueOf(config.get(configKey)));
                }
            });
        }

        // Add client rack label.
        if (config.containsKey(CommonClientConfigs.CLIENT_RACK_CONFIG)) {
            addAttribute(resourceBuilder, CLIENT_RACK, String.valueOf(config.get(CommonClientConfigs.CLIENT_RACK_CONFIG)));
        }

        this.resource = resourceBuilder.build();
    }

    /**
     * The metrics resource for this provider which will be used to generate the metrics.
     *
     * @return A fully formed {@link Resource} with all the tags.
     */
    public Resource resource() {
        return this.resource;
    }

    /**
     * Domain of the active provider i.e. specifies prefix to the metrics.
     *
     * @return Domain in string format.
     */
    public String domain() {
        return DOMAIN;
    }

    private void addAttribute(Resource.Builder resourceBuilder, String key, String value) {
        final KeyValue.Builder kv = KeyValue.newBuilder()
            .setKey(key)
            .setValue(AnyValue.newBuilder().setStringValue(value));
        resourceBuilder.addAttributes(kv);
    }
}
