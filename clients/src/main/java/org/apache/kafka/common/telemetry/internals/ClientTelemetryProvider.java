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
package org.apache.kafka.common.telemetry.internals;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.metrics.MetricsContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ClientTelemetryProvider implements Configurable {

    public static final String DOMAIN = "org.apache.kafka";
    // Client metrics tags
    public static final String CLIENT_RACK = "client_rack";
    public static final String GROUP_ID = "group_id";
    public static final String GROUP_INSTANCE_ID = "group_instance_id";
    public static final String GROUP_MEMBER_ID = "group_member_id";
    public static final String TRANSACTIONAL_ID = "transactional_id";

    private static final String PRODUCER_NAMESPACE = "kafka.producer";
    private static final String CONSUMER_NAMESPACE = "kafka.consumer";

    private static final Map<String, String> PRODUCER_CONFIG_MAPPING = new HashMap<>();
    private static final Map<String, String> CONSUMER_CONFIG_MAPPING = new HashMap<>();

    private volatile Resource resource = null;
    private Map<String, ?> config = null;

    // Mapping of config keys to telemetry keys. Contains only keys which can be fetched from config.
    // Config like group_member_id is not present here as it is not fetched from config.
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
     * Validate that all the data required for generating correct metrics is present.
     *
     * @param metricsContext {@link MetricsContext}
     * @return false if all the data required for generating correct metrics is missing, true
     * otherwise.
     */
    boolean validate(MetricsContext metricsContext) {
        return ClientTelemetryUtils.validateRequiredResourceLabels(metricsContext.contextLabels());
    }

    /**
     * Sets the metrics tags for the service or library exposing metrics. This will be called before
     * {@link org.apache.kafka.common.metrics.MetricsReporter#init(List)} and may be called anytime
     * after that.
     *
     * @param metricsContext {@link MetricsContext}
     */
    synchronized void contextChange(MetricsContext metricsContext) {
        final Resource.Builder resourceBuilder = Resource.newBuilder();

        final String namespace = metricsContext.contextLabels().get(MetricsContext.NAMESPACE);
        if (PRODUCER_NAMESPACE.equals(namespace)) {
            // Add producer resource labels.
            PRODUCER_CONFIG_MAPPING.forEach((configKey, telemetryKey) -> {
                if (config.containsKey(configKey)) {
                    addAttribute(resourceBuilder, telemetryKey, String.valueOf(config.get(configKey)));
                }
            });
        } else if (CONSUMER_NAMESPACE.equals(namespace)) {
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

        resource = resourceBuilder.build();
    }

    /**
     * Updates the resource labels/tags for the service or library exposing metrics.
     *
     * @param labels Map of labels to be updated.
     */
    synchronized void updateLabels(Map<String, String> labels) {
        final Resource.Builder resourceBuilder = resource.toBuilder();
        Map<String, String> finalLabels = resource.getAttributesList().stream().collect(Collectors.toMap(
            KeyValue::getKey, kv -> kv.getValue().getStringValue()));
        finalLabels.putAll(labels);

        resourceBuilder.clearAttributes();
        finalLabels.forEach((key, value) -> addAttribute(resourceBuilder, key, value));
        resource = resourceBuilder.build();
    }

    /**
     * The metrics resource for this provider which will be used to generate the metrics.
     *
     * @return A fully formed {@link Resource} with all the tags.
     */
    Resource resource() {
        return resource;
    }

    /**
     * Domain of the active provider i.e. specifies prefix to the metrics.
     *
     * @return Domain in string format.
     */
    String domain() {
        return DOMAIN;
    }

    private void addAttribute(Resource.Builder resourceBuilder, String key, String value) {
        final KeyValue.Builder kv = KeyValue.newBuilder()
            .setKey(key)
            .setValue(AnyValue.newBuilder().setStringValue(value));
        resourceBuilder.addAttributes(kv);
    }
}
