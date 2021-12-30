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

import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.Sum;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.message.GetTelemetrySubscriptionsResponseData;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.GetTelemetrySubscriptionRequest;
import org.apache.kafka.common.requests.GetTelemetrySubscriptionRequest.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TelemetryMetricsReporter implements MetricsReporter {

    private static final Logger log = LoggerFactory.getLogger(TelemetryMetricsReporter.class);

    private volatile TelemetrySubscription telemetrySubscription;

    private final Map<MetricName, KafkaMetric> metrics = new HashMap<>();

    @Override
    public void configure(final Map<String, ?> configs) {
        log.warn("configure - configs: {}", configs);
    }

    @Override
    public void init(final List<KafkaMetric> metrics) {
        log.warn("init - metrics: {}", metrics);
    }

    @Override
    public void metricChange(final KafkaMetric metric) {
        if (metric.metricName().name().startsWith("org.apache.kafka")) {
            log.warn("metricChange - metric: {}", metric);
            metrics.put(metric.metricName(), metric);
        }
    }

    @Override
    public void metricRemoval(final KafkaMetric metric) {
        if (metric.metricName().name().startsWith("org.apache.kafka")) {
            log.warn("metricRemoval - metric: {}", metric);
            metrics.remove(metric.metricName());
        }
    }

    @Override
    public void close() {
        log.warn("close");

        for (KafkaMetric metric : metrics.values()) {
            MetricName name = metric.metricName();
            Object value = metric.metricValue();
            log.warn("close - name: {}, value: {}", name, value);

            double doubleValue = Double.parseDouble(value.toString());
            log.warn("close - doubleValue: {}", doubleValue);
            long longValue = Double.valueOf(doubleValue).longValue();
            log.warn("close - longValue: {}", longValue);

            NumberDataPoint numberDataPoint = NumberDataPoint.newBuilder()
                .setAsInt(longValue)
                .build();

            log.warn("close - numberDataPoint: {}", numberDataPoint);

            Sum sum = Sum.newBuilder()
                .addDataPoints(numberDataPoint)
                .build();

            log.warn("close - sum: {}", sum);

            Metric otlpMetric = Metric.newBuilder()
                .setName(name.name())
                .setDescription(name.description())
                .setSum(sum)
                .build();

            log.warn("close - otlpMetric: {}", otlpMetric);

            byte[] bytes = otlpMetric.toByteArray();
        }
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        log.warn("reconfigurableConfigs");
        return MetricsReporter.super.reconfigurableConfigs();
    }

    @Override
    public void validateReconfiguration(final Map<String, ?> configs) throws ConfigException {
        log.warn("validateReconfiguration - configs: {}", configs);
        MetricsReporter.super.validateReconfiguration(configs);
    }

    @Override
    public void reconfigure(final Map<String, ?> configs) {
        log.warn("reconfigure - configs: {}", configs);
        MetricsReporter.super.reconfigure(configs);
    }

    @Override
    public void contextChange(final MetricsContext metricsContext) {
        log.warn("contextChange - metricsContext: {}", metricsContext);
        MetricsReporter.super.contextChange(metricsContext);
    }
    private void updateTelemetrySubscription(long now) throws IOException {
        GetTelemetrySubscriptionsResponseData data;

        try {
            AbstractRequest.Builder<GetTelemetrySubscriptionRequest> builder = new Builder(
                telemetrySubscription != null ? telemetrySubscription.clientInstanceId() : null);
//            ClientRequest clientRequest = client.newClientRequest(node.idString(), builder, now, true);
//            ClientResponse clientResponse = NetworkClientUtils.sendAndReceive(client, clientRequest, time);
//            GetTelemetrySubscriptionResponse responseBody = (GetTelemetrySubscriptionResponse) clientResponse.responseBody();
//            data = responseBody.data();
            data = new GetTelemetrySubscriptionsResponseData();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }

        List<String> requestedMetricsRaw = data.requestedMetrics();
        Set<MetricName> metricNames;

        if (requestedMetricsRaw == null || requestedMetricsRaw.isEmpty()) {
            // no metrics
            metricNames = Collections.emptySet();
        } else if (requestedMetricsRaw.size() == 1 && requestedMetricsRaw.get(0).isEmpty()) {
            // all metrics
            metricNames = new HashSet<>();
        } else {
            // prefix string match...
            metricNames = new HashSet<>();
        }

        List<Byte> acceptedCompressionTypesRaw = data.acceptedCompressionTypes();
        Set<CompressionType> acceptedCompressionTypes;

        if (acceptedCompressionTypesRaw == null || acceptedCompressionTypesRaw.isEmpty()) {
            acceptedCompressionTypes = Collections.emptySet();
        } else {
            acceptedCompressionTypes = new HashSet<>();
        }

        telemetrySubscription = new TelemetrySubscription(data.throttleTimeMs(),
            data.clientInstanceId(),
            data.subscriptionId(),
            acceptedCompressionTypes,
            data.pushIntervalMs(),
            data.deltaTemporality(),
            metricNames);
    }

}
