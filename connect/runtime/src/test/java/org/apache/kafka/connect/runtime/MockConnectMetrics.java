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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.utils.MockTime;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A specialization of {@link ConnectMetrics} that uses a custom {@link MetricsReporter} to capture the metrics
 * that were created, and makes those metrics available even after the metrics were removed from the
 * {@link org.apache.kafka.common.metrics.Metrics} registry.
 *
 * This is needed because many of the Connect metric groups are specific to connectors and/or tasks, and therefore
 * their metrics are removed from the {@link org.apache.kafka.common.metrics.Metrics} registry when the connector
 * and tasks are closed. This instance keeps track of the metrics that were created so that it is possible for
 * tests to {@link #currentMetricValue(MetricGroup, String) read the metrics' value} even after the connector
 * and/or tasks have been closed.
 *
 * If the same metric is created a second time (e.g., a worker task is re-created), the new metric will replace
 * the previous metric in the custom reporter.
 */
@SuppressWarnings("deprecation")
public class MockConnectMetrics extends ConnectMetrics {

    private static final Map<String, String> DEFAULT_WORKER_CONFIG = new HashMap<>();

    static {
        DEFAULT_WORKER_CONFIG.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        DEFAULT_WORKER_CONFIG.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        DEFAULT_WORKER_CONFIG.put(WorkerConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        DEFAULT_WORKER_CONFIG.put(WorkerConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        DEFAULT_WORKER_CONFIG.put(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
    }

    public MockConnectMetrics() {
        this(new MockTime());
    }

    public MockConnectMetrics(MockTime time) {
        super("mock", new WorkerConfig(WorkerConfig.baseConfigDef(), DEFAULT_WORKER_CONFIG), time, "cluster-1");
    }

    @Override
    public MockTime time() {
        return (MockTime) super.time();
    }

    /**
     * Get the current value of the named metric, which may have already been removed from the
     * {@link org.apache.kafka.common.metrics.Metrics} but will have been captured before it was removed.
     *
     * @param metricGroup the metric metricGroup that contained the metric
     * @param name        the name of the metric
     * @return the current value of the metric
     */
    public Object currentMetricValue(MetricGroup metricGroup, String name) {
        return currentMetricValue(this, metricGroup, name);
    }

    /**
     * Get the current value of the named metric, which may have already been removed from the
     * {@link org.apache.kafka.common.metrics.Metrics} but will have been captured before it was removed.
     *
     * @param metricGroup the metric metricGroup that contained the metric
     * @param name        the name of the metric
     * @return the current value of the metric
     */
    public double currentMetricValueAsDouble(MetricGroup metricGroup, String name) {
        Object value = currentMetricValue(metricGroup, name);
        return value instanceof Double ? (Double) value : Double.NaN;
    }

    /**
     * Get the current value of the named metric, which may have already been removed from the
     * {@link org.apache.kafka.common.metrics.Metrics} but will have been captured before it was removed.
     *
     * @param metricGroup the metric metricGroup that contained the metric
     * @param name        the name of the metric
     * @return the current value of the metric
     */
    public String currentMetricValueAsString(MetricGroup metricGroup, String name) {
        Object value = currentMetricValue(metricGroup, name);
        return value instanceof String ? (String) value : null;
    }

    /**
     * Get the current value of the named metric, which may have already been removed from the
     * {@link org.apache.kafka.common.metrics.Metrics} but will have been captured before it was removed.
     *
     * @param metrics the {@link ConnectMetrics} instance
     * @param metricGroup the metric metricGroup that contained the metric
     * @param name        the name of the metric
     * @return the current value of the metric
     */
    public static Object currentMetricValue(ConnectMetrics metrics, MetricGroup metricGroup, String name) {
        MetricName metricName = metricGroup.metricName(name);
        for (MetricsReporter reporter : metrics.metrics().reporters()) {
            if (reporter instanceof MockMetricsReporter) {
                return ((MockMetricsReporter) reporter).currentMetricValue(metricName);
            }
        }
        return null;
    }

    /**
     * Get the current value of the named metric, which may have already been removed from the
     * {@link org.apache.kafka.common.metrics.Metrics} but will have been captured before it was removed.
     *
     * @param metrics the {@link ConnectMetrics} instance
     * @param metricGroup the metric metricGroup that contained the metric
     * @param name        the name of the metric
     * @return the current value of the metric
     */
    public static double currentMetricValueAsDouble(ConnectMetrics metrics, MetricGroup metricGroup, String name) {
        Object value = currentMetricValue(metrics, metricGroup, name);
        return value instanceof Double ? (Double) value : Double.NaN;
    }

    /**
     * Get the current value of the named metric, which may have already been removed from the
     * {@link org.apache.kafka.common.metrics.Metrics} but will have been captured before it was removed.
     *
     * @param metrics the {@link ConnectMetrics} instance
     * @param metricGroup the metric metricGroup that contained the metric
     * @param name        the name of the metric
     * @return the current value of the metric
     */
    public static String currentMetricValueAsString(ConnectMetrics metrics, MetricGroup metricGroup, String name) {
        Object value = currentMetricValue(metrics, metricGroup, name);
        return value instanceof String ? (String) value : null;
    }

    public static class MockMetricsReporter implements MetricsReporter {
        private Map<MetricName, KafkaMetric> metricsByName = new HashMap<>();

        private MetricsContext metricsContext;

        public MockMetricsReporter() {
        }

        @Override
        public void configure(Map<String, ?> configs) {
            // do nothing
        }

        @Override
        public void init(List<KafkaMetric> metrics) {
            for (KafkaMetric metric : metrics) {
                metricsByName.put(metric.metricName(), metric);
            }
        }

        @Override
        public void metricChange(KafkaMetric metric) {
            metricsByName.put(metric.metricName(), metric);
        }

        @Override
        public void metricRemoval(KafkaMetric metric) {
            // don't remove metrics, or else we won't be able to access them after the metric metricGroup is closed
        }

        @Override
        public void close() {
            // do nothing
        }

        /**
         * Get the current value of the metric.
         *
         * @param metricName the name of the metric that was registered most recently
         * @return the current value of the metric
         */
        public Object currentMetricValue(MetricName metricName) {
            KafkaMetric metric = metricsByName.get(metricName);
            return metric != null ? metric.metricValue() : null;
        }

        @Override
        public void contextChange(MetricsContext metricsContext) {
            this.metricsContext = metricsContext;
        }

        public MetricsContext getMetricsContext() {
            return this.metricsContext;
        }
    }
}
