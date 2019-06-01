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
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * The Connect metrics with JMX reporter.
 */
public class ConnectMetrics {

    public static final String JMX_PREFIX = "kafka.connect";

    private static final Logger LOG = LoggerFactory.getLogger(ConnectMetrics.class);

    private final Metrics metrics;
    private final Time time;
    private final String workerId;
    private final ConcurrentMap<MetricGroupId, MetricGroup> groupsByName = new ConcurrentHashMap<>();
    private final ConnectMetricsRegistry registry = new ConnectMetricsRegistry();

    /**
     * Create an instance.
     *
     * @param workerId the worker identifier; may not be null
     * @param config   the worker configuration; may not be null
     * @param time     the time; may not be null
     */
    public ConnectMetrics(String workerId, WorkerConfig config, Time time) {
        this(workerId, time, config.getInt(CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG),
                config.getLong(CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG),
                config.getString(CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG),
                config.getConfiguredInstances(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, MetricsReporter.class));
    }

    public ConnectMetrics(String workerId, Time time, int numSamples, long sampleWindowMs, String metricsRecordingLevel,
                          List<MetricsReporter> reporters) {
        this.workerId = workerId;
        this.time = time;

        MetricConfig metricConfig = new MetricConfig().samples(numSamples)
                .timeWindow(sampleWindowMs, TimeUnit.MILLISECONDS).recordLevel(
                        Sensor.RecordingLevel.forName(metricsRecordingLevel));
        reporters.add(new JmxReporter(JMX_PREFIX));
        this.metrics = new Metrics(metricConfig, reporters, time);
        LOG.debug("Registering Connect metrics with JMX for worker '{}'", workerId);
        AppInfoParser.registerAppInfo(JMX_PREFIX, workerId, metrics, time.milliseconds());
    }

    /**
     * Get the worker identifier.
     *
     * @return the worker ID; never null
     */
    public String workerId() {
        return workerId;
    }

    /**
     * Get the {@link Metrics Kafka Metrics} that are managed by this object and that should be used to
     * add sensors and individual metrics.
     *
     * @return the Kafka Metrics instance; never null
     */
    public Metrics metrics() {
        return metrics;
    }

    /**
     * Get the registry of metric names.
     *
     * @return the registry for the Connect metrics; never null
     */
    public ConnectMetricsRegistry registry() {
        return registry;
    }

    /**
     * Get or create a {@link MetricGroup} with the specified group name and the given tags.
     * Each group is uniquely identified by the name and tags.
     *
     * @param groupName    the name of the metric group; may not be null
     * @param tagKeyValues pairs of tag name and values
     * @return the {@link MetricGroup} that can be used to create metrics; never null
     * @throws IllegalArgumentException if the group name is not valid
     */
    public MetricGroup group(String groupName, String... tagKeyValues) {
        MetricGroupId groupId = groupId(groupName, tagKeyValues);
        MetricGroup group = groupsByName.get(groupId);
        if (group == null) {
            group = new MetricGroup(groupId);
            MetricGroup previous = groupsByName.putIfAbsent(groupId, group);
            if (previous != null)
                group = previous;
        }
        return group;
    }

    protected MetricGroupId groupId(String groupName, String... tagKeyValues) {
        Map<String, String> tags = tags(tagKeyValues);
        return new MetricGroupId(groupName, tags);
    }

    /**
     * Get the time.
     *
     * @return the time; never null
     */
    public Time time() {
        return time;
    }

    /**
     * Stop and unregister the metrics from any reporters.
     */
    public void stop() {
        metrics.close();
        LOG.debug("Unregistering Connect metrics with JMX for worker '{}'", workerId);
        AppInfoParser.unregisterAppInfo(JMX_PREFIX, workerId, metrics);
    }

    public static class MetricGroupId {
        private final String groupName;
        private final Map<String, String> tags;
        private final int hc;
        private final String str;

        public MetricGroupId(String groupName, Map<String, String> tags) {
            Objects.requireNonNull(groupName);
            Objects.requireNonNull(tags);
            this.groupName = groupName;
            this.tags = Collections.unmodifiableMap(new LinkedHashMap<>(tags));
            this.hc = Objects.hash(this.groupName, this.tags);
            StringBuilder sb = new StringBuilder(this.groupName);
            for (Map.Entry<String, String> entry : this.tags.entrySet()) {
                sb.append(";").append(entry.getKey()).append('=').append(entry.getValue());
            }
            this.str = sb.toString();
        }

        /**
         * Get the group name.
         *
         * @return the group name; never null
         */
        public String groupName() {
            return groupName;
        }

        /**
         * Get the immutable map of tag names and values.
         *
         * @return the tags; never null
         */
        public Map<String, String> tags() {
            return tags;
        }

        /**
         * Determine if the supplied metric name is part of this group identifier.
         *
         * @param metricName the metric name
         * @return true if the metric name's group and tags match this group identifier, or false otherwise
         */
        public boolean includes(MetricName metricName) {
            return metricName != null && groupName.equals(metricName.group()) && tags.equals(metricName.tags());
        }

        @Override
        public int hashCode() {
            return hc;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (obj instanceof MetricGroupId) {
                MetricGroupId that = (MetricGroupId) obj;
                return this.groupName.equals(that.groupName) && this.tags.equals(that.tags);
            }
            return false;
        }

        @Override
        public String toString() {
            return str;
        }
    }

    /**
     * A group of metrics. Each group maps to a JMX MBean and each metric maps to an MBean attribute.
     * <p>
     * Sensors should be added via the {@code sensor} methods on this class, rather than directly through
     * the {@link Metrics} class, so that the sensor names are made to be unique (based on the group name)
     * and so the sensors are removed when this group is {@link #close() closed}.
     */
    public class MetricGroup implements AutoCloseable {
        private final MetricGroupId groupId;
        private final Set<String> sensorNames = new HashSet<>();
        private final String sensorPrefix;

        /**
         * Create a group of Connect metrics.
         *
         * @param groupId the identifier of the group; may not be null and must be valid
         */
        protected MetricGroup(MetricGroupId groupId) {
            Objects.requireNonNull(groupId);
            this.groupId = groupId;
            sensorPrefix = "connect-sensor-group: " + groupId.toString() + ";";
        }

        /**
         * Get the group identifier.
         *
         * @return the group identifier; never null
         */
        public MetricGroupId groupId() {
            return groupId;
        }

        /**
         * Create the name of a metric that belongs to this group and has the group's tags.
         *
         * @param template the name template for the metric; may not be null
         * @return the metric name; never null
         * @throws IllegalArgumentException if the name is not valid
         */
        public MetricName metricName(MetricNameTemplate template) {
            return metrics.metricInstance(template, groupId.tags());
        }

        // for testing only
        MetricName metricName(String name) {
            return metrics.metricName(name, groupId.groupName(), "", groupId.tags());
        }

        /**
         * The {@link Metrics} that this group belongs to.
         * <p>
         * Do not use this to add {@link Sensor Sensors}, since they will not be removed when this group is
         * {@link #close() closed}. Metrics can be added directly, as long as the metric names are obtained from
         * this group via the {@link #metricName(MetricNameTemplate)} method.
         *
         * @return the metrics; never null
         */
        public Metrics metrics() {
            return metrics;
        }

        /**
         * The tags of this group.
         *
         * @return the unmodifiable tags; never null but may be empty
         */
        Map<String, String> tags() {
            return groupId.tags();
        }

        /**
         * Add to this group an indicator metric with a function that returns the current value.
         *
         * @param nameTemplate the name template for the metric; may not be null
         * @param supplier     the function used to determine the literal value of the metric; may not be null
         * @throws IllegalArgumentException if the name is not valid
         */
        public <T> void addValueMetric(MetricNameTemplate nameTemplate, final LiteralSupplier<T> supplier) {
            MetricName metricName = metricName(nameTemplate);
            if (metrics().metric(metricName) == null) {
                metrics().addMetric(metricName, new Gauge<T>() {
                    @Override
                    public T value(MetricConfig config, long now) {
                        return supplier.metricValue(now);
                    }
                });
            }
        }

        /**
         * Add to this group an indicator metric that always returns the specified value.
         *
         * @param nameTemplate the name template for the metric; may not be null
         * @param value        the value; may not be null
         * @throws IllegalArgumentException if the name is not valid
         */
        public <T> void addImmutableValueMetric(MetricNameTemplate nameTemplate, final T value) {
            MetricName metricName = metricName(nameTemplate);
            if (metrics().metric(metricName) == null) {
                metrics().addMetric(metricName, new Gauge<T>() {
                    @Override
                    public T value(MetricConfig config, long now) {
                        return value;
                    }
                });
            }
        }

        /**
         * Get or create a sensor with the given unique name and no parent sensors. This uses
         * a default recording level of INFO.
         *
         * @param name The sensor name
         * @return The sensor
         */
        public Sensor sensor(String name) {
            return sensor(name, null, Sensor.RecordingLevel.INFO);
        }

        /**
         * Get or create a sensor with the given unique name and no parent sensors. This uses
         * a default recording level of INFO.
         *
         * @param name The sensor name
         * @return The sensor
         */
        public Sensor sensor(String name, Sensor... parents) {
            return sensor(name, null, Sensor.RecordingLevel.INFO, parents);
        }

        /**
         * Get or create a sensor with the given unique name and zero or more parent sensors. All parent sensors will
         * receive every value recorded with this sensor.
         *
         * @param name           The name of the sensor
         * @param recordingLevel The recording level.
         * @param parents        The parent sensors
         * @return The sensor that is created
         */
        public Sensor sensor(String name, Sensor.RecordingLevel recordingLevel, Sensor... parents) {
            return sensor(name, null, recordingLevel, parents);
        }

        /**
         * Get or create a sensor with the given unique name and zero or more parent sensors. All parent sensors will
         * receive every value recorded with this sensor.
         *
         * @param name    The name of the sensor
         * @param config  A default configuration to use for this sensor for metrics that don't have their own config
         * @param parents The parent sensors
         * @return The sensor that is created
         */
        public Sensor sensor(String name, MetricConfig config, Sensor... parents) {
            return sensor(name, config, Sensor.RecordingLevel.INFO, parents);
        }

        /**
         * Get or create a sensor with the given unique name and zero or more parent sensors. All parent sensors will
         * receive every value recorded with this sensor.
         *
         * @param name           The name of the sensor
         * @param config         A default configuration to use for this sensor for metrics that don't have their own config
         * @param recordingLevel The recording level.
         * @param parents        The parent sensors
         * @return The sensor that is created
         */
        public synchronized Sensor sensor(String name, MetricConfig config, Sensor.RecordingLevel recordingLevel, Sensor... parents) {
            // We need to make sure that all sensor names are unique across all groups, so use the sensor prefix
            Sensor result = metrics.sensor(sensorPrefix + name, config, Long.MAX_VALUE, recordingLevel, parents);
            if (result != null)
                sensorNames.add(result.name());
            return result;
        }

        /**
         * Remove all sensors and metrics associated with this group.
         */
        public synchronized void close() {
            for (String sensorName : sensorNames) {
                metrics.removeSensor(sensorName);
            }
            sensorNames.clear();
            for (MetricName metricName : new HashSet<>(metrics.metrics().keySet())) {
                if (groupId.includes(metricName)) {
                    metrics.removeMetric(metricName);
                }
            }
        }
    }

    /**
     * A simple functional interface that returns a literal value.
     */
    public interface LiteralSupplier<T> {

        /**
         * Return the literal value for the metric.
         *
         * @param now the current time in milliseconds
         * @return the literal metric value; may not be null
         */
        T metricValue(long now);
    }

    /**
     * Create a set of tags using the supplied key and value pairs. The order of the tags will be kept.
     *
     * @param keyValue the key and value pairs for the tags; must be an even number
     * @return the map of tags that can be supplied to the {@link Metrics} methods; never null
     */
    static Map<String, String> tags(String... keyValue) {
        if ((keyValue.length % 2) != 0)
            throw new IllegalArgumentException("keyValue needs to be specified in pairs");
        Map<String, String> tags = new LinkedHashMap<>();
        for (int i = 0; i < keyValue.length; i += 2) {
            tags.put(keyValue[i], keyValue[i + 1]);
        }
        return tags;
    }

    /**
     * Utility to generate the documentation for the Connect metrics.
     *
     * @param args the arguments
     */
    public static void main(String[] args) {
        ConnectMetricsRegistry metrics = new ConnectMetricsRegistry();
        System.out.println(Metrics.toHtmlTable(JMX_PREFIX, metrics.getAllTemplates()));
    }
}