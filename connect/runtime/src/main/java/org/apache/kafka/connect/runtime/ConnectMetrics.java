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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The Connect metrics with JMX reporter.
 */
public class ConnectMetrics {

    public static final String JMX_PREFIX = "kafka.connect";
    public static final String WORKER_ID_TAG_NAME = "worker-id";

    private static final Logger LOG = LoggerFactory.getLogger(ConnectMetrics.class);
    private static final AtomicInteger CONNECT_WORKER_ID_SEQUENCE = new AtomicInteger(1);

    /**
     * Utility to ensure the supplied name contains valid characters, replacing sequences of 1 or more
     * ':', '/', '\', '*', '?', '[', ']', '=', or comma characters with a single '-'.
     *
     * @param name the name; may not be null
     * @return the validated name; never null
     */
    static String makeValidName(String name) {
        Objects.requireNonNull(name);
        name = name.trim();
        if (!name.isEmpty()) {
            name = name.replaceAll("[:/\\\\*?,;\\[\\]\\\\=]+", "-");
        }
        return name;
    }

    private final Metrics metrics;
    private final Time time;
    private final String workerId;
    private final ConcurrentMap<String, MetricGroup> groupsByName = new ConcurrentHashMap<>();

    /**
     * Create an instance.
     *
     * @param workerId the worker identifier; may be null
     * @param config   the worker configuration; may not be null
     * @param time     the time; may not be null
     */
    public ConnectMetrics(String workerId, WorkerConfig config, Time time) {
        this.workerId = workerId != null ? makeValidName(workerId) : "worker-" + CONNECT_WORKER_ID_SEQUENCE.getAndIncrement();
        this.time = time;

        MetricConfig metricConfig = new MetricConfig().samples(config.getInt(CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG))
                                            .timeWindow(config.getLong(CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                                            .recordLevel(Sensor.RecordingLevel.forName(config.getString(CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG)));
        List<MetricsReporter> reporters = config.getConfiguredInstances(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, MetricsReporter.class);
        reporters.add(new JmxReporter(JMX_PREFIX));
        this.metrics = new Metrics(metricConfig, reporters, time);
        LOG.debug("Registering Connect metrics with JMX for worker '{}'", workerId);
        AppInfoParser.registerAppInfo(JMX_PREFIX, workerId);
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
     * Get or create a {@link MetricGroup} with the specified group name.
     *
     * @param groupName the name of the metric group; may not be null
     * @return the {@link MetricGroup} that can be used to create metrics; never null
     */
    public synchronized MetricGroup group(String groupName) {
        return group(groupName, false);
    }

    /**
     * Get or create a {@link MetricGroup} with the specified group name and the given tags.
     *
     * @param groupName    the name of the metric group; may not be null
     * @param tagKeyValues pairs of tag name and values
     * @return the {@link MetricGroup} that can be used to create metrics; never null
     */
    public synchronized MetricGroup group(String groupName, String... tagKeyValues) {
        return group(groupName, false, tagKeyValues);
    }

    /**
     * Get or create a {@link MetricGroup} with the specified group name and the given tags.
     *
     * @param groupName       the name of the metric group; may not be null
     * @param includeWorkerId true if the tags should include the worker ID
     * @param tagKeyValues    pairs of tag name and values
     * @return the {@link MetricGroup} that can be used to create metrics; never null
     */
    public synchronized MetricGroup group(String groupName, boolean includeWorkerId, String... tagKeyValues) {
        MetricGroup group = groupsByName.get(groupName);
        if (group == null) {
            Map<String, String> tags = tags(includeWorkerId ? workerId : null, tagKeyValues);
            group = new MetricGroup(groupName, tags);
            groupsByName.putIfAbsent(groupName, group);
        }
        return group;
    }

    /**
     * Create a set of tags using the supplied key and value pairs.
     *
     * @param workerId the worker ID that should be included first in the tags; may be null if not to be included
     * @param keyValue the key and value pairs for the tags; must be an even number
     * @return the map of tags that can be supplied to the {@link Metrics} methods; never null
     */
    static Map<String, String> tags(String workerId, String... keyValue) {
        if ((keyValue.length % 2) != 0)
            throw new IllegalArgumentException("keyValue needs to be specified in pairs");
        Map<String, String> tags = new HashMap<>();
        if (workerId != null && !workerId.trim().isEmpty()) {
            tags.put(WORKER_ID_TAG_NAME, workerId);
        }
        for (int i = 0; i < keyValue.length; i += 2) {
            tags.put(keyValue[i], keyValue[i + 1]);
        }
        return tags;
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
        AppInfoParser.unregisterAppInfo(JMX_PREFIX, workerId);
    }

    /**
     * A group of metrics. Each group maps to a JMX MBean and each metric maps to an MBean attribute.
     */
    public class MetricGroup {
        private final String groupName;
        private final Map<String, String> tags;

        protected MetricGroup(String groupName, Map<String, String> tags) {
            this.groupName = groupName;
            this.tags = tags;
        }

        /**
         * Create the name of a metric that belongs to this group and has the group's tags.
         *
         * @param name the name of the metric/attribute; may not be null
         * @param desc the description for the metric/attribute; may not be null
         * @return the metric name; never null
         */
        public MetricName metricName(String name, String desc) {
            return metrics.metricName(name, groupName, desc, tags);
        }

        /**
         * The {@link Metrics} that this group belongs to.
         *
         * @return the metrics; never null
         */
        public Metrics metrics() {
            return metrics;
        }

        Map<String, String> tags() {
            return Collections.unmodifiableMap(tags);
        }
    }
}