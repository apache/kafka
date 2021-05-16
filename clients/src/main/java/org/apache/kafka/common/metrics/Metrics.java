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
package org.apache.kafka.common.metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.internals.MetricsUtils;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;

/**
 * A registry of sensors and metrics.
 * <p>
 * A metric is a named, numerical measurement. A sensor is a handle to record numerical measurements as they occur. Each
 * Sensor has zero or more associated metrics. For example a Sensor might represent message sizes and we might associate
 * with this sensor a metric for the average, maximum, or other statistics computed off the sequence of message sizes
 * that are recorded by the sensor.
 * <p>
 * Usage looks something like this:
 * 
 * <pre>
 * // set up metrics:
 * Metrics metrics = new Metrics(); // this is the global repository of metrics and sensors
 * Sensor sensor = metrics.sensor(&quot;message-sizes&quot;);
 * MetricName metricName = new MetricName(&quot;message-size-avg&quot;, &quot;producer-metrics&quot;);
 * sensor.add(metricName, new Avg());
 * metricName = new MetricName(&quot;message-size-max&quot;, &quot;producer-metrics&quot;);
 * sensor.add(metricName, new Max());
 * 
 * // as messages are sent we record the sizes
 * sensor.record(messageSize);
 * </pre>
 */
public class Metrics implements Closeable {

    private final MetricConfig config;
    private final ConcurrentMap<MetricName, KafkaMetric> metrics;
    private final ConcurrentMap<String, Sensor> sensors;
    private final ConcurrentMap<Sensor, List<Sensor>> childrenSensors;
    private final List<MetricsReporter> reporters;
    private final Time time;
    private final ScheduledThreadPoolExecutor metricsScheduler;
    private static final Logger log = LoggerFactory.getLogger(Metrics.class);

    /**
     * Create a metrics repository with no metric reporters and default configuration.
     * Expiration of Sensors is disabled.
     */
    public Metrics() {
        this(new MetricConfig());
    }

    /**
     * Create a metrics repository with no metric reporters and default configuration.
     * Expiration of Sensors is disabled.
     */
    public Metrics(Time time) {
        this(new MetricConfig(), new ArrayList<>(0), time);
    }

    /**
     * Create a metrics repository with no metric reporters and the given default configuration.
     * Expiration of Sensors is disabled.
     */
    public Metrics(MetricConfig defaultConfig, Time time) {
        this(defaultConfig, new ArrayList<>(0), time);
    }


  /**
     * Create a metrics repository with no reporters and the given default config. This config will be used for any
     * metric that doesn't override its own config. Expiration of Sensors is disabled.
     * @param defaultConfig The default config to use for all metrics that don't override their config
     */
    public Metrics(MetricConfig defaultConfig) {
        this(defaultConfig, new ArrayList<>(0), Time.SYSTEM);
    }

    /**
     * Create a metrics repository with a default config and the given metric reporters.
     * Expiration of Sensors is disabled.
     * @param defaultConfig The default config
     * @param reporters The metrics reporters
     * @param time The time instance to use with the metrics
     */
    public Metrics(MetricConfig defaultConfig, List<MetricsReporter> reporters, Time time) {
        this(defaultConfig, reporters, time, false);
    }

    /**
     * Create a metrics repository with a default config, metric reporters and metric context
     * Expiration of Sensors is disabled.
     * @param defaultConfig The default config
     * @param reporters The metrics reporters
     * @param time The time instance to use with the metrics
     * @param metricsContext The metricsContext to initialize metrics reporter with
     */
    public Metrics(MetricConfig defaultConfig, List<MetricsReporter> reporters, Time time, MetricsContext metricsContext) {
        this(defaultConfig, reporters, time, false, metricsContext);
    }

    /**
     * Create a metrics repository with a default config, given metric reporters and the ability to expire eligible sensors
     * @param defaultConfig The default config
     * @param reporters The metrics reporters
     * @param time The time instance to use with the metrics
     * @param enableExpiration true if the metrics instance can garbage collect inactive sensors, false otherwise
     */
    public Metrics(MetricConfig defaultConfig, List<MetricsReporter> reporters, Time time, boolean enableExpiration) {
        this(defaultConfig, reporters, time, enableExpiration, new KafkaMetricsContext(""));
    }

    /**
     * Create a metrics repository with a default config, given metric reporters, the ability to expire eligible sensors
     * and MetricContext
     * @param defaultConfig The default config
     * @param reporters The metrics reporters
     * @param time The time instance to use with the metrics
     * @param enableExpiration true if the metrics instance can garbage collect inactive sensors, false otherwise
     * @param metricsContext The metricsContext to initialize metrics reporter with
     */
    public Metrics(MetricConfig defaultConfig, List<MetricsReporter> reporters, Time time, boolean enableExpiration,
                   MetricsContext metricsContext) {
        this.config = defaultConfig;
        this.sensors = new ConcurrentHashMap<>();
        this.metrics = new ConcurrentHashMap<>();
        this.childrenSensors = new ConcurrentHashMap<>();
        this.reporters = Objects.requireNonNull(reporters);
        this.time = time;
        for (MetricsReporter reporter : reporters) {
            reporter.contextChange(metricsContext);
            reporter.init(new ArrayList<>());
        }

        // Create the ThreadPoolExecutor only if expiration of Sensors is enabled.
        if (enableExpiration) {
            this.metricsScheduler = new ScheduledThreadPoolExecutor(1);
            // Creating a daemon thread to not block shutdown
            this.metricsScheduler.setThreadFactory(runnable -> KafkaThread.daemon("SensorExpiryThread", runnable));
            this.metricsScheduler.scheduleAtFixedRate(new ExpireSensorTask(), 30, 30, TimeUnit.SECONDS);
        } else {
            this.metricsScheduler = null;
        }

        addMetric(metricName("count", "kafka-metrics-count", "total number of registered metrics"),
            (config, now) -> metrics.size());
    }

    /**
     * Create a MetricName with the given name, group, description and tags, plus default tags specified in the metric
     * configuration. Tag in tags takes precedence if the same tag key is specified in the default metric configuration.
     *
     * @param name        The name of the metric
     * @param group       logical group name of the metrics to which this metric belongs
     * @param description A human-readable description to include in the metric
     * @param tags        additional key/value attributes of the metric
     */
    public MetricName metricName(String name, String group, String description, Map<String, String> tags) {
        Map<String, String> combinedTag = new LinkedHashMap<>(config.tags());
        combinedTag.putAll(tags);
        return new MetricName(name, group, description, combinedTag);
    }

    /**
     * Create a MetricName with the given name, group, description, and default tags
     * specified in the metric configuration.
     *
     * @param name        The name of the metric
     * @param group       logical group name of the metrics to which this metric belongs
     * @param description A human-readable description to include in the metric
     */
    public MetricName metricName(String name, String group, String description) {
        return metricName(name, group, description, new HashMap<>());
    }

    /**
     * Create a MetricName with the given name, group and default tags specified in the metric configuration.
     *
     * @param name        The name of the metric
     * @param group       logical group name of the metrics to which this metric belongs
     */
    public MetricName metricName(String name, String group) {
        return metricName(name, group, "", new HashMap<>());
    }

    /**
     * Create a MetricName with the given name, group, description, and keyValue as tags,  plus default tags specified in the metric
     * configuration. Tag in keyValue takes precedence if the same tag key is specified in the default metric configuration.
     *
     * @param name          The name of the metric
     * @param group         logical group name of the metrics to which this metric belongs
     * @param description   A human-readable description to include in the metric
     * @param keyValue      additional key/value attributes of the metric (must come in pairs)
     */
    public MetricName metricName(String name, String group, String description, String... keyValue) {
        return metricName(name, group, description, MetricsUtils.getTags(keyValue));
    }

    /**
     * Create a MetricName with the given name, group and tags, plus default tags specified in the metric
     * configuration. Tag in tags takes precedence if the same tag key is specified in the default metric configuration.
     *
     * @param name  The name of the metric
     * @param group logical group name of the metrics to which this metric belongs
     * @param tags  key/value attributes of the metric
     */
    public MetricName metricName(String name, String group, Map<String, String> tags) {
        return metricName(name, group, "", tags);
    }

    /**
     * Use the specified domain and metric name templates to generate an HTML table documenting the metrics. A separate table section
     * will be generated for each of the MBeans and the associated attributes. The MBean names are lexicographically sorted to
     * determine the order of these sections. This order is therefore dependent upon the order of the
     * tags in each {@link MetricNameTemplate}.
     *
     * @param domain the domain or prefix for the JMX MBean names; may not be null
     * @param allMetrics the collection of all {@link MetricNameTemplate} instances each describing one metric; may not be null
     * @return the string containing the HTML table; never null
     */
    public static String toHtmlTable(String domain, Iterable<MetricNameTemplate> allMetrics) {
        Map<String, Map<String, String>> beansAndAttributes = new TreeMap<>();
    
        try (Metrics metrics = new Metrics()) {
            for (MetricNameTemplate template : allMetrics) {
                Map<String, String> tags = new LinkedHashMap<>();
                for (String s : template.tags()) {
                    tags.put(s, "{" + s + "}");
                }
    
                MetricName metricName = metrics.metricName(template.name(), template.group(), template.description(), tags);
                String mBeanName = JmxReporter.getMBeanName(domain, metricName);
                if (!beansAndAttributes.containsKey(mBeanName)) {
                    beansAndAttributes.put(mBeanName, new TreeMap<>());
                }
                Map<String, String> attrAndDesc = beansAndAttributes.get(mBeanName);
                if (!attrAndDesc.containsKey(template.name())) {
                    attrAndDesc.put(template.name(), template.description());
                } else {
                    throw new IllegalArgumentException("mBean '" + mBeanName + "' attribute '" + template.name() + "' is defined twice.");
                }
            }
        }
        
        StringBuilder b = new StringBuilder();
        b.append("<table class=\"data-table\"><tbody>\n");
    
        for (Entry<String, Map<String, String>> e : beansAndAttributes.entrySet()) {
            b.append("<tr>\n");
            b.append("<td colspan=3 class=\"mbeanName\" style=\"background-color:#ccc; font-weight: bold;\">");
            b.append(e.getKey());
            b.append("</td>");
            b.append("</tr>\n");
            
            b.append("<tr>\n");
            b.append("<th style=\"width: 90px\"></th>\n");
            b.append("<th>Attribute name</th>\n");
            b.append("<th>Description</th>\n");
            b.append("</tr>\n");
            
            for (Entry<String, String> e2 : e.getValue().entrySet()) {
                b.append("<tr>\n");
                b.append("<td></td>");
                b.append("<td>");
                b.append(e2.getKey());
                b.append("</td>");
                b.append("<td>");
                b.append(e2.getValue());
                b.append("</td>");
                b.append("</tr>\n");
            }
    
        }
        b.append("</tbody></table>");
    
        return b.toString();
    
    }

    public MetricConfig config() {
        return config;
    }

    /**
     * Get the sensor with the given name if it exists
     * @param name The name of the sensor
     * @return Return the sensor or null if no such sensor exists
     */
    public Sensor getSensor(String name) {
        return this.sensors.get(Objects.requireNonNull(name));
    }

    /**
     * Get or create a sensor with the given unique name and no parent sensors. This uses
     * a default recording level of INFO.
     * @param name The sensor name
     * @return The sensor
     */
    public Sensor sensor(String name) {
        return this.sensor(name, Sensor.RecordingLevel.INFO);
    }

    /**
     * Get or create a sensor with the given unique name and no parent sensors and with a given
     * recording level.
     * @param name The sensor name.
     * @param recordingLevel The recording level.
     * @return The sensor
     */
    public Sensor sensor(String name, Sensor.RecordingLevel recordingLevel) {
        return sensor(name, null, recordingLevel, (Sensor[]) null);
    }


    /**
     * Get or create a sensor with the given unique name and zero or more parent sensors. All parent sensors will
     * receive every value recorded with this sensor. This uses a default recording level of INFO.
     * @param name The name of the sensor
     * @param parents The parent sensors
     * @return The sensor that is created
     */
    public Sensor sensor(String name, Sensor... parents) {
        return this.sensor(name, Sensor.RecordingLevel.INFO, parents);
    }

    /**
     * Get or create a sensor with the given unique name and zero or more parent sensors. All parent sensors will
     * receive every value recorded with this sensor.
     * @param name The name of the sensor.
     * @param parents The parent sensors.
     * @param recordingLevel The recording level.
     * @return The sensor that is created
     */
    public Sensor sensor(String name, Sensor.RecordingLevel recordingLevel, Sensor... parents) {
        return sensor(name, null, recordingLevel, parents);
    }

    /**
     * Get or create a sensor with the given unique name and zero or more parent sensors. All parent sensors will
     * receive every value recorded with this sensor. This uses a default recording level of INFO.
     * @param name The name of the sensor
     * @param config A default configuration to use for this sensor for metrics that don't have their own config
     * @param parents The parent sensors
     * @return The sensor that is created
     */
    public synchronized Sensor sensor(String name, MetricConfig config, Sensor... parents) {
        return this.sensor(name, config, Sensor.RecordingLevel.INFO, parents);
    }


    /**
     * Get or create a sensor with the given unique name and zero or more parent sensors. All parent sensors will
     * receive every value recorded with this sensor.
     * @param name The name of the sensor
     * @param config A default configuration to use for this sensor for metrics that don't have their own config
     * @param recordingLevel The recording level.
     * @param parents The parent sensors
     * @return The sensor that is created
     */
    public synchronized Sensor sensor(String name, MetricConfig config, Sensor.RecordingLevel recordingLevel, Sensor... parents) {
        return sensor(name, config, Long.MAX_VALUE, recordingLevel, parents);
    }

    /**
     * Get or create a sensor with the given unique name and zero or more parent sensors. All parent sensors will
     * receive every value recorded with this sensor.
     * @param name The name of the sensor
     * @param config A default configuration to use for this sensor for metrics that don't have their own config
     * @param inactiveSensorExpirationTimeSeconds If no value if recorded on the Sensor for this duration of time,
     *                                        it is eligible for removal
     * @param parents The parent sensors
     * @param recordingLevel The recording level.
     * @return The sensor that is created
     */
    public synchronized Sensor sensor(String name, MetricConfig config, long inactiveSensorExpirationTimeSeconds, Sensor.RecordingLevel recordingLevel, Sensor... parents) {
        Sensor s = getSensor(name);
        if (s == null) {
            s = new Sensor(this, name, parents, config == null ? this.config : config, time, inactiveSensorExpirationTimeSeconds, recordingLevel);
            this.sensors.put(name, s);
            if (parents != null) {
                for (Sensor parent : parents) {
                    List<Sensor> children = childrenSensors.computeIfAbsent(parent, k -> new ArrayList<>());
                    children.add(s);
                }
            }
            log.trace("Added sensor with name {}", name);
        }
        return s;
    }

    /**
     * Get or create a sensor with the given unique name and zero or more parent sensors. All parent sensors will
     * receive every value recorded with this sensor. This uses a default recording level of INFO.
     * @param name The name of the sensor
     * @param config A default configuration to use for this sensor for metrics that don't have their own config
     * @param inactiveSensorExpirationTimeSeconds If no value if recorded on the Sensor for this duration of time,
     *                                        it is eligible for removal
     * @param parents The parent sensors
     * @return The sensor that is created
     */
    public synchronized Sensor sensor(String name, MetricConfig config, long inactiveSensorExpirationTimeSeconds, Sensor... parents) {
        return this.sensor(name, config, inactiveSensorExpirationTimeSeconds, Sensor.RecordingLevel.INFO, parents);
    }

    /**
     * Remove a sensor (if it exists), associated metrics and its children.
     *
     * @param name The name of the sensor to be removed
     */
    public void removeSensor(String name) {
        Sensor sensor = sensors.get(name);
        if (sensor != null) {
            List<Sensor> childSensors = null;
            synchronized (sensor) {
                synchronized (this) {
                    if (sensors.remove(name, sensor)) {
                        for (KafkaMetric metric : sensor.metrics())
                            removeMetric(metric.metricName());
                        log.trace("Removed sensor with name {}", name);
                        childSensors = childrenSensors.remove(sensor);
                        for (final Sensor parent : sensor.parents()) {
                            childrenSensors.getOrDefault(parent, emptyList()).remove(sensor);
                        }
                    }
                }
            }
            if (childSensors != null) {
                for (Sensor childSensor : childSensors)
                    removeSensor(childSensor.name());
            }
        }
    }

    /**
     * Add a metric to monitor an object that implements measurable. This metric won't be associated with any sensor.
     * This is a way to expose existing values as metrics.
     *
     * This method is kept for binary compatibility purposes, it has the same behaviour as
     * {@link #addMetric(MetricName, MetricValueProvider)}.
     *
     * @param metricName The name of the metric
     * @param measurable The measurable that will be measured by this metric
     */
    public void addMetric(MetricName metricName, Measurable measurable) {
        addMetric(metricName, null, measurable);
    }

    /**
     * Add a metric to monitor an object that implements Measurable. This metric won't be associated with any sensor.
     * This is a way to expose existing values as metrics.
     *
     * This method is kept for binary compatibility purposes, it has the same behaviour as
     * {@link #addMetric(MetricName, MetricConfig, MetricValueProvider)}.
     *
     * @param metricName The name of the metric
     * @param config The configuration to use when measuring this measurable
     * @param measurable The measurable that will be measured by this metric
     */
    public void addMetric(MetricName metricName, MetricConfig config, Measurable measurable) {
        addMetric(metricName, config, (MetricValueProvider<?>) measurable);
    }

    /**
     * Add a metric to monitor an object that implements MetricValueProvider. This metric won't be associated with any
     * sensor. This is a way to expose existing values as metrics. User is expected to add any additional
     * synchronization to update and access metric values, if required.
     *
     * @param metricName The name of the metric
     * @param metricValueProvider The metric value provider associated with this metric
     */
    public void addMetric(MetricName metricName, MetricConfig config, MetricValueProvider<?> metricValueProvider) {
        KafkaMetric m = new KafkaMetric(new Object(),
                                        Objects.requireNonNull(metricName),
                                        Objects.requireNonNull(metricValueProvider),
                                        config == null ? this.config : config,
                                        time);
        registerMetric(m);
    }

    /**
     * Add a metric to monitor an object that implements MetricValueProvider. This metric won't be associated with any
     * sensor. This is a way to expose existing values as metrics. User is expected to add any additional
     * synchronization to update and access metric values, if required.
     *
     * @param metricName The name of the metric
     * @param metricValueProvider The metric value provider associated with this metric
     */
    public void addMetric(MetricName metricName, MetricValueProvider<?> metricValueProvider) {
        addMetric(metricName, null, metricValueProvider);
    }

    /**
     * Remove a metric if it exists and return it. Return null otherwise. If a metric is removed, `metricRemoval`
     * will be invoked for each reporter.
     *
     * @param metricName The name of the metric
     * @return the removed `KafkaMetric` or null if no such metric exists
     */
    public synchronized KafkaMetric removeMetric(MetricName metricName) {
        KafkaMetric metric = this.metrics.remove(metricName);
        if (metric != null) {
            for (MetricsReporter reporter : reporters) {
                try {
                    reporter.metricRemoval(metric);
                } catch (Exception e) {
                    log.error("Error when removing metric from " + reporter.getClass().getName(), e);
                }
            }
            log.trace("Removed metric named {}", metricName);
        }
        return metric;
    }

    /**
     * Add a MetricReporter
     */
    public synchronized void addReporter(MetricsReporter reporter) {
        Objects.requireNonNull(reporter).init(new ArrayList<>(metrics.values()));
        this.reporters.add(reporter);
    }

    /**
     * Remove a MetricReporter
     */
    public synchronized void removeReporter(MetricsReporter reporter) {
        if (this.reporters.remove(reporter)) {
            reporter.close();
        }
    }

    synchronized void registerMetric(KafkaMetric metric) {
        MetricName metricName = metric.metricName();
        if (this.metrics.containsKey(metricName))
            throw new IllegalArgumentException("A metric named '" + metricName + "' already exists, can't register another one.");
        this.metrics.put(metricName, metric);
        for (MetricsReporter reporter : reporters) {
            try {
                reporter.metricChange(metric);
            } catch (Exception e) {
                log.error("Error when registering metric on " + reporter.getClass().getName(), e);
            }
        }
        log.trace("Registered metric named {}", metricName);
    }

    /**
     * Get all the metrics currently maintained indexed by metricName
     */
    public Map<MetricName, KafkaMetric> metrics() {
        return this.metrics;
    }

    public List<MetricsReporter> reporters() {
        return this.reporters;
    }

    public KafkaMetric metric(MetricName metricName) {
        return this.metrics.get(metricName);
    }

    /**
     * This iterates over every Sensor and triggers a removeSensor if it has expired
     * Package private for testing
     */
    class ExpireSensorTask implements Runnable {
        @Override
        public void run() {
            for (Map.Entry<String, Sensor> sensorEntry : sensors.entrySet()) {
                // removeSensor also locks the sensor object. This is fine because synchronized is reentrant
                // There is however a minor race condition here. Assume we have a parent sensor P and child sensor C.
                // Calling record on C would cause a record on P as well.
                // So expiration time for P == expiration time for C. If the record on P happens via C just after P is removed,
                // that will cause C to also get removed.
                // Since the expiration time is typically high it is not expected to be a significant concern
                // and thus not necessary to optimize
                synchronized (sensorEntry.getValue()) {
                    if (sensorEntry.getValue().hasExpired()) {
                        log.debug("Removing expired sensor {}", sensorEntry.getKey());
                        removeSensor(sensorEntry.getKey());
                    }
                }
            }
        }
    }

    /* For testing use only. */
    Map<Sensor, List<Sensor>> childrenSensors() {
        return Collections.unmodifiableMap(childrenSensors);
    }

    public MetricName metricInstance(MetricNameTemplate template, String... keyValue) {
        return metricInstance(template, MetricsUtils.getTags(keyValue));
    }

    public MetricName metricInstance(MetricNameTemplate template, Map<String, String> tags) {
        // check to make sure that the runtime defined tags contain all the template tags.
        Set<String> runtimeTagKeys = new HashSet<>(tags.keySet());
        runtimeTagKeys.addAll(config().tags().keySet());
        
        Set<String> templateTagKeys = template.tags();
        
        if (!runtimeTagKeys.equals(templateTagKeys)) {
            throw new IllegalArgumentException("For '" + template.name() + "', runtime-defined metric tags do not match the tags in the template. "
                    + "Runtime = " + runtimeTagKeys.toString() + " Template = " + templateTagKeys.toString());
        }
                
        return this.metricName(template.name(), template.group(), template.description(), tags);
    }

    /**
     * Close this metrics repository.
     */
    @Override
    public void close() {
        if (this.metricsScheduler != null) {
            this.metricsScheduler.shutdown();
            try {
                this.metricsScheduler.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                // ignore and continue shutdown
                Thread.currentThread().interrupt();
            }
        }
        log.info("Metrics scheduler closed");

        for (MetricsReporter reporter : reporters) {
            try {
                log.info("Closing reporter {}", reporter.getClass().getName());
                reporter.close();
            } catch (Exception e) {
                log.error("Error when closing " + reporter.getClass().getName(), e);
            }
        }
        log.info("Metrics reporters closed");
    }
}
