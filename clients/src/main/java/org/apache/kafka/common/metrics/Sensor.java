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

import java.util.function.Supplier;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.CompoundStat.NamedMeasurable;
import org.apache.kafka.common.metrics.stats.TokenBucket;
import org.apache.kafka.common.utils.Time;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

/**
 * A sensor applies a continuous sequence of numerical values to a set of associated metrics. For example a sensor on
 * message size would record a sequence of message sizes using the {@link #record(double)} api and would maintain a set
 * of metrics about request sizes such as the average or max.
 */
public final class Sensor {

    private final Metrics registry;
    private final String name;
    private final Sensor[] parents;
    private final List<StatAndConfig> stats;
    private final Map<MetricName, KafkaMetric> metrics;
    private final MetricConfig config;
    private final Time time;
    private volatile long lastRecordTime;
    private final long inactiveSensorExpirationTimeMs;
    private final Object metricLock;

    private static class StatAndConfig {
        private final Stat stat;
        private final Supplier<MetricConfig> configSupplier;

        StatAndConfig(Stat stat, Supplier<MetricConfig> configSupplier) {
            this.stat = stat;
            this.configSupplier = configSupplier;
        }

        public Stat stat() {
            return stat;
        }

        public MetricConfig config() {
            return configSupplier.get();
        }

        @Override
        public String toString() {
            return "StatAndConfig(stat=" + stat + ')';
        }
    }

    public enum RecordingLevel {
        INFO(0, "INFO"), DEBUG(1, "DEBUG"), TRACE(2, "TRACE");

        private static final RecordingLevel[] ID_TO_TYPE;
        private static final int MIN_RECORDING_LEVEL_KEY = 0;
        public static final int MAX_RECORDING_LEVEL_KEY;

        static {
            int maxRL = -1;
            for (RecordingLevel level : RecordingLevel.values()) {
                maxRL = Math.max(maxRL, level.id);
            }
            RecordingLevel[] idToName = new RecordingLevel[maxRL + 1];
            for (RecordingLevel level : RecordingLevel.values()) {
                idToName[level.id] = level;
            }
            ID_TO_TYPE = idToName;
            MAX_RECORDING_LEVEL_KEY = maxRL;
        }

        /** an english description of the api--this is for debugging and can change */
        public final String name;

        /** the permanent and immutable id of an API--this can't change ever */
        public final short id;

        RecordingLevel(int id, String name) {
            this.id = (short) id;
            this.name = name;
        }

        public static RecordingLevel forId(int id) {
            if (id < MIN_RECORDING_LEVEL_KEY || id > MAX_RECORDING_LEVEL_KEY)
                throw new IllegalArgumentException(String.format("Unexpected RecordLevel id `%d`, it should be between `%d` " +
                    "and `%d` (inclusive)", id, MIN_RECORDING_LEVEL_KEY, MAX_RECORDING_LEVEL_KEY));
            return ID_TO_TYPE[id];
        }

        /** Case insensitive lookup by protocol name */
        public static RecordingLevel forName(String name) {
            return RecordingLevel.valueOf(name.toUpperCase(Locale.ROOT));
        }

        public boolean shouldRecord(final int configId) {
            if (configId == INFO.id) {
                return this.id == INFO.id;
            } else if (configId == DEBUG.id) {
                return this.id == INFO.id || this.id == DEBUG.id;
            } else if (configId == TRACE.id) {
                return true;
            } else {
                throw new IllegalStateException("Did not recognize recording level " + configId);
            }
        }
    }

    private final RecordingLevel recordingLevel;

    Sensor(Metrics registry, String name, Sensor[] parents, MetricConfig config, Time time,
           long inactiveSensorExpirationTimeSeconds, RecordingLevel recordingLevel) {
        super();
        this.registry = registry;
        this.name = Objects.requireNonNull(name);
        this.parents = parents == null ? new Sensor[0] : parents;
        this.metrics = new LinkedHashMap<>();
        this.stats = new ArrayList<>();
        this.config = config;
        this.time = time;
        this.inactiveSensorExpirationTimeMs = TimeUnit.MILLISECONDS.convert(inactiveSensorExpirationTimeSeconds, TimeUnit.SECONDS);
        this.lastRecordTime = time.milliseconds();
        this.recordingLevel = recordingLevel;
        this.metricLock = new Object();
        checkForest(new HashSet<>());
    }

    /* Validate that this sensor doesn't end up referencing itself */
    private void checkForest(Set<Sensor> sensors) {
        if (!sensors.add(this))
            throw new IllegalArgumentException("Circular dependency in sensors: " + name() + " is its own parent.");
        for (Sensor parent : parents)
            parent.checkForest(sensors);
    }

    /**
     * The name this sensor is registered with. This name will be unique among all registered sensors.
     */
    public String name() {
        return this.name;
    }

    List<Sensor> parents() {
        return unmodifiableList(asList(parents));
    }

    /**
     * @return true if the sensor's record level indicates that the metric will be recorded, false otherwise
     */
    public boolean shouldRecord() {
        return this.recordingLevel.shouldRecord(config.recordLevel().id);
    }

    /**
     * Record an occurrence, this is just short-hand for {@link #record(double) record(1.0)}
     */
    public void record() {
        if (shouldRecord()) {
            recordInternal(1.0d, time.milliseconds(), true);
        }
    }

    /**
     * Record a value with this sensor
     * @param value The value to record
     * @throws QuotaViolationException if recording this value moves a metric beyond its configured maximum or minimum
     *         bound
     */
    public void record(double value) {
        if (shouldRecord()) {
            recordInternal(value, time.milliseconds(), true);
        }
    }

    /**
     * Record a value at a known time. This method is slightly faster than {@link #record(double)} since it will reuse
     * the time stamp.
     * @param value The value we are recording
     * @param timeMs The current POSIX time in milliseconds
     * @throws QuotaViolationException if recording this value moves a metric beyond its configured maximum or minimum
     *         bound
     */
    public void record(double value, long timeMs) {
        if (shouldRecord()) {
            recordInternal(value, timeMs, true);
        }
    }

    /**
     * Record a value at a known time. This method is slightly faster than {@link #record(double)} since it will reuse
     * the time stamp.
     * @param value The value we are recording
     * @param timeMs The current POSIX time in milliseconds
     * @param checkQuotas Indicate if quota must be enforced or not
     * @throws QuotaViolationException if recording this value moves a metric beyond its configured maximum or minimum
     *         bound
     */
    public void record(double value, long timeMs, boolean checkQuotas) {
        if (shouldRecord()) {
            recordInternal(value, timeMs, checkQuotas);
        }
    }

    private void recordInternal(double value, long timeMs, boolean checkQuotas) {
        this.lastRecordTime = timeMs;
        synchronized (this) {
            synchronized (metricLock()) {
                // increment all the stats
                for (StatAndConfig statAndConfig : this.stats) {
                    statAndConfig.stat.record(statAndConfig.config(), value, timeMs);
                }
            }
            if (checkQuotas)
                checkQuotas(timeMs);
        }
        for (Sensor parent : parents)
            parent.record(value, timeMs, checkQuotas);
    }

    /**
     * Check if we have violated our quota for any metric that has a configured quota
     */
    public void checkQuotas() {
        checkQuotas(time.milliseconds());
    }

    public void checkQuotas(long timeMs) {
        for (KafkaMetric metric : this.metrics.values()) {
            MetricConfig config = metric.config();
            if (config != null) {
                Quota quota = config.quota();
                if (quota != null) {
                    double value = metric.measurableValue(timeMs);
                    if (metric.measurable() instanceof TokenBucket) {
                        if (value < 0) {
                            throw new QuotaViolationException(metric, value, quota.bound());
                        }
                    } else {
                        if (!quota.acceptable(value)) {
                            throw new QuotaViolationException(metric, value, quota.bound());
                        }
                    }
                }
            }
        }
    }

    /**
     * Register a compound statistic with this sensor with no config override
     * @param stat The stat to register
     * @return true if stat is added to sensor, false if sensor is expired
     */
    public boolean add(CompoundStat stat) {
        return add(stat, null);
    }

    /**
     * Register a compound statistic with this sensor which yields multiple measurable quantities (like a histogram)
     * @param stat The stat to register
     * @param config The configuration for this stat. If null then the stat will use the default configuration for this
     *        sensor.
     * @return true if stat is added to sensor, false if sensor is expired
     */
    public synchronized boolean add(CompoundStat stat, MetricConfig config) {
        if (hasExpired())
            return false;

        final MetricConfig statConfig = config == null ? this.config : config;
        stats.add(new StatAndConfig(Objects.requireNonNull(stat), () -> statConfig));
        Object lock = metricLock();
        for (NamedMeasurable m : stat.stats()) {
            final KafkaMetric metric = new KafkaMetric(lock, m.name(), m.stat(), statConfig, time);
            if (!metrics.containsKey(metric.metricName())) {
                registry.registerMetric(metric);
                metrics.put(metric.metricName(), metric);
            }
        }
        return true;
    }

    /**
     * Register a metric with this sensor
     * @param metricName The name of the metric
     * @param stat The statistic to keep
     * @return true if metric is added to sensor, false if sensor is expired
     */
    public boolean add(MetricName metricName, MeasurableStat stat) {
        return add(metricName, stat, null);
    }

    /**
     * Register a metric with this sensor
     *
     * @param metricName The name of the metric
     * @param stat       The statistic to keep
     * @param config     A special configuration for this metric. If null use the sensor default configuration.
     * @return true if metric is added to sensor, false if sensor is expired
     */
    public synchronized boolean add(final MetricName metricName, final MeasurableStat stat, final MetricConfig config) {
        if (hasExpired()) {
            return false;
        } else if (metrics.containsKey(metricName)) {
            return true;
        } else {
            final MetricConfig statConfig = config == null ? this.config : config;
            final KafkaMetric metric = new KafkaMetric(
                metricLock(),
                Objects.requireNonNull(metricName),
                Objects.requireNonNull(stat),
                statConfig,
                time
            );
            registry.registerMetric(metric);
            metrics.put(metric.metricName(), metric);
            stats.add(new StatAndConfig(Objects.requireNonNull(stat), metric::config));
            return true;
        }
    }

    /**
     * Return if metrics were registered with this sensor.
     *
     * @return true if metrics were registered, false otherwise
     */
    public synchronized boolean hasMetrics() {
        return !metrics.isEmpty();
    }

    /**
     * Return true if the Sensor is eligible for removal due to inactivity.
     *        false otherwise
     */
    public boolean hasExpired() {
        return (time.milliseconds() - this.lastRecordTime) > this.inactiveSensorExpirationTimeMs;
    }

    synchronized List<KafkaMetric> metrics() {
        return unmodifiableList(new ArrayList<>(this.metrics.values()));
    }

    /**
     * KafkaMetrics of sensors which use SampledStat should be synchronized on the same lock
     * for sensor record and metric value read to allow concurrent reads and updates. For simplicity,
     * all sensors are synchronized on this object.
     * <p>
     * Sensor object is not used as a lock for reading metric value since metrics reporter is
     * invoked while holding Sensor and Metrics locks to report addition and removal of metrics
     * and synchronized reporters may deadlock if Sensor lock is used for reading metrics values.
     * Note that Sensor object itself is used as a lock to protect the access to stats and metrics
     * while recording metric values, adding and deleting sensors.
     * </p><p>
     * Locking order (assume all MetricsReporter methods may be synchronized):
     * <ul>
     *   <li>Sensor#add: Sensor -> Metrics -> MetricsReporter</li>
     *   <li>Metrics#removeSensor: Sensor -> Metrics -> MetricsReporter</li>
     *   <li>KafkaMetric#metricValue: MetricsReporter -> Sensor#metricLock</li>
     *   <li>Sensor#record: Sensor -> Sensor#metricLock</li>
     * </ul>
     * </p>
     */
    private Object metricLock() {
        return metricLock;
    }
}
