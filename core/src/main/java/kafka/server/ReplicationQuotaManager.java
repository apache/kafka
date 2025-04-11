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
package kafka.server;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.common.metrics.QuotaViolationException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.SimpleRate;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.config.ReplicationQuotaManagerConfig;
import org.apache.kafka.server.quota.QuotaType;
import org.apache.kafka.server.quota.SensorAccess;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ReplicationQuotaManager implements ReplicaQuota {
    public static final List<Integer> ALL_REPLICAS = List.of(-1);
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationQuotaManager.class);
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ConcurrentHashMap<String, List<Integer>> throttledPartitions = new ConcurrentHashMap<>();
    private final ReplicationQuotaManagerConfig config;
    private final Metrics metrics;
    private final QuotaType replicationType;
    private final Time time;
    private final SensorAccess sensorAccess;
    private final MetricName rateMetricName;
    private Quota quota;

    public ReplicationQuotaManager(ReplicationQuotaManagerConfig config, Metrics metrics, QuotaType replicationType, Time time) {
        this.config = config;
        this.metrics = metrics;
        this.replicationType = replicationType;
        this.time = time;
        this.sensorAccess = new SensorAccess(lock, metrics);
        this.rateMetricName = metrics.metricName("byte-rate", replicationType.toString(), "Tracking byte-rate for " + replicationType);
    }

    /**
     * Update the quota
     */
    public void updateQuota(Quota quota) {
        lock.writeLock().lock();
        try {
            this.quota = quota;
            KafkaMetric metric = metrics.metrics().get(rateMetricName);
            if (metric != null) {
                metric.config(getQuotaMetricConfig(quota));
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Check if the quota is currently exceeded
     */
    @Override
    public boolean isQuotaExceeded() {
        try {
            sensor().checkQuotas();
            return false;
        } catch (QuotaViolationException qve) {
            LOGGER.trace("{}: Quota violated for sensor ({}), metric: ({}), metric-value: ({}), bound: ({})",
                replicationType, sensor().name(), qve.metric().metricName(), qve.value(), qve.bound());
            return true;
        }
    }

    /**
     * Is the passed partition throttled by this ReplicationQuotaManager
     */
    @Override
    public boolean isThrottled(TopicPartition topicPartition) {
        List<Integer> partitions = throttledPartitions.get(topicPartition.topic());
        return partitions != null && (partitions.equals(ALL_REPLICAS) || partitions.contains(topicPartition.partition()));
    }

    /**
     * Add the passed value to the throttled rate. This method ignores the quota with
     * the value being added to the rate even if the quota is exceeded
     */
    @Override
    public void record(long value) {
        sensor().record((double) value, time.milliseconds(), false);
    }

    /**
     * Update the set of throttled partitions for this QuotaManager. The partitions passed, for
     * any single topic, will replace any previous
     */
    public void markThrottled(String topic, List<Integer> partitions) {
        throttledPartitions.put(topic, partitions);
    }

    /**
     * Mark all replicas for this topic as throttled
     */
    public void markThrottled(String topic) {
        markThrottled(topic, ALL_REPLICAS);
    }

    public void removeThrottle(String topic) {
        throttledPartitions.remove(topic);
    }

    public long upperBound() {
        lock.readLock().lock();
        try {
            return quota != null ? (long) quota.bound() : Long.MAX_VALUE;
        } finally {
            lock.readLock().unlock();
        }
    }

    private MetricConfig getQuotaMetricConfig(Quota quota) {
        return new MetricConfig()
            .timeWindow(config.quotaWindowSizeSeconds, TimeUnit.SECONDS)
            .samples(config.numQuotaSamples)
            .quota(quota);
    }

    private Sensor sensor() {
        return sensorAccess.getOrCreate(
            replicationType.toString(),
            ReplicationQuotaManagerConfig.INACTIVE_SENSOR_EXPIRATION_TIME_SECONDS,
            sensor -> sensor.add(rateMetricName, new SimpleRate(), getQuotaMetricConfig(quota))
        );
    }
}
