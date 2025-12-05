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
package org.apache.kafka.server.log.remote.quota;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.common.metrics.QuotaViolationException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.SimpleRate;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.quota.QuotaType;
import org.apache.kafka.server.quota.QuotaUtils;
import org.apache.kafka.server.quota.SensorAccess;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RLMQuotaManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(RLMQuotaManager.class);

    private final RLMQuotaManagerConfig config;
    private final Metrics metrics;
    private final QuotaType quotaType;
    private final String description;
    private final Time time;
    private final BrokerTopicStats brokerTopicStats;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final SensorAccess sensorAccess;
    private Quota quota;

    public RLMQuotaManager(RLMQuotaManagerConfig config, Metrics metrics, QuotaType quotaType, String description, Time time) {
        this(config, metrics, quotaType, description, time, null);
    }

    public RLMQuotaManager(RLMQuotaManagerConfig config, Metrics metrics, QuotaType quotaType, String description, Time time, BrokerTopicStats brokerTopicStats) {
        this.config = config;
        this.metrics = metrics;
        this.quotaType = quotaType;
        this.description = description;
        this.time = time;
        this.brokerTopicStats = brokerTopicStats;

        this.quota = new Quota(config.quotaBytesPerSecond(), true);
        this.sensorAccess = new SensorAccess(lock, metrics);
    }

    public void updateQuota(Quota newQuota) {
        lock.writeLock().lock();
        try {
            this.quota = newQuota;

            Map<MetricName, KafkaMetric> allMetrics = metrics.metrics();
            MetricName quotaMetricName = metricName();
            KafkaMetric metric = allMetrics.get(quotaMetricName);
            if (metric != null) {
                LOGGER.info("Sensor for quota-id {} already exists. Setting quota to {} in MetricConfig", quotaMetricName, newQuota);
                metric.config(getQuotaMetricConfig(newQuota));
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public long getThrottleTimeMs() {
        return getThrottleTimeMsWithTopic(null);
    }

    public long getThrottleTimeMsWithTopic(String topic) {
        Sensor sensorInstance = sensor();
        try {
            sensorInstance.checkQuotas();
        } catch (QuotaViolationException qve) {
            long throttleTimeMs = QuotaUtils.throttleTime(qve, time.milliseconds());

            LOGGER.debug("Quota violated for sensor ({}), metric: ({}), metric-value: ({}), bound: ({})",
                sensorInstance.name(), qve.metric().metricName(), qve.value(), qve.bound());

            // Record throttling metrics if topic provided
            try {
                if (brokerTopicStats != null && topic != null) {
                    var throttledMeter = brokerTopicStats.topicStats(topic).remoteFetchQuotaThrottledRate();
                    if (throttledMeter != null) {
                        throttledMeter.mark();
                    }
                    var throttleTimeHistogram = brokerTopicStats.topicStats(topic).remoteFetchQuotaThrottleTimeMs();
                    if (throttleTimeHistogram != null) {
                        throttleTimeHistogram.update(throttleTimeMs);
                    }
                }
            } catch (Exception e) {
                LOGGER.warn("Failed to record throttling metrics for topic {}: {}", topic, e.getMessage());
            }

            return throttleTimeMs;
        }
        return 0L;
    }

    public void record(double value) {
        sensor().record(value, time.milliseconds(), false);
    }

    /**
     * Record quota usage with topic context for observability
     */
    public void recordWithTopic(String topic, double value) {
        // Record global quota
        sensor().record(value, time.milliseconds(), false);

        // Record per-topic quota usage if brokerTopicStats available
        if (brokerTopicStats != null && value > 0) {
            var meter = brokerTopicStats.topicStats(topic).remoteFetchQuotaBytesRate();
            if (meter != null) {
                meter.mark((long) value);
            }
        }
    }

    private MetricConfig getQuotaMetricConfig(Quota quota) {
        return new MetricConfig()
            .timeWindow(config.quotaWindowSizeSeconds(), TimeUnit.SECONDS)
            .samples(config.numQuotaSamples())
            .quota(quota);
    }

    private MetricName metricName() {
        return metrics.metricName("byte-rate", quotaType.toString(), description, Map.of());
    }

    private Sensor sensor() {
        return sensorAccess.getOrCreate(
            quotaType.toString(),
            RLMQuotaManagerConfig.INACTIVE_SENSOR_EXPIRATION_TIME_SECONDS,
            sensor -> sensor.add(metricName(), new SimpleRate(), getQuotaMetricConfig(quota))
        );
    }
}