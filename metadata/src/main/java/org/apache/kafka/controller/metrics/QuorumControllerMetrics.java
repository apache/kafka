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

package org.apache.kafka.controller.metrics;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * These are the metrics which are managed by the QuorumController class. They generally pertain to
 * aspects of the internal operation of the controller, such as the time events spend on the
 * controller queue.
 *
 * IMPORTANT: Metrics which relate to the metadata itself (like number of topics, etc.) should go in
 * @link{org.apache.kafka.controller.metrics.ControllerMetadataMetrics}, not here.
 */
public class QuorumControllerMetrics implements AutoCloseable {
    private final static MetricName ACTIVE_CONTROLLER_COUNT = getMetricName(
        "KafkaController", "ActiveControllerCount");
    private final static MetricName EVENT_QUEUE_TIME_MS = getMetricName(
        "ControllerEventManager", "EventQueueTimeMs");
    private final static MetricName EVENT_QUEUE_PROCESSING_TIME_MS = getMetricName(
        "ControllerEventManager", "EventQueueProcessingTimeMs");
    private final static MetricName LAST_APPLIED_RECORD_OFFSET = getMetricName(
        "KafkaController", "LastAppliedRecordOffset");
    private final static MetricName LAST_COMMITTED_RECORD_OFFSET = getMetricName(
        "KafkaController", "LastCommittedRecordOffset");
    private final static MetricName LAST_APPLIED_RECORD_TIMESTAMP = getMetricName(
        "KafkaController", "LastAppliedRecordTimestamp");
    private final static MetricName LAST_APPLIED_RECORD_LAG_MS = getMetricName(
        "KafkaController", "LastAppliedRecordLagMs");

    private final Optional<MetricsRegistry> registry;
    private volatile boolean active;
    private final AtomicLong lastAppliedRecordOffset = new AtomicLong(0);
    private final AtomicLong lastCommittedRecordOffset = new AtomicLong(0);
    private final AtomicLong lastAppliedRecordTimestamp = new AtomicLong(0);
    private final Consumer<Long> eventQueueTimeUpdater;
    private final Consumer<Long> eventQueueProcessingTimeUpdater;
    private final AtomicLong timedOutHeartbeats = new AtomicLong(0);

    private Consumer<Long> newHistogram(MetricName name, boolean biased) {
        if (registry.isPresent()) {
            Histogram histogram = registry.get().newHistogram(name, biased);
            return e -> histogram.update(e);
        } else {
            return __ -> { };
        }
    }

    public QuorumControllerMetrics(
        Optional<MetricsRegistry> registry,
        Time time
    ) {
        this.registry = registry;
        this.active = false;
        registry.ifPresent(r -> r.newGauge(ACTIVE_CONTROLLER_COUNT, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return active ? 1 : 0;
            }
        }));
        this.eventQueueTimeUpdater = newHistogram(EVENT_QUEUE_TIME_MS, true);
        this.eventQueueProcessingTimeUpdater = newHistogram(EVENT_QUEUE_PROCESSING_TIME_MS, true);
        registry.ifPresent(r -> r.newGauge(LAST_APPLIED_RECORD_OFFSET, new Gauge<Long>() {
            @Override
            public Long value() {
                return lastAppliedRecordOffset();
            }
        }));
        registry.ifPresent(r -> r.newGauge(LAST_COMMITTED_RECORD_OFFSET, new Gauge<Long>() {
            @Override
            public Long value() {
                return lastCommittedRecordOffset();
            }
        }));
        registry.ifPresent(r -> r.newGauge(LAST_APPLIED_RECORD_TIMESTAMP, new Gauge<Long>() {
            @Override
            public Long value() {
                return lastAppliedRecordTimestamp();
            }
        }));
        registry.ifPresent(r -> r.newGauge(LAST_APPLIED_RECORD_LAG_MS, new Gauge<Long>() {
            @Override
            public Long value() {
                return time.milliseconds() - lastAppliedRecordTimestamp();
            }
        }));
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public boolean active() {
        return this.active;
    }

    public void updateEventQueueTime(long durationMs) {
        eventQueueTimeUpdater.accept(durationMs);
    }

    public void updateEventQueueProcessingTime(long durationMs) {
        eventQueueProcessingTimeUpdater.accept(durationMs);
    }

    public void setLastAppliedRecordOffset(long offset) {
        lastAppliedRecordOffset.set(offset);
    }

    public long lastAppliedRecordOffset() {
        return lastAppliedRecordOffset.get();
    }

    public void setLastCommittedRecordOffset(long offset) {
        lastCommittedRecordOffset.set(offset);
    }

    public long lastCommittedRecordOffset() {
        return lastCommittedRecordOffset.get();
    }

    public void setLastAppliedRecordTimestamp(long timestamp) {
        lastAppliedRecordTimestamp.set(timestamp);
    }

    public long lastAppliedRecordTimestamp() {
        return lastAppliedRecordTimestamp.get();
    }

    public void incrementTimedOutHeartbeats() {
        timedOutHeartbeats.addAndGet(1);
    }

    public void setTimedOutHeartbeats(long heartbeats) {
        timedOutHeartbeats.set(heartbeats);
    }

    public long timedOutHeartbeats() {
        return timedOutHeartbeats.get();
    }

    @Override
    public void close() {
        registry.ifPresent(r -> Arrays.asList(
            ACTIVE_CONTROLLER_COUNT,
            EVENT_QUEUE_TIME_MS,
            EVENT_QUEUE_PROCESSING_TIME_MS,
            LAST_APPLIED_RECORD_OFFSET,
            LAST_COMMITTED_RECORD_OFFSET,
            LAST_APPLIED_RECORD_TIMESTAMP,
            LAST_APPLIED_RECORD_LAG_MS
        ).forEach(r::removeMetric));
    }

    private static MetricName getMetricName(String type, String name) {
        return KafkaYammerMetrics.getMetricName("kafka.controller", type, name);
    }
}
