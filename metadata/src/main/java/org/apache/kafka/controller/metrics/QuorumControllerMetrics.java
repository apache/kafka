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

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.server.metrics.TimeRatio;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * These are the metrics which are managed by the QuorumController class. They generally pertain to
 * aspects of the internal operation of the controller, such as the time events spend on the
 * controller queue.
 *
 * IMPORTANT: Metrics which relate to the metadata itself (like number of topics, etc.) should go in
 * {@link org.apache.kafka.controller.metrics.ControllerMetadataMetrics}, not here.
 */
public class QuorumControllerMetrics implements AutoCloseable {
    private static final MetricName ACTIVE_CONTROLLER_COUNT = getMetricName(
        "KafkaController", "ActiveControllerCount");
    private static final MetricName EVENT_QUEUE_TIME_MS = getMetricName(
        "ControllerEventManager", "EventQueueTimeMs");
    private static final MetricName EVENT_QUEUE_PROCESSING_TIME_MS = getMetricName(
        "ControllerEventManager", "EventQueueProcessingTimeMs");
    private static final MetricName AVERAGE_IDLE_RATIO = getMetricName(
        "ControllerEventManager", "AvgIdleRatio");
    private static final MetricName LAST_APPLIED_RECORD_OFFSET = getMetricName(
        "KafkaController", "LastAppliedRecordOffset");
    private static final MetricName LAST_COMMITTED_RECORD_OFFSET = getMetricName(
        "KafkaController", "LastCommittedRecordOffset");
    private static final MetricName LAST_APPLIED_RECORD_TIMESTAMP = getMetricName(
        "KafkaController", "LastAppliedRecordTimestamp");
    private static final MetricName LAST_APPLIED_RECORD_LAG_MS = getMetricName(
        "KafkaController", "LastAppliedRecordLagMs");
    private static final MetricName TIMED_OUT_BROKER_HEARTBEAT_COUNT = getMetricName(
        "KafkaController", "TimedOutBrokerHeartbeatCount");
    private static final MetricName EVENT_QUEUE_OPERATIONS_STARTED_COUNT = getMetricName(
        "KafkaController", "EventQueueOperationsStartedCount");
    private static final MetricName EVENT_QUEUE_OPERATIONS_TIMED_OUT_COUNT = getMetricName(
        "KafkaController", "EventQueueOperationsTimedOutCount");
    private static final MetricName NEW_ACTIVE_CONTROLLERS_COUNT = getMetricName(
        "KafkaController", "NewActiveControllersCount");
    private static final MetricName PREFERRED_LEADER_ELECTIONS_PER_RUN = getMetricName(
        "KafkaController", "PreferredLeaderElectionsPerRun");
    private static final MetricName GATED_PREFERRED_LEADER_BROKER_COUNT = getMetricName(
        "KafkaController", "GatedPreferredLeaderBrokerCount");
    private static final MetricName PREFERRED_LEADER_ELECTION_THROTTLED_RUN_COUNT = getMetricName(
        "KafkaController", "PreferredLeaderElectionThrottledRunCount");
    private static final MetricName PREFERRED_LEADER_ELECTION_ESCAPE_HATCH_COUNT = getMetricName(
        "KafkaController", "PreferredLeaderElectionEscapeHatchCount");

    private static final String TIME_SINCE_LAST_HEARTBEAT_RECEIVED_METRIC_NAME = "TimeSinceLastHeartbeatReceivedMs";
    private static final String OUT_OF_SYNC_PREFERRED_PARTITION_COUNT_METRIC_NAME = "OutOfSyncPreferredPartitionCount";
    private static final String BROKER_ID_TAG = "broker";

    private final Optional<MetricsRegistry> registry;
    private final Time time;
    private volatile boolean active;
    private final AtomicLong lastAppliedRecordOffset = new AtomicLong(0);
    private final AtomicLong lastCommittedRecordOffset = new AtomicLong(0);
    private final AtomicLong lastAppliedRecordTimestamp = new AtomicLong(0);
    private final Consumer<Long> eventQueueTimeUpdater;
    private final Consumer<Long> eventQueueProcessingTimeUpdater;
    private final TimeRatio avgIdleTimeRatio;

    private final AtomicLong timedOutHeartbeats = new AtomicLong(0);
    private final AtomicLong operationsStarted = new AtomicLong(0);
    private final AtomicLong operationsTimedOut = new AtomicLong(0);
    private final AtomicLong newActiveControllers = new AtomicLong(0);
    private final AtomicLong preferredLeaderElectionsPerRun = new AtomicLong(0);
    private final AtomicLong gatedPreferredLeaderBrokerCount = new AtomicLong(0);
    private final AtomicLong preferredLeaderElectionThrottledRunCount = new AtomicLong(0);
    private final AtomicLong preferredLeaderElectionEscapeHatchCount = new AtomicLong(0);
    private final Map<Integer, Long> brokerContactTimesMs = new ConcurrentHashMap<>();
    private final Map<Integer, Integer> brokerOutOfSyncCounts = new ConcurrentHashMap<>();
    private final int sessionTimeoutMs;

    private Consumer<Long> newHistogram(MetricName name, boolean biased) {
        if (registry.isPresent()) {
            Histogram histogram = registry.get().newHistogram(name, biased);
            return histogram::update;
        } else {
            return __ -> { };
        }
    }

    public QuorumControllerMetrics(
        Optional<MetricsRegistry> registry,
        Time time,
        int sessionTimeoutMs
    ) {
        this.registry = registry;
        this.time = time;
        this.active = false;
        registry.ifPresent(r -> r.newGauge(ACTIVE_CONTROLLER_COUNT, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return active ? 1 : 0;
            }
        }));
        this.eventQueueTimeUpdater = newHistogram(EVENT_QUEUE_TIME_MS, true);
        this.eventQueueProcessingTimeUpdater = newHistogram(EVENT_QUEUE_PROCESSING_TIME_MS, true);
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.avgIdleTimeRatio = new TimeRatio(1);
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
        registry.ifPresent(r -> r.newGauge(TIMED_OUT_BROKER_HEARTBEAT_COUNT, new Gauge<Long>() {
            @Override
            public Long value() {
                return timedOutHeartbeats();
            }
        }));
        registry.ifPresent(r -> r.newGauge(EVENT_QUEUE_OPERATIONS_STARTED_COUNT, new Gauge<Long>() {
            @Override
            public Long value() {
                return operationsStarted();
            }
        }));
        registry.ifPresent(r -> r.newGauge(EVENT_QUEUE_OPERATIONS_TIMED_OUT_COUNT, new Gauge<Long>() {
            @Override
            public Long value() {
                return operationsTimedOut();
            }
        }));
        registry.ifPresent(r -> r.newGauge(NEW_ACTIVE_CONTROLLERS_COUNT, new Gauge<Long>() {
            @Override
            public Long value() {
                return newActiveControllers();
            }
        }));
        registry.ifPresent(r -> r.newGauge(PREFERRED_LEADER_ELECTIONS_PER_RUN, new Gauge<Long>() {
            @Override
            public Long value() {
                return preferredLeaderElectionsPerRun();
            }
        }));
        registry.ifPresent(r -> r.newGauge(GATED_PREFERRED_LEADER_BROKER_COUNT, new Gauge<Long>() {
            @Override
            public Long value() {
                return gatedPreferredLeaderBrokerCount();
            }
        }));
        registry.ifPresent(r -> r.newGauge(PREFERRED_LEADER_ELECTION_THROTTLED_RUN_COUNT, new Gauge<Long>() {
            @Override
            public Long value() {
                return preferredLeaderElectionThrottledRunCount();
            }
        }));
        registry.ifPresent(r -> r.newGauge(PREFERRED_LEADER_ELECTION_ESCAPE_HATCH_COUNT, new Gauge<Long>() {
            @Override
            public Long value() {
                return preferredLeaderElectionEscapeHatchCount();
            }
        }));
        registry.ifPresent(r -> r.newGauge(AVERAGE_IDLE_RATIO, new Gauge<Double>() {
            @Override
            public Double value() {
                synchronized (avgIdleTimeRatio) {
                    return avgIdleTimeRatio.measure();
                }
            }
        }));
    }

    public void updateIdleTime(long idleDurationMs, long currentTimeMs) {
        synchronized (avgIdleTimeRatio) {
            avgIdleTimeRatio.record((double) idleDurationMs, currentTimeMs);
        }
    }

    public void addTimeSinceLastHeartbeatMetric(int brokerId) {
        brokerContactTimesMs.put(brokerId, time.milliseconds());
        registry.ifPresent(r -> r.newGauge(
            getBrokerIdTagMetricName(
                "KafkaController",
                TIME_SINCE_LAST_HEARTBEAT_RECEIVED_METRIC_NAME,
                brokerId
            ),
            new Gauge<Integer>() {
                @Override
                public Integer value() {
                    return timeSinceLastHeartbeatMs(brokerId);
                }
            }
        ));
    }

    public void removeTimeSinceLastHeartbeatMetric(int brokerId) {
        registry.ifPresent(r -> r.removeMetric(
            getBrokerIdTagMetricName(
                "KafkaController",
                TIME_SINCE_LAST_HEARTBEAT_RECEIVED_METRIC_NAME,
                brokerId
            )
        ));
        brokerContactTimesMs.remove(brokerId);
    }

    public void removeTimeSinceLastHeartbeatMetrics() {
        for (int brokerId : brokerContactTimesMs.keySet()) {
            removeTimeSinceLastHeartbeatMetric(brokerId);
        }
        brokerContactTimesMs.clear();
    }

    public void updateBrokerOutOfSyncCounts(Map<Integer, Integer> counts) {
        // Remove metrics for brokers no longer present.
        List<Integer> toRemove = new ArrayList<>(brokerOutOfSyncCounts.keySet());
        toRemove.removeAll(counts.keySet());
        for (int brokerId : toRemove) {
            registry.ifPresent(r -> r.removeMetric(
                getBrokerIdTagMetricName(
                    "KafkaController",
                    OUT_OF_SYNC_PREFERRED_PARTITION_COUNT_METRIC_NAME,
                    brokerId
                )
            ));
            brokerOutOfSyncCounts.remove(brokerId);
        }
        // Register gauges for new brokers; update counts for existing ones.
        for (Map.Entry<Integer, Integer> entry : counts.entrySet()) {
            int brokerId = entry.getKey();
            if (!brokerOutOfSyncCounts.containsKey(brokerId)) {
                registry.ifPresent(r -> r.newGauge(
                    getBrokerIdTagMetricName(
                        "KafkaController",
                        OUT_OF_SYNC_PREFERRED_PARTITION_COUNT_METRIC_NAME,
                        brokerId
                    ),
                    new Gauge<Integer>() {
                        @Override
                        public Integer value() {
                            return brokerOutOfSyncCounts.getOrDefault(brokerId, 0);
                        }
                    }
                ));
            }
            brokerOutOfSyncCounts.put(brokerId, entry.getValue());
        }
    }

    public void removeAllBrokerOutOfSyncCountMetrics() {
        List<Integer> brokerIds = new ArrayList<>(brokerOutOfSyncCounts.keySet());
        for (int brokerId : brokerIds) {
            registry.ifPresent(r -> r.removeMetric(
                getBrokerIdTagMetricName(
                    "KafkaController",
                    OUT_OF_SYNC_PREFERRED_PARTITION_COUNT_METRIC_NAME,
                    brokerId
                )
            ));
        }
        brokerOutOfSyncCounts.clear();
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
        timedOutHeartbeats.incrementAndGet();
    }

    public long timedOutHeartbeats() {
        return timedOutHeartbeats.get();
    }

    public void incrementOperationsStarted() {
        operationsStarted.incrementAndGet();
    }

    public long operationsStarted() {
        return operationsStarted.get();
    }

    public void incrementOperationsTimedOut() {
        operationsTimedOut.incrementAndGet();
    }

    public long operationsTimedOut() {
        return operationsTimedOut.get();
    }

    public void incrementNewActiveControllers() {
        newActiveControllers.incrementAndGet();
    }

    public long newActiveControllers() {
        return newActiveControllers.get();
    }

    public void setPreferredLeaderElectionsPerRun(long count) {
        preferredLeaderElectionsPerRun.set(count);
    }

    public long preferredLeaderElectionsPerRun() {
        return preferredLeaderElectionsPerRun.get();
    }

    public void setGatedPreferredLeaderBrokerCount(long count) {
        gatedPreferredLeaderBrokerCount.set(count);
    }

    public long gatedPreferredLeaderBrokerCount() {
        return gatedPreferredLeaderBrokerCount.get();
    }

    public void incrementPreferredLeaderElectionThrottledRunCount() {
        preferredLeaderElectionThrottledRunCount.incrementAndGet();
    }

    public long preferredLeaderElectionThrottledRunCount() {
        return preferredLeaderElectionThrottledRunCount.get();
    }

    public void setPreferredLeaderElectionEscapeHatchCount(long count) {
        preferredLeaderElectionEscapeHatchCount.set(count);
    }

    public long preferredLeaderElectionEscapeHatchCount() {
        return preferredLeaderElectionEscapeHatchCount.get();
    }

    public void updateBrokerContactTime(int brokerId) {
        brokerContactTimesMs.put(brokerId, time.milliseconds());
    }

    public int timeSinceLastHeartbeatMs(int brokerId) {
        Long lastTime = brokerContactTimesMs.get(brokerId);
        if (lastTime == null) {
            return sessionTimeoutMs;
        }
        return Math.min((int) (time.milliseconds() - lastTime), sessionTimeoutMs);
    }

    @Override
    public void close() {
        registry.ifPresent(r -> List.of(
            ACTIVE_CONTROLLER_COUNT,
            EVENT_QUEUE_TIME_MS,
            EVENT_QUEUE_PROCESSING_TIME_MS,
            LAST_APPLIED_RECORD_OFFSET,
            LAST_COMMITTED_RECORD_OFFSET,
            LAST_APPLIED_RECORD_TIMESTAMP,
            LAST_APPLIED_RECORD_LAG_MS,
            TIMED_OUT_BROKER_HEARTBEAT_COUNT,
            EVENT_QUEUE_OPERATIONS_STARTED_COUNT,
            EVENT_QUEUE_OPERATIONS_TIMED_OUT_COUNT,
            NEW_ACTIVE_CONTROLLERS_COUNT,
            AVERAGE_IDLE_RATIO,
            PREFERRED_LEADER_ELECTIONS_PER_RUN,
            GATED_PREFERRED_LEADER_BROKER_COUNT,
            PREFERRED_LEADER_ELECTION_THROTTLED_RUN_COUNT,
            PREFERRED_LEADER_ELECTION_ESCAPE_HATCH_COUNT
        ).forEach(r::removeMetric));
        removeTimeSinceLastHeartbeatMetrics();
        removeAllBrokerOutOfSyncCountMetrics();
    }

    private static MetricName getMetricName(String type, String name) {
        return KafkaYammerMetrics.getMetricName("kafka.controller", type, name);
    }

    private static MetricName getBrokerIdTagMetricName(String type, String name, int brokerId) {
        LinkedHashMap<String, String> brokerIdTag = new LinkedHashMap<>();
        brokerIdTag.put(BROKER_ID_TAG, Integer.toString(brokerId));
        return KafkaYammerMetrics.getMetricName("kafka.controller", type, name, brokerIdTag);
    }
}
