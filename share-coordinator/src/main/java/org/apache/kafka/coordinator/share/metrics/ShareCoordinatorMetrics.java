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

package org.apache.kafka.coordinator.share.metrics;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetrics;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetricsShard;
import org.apache.kafka.timeline.SnapshotRegistry;

import com.yammer.metrics.core.MetricsRegistry;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class ShareCoordinatorMetrics extends CoordinatorMetrics implements AutoCloseable {
    //write (write-rate and write-total) Meter share-coordinator-metric The number of share-group state write calls per second.
    //write-latency (write-latency-avg and write-latency-total) Meter share-coordinator-metrics The time taken for a share-group state write call, including the time to write to the share-group state topic.
    public static final String METRICS_GROUP = "share-coordinator-metrics";

    private final Metrics metrics;
    private final Map<TopicPartition, ShareCoordinatorMetricsShard> shards = new ConcurrentHashMap<>();

    public static final String SHARE_COORDINATOR_WRITE_SENSOR_NAME = "ShareCoordinatorWrite";
    public static final String SHARE_COORDINATOR_WRITE_LATENCY_SENSOR_NAME = "ShareCoordinatorWriteLatency";

    /**
     * Global sensors. These are shared across all metrics shards.
     */
    public final Map<String, Sensor> globalSensors;

    public ShareCoordinatorMetrics() {
        this(new Metrics());
    }

    public ShareCoordinatorMetrics(Metrics metrics) {
        this.metrics = Objects.requireNonNull(metrics);

        Sensor shareCoordinatorWriteSensor = metrics.sensor(SHARE_COORDINATOR_WRITE_SENSOR_NAME);
        shareCoordinatorWriteSensor.add(new Meter(
            metrics.metricName("write-rate",
                METRICS_GROUP,
                "The number of share-group state write calls per second."),
            metrics.metricName("write-total",
                METRICS_GROUP,
                "Total number of share-group state write calls.")));

        Sensor shareCoordinatorWriteLatencySensor = metrics.sensor(SHARE_COORDINATOR_WRITE_LATENCY_SENSOR_NAME);
        shareCoordinatorWriteLatencySensor.add(
            metrics.metricName("write-latency-avg",
                METRICS_GROUP,
                "The average time taken for a share-group state write call, including the time to write to the share-group state topic."),
            new Avg());
        shareCoordinatorWriteLatencySensor.add(
            metrics.metricName("write-latency-max",
                METRICS_GROUP,
                "The maximum time taken for a share-group state write call, including the time to write to the share-group state topic."),
            new Max());

        this.globalSensors = Collections.unmodifiableMap(Utils.mkMap(
            Utils.mkEntry(SHARE_COORDINATOR_WRITE_SENSOR_NAME, shareCoordinatorWriteSensor),
            Utils.mkEntry(SHARE_COORDINATOR_WRITE_LATENCY_SENSOR_NAME, shareCoordinatorWriteLatencySensor)
        ));
    }

    @Override
    public void close() throws Exception {
        Arrays.asList(
            SHARE_COORDINATOR_WRITE_SENSOR_NAME,
            SHARE_COORDINATOR_WRITE_LATENCY_SENSOR_NAME
        ).forEach(metrics::removeSensor);
    }

    @Override
    public ShareCoordinatorMetricsShard newMetricsShard(SnapshotRegistry snapshotRegistry, TopicPartition tp) {
        return new ShareCoordinatorMetricsShard(snapshotRegistry, globalSensors, tp);
    }

    @Override
    public void activateMetricsShard(CoordinatorMetricsShard shard) {
        if (!(shard instanceof ShareCoordinatorMetricsShard)) {
            throw new IllegalArgumentException("ShareCoordinatorMetrics can only activate ShareCoordinatorMetricShard");
        }
        shards.put(shard.topicPartition(), (ShareCoordinatorMetricsShard) shard);
    }

    @Override
    public void deactivateMetricsShard(CoordinatorMetricsShard shard) {
        if (!(shard instanceof ShareCoordinatorMetricsShard)) {
            throw new IllegalArgumentException("ShareCoordinatorMetrics can only deactivate ShareCoordinatorMetricShard");
        }
        shards.remove(shard.topicPartition());
    }

    @Override
    public MetricsRegistry registry() {
        // we are not using MetricsRegistry in share coordinator
        // but this method is part for implemented interface
        return null;
    }

    @Override
    public void onUpdateLastCommittedOffset(TopicPartition tp, long offset) {
        CoordinatorMetricsShard shard = shards.get(tp);
        if (shard != null) {
            shard.commitUpTo(offset);
        }
    }

    /**
     * This method can be used to record on any sensor
     * defined as part of global sensors
     *
     * @param sensorName - String representing name of sensor
     */
    public void record(String sensorName, double value) {
        if (globalSensors.containsKey(sensorName)) {
            globalSensors.get(sensorName).record(value);
        }
    }

    /**
     * This method can be used to record on any sensor
     * defined as part of global sensors
     *
     * @param sensorName - String representing name of sensor
     */
    public void record(String sensorName) {
        if (globalSensors.containsKey(sensorName)) {
            globalSensors.get(sensorName).record();
        }
    }
}
