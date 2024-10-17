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
package org.apache.kafka.coordinator.common.runtime;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.timeline.SnapshotRegistry;

import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

/**
 * CoordinatorMetrics contain all coordinator related metrics. It delegates metrics collection to
 * {@link CoordinatorMetricsShard}s and aggregates them all when it reports to the metrics registry.
 */
public abstract class CoordinatorMetrics {

    /**
     * Create a new metrics shard.
     * @param snapshotRegistry  The snapshot registry.
     * @param tp                The topic partition corresponding to the shard.
     *
     * @return The metrics shard.
     */
    public abstract CoordinatorMetricsShard newMetricsShard(SnapshotRegistry snapshotRegistry, TopicPartition tp);

    /**
     * Activate the metrics shard. This shard is now able to collect metrics.
     *
     * @param shard  The metrics shard.
     */
    public abstract void activateMetricsShard(CoordinatorMetricsShard shard);

    /**
     * Deactivate the metrics shard. This shard should not be part of the metrics aggregation.
     *
     * @param shard  The metrics shard.
     */
    public abstract void deactivateMetricsShard(CoordinatorMetricsShard shard);

    /**
     * @return The metrics registry.
     */
    public abstract MetricsRegistry registry();

    /**
     * Generate the Yammer MetricName.
     *
     * @param group The metric group.
     * @param type  The metric type.
     * @param name  The metric name.
     *
     * @return the yammer metric name.
     */
    public static MetricName getMetricName(String group, String type, String name) {
        return KafkaYammerMetrics.getMetricName(group, type, name);
    }

    /**
     * Invoked when the last committed offset has been updated. This is used as a listener for metrics
     * relying on a snapshot registry.
     *
     * @param tp      The topic partition.
     * @param offset  The updated offset.
     */
    public abstract void onUpdateLastCommittedOffset(TopicPartition tp, long offset);
}
