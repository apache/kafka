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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.WindowedCount;

class ConsumerCoordinatorMetrics {

    final Sensor commitSensor;
    final Sensor revokeCallbackSensor;
    final Sensor assignCallbackSensor;
    final Sensor loseCallbackSensor;

    ConsumerCoordinatorMetrics(SubscriptionState subscriptions,
                               Metrics metrics,
                               String metricGrpPrefix) {
        String metricGrpName = metricGrpPrefix + "-coordinator-metrics";

        this.commitSensor = metrics.sensor("commit-latency");
        this.commitSensor.add(metrics.metricName("commit-latency-avg",
                metricGrpName,
                "The average time taken for a commit request"), new Avg());
        this.commitSensor.add(metrics.metricName("commit-latency-max",
                metricGrpName,
                "The max time taken for a commit request"), new Max());
        this.commitSensor.add(new Meter(new WindowedCount(),
                metrics.metricName("commit-rate", metricGrpName,
                        "The number of commit calls per second"),
                metrics.metricName("commit-total", metricGrpName,
                        "The total number of commit calls")));

        this.revokeCallbackSensor = metrics.sensor("partition-revoked-latency");
        this.revokeCallbackSensor.add(metrics.metricName("partition-revoked-latency-avg",
                metricGrpName,
                "The average time taken for a partition-revoked rebalance listener callback"), new Avg());
        this.revokeCallbackSensor.add(metrics.metricName("partition-revoked-latency-max",
                metricGrpName,
                "The max time taken for a partition-revoked rebalance listener callback"), new Max());

        this.assignCallbackSensor = metrics.sensor("partition-assigned-latency");
        this.assignCallbackSensor.add(metrics.metricName("partition-assigned-latency-avg",
                metricGrpName,
                "The average time taken for a partition-assigned rebalance listener callback"), new Avg());
        this.assignCallbackSensor.add(metrics.metricName("partition-assigned-latency-max",
                metricGrpName,
                "The max time taken for a partition-assigned rebalance listener callback"), new Max());

        this.loseCallbackSensor = metrics.sensor("partition-lost-latency");
        this.loseCallbackSensor.add(metrics.metricName("partition-lost-latency-avg",
                metricGrpName,
                "The average time taken for a partition-lost rebalance listener callback"), new Avg());
        this.loseCallbackSensor.add(metrics.metricName("partition-lost-latency-max",
                metricGrpName,
                "The max time taken for a partition-lost rebalance listener callback"), new Max());

        Measurable numParts = (config, now) -> subscriptions.numAssignedPartitions();
        metrics.addMetric(metrics.metricName("assigned-partitions",
                metricGrpName,
                "The number of partitions currently assigned to this consumer"), numParts);
    }
}
