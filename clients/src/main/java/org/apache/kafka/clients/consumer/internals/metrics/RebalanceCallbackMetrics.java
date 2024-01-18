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
package org.apache.kafka.clients.consumer.internals.metrics;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;

import java.util.Optional;

public class RebalanceCallbackMetrics extends AbstractConsumerMetrics {
    public final Sensor revokeCallbackSensor;
    public final Sensor assignCallbackSensor;
    public final Sensor loseCallbackSensor;

    public RebalanceCallbackMetrics(Metrics metrics) {
        this(metrics, null);
    }

    public RebalanceCallbackMetrics(Metrics metrics, String grpMetricsPrefix) {
        super(Optional.ofNullable(grpMetricsPrefix));
        revokeCallbackSensor = metrics.sensor("partition-revoked-latency");
        revokeCallbackSensor.add(metrics.metricName("partition-revoked-latency-avg",
            groupMetricsName,
            "The average time taken for a partition-revoked rebalance listener callback"), new Avg());
        revokeCallbackSensor.add(metrics.metricName("partition-revoked-latency-max",
            groupMetricsName,
            "The max time taken for a partition-revoked rebalance listener callback"), new Max());

        assignCallbackSensor = metrics.sensor("partition-assigned-latency");
        assignCallbackSensor.add(metrics.metricName("partition-assigned-latency-avg",
            groupMetricsName,
            "The average time taken for a partition-assigned rebalance listener callback"), new Avg());
        assignCallbackSensor.add(metrics.metricName("partition-assigned-latency-max",
            groupMetricsName,
            "The max time taken for a partition-assigned rebalance listener callback"), new Max());

        loseCallbackSensor = metrics.sensor("partition-lost-latency");
        loseCallbackSensor.add(metrics.metricName("partition-lost-latency-avg",
            groupMetricsName,
            "The average time taken for a partition-lost rebalance listener callback"), new Avg());
        loseCallbackSensor.add(metrics.metricName("partition-lost-latency-max",
            groupMetricsName,
            "The max time taken for a partition-lost rebalance listener callback"), new Max());
    }
}
