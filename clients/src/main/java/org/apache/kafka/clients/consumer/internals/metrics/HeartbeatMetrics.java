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
import org.apache.kafka.common.metrics.stats.Max;

public class HeartbeatMetrics extends AbstractConsumerMetrics {

    public final Sensor heartbeatSensor;

    public HeartbeatMetrics(Metrics metrics) {
        super(MetricSuffix.COORDINATOR);
        heartbeatSensor = metrics.sensor("heartbeat-latency");
        heartbeatSensor.add(metrics.metricName("heartbeat-response-time-max",
                groupMetricsName,
                "The max time taken to receive a response to a heartbeat request"),
                new Max());
        heartbeatSensor.add(createMeter(metrics,
                "heartbeat",
                "heartbeats"));
    }
}
