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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;

public abstract class RebalanceMetricsManager {
    protected final String metricGroupName;

    RebalanceMetricsManager(String metricGroupName) {
        this.metricGroupName = metricGroupName;
    }

    protected MetricName createMetric(Metrics metrics, String name, String description) {
        return metrics.metricName(name, metricGroupName, description);
    }

    public abstract void recordRebalanceStarted(long nowMs);

    public abstract void recordRebalanceEnded(long nowMs);

    public void maybeRecordRebalanceFailed() {
    }

    public abstract boolean rebalanceStarted();
}