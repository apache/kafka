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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

class ConsumerMetricsView extends AbstractMap<MetricName, KafkaMetric> {

    private final Metrics metrics;
    private volatile Map<MetricName, KafkaMetric> frozenMetrics;

    ConsumerMetricsView(Metrics metrics) {
        this.metrics = metrics;
    }

    void freeze() {
        if (frozenMetrics == null) {
            frozenMetrics = Collections.unmodifiableMap(new HashMap<>(metrics.metrics()));
        }
    }

    @Override
    public boolean containsKey(Object key) {
        return delegate().containsKey(key);
    }

    @Override
    public KafkaMetric get(Object key) {
        return delegate().get(key);
    }

    @Override
    public Set<Entry<MetricName, KafkaMetric>> entrySet() {
        return Collections.unmodifiableMap(delegate()).entrySet();
    }

    private Map<MetricName, KafkaMetric> delegate() {
        Map<MetricName, KafkaMetric> snapshot = frozenMetrics;
        return snapshot == null ? metrics.metrics() : snapshot;
    }
}
