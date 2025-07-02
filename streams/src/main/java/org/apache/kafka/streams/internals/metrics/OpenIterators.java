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
package org.apache.kafka.streams.internals.metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.MeteredIterator;
import org.apache.kafka.streams.state.internals.metrics.StateStoreMetrics;

import java.util.Comparator;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;

public class OpenIterators {
    private final TaskId taskId;
    private final String metricsScope;
    private final String name;
    private final StreamsMetricsImpl streamsMetrics;

    private final NavigableSet<MeteredIterator> openIterators = new ConcurrentSkipListSet<>(Comparator.comparingLong(MeteredIterator::startTimestamp));

    private MetricName metricName;

    public OpenIterators(final TaskId taskId,
                         final String metricsScope,
                         final String name,
                         final StreamsMetricsImpl streamsMetrics) {
        this.taskId = taskId;
        this.metricsScope = metricsScope;
        this.name = name;
        this.streamsMetrics = streamsMetrics;
    }

    public void add(final MeteredIterator iterator) {
        openIterators.add(iterator);

        if (openIterators.size() == 1) {
            metricName = StateStoreMetrics.addOldestOpenIteratorGauge(taskId.toString(), metricsScope, name, streamsMetrics,
                (config, now) -> openIterators.first().startTimestamp()
            );
        }
    }

    public void remove(final MeteredIterator iterator) {
        if (openIterators.size() == 1) {
            streamsMetrics.removeMetric(metricName);
        }
        openIterators.remove(iterator);
    }

    public long sum() {
        return openIterators.size();
    }
}
