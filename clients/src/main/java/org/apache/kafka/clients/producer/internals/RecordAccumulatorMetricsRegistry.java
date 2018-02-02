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
package org.apache.kafka.clients.producer.internals;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;

public class RecordAccumulatorMetricsRegistry {

    private final Metrics metrics;
    private final Set<String> tags;
    private final List<MetricNameTemplate> allTemplates;

    final static String METRIC_GROUP_NAME = "producer-metrics";

    public final MetricName waitingThreads;
    public final MetricName bufferTotalBytes;
    public final MetricName bufferAvailableBytes;
    public final MetricName bufferExhaustedRate;
    public final MetricName bufferExhaustedTotal;

    public final BufferPoolMetricsRegistry bufferPoolMetrics;

    public RecordAccumulatorMetricsRegistry(Metrics metrics) {
        this.metrics = metrics;
        this.tags = this.metrics.config().tags().keySet();
        this.allTemplates = new ArrayList<MetricNameTemplate>();

        this.bufferPoolMetrics = new BufferPoolMetricsRegistry(metrics);
        
        this.waitingThreads = createMetricName("waiting-threads",
                "The number of user threads blocked waiting for buffer memory to enqueue their records");
        this.bufferTotalBytes = createMetricName("buffer-total-bytes",
                "The maximum amount of buffer memory the client can use (whether or not it is currently used).");
        this.bufferAvailableBytes = createMetricName("buffer-available-bytes",
                "The total amount of buffer memory that is not being used (either unallocated or in the free list).");
        this.bufferExhaustedRate = createMetricName("buffer-exhausted-rate",
                "The average per-second number of record sends that are dropped due to buffer exhaustion");
        this.bufferExhaustedTotal = createMetricName("buffer-exhausted-total",
                "The total number of record sends that are dropped due to buffer exhaustion");

    }
    
    public List<MetricNameTemplate> allTemplates() {
        List<MetricNameTemplate> l = new ArrayList<>();
        l.addAll(this.bufferPoolMetrics.allTemplates());
        l.addAll(this.allTemplates);
        return l;
    }

    protected MetricName createMetricName(String name, String description) {
        MetricNameTemplate template = new MetricNameTemplate(name, METRIC_GROUP_NAME, description, this.tags);
        this.allTemplates.add(template);
        return this.metrics.metricInstance(template);
    }

    public Sensor sensor(String name) {
        return this.metrics.sensor(name);
    }

    public void addMetric(MetricName m, Measurable measurable) {
        this.metrics.addMetric(m, measurable);
    }

}
