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

public class BufferPoolMetricsRegistry {

    private final Metrics metrics;
    private final Set<String> tags;
    private final List<MetricNameTemplate> allTemplates;

    private final static String METRIC_GROUP_NAME = "producer-metrics";
    
    public final MetricName bufferPoolWaitRatio;
    public final MetricName bufferPoolWaitTimeTotal;

    public BufferPoolMetricsRegistry(Metrics metrics) {
        this.metrics = metrics;
        this.tags = this.metrics.config().tags().keySet();
        this.allTemplates = new ArrayList<MetricNameTemplate>();

        this.bufferPoolWaitRatio = createMetricName("bufferpool-wait-ratio",
                "The fraction of time an appender waits for space allocation.");
        this.bufferPoolWaitTimeTotal = createMetricName("bufferpool-wait-time-total",
                "The total time an appender waits for space allocation.");
    }

    protected MetricName createMetricName(String name, String description) {
        MetricNameTemplate template = new MetricNameTemplate(name, METRIC_GROUP_NAME, description, this.tags);
        this.allTemplates.add(template);
        return this.metrics.metricInstance(template);
    }

    public List<MetricNameTemplate> allTemplates() {
        return allTemplates;
    }

    public Sensor sensor(String name) {
        return this.metrics.sensor(name);
    }

    public void addMetric(MetricName m, Measurable measurable) {
        this.metrics.addMetric(m, measurable);
    }

}
