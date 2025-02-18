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
package org.apache.kafka.connect.integration;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;
import java.util.Map;

public class MonitorableSinkConnector extends TestableSinkConnector {

    public static final String VALUE = "started";
    public static MetricName metricsName = null;

    @Override
    public void start(Map<String, String> props) {
        super.start(props);
        PluginMetrics pluginMetrics = context.pluginMetrics();
        metricsName = pluginMetrics.metricName("start", "description", Map.of());
        pluginMetrics.addMetric(metricsName, (Gauge<Object>) (config, now) -> VALUE);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MonitorableSinkTask.class;
    }

    public static class MonitorableSinkTask extends TestableSinkTask {

        public static MetricName metricsName = null;
        private int count = 0;

        @Override
        public void start(Map<String, String> props) {
            super.start(props);
            PluginMetrics pluginMetrics = context.pluginMetrics();
            metricsName = pluginMetrics.metricName("put", "description", Map.of());
            pluginMetrics.addMetric(metricsName, (Measurable) (config, now) -> count);
        }

        @Override
        public void put(Collection<SinkRecord> records) {
            super.put(records);
            for (SinkRecord ignore : records) {
                count++;
            }
        }

    }
}
