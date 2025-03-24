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
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;
import java.util.Map;

public class MonitorableSourceConnector extends TestableSourceConnector {

    public static MetricName metricsName = null;
    public static final String VALUE = "started";

    @Override
    public void start(Map<String, String> props) {
        super.start(props);
        PluginMetrics pluginMetrics = context.pluginMetrics();
        metricsName = pluginMetrics.metricName("start", "description", Map.of());
        pluginMetrics.addMetric(metricsName, (Gauge<Object>) (config, now) -> VALUE);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MonitorableSourceTask.class;
    }

    public static class MonitorableSourceTask extends TestableSourceTask {

        public static MetricName metricsName = null;
        private int count = 0;

        @Override
        public void start(Map<String, String> props) {
            super.start(props);
            PluginMetrics pluginMetrics = context.pluginMetrics();
            metricsName = pluginMetrics.metricName("poll", "description", Map.of());
            pluginMetrics.addMetric(metricsName, (Measurable) (config, now) -> count);
        }

        @Override
        public List<SourceRecord> poll() {
            List<SourceRecord> records = super.poll();
            if (records != null) {
                count += records.size();
            }
            return records;
        }

    }
}
