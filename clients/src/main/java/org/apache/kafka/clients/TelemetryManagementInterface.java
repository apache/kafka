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
package org.apache.kafka.clients;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.utils.Time;

public class TelemetryManagementInterface implements AutoCloseable {

    private static final String JMX_PREFIX = "kafka.consumer";

    private static final String CLIENT_ID_METRIC_TAG = "client-id";

    private final Metrics metrics;

    public TelemetryManagementInterface(Metrics metrics) {
        this.metrics = metrics;
    }

    public static TelemetryManagementInterface instance(Time time, String clientId) {
        // TODO: this should be a map or something with a usage count increment
        Map<String, String> metricsTags = Collections.singletonMap(CLIENT_ID_METRIC_TAG, clientId);
        MetricConfig metricConfig = new MetricConfig()
            .tags(metricsTags);
        TelemetryMetricsReporter reporter = new TelemetryMetricsReporter();
        MetricsContext metricsContext = new KafkaMetricsContext(JMX_PREFIX);
        Metrics metrics = new Metrics(metricConfig,
            Collections.singletonList(reporter),
            time,
            metricsContext);
        return new TelemetryManagementInterface(metrics);
    }

    public Metrics metrics() {
        return metrics;
    }

    public String clientInstanceId(Duration timeout) {
        return null;
    }

    @Override
    public void close() {
        // TODO: decrement usage counter and only close on last one out...
        //       If it's the last one out, we need to upload our "terminating" set of metrics...
    }

}
