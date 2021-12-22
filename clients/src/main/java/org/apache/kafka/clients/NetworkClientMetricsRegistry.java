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

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.Metrics;

public class NetworkClientMetricsRegistry extends ClientMetricsRegistry {

    final static String CLIENT_METRIC_GROUP_NAME = "client-metrics";

    public final MetricName clientConnectionCount;

    public final MetricNameTemplate clientConnectionCreations;

    public NetworkClientMetricsRegistry(Metrics metrics) {
        super(metrics);

        Set<String> brokerIdTags = new LinkedHashSet<>(tags);
        brokerIdTags.add("broker_id");

        this.clientConnectionCreations = createTemplate("org.apache.kafka.client.connection.creations",
            CLIENT_METRIC_GROUP_NAME,
            "Total number of broker connections made.",
            brokerIdTags);
        this.clientConnectionCount = createMetricName("org.apache.kafka.client.connection.count",
            "Current number of broker connections.");
    }

    private MetricName createMetricName(String name, String description) {
        return this.metrics.metricInstance(createTemplate(name, CLIENT_METRIC_GROUP_NAME, description, this.tags));
    }

    public MetricName clientConnectionCreations(Map<String, String> tags) {
        return this.metrics.metricInstance(this.clientConnectionCreations, tags);
    }

}
