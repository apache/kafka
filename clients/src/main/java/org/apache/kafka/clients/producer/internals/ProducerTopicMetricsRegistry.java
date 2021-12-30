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

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.ClientMetricsRegistry;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.Metrics;

public class ProducerTopicMetricsRegistry extends ClientMetricsRegistry {

    final static String PRODUCER_TOPIC_METRIC_GROUP_NAME = "producer-topic-metrics";

    public final MetricNameTemplate recordSuccessTotal;

    public final MetricNameTemplate recordFailureTotal;

    public ProducerTopicMetricsRegistry(Metrics metrics) {
        super(metrics);

        Set<String> topicAndPartitionTags = new LinkedHashSet<>(tags);
        topicAndPartitionTags.add("topic");
        topicAndPartitionTags.add("partition");

        this.recordSuccessTotal = createTemplate("org.apache.kafka.client.producer.partition.record.success",
            PRODUCER_TOPIC_METRIC_GROUP_NAME,
            "Number of records that have been successfully produced.",
            topicAndPartitionTags);
        this.recordFailureTotal = createTemplate("org.apache.kafka.client.producer.partition.record.failures",
            PRODUCER_TOPIC_METRIC_GROUP_NAME,
            "Number of records that permanently failed delivery.",
            topicAndPartitionTags);
    }

    public MetricName recordSuccessTotal(Map<String, String> tags) {
        return this.metrics.metricInstance(this.recordSuccessTotal, tags);
    }

    public MetricName recordFailureTotal(Map<String, String> tags) {
        return this.metrics.metricInstance(this.recordFailureTotal, tags);
    }

}
