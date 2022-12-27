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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;

/**
 * Since we parse the message data for each partition from each fetch response lazily, fetch-level
 * metrics need to be aggregated as the messages from each partition are parsed. This class is used
 * to facilitate this incremental aggregation.
 */
public class FetchResponseMetricAggregator {
    private final FetchManagerMetrics sensors;
    private final Set<TopicPartition> unrecordedPartitions;

    private final FetchMetrics fetchMetrics = new FetchMetrics();
    private final Map<String, FetchMetrics> topicFetchMetrics = new HashMap<>();

    FetchResponseMetricAggregator(FetchManagerMetrics sensors, Set<TopicPartition> partitions) {
        this.sensors = sensors;
        this.unrecordedPartitions = partitions;
    }

    /**
     * After each partition is parsed, we update the current metric totals with the total bytes
     * and number of records parsed. After all partitions have reported, we write the metric.
     */
    public void record(TopicPartition partition, int bytes, int records) {
        this.unrecordedPartitions.remove(partition);
        this.fetchMetrics.increment(bytes, records);

        // collect and aggregate per-topic metrics
        String topic = partition.topic();
        FetchMetrics topicFetchMetric = this.topicFetchMetrics.get(topic);
        if (topicFetchMetric == null) {
            topicFetchMetric = new FetchMetrics();
            this.topicFetchMetrics.put(topic, topicFetchMetric);
        }
        topicFetchMetric.increment(bytes, records);

        if (this.unrecordedPartitions.isEmpty()) {
            // once all expected partitions from the fetch have reported in, record the metrics
            this.sensors.bytesFetched.record(this.fetchMetrics.fetchBytes);
            this.sensors.recordsFetched.record(this.fetchMetrics.fetchRecords);

            // also record per-topic metrics
            for (Map.Entry<String, FetchMetrics> entry: this.topicFetchMetrics.entrySet()) {
                FetchMetrics metric = entry.getValue();
                this.sensors.recordTopicFetchMetrics(entry.getKey(), metric.fetchBytes, metric.fetchRecords);
            }
        }
    }

    private static class FetchMetrics {
        private int fetchBytes;
        private int fetchRecords;

        protected void increment(int bytes, int records) {
            this.fetchBytes += bytes;
            this.fetchRecords += records;
        }
    }
}
