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

import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Since we parse the message data for each partition from each fetch response lazily, fetch-level
 * metrics need to be aggregated as the messages from each partition are parsed. This class is used
 * to facilitate this incremental aggregation.
 */
class FetchMetricsAggregator {

    private final FetchMetricsManager metricsManager;
    private final Set<TopicPartition> unrecordedPartitions;
    private final FetchMetrics fetchFetchMetrics = new FetchMetrics();
    private final Map<String, FetchMetrics> perTopicFetchMetrics = new HashMap<>();

    FetchMetricsAggregator(FetchMetricsManager metricsManager, Set<TopicPartition> partitions) {
        this.metricsManager = metricsManager;
        this.unrecordedPartitions = new HashSet<>(partitions);
    }

    /**
     * After each partition is parsed, we update the current metric totals with the total bytes
     * and number of records parsed. After all partitions have reported, we write the metric.
     */
    void record(TopicPartition partition, int bytes, int records) {
        // Aggregate the metrics at the fetch level
        fetchFetchMetrics.increment(bytes, records);

        // Also aggregate the metrics on a per-topic basis.
        perTopicFetchMetrics.computeIfAbsent(partition.topic(), t -> new FetchMetrics())
                        .increment(bytes, records);

        maybeRecordMetrics(partition);
    }

    /**
     * Once we've detected that all of the {@link TopicPartition partitions} for the fetch have been handled, we
     * can then record the aggregated metrics values. This is done at the fetch level and on a per-topic basis.
     *
     * @param partition {@link TopicPartition}
     */
    private void maybeRecordMetrics(TopicPartition partition) {
        unrecordedPartitions.remove(partition);

        if (!unrecordedPartitions.isEmpty())
            return;

        // Record the metrics aggregated at the fetch level.
        metricsManager.recordBytesFetched(fetchFetchMetrics.bytes);
        metricsManager.recordRecordsFetched(fetchFetchMetrics.records);

        // Also record the metrics aggregated on a per-topic basis.
        for (Map.Entry<String, FetchMetrics> entry: perTopicFetchMetrics.entrySet()) {
            String topic = entry.getKey();
            FetchMetrics fetchMetrics = entry.getValue();
            metricsManager.recordBytesFetched(topic, fetchMetrics.bytes);
            metricsManager.recordRecordsFetched(topic, fetchMetrics.records);
        }
    }

    private static class FetchMetrics {

        private int bytes;
        private int records;

        private void increment(int bytes, int records) {
            this.bytes += bytes;
            this.records += records;
        }

    }

}