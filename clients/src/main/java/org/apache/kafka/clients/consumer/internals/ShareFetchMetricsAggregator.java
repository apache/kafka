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

import java.util.Set;

public class ShareFetchMetricsAggregator {
    private final ShareFetchMetricsManager shareFetchMetricsManager;
    private final FetchMetrics fetchMetrics = new FetchMetrics();
    private final Set<TopicPartition> unrecordedPartitions;

    public ShareFetchMetricsAggregator(ShareFetchMetricsManager shareFetchMetricsManager, Set<TopicPartition> partitions) {
        this.shareFetchMetricsManager = shareFetchMetricsManager;
        this.unrecordedPartitions = partitions;
    }

    public void record(TopicPartition partition, int bytes, int records) {
        // Aggregate the metrics at the fetch level
        fetchMetrics.increment(bytes, records);

        maybeRecordMetrics(partition);
    }

    private void maybeRecordMetrics(TopicPartition partition) {
        unrecordedPartitions.remove(partition);

        if (!unrecordedPartitions.isEmpty())
            return;

        // Record the metrics aggregated at the fetch level.
        shareFetchMetricsManager.recordRecordsFetched(fetchMetrics.records);
        shareFetchMetricsManager.recordBytesFetched(fetchMetrics.bytes);
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
