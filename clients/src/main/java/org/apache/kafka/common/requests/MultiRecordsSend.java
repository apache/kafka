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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.RecordsProcessingStats;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionRecordsStats;
import org.apache.kafka.common.network.MultiSend;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.record.LazyDownConversionRecordsSend;
import org.apache.kafka.common.record.RecordsSend;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

/**
 * An extension of {@link MultiSend}, specifically for cases where {@link RecordsSend} are nested in. This abstraction
 * has the ability to track and maintain {@link RecordsProcessingStats} for each of the underlying {@link RecordsSend}.
 * This is needed when the {@link RecordsSend} needs to further process the enclosed {@link org.apache.kafka.common.record.Records Records},
 * for example when messages are lazily down-converted using {@link org.apache.kafka.common.record.LazyDownConversionRecords LazyDownConversionRecords}.
 */
public class MultiRecordsSend extends MultiSend {
    private final Map<TopicPartition, RecordsProcessingStats> processingStats = new HashMap<>();

    public MultiRecordsSend(String dest, Queue<Send> sends) {
        super(dest, sends);
    }

    public Map<TopicPartition, RecordsProcessingStats> processingStats() {
        return processingStats;
    }

    private void addTopicPartitionProcessingStats(TopicPartitionRecordsStats stats) {
        if (stats != null)
            processingStats.put(stats.topicPartition(), stats.recordsProcessingStats());
    }

    @Override
    protected void onComplete(Send completedSend) {
        // The underlying send might have accumulated statistics that need to be recorded. For example,
        // LazyDownConversionRecordsSend accumulates statistics related to the number of bytes down-converted, the amount
        // of temporary memory used for down-conversion, etc. Pull out any such statistics from the underlying send
        // and fold it up appropriately.
        if (completedSend instanceof LazyDownConversionRecordsSend)
            addTopicPartitionProcessingStats(((LazyDownConversionRecordsSend) completedSend).recordsProcessingStats());
    }
}
