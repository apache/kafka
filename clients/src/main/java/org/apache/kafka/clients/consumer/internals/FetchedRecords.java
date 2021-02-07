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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FetchedRecords<K, V> {
    private final Map<TopicPartition, List<ConsumerRecord<K, V>>> records;
    private final Map<TopicPartition, FetchMetadata> metadata;

    public static final class FetchMetadata {

        private final long receivedTimestamp;
        private final SubscriptionState.FetchPosition position;
        private final Long endOffset;

        public FetchMetadata(final long receivedTimestamp,
                             final SubscriptionState.FetchPosition position,
                             final Long endOffset) {
            this.receivedTimestamp = receivedTimestamp;
            this.position = position;
            this.endOffset = endOffset;
        }

        public long receivedTimestamp() {
            return receivedTimestamp;
        }

        public SubscriptionState.FetchPosition position() {
            return position;
        }

        public Long endOffset() {
            return endOffset;
        }

        @Override
        public String toString() {
            return "FetchMetadata{" +
                "receivedTimestamp=" + receivedTimestamp +
                ", position=" + position +
                ", endOffset=" + endOffset +
                '}';
        }
    }

    public FetchedRecords() {
        records = new HashMap<>();
        metadata = new HashMap<>();
    }

    public void addRecords(final TopicPartition topicPartition, final List<ConsumerRecord<K, V>> records) {
        if (this.records.containsKey(topicPartition)) {
            // this case shouldn't usually happen because we only send one fetch at a time per partition,
            // but it might conceivably happen in some rare cases (such as partition leader changes).
            // we have to copy to a new list because the old one may be immutable
            final List<ConsumerRecord<K, V>> currentRecords = this.records.get(topicPartition);
            final List<ConsumerRecord<K, V>> newRecords = new ArrayList<>(records.size() + currentRecords.size());
            newRecords.addAll(currentRecords);
            newRecords.addAll(records);
            this.records.put(topicPartition, newRecords);
        } else {
            this.records.put(topicPartition, records);
        }
    }

    public Map<TopicPartition, List<ConsumerRecord<K, V>>> records() {
        return records;
    }

    public void addMetadata(final TopicPartition partition, final FetchMetadata fetchMetadata) {
        metadata.put(partition, fetchMetadata);
    }

    public Map<TopicPartition, FetchMetadata> metadata() {
        return metadata;
    }

    public boolean isEmpty() {
        return records.isEmpty() && metadata.isEmpty();
    }
}
