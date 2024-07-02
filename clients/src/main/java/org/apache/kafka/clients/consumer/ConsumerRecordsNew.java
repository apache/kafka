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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.AbstractIterator;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * A container that holds the list {@link ConsumerRecord} per partition for a
 * particular topic. There is one {@link ConsumerRecord} list for every topic
 * partition returned by a {@link Consumer#poll(java.time.Duration)} operation.
 */
public class ConsumerRecordsNew<K, V> implements Iterable<ConsumerRecord<K, V>> {
    public static final ConsumerRecordsNew<Object, Object> EMPTY = new ConsumerRecordsNew<>(Collections.emptyMap());

    private final Map<TopicPartition, List<ConsumerRecord<K, V>>> records;

    public ConsumerRecordsNew(Map<TopicPartition, List<ConsumerRecord<K, V>>> records) {
        this.records = records;
    }

    /**
     * Get just the records for the given partition
     *
     * @param partition The partition to get records for
     */
    public List<ConsumerRecord<K, V>> records(TopicPartition partition) {
        List<ConsumerRecord<K, V>> recs = this.records.get(partition);
        if (recs == null)
            return Collections.emptyList();
        else
            return Collections.unmodifiableList(recs);
    }

    public Iterable<ConsumerRecord<K, V>> records(String topic) {
        if (topic == null)
            throw new IllegalArgumentException("Topic must be non-null.");
        return new ConcatenatedIterable<>(records, record -> record.topic().equals(topic));
    }

    /**
     * Get the partitions which have records contained in this record set.
     *
     * @return the set of partitions with data in this record set (may be empty if no data was returned)
     */
    public Set<TopicPartition> partitions() {
        return Collections.unmodifiableSet(records.keySet());
    }

    @Override
    public Iterator<ConsumerRecord<K, V>> iterator() {
        return new ConcatenatedIterable<>(records).iterator();
    }

    /**
     * The number of records for all topics
     */
    public int count() {
        int count = 0;
        for (List<ConsumerRecord<K, V>> recs : this.records.values())
            count += recs.size();
        return count;
    }

    private static class ConcatenatedIterable<K, V> implements Iterable<ConsumerRecord<K, V>> {

        private final Map<TopicPartition, ? extends Iterable<ConsumerRecord<K, V>>> topicPartitionToRecords;
        private final Predicate<TopicPartition> predicate;

        public ConcatenatedIterable(Map<TopicPartition, ? extends Iterable<ConsumerRecord<K, V>>> topicPartitionToRecords) {
            this(topicPartitionToRecords, null);
        }

        public ConcatenatedIterable(Map<TopicPartition, ? extends Iterable<ConsumerRecord<K, V>>> topicPartitionToRecords, Predicate<TopicPartition> predicate) {
            this.topicPartitionToRecords = topicPartitionToRecords;
            this.predicate = predicate;
        }

        @Override
        public Iterator<ConsumerRecord<K, V>> iterator() {
            return new AbstractIterator<ConsumerRecord<K, V>>() {
                final Iterator<? extends Map.Entry<TopicPartition, ? extends Iterable<ConsumerRecord<K, V>>>> iterator
                        = topicPartitionToRecords.entrySet().iterator();
                Iterator<ConsumerRecord<K, V>> current;

                protected ConsumerRecord<K, V> makeNext() {
                    while (current == null || !current.hasNext()) {
                        if (!advanceToNextIterator()) {
                            return allDone();
                        }
                    }
                    return current.next();
                }

                private boolean advanceToNextIterator() {
                    while (iterator.hasNext()) {
                        Map.Entry<TopicPartition, ? extends Iterable<ConsumerRecord<K, V>>> next = iterator.next();
                        if (predicate == null || predicate.test(next.getKey())) {
                            current = next.getValue().iterator();
                            return true;
                        }
                    }
                    return false;
                }
            };
        }
    }

    public boolean isEmpty() {
        return records.isEmpty();
    }

    @SuppressWarnings("unchecked")
    public static <K, V> ConsumerRecordsNew<K, V> empty() {
        return (ConsumerRecordsNew<K, V>) EMPTY;
    }

}
