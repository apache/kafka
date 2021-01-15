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

import org.apache.kafka.clients.consumer.internals.FetchedRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.AbstractIterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A container that holds the list {@link ConsumerRecord} per partition for a
 * particular topic. There is one {@link ConsumerRecord} list for every topic
 * partition returned by a {@link Consumer#poll(java.time.Duration)} operation.
 */
public class ConsumerRecords<K, V> implements Iterable<ConsumerRecord<K, V>> {
    public static final ConsumerRecords<Object, Object> EMPTY = new ConsumerRecords<>(
        Collections.emptyMap(),
        Collections.emptyMap()
    );

    private final Map<TopicPartition, List<ConsumerRecord<K, V>>> records;
    private final Map<TopicPartition, Metadata> metadata;

    public static final class Metadata {

        private final long receivedTimestamp;
        private final Long position;
        private final Long endOffset;

        public Metadata(final long receivedTimestamp,
                        final Long position,
                        final Long endOffset) {
            this.receivedTimestamp = receivedTimestamp;
            this.position = position;
            this.endOffset = endOffset;
        }

        /**
         * @return The timestamp of the broker response that contained this metadata
         */
        public long receivedTimestamp() {
            return receivedTimestamp;
        }

        /**
         * @return The next position the consumer will fetch, or null if the consumer has no position.
         */
        public Long position() {
            return position;
        }

        /**
         * @return The lag between the next position to fetch and the current end of the partition, or
         * null if the end offset is not known or there is no position.
         */
        public Long lag() {
            return endOffset == null || position == null ? null : endOffset - position;
        }

        /**
         * @return The current last offset in the partition. The determination of the "last" offset
         * depends on the Consumer's isolation level. Under "read_uncommitted," this is the last successfully
         * replicated offset plus one. Under "read_committed," this is the minimum of the last successfully
         * replicated offset plus one or the smallest offset of any open transaction. Null if the end offset
         * is not known.
         */
        public Long endOffset() {
            return endOffset;
        }

        @Override
        public String toString() {
            return "Metadata{" +
                "receivedTimestamp=" + receivedTimestamp +
                ", position=" + position +
                ", endOffset=" + endOffset +
                '}';
        }
    }

    private static <K, V> Map<TopicPartition, Metadata> extractMetadata(final FetchedRecords<K, V> fetchedRecords) {
        final Map<TopicPartition, Metadata> metadata = new HashMap<>();
        for (final Map.Entry<TopicPartition, FetchedRecords.FetchMetadata> entry : fetchedRecords.metadata().entrySet()) {
            metadata.put(
                entry.getKey(),
                new Metadata(
                    entry.getValue().receivedTimestamp(),
                    entry.getValue().position() == null ? null : entry.getValue().position().offset,
                    entry.getValue().endOffset()
                )
            );
        }
        return metadata;
    }

    public ConsumerRecords(final Map<TopicPartition, List<ConsumerRecord<K, V>>> records) {
        this.records = records;
        this.metadata = new HashMap<>();
    }

    public ConsumerRecords(final Map<TopicPartition, List<ConsumerRecord<K, V>>> records,
                           final Map<TopicPartition, Metadata> metadata) {
        this.records = records;
        this.metadata = metadata;
    }

    public ConsumerRecords(final FetchedRecords<K, V> fetchedRecords) {
        this(fetchedRecords.records(), extractMetadata(fetchedRecords));
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

    /**
     * Get the updated metadata returned by the brokers along with this record set.
     * May be empty or partial depending on the responses from the broker during this particular poll.
     * May also include metadata for additional partitions than the ones for which there are records
     * in this {@code ConsumerRecords} object.
     */
    public Map<TopicPartition, Metadata> metadata() {
        return Collections.unmodifiableMap(metadata);
    }

    /**
     * Get just the records for the given topic
     */
    public Iterable<ConsumerRecord<K, V>> records(String topic) {
        if (topic == null)
            throw new IllegalArgumentException("Topic must be non-null.");
        List<List<ConsumerRecord<K, V>>> recs = new ArrayList<>();
        for (Map.Entry<TopicPartition, List<ConsumerRecord<K, V>>> entry : records.entrySet()) {
            if (entry.getKey().topic().equals(topic))
                recs.add(entry.getValue());
        }
        return new ConcatenatedIterable<>(recs);
    }

    /**
     * Get the partitions which have records contained in this record set.
     * @return the set of partitions with data in this record set (may be empty if no data was returned)
     */
    public Set<TopicPartition> partitions() {
        return Collections.unmodifiableSet(records.keySet());
    }

    @Override
    public Iterator<ConsumerRecord<K, V>> iterator() {
        return new ConcatenatedIterable<>(records.values()).iterator();
    }

    /**
     * The number of records for all topics
     */
    public int count() {
        int count = 0;
        for (List<ConsumerRecord<K, V>> recs: this.records.values())
            count += recs.size();
        return count;
    }

    private static class ConcatenatedIterable<K, V> implements Iterable<ConsumerRecord<K, V>> {

        private final Iterable<? extends Iterable<ConsumerRecord<K, V>>> iterables;

        public ConcatenatedIterable(Iterable<? extends Iterable<ConsumerRecord<K, V>>> iterables) {
            this.iterables = iterables;
        }

        @Override
        public Iterator<ConsumerRecord<K, V>> iterator() {
            return new AbstractIterator<ConsumerRecord<K, V>>() {
                Iterator<? extends Iterable<ConsumerRecord<K, V>>> iters = iterables.iterator();
                Iterator<ConsumerRecord<K, V>> current;

                public ConsumerRecord<K, V> makeNext() {
                    while (current == null || !current.hasNext()) {
                        if (iters.hasNext())
                            current = iters.next().iterator();
                        else
                            return allDone();
                    }
                    return current.next();
                }
            };
        }
    }

    public boolean isEmpty() {
        return records.isEmpty();
    }

    @SuppressWarnings("unchecked")
    public static <K, V> ConsumerRecords<K, V> empty() {
        return (ConsumerRecords<K, V>) EMPTY;
    }

}
