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
import org.apache.kafka.common.annotation.InterfaceAudience;
import org.apache.kafka.common.utils.internals.AbstractIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A container that holds the list {@link ConsumerRecord} per partition for a
 * particular topic. There is one {@link ConsumerRecord} list for every topic
 * partition returned by a {@link Consumer#poll(java.time.Duration)} operation.
 */
@InterfaceAudience.Public
public class ConsumerRecords<K, V> implements Iterable<ConsumerRecord<K, V>> {
    private static final Logger log = LoggerFactory.getLogger(ConsumerRecords.class);

    public static final ConsumerRecords<Object, Object> EMPTY = new ConsumerRecords<>(Map.of(), Map.of());

    private final Map<TopicPartition, List<ConsumerRecord<K, V>>> records;
    private final Map<TopicPartition, OffsetAndMetadata> nextOffsets;

    // Flag to detect if legacy ConsumerRecords(Map) constructor is used. See KAFKA-20660 for more details.
    private final boolean tainted;
    // Visible for testing
    static final long TAINT_LOG_INTERVAL_NS = TimeUnit.MINUTES.toNanos(5);
    // Visible for testing
    static final AtomicLong TAINTED_NEXT_OFFSETS_LAST_LOG_NS = new AtomicLong(System.nanoTime() - TAINT_LOG_INTERVAL_NS);

    /**
     * @deprecated Since 4.0. Use {@link #ConsumerRecords(Map, Map)} instead.
     */
    @Deprecated(since = "4.0", forRemoval = true)
    public ConsumerRecords(Map<TopicPartition, List<ConsumerRecord<K, V>>> records) {
        this(records, Map.of(), true);
    }

    /**
     * Constructs a new ConsumerRecords with the given records and next offsets.
     *
     * @param records The records for each partition
     * @param nextOffsets The next offset and metadata for each partition whose position was advanced
     *                    during the poll call. These represent the offsets that the consumer will
     *                    start reading from on the next poll.
     */
    public ConsumerRecords(Map<TopicPartition, List<ConsumerRecord<K, V>>> records, final Map<TopicPartition, OffsetAndMetadata> nextOffsets) {
        this(records, nextOffsets, false);
    }

    private ConsumerRecords(Map<TopicPartition, List<ConsumerRecord<K, V>>> records, final Map<TopicPartition, OffsetAndMetadata> nextOffsets, boolean tainted) {
        this.records = records;
        this.nextOffsets = Map.copyOf(nextOffsets);
        this.tainted = tainted;
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
     * Get the next offsets and metadata corresponding to all topic partitions for which the position have been advanced in this poll call
     * @return The next offsets that the consumer will consume
     */
    public Map<TopicPartition, OffsetAndMetadata> nextOffsets() {
        if (this.tainted) {
            final long now = System.nanoTime();
            final long lastLog = TAINTED_NEXT_OFFSETS_LAST_LOG_NS.get();
            // nextOffsets() is called on every poll, so a tainted instance would otherwise log the deprecation error log on
            // every call. A time based approach is used to avoid this. See KAFKA-20660 for more details.
            if (now - lastLog >= TAINT_LOG_INTERVAL_NS && TAINTED_NEXT_OFFSETS_LAST_LOG_NS.compareAndSet(lastLog, now)) {
                log.error("ConsumerRecords#nextOffsets() returned empty because this instance was built with the " +
                        "deprecated ConsumerRecords(Map) constructor (see KIP-1094), which does not supply next offsets. " +
                        "Downstream logic that relies on these offsets to advance the consumer's committed position " +
                        "(for example, Kafka Streams under exactly-once semantics) will be unable to commit, leading to " +
                        "reprocessing. Update the interceptor or wrapper that constructed it to use the " +
                        "ConsumerRecords(Map, Map) constructor that supplies next offsets.");
            }
        }
        return nextOffsets;
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
     * @return The set of partitions with data in this record set (may be empty if no data was returned)
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
            return new AbstractIterator<>() {
                final Iterator<? extends Iterable<ConsumerRecord<K, V>>> iters = iterables.iterator();
                Iterator<ConsumerRecord<K, V>> current;

                protected ConsumerRecord<K, V> makeNext() {
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

    /**
     * Returns whether this container has any records.
     *
     * @return True if there are no records, false otherwise
     */
    public boolean isEmpty() {
        return records.isEmpty();
    }

    /**
     * Returns an empty ConsumerRecords instance.
     *
     * @param <K> The key type
     * @param <V> The value type
     * @return An empty ConsumerRecords
     */
    @SuppressWarnings("unchecked")
    public static <K, V> ConsumerRecords<K, V> empty() {
        return (ConsumerRecords<K, V>) EMPTY;
    }

}
