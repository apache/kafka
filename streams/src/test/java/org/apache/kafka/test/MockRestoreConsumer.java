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
package org.apache.kafka.test;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class MockRestoreConsumer<K, V> extends MockConsumer<byte[], byte[]> {
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;

    private TopicPartition assignedPartition = null;
    private long seekOffset = -1L;
    private long endOffset = 0L;
    private long currentOffset = 0L;

    private ArrayList<ConsumerRecord<byte[], byte[]>> recordBuffer = new ArrayList<>();

    public MockRestoreConsumer(final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
        super(OffsetResetStrategy.EARLIEST);

        reset();
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    // reset this mock restore consumer for a state store registration
    public void reset() {
        assignedPartition = null;
        seekOffset = -1L;
        endOffset = 0L;
        recordBuffer.clear();
    }

    // buffer a record (we cannot use addRecord because we need to add records before assigning a partition)
    public void bufferRecord(final ConsumerRecord<K, V> record) {
        recordBuffer.add(
            new ConsumerRecord<>(record.topic(), record.partition(), record.offset(), record.timestamp(),
                                 record.timestampType(), 0L, 0, 0,
                                 keySerializer.serialize(record.topic(), record.headers(), record.key()),
                                 valueSerializer.serialize(record.topic(), record.headers(), record.value()),
                                 record.headers()));
        endOffset = record.offset();

        super.updateEndOffsets(Collections.singletonMap(assignedPartition, endOffset));
    }

    @Override
    public synchronized void assign(final Collection<TopicPartition> partitions) {
        final int numPartitions = partitions.size();
        if (numPartitions > 1)
            throw new IllegalArgumentException("RestoreConsumer: more than one partition specified");

        if (numPartitions == 1) {
            if (assignedPartition != null)
                throw new IllegalStateException("RestoreConsumer: partition already assigned");
            assignedPartition = partitions.iterator().next();

            // set the beginning offset to 0
            // NOTE: this is users responsible to set the initial lEO.
            super.updateBeginningOffsets(Collections.singletonMap(assignedPartition, 0L));
        }

        super.assign(partitions);
    }

    @Override
    public ConsumerRecords<byte[], byte[]> poll(final Duration timeout) {
        // add buffered records to MockConsumer
        for (final ConsumerRecord<byte[], byte[]> record : recordBuffer) {
            super.addRecord(record);
        }
        recordBuffer.clear();

        final ConsumerRecords<byte[], byte[]> records = super.poll(timeout);

        // set the current offset
        final Iterable<ConsumerRecord<byte[], byte[]>> partitionRecords = records.records(assignedPartition);
        for (final ConsumerRecord<byte[], byte[]> record : partitionRecords) {
            currentOffset = record.offset();
        }

        return records;
    }

    @Override
    public synchronized long position(final TopicPartition partition) {
        if (!partition.equals(assignedPartition))
            throw new IllegalStateException("RestoreConsumer: unassigned partition");

        return currentOffset;
    }

    @Override
    public synchronized void seek(final TopicPartition partition, final long offset) {
        if (offset < 0)
            throw new IllegalArgumentException("RestoreConsumer: offset should not be negative");

        if (seekOffset >= 0)
            throw new IllegalStateException("RestoreConsumer: offset already seeked");

        seekOffset = offset;
        currentOffset = offset;
        super.seek(partition, offset);
    }

    @Override
    public synchronized void seekToBeginning(final Collection<TopicPartition> partitions) {
        if (partitions.size() != 1)
            throw new IllegalStateException("RestoreConsumer: other than one partition specified");

        for (final TopicPartition partition : partitions) {
            if (!partition.equals(assignedPartition))
                throw new IllegalStateException("RestoreConsumer: seek-to-end not on the assigned partition");
        }

        currentOffset = 0L;
    }


    @Override
    public Map<TopicPartition, Long> endOffsets(final Collection<TopicPartition> partitions) {
        if (partitions.size() != 1)
            throw new IllegalStateException("RestoreConsumer: other than one partition specified");

        for (final TopicPartition partition : partitions) {
            if (!partition.equals(assignedPartition))
                throw new IllegalStateException("RestoreConsumer: seek-to-end not on the assigned partition");
        }

        currentOffset = endOffset;
        return super.endOffsets(partitions);
    }
}
