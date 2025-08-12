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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

class SynchronizedPartitionGroup extends AbstractPartitionGroup {

    private final AbstractPartitionGroup wrapped;

    public SynchronizedPartitionGroup(final AbstractPartitionGroup wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    synchronized boolean readyToProcess(final long wallClockTime) {
        return wrapped.readyToProcess(wallClockTime);
    }

    @Override
    synchronized void updatePartitions(final Set<TopicPartition> inputPartitions, final Function<TopicPartition, RecordQueue> recordQueueCreator) {
        wrapped.updatePartitions(inputPartitions, recordQueueCreator);
    }

    @Override
    synchronized void setPartitionTime(final TopicPartition partition, final long partitionTime) {
        wrapped.setPartitionTime(partition, partitionTime);
    }

    @Override
    synchronized StampedRecord nextRecord(final RecordInfo info, final long wallClockTime) {
        return wrapped.nextRecord(info, wallClockTime);
    }

    @Override
    synchronized int addRawRecords(final TopicPartition partition, final Iterable<ConsumerRecord<byte[], byte[]>> rawRecords) {
        return wrapped.addRawRecords(partition, rawRecords);
    }

    @Override
    synchronized long partitionTimestamp(final TopicPartition partition) {
        return wrapped.partitionTimestamp(partition);
    }

    @Override
    synchronized long streamTime() {
        return wrapped.streamTime();
    }

    @Override
    synchronized Long headRecordOffset(final TopicPartition partition) {
        return wrapped.headRecordOffset(partition);
    }

    @Override
    Optional<Integer> headRecordLeaderEpoch(final TopicPartition partition) {
        return Optional.empty();
    }

    @Override
    synchronized int numBuffered() {
        return wrapped.numBuffered();
    }

    @Override
    synchronized int numBuffered(final TopicPartition tp) {
        return wrapped.numBuffered(tp);
    }

    @Override
    synchronized void clear() {
        wrapped.clear();
    }

    @Override
    synchronized void updateLags() {
        wrapped.updateLags();
    }

    @Override
    synchronized void close() {
        wrapped.close();
    }

    @Override
    synchronized Set<TopicPartition> partitions() {
        return wrapped.partitions();
    }
}
