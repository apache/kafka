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

abstract class AbstractPartitionGroup {

    abstract boolean readyToProcess(long wallClockTime);

    // creates queues for new partitions, removes old queues, saves cached records for previously assigned partitions
    abstract void updatePartitions(Set<TopicPartition> inputPartitions, Function<TopicPartition, RecordQueue> recordQueueCreator);

    abstract void setPartitionTime(TopicPartition partition, long partitionTime);

    /**
     * Get the next record and queue
     *
     * @return StampedRecord
     */
    abstract StampedRecord nextRecord(RecordInfo info, long wallClockTime);

    /**
     * Adds raw records to this partition group
     *
     * @param partition  the partition
     * @param rawRecords the raw records
     * @return the queue size for the partition
     */
    abstract int addRawRecords(TopicPartition partition, Iterable<ConsumerRecord<byte[], byte[]>> rawRecords);

    abstract long partitionTimestamp(final TopicPartition partition);

    /**
     * Return the stream-time of this partition group defined as the largest timestamp seen across all partitions
     */
    abstract long streamTime();

    abstract Long headRecordOffset(final TopicPartition partition);

    abstract Optional<Integer> headRecordLeaderEpoch(final TopicPartition partition);

    abstract int numBuffered();

    abstract int numBuffered(TopicPartition tp);

    abstract void clear();

    abstract void updateLags();

    abstract void close();

    abstract Set<TopicPartition> partitions();

    static class RecordInfo {
        RecordQueue queue;

        ProcessorNode<?, ?, ?, ?> node() {
            return queue.source();
        }

        TopicPartition partition() {
            return queue.partition();
        }

        RecordQueue queue() {
            return queue;
        }
    }
}
