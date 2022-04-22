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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionRatioEstimator;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.DefaultRecord;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.utils.Utils;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Built-in default partitioner.  Note, that this is just a utility class that is used directly from
 * RecordAccumulator, it does not implement the Partitioner interface.
 *
 * The class keeps track of various bookkeeping information required for adaptive sticky partitioning
 * (described in detail in KIP-794).  There is one partitioner object per topic.
 */
public class BuiltInPartitioner {
    private final String topic;
    private final int stickyBatchSize;
    private final CompressionType compression;
    private final ApiVersions apiVersions;

    private volatile PartitionLoadStats partitionLoadStats = null;
    private final AtomicReference<StickyPartitionInfo> stickyPartitionInfo = new AtomicReference<>();

    // Visible and used for testing only.
    static volatile public Supplier<Integer> mockRandom = null;

    /**
     * BuiltInPartitioner constructor.
     *
     * @param topic The topic
     * @param stickyBatchSize How much to produce to partition before switch
     * @param compression The compression codec for the records
     * @param apiVersions Request API versions for current connected brokers
     */
    public BuiltInPartitioner(String topic,
                              int stickyBatchSize,
                              CompressionType compression,
                              ApiVersions apiVersions) {
        this.topic = topic;
        this.stickyBatchSize = stickyBatchSize;
        this.compression = compression;
        this.apiVersions = apiVersions;
    }

    /**
     * Calculate the next partition for the topic based on the partition load stats.
     */
    private int nextPartition(Cluster cluster) {
        int random = mockRandom != null ? mockRandom.get() : Utils.toPositive(ThreadLocalRandom.current().nextInt());

        // Cache volatile variable in local variable.
        PartitionLoadStats partitionLoadStats = this.partitionLoadStats;

        if (partitionLoadStats == null) {
            // We don't have stats to do adaptive partitioning (or it's disabled), just switch to the next
            // partition based on uniform distribution.
            List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
            if (availablePartitions.size() > 0)
                return availablePartitions.get(random % availablePartitions.size()).partition();

            // We don't have available partitions, just pick one among all partitions.
            List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
            return random % partitions.size();
        } else {
            // Calculate next partition based on load distribution.
            assert partitionLoadStats.length > 0;

            int[] cumulativeFrequencyTable = partitionLoadStats.cumulativeFrequencyTable;
            int weightedRandom = random % cumulativeFrequencyTable[partitionLoadStats.length - 1];

            // By construction, the cumulative frequency table is sorted, so we can use binary
            // search to find the desired index.
            int searchResult = Arrays.binarySearch(cumulativeFrequencyTable, 0, partitionLoadStats.length, weightedRandom);

            // binarySearch results the index of the found element, or -(insertion_point) - 1
            // (where insertion_point is the index of the first element greater than the key).
            // We need to get the index of the first value that is strictly greater, which
            // would be the insertion point, except if we found the element that's equal to
            // the searched value (in this case we need to get next).  For example, if we have
            //  4 5 8
            // and we're looking for 3, then we'd get the insertion_point = 0, and the function
            // would return -0 - 1 = -1, by adding 1 we'd get 0.  If we're looking for 4, we'd
            // get 0, and we need the next one, so adding 1 works here as well.
            int partitionIndex = Math.abs(searchResult + 1);
            assert partitionIndex < partitionLoadStats.length;
            return partitionLoadStats.partitionIds[partitionIndex];
        }
    }

    /**
     * Test-only function.  When partition load stats are defined, return the end of range for the
     * random number.
     */
    public int loadStatsRangeEnd() {
        assert partitionLoadStats != null;
        assert partitionLoadStats.length > 0;
        return partitionLoadStats.cumulativeFrequencyTable[partitionLoadStats.length - 1];
    }

    /**
     * Calculate the partition trying to optimize for batching and broker load.
     * We keep track of bytes produced to partition and switch to a new one only after a certain amount of
     * bytes has been produced (a.k.a. "sticky" partitioning logic).
     *
     * @param key The record key
     * @param value The record value
     * @param headers The record header
     * @param byteSizeStatsMap The map partition -> byte size stats
     * @param cluster The cluster information
     * @return The partition to use for this record
     */
    public int partition(byte[] key, byte[] value, Header[] headers,
                         ConcurrentMap<Integer, PartitionByteSizeStats> byteSizeStatsMap, Cluster cluster) {
        // Loop to retry if our atomic ops are raced.
        while (true) {
            StickyPartitionInfo partitionInfo = stickyPartitionInfo.get();
            if (partitionInfo == null || partitionInfo.producedBytes.get() >= stickyBatchSize) {
                // The partition has exceeded the "stickiness" limit, need to switch.
                int partition = nextPartition(cluster);
                StickyPartitionInfo newPartitionInfo = new StickyPartitionInfo(partition);
                if (!stickyPartitionInfo.compareAndSet(partitionInfo, newPartitionInfo)) {
                    // We've got raced, retry.
                    continue;
                }
                partitionInfo = newPartitionInfo;
            }

            // Try to update bookkeeping information for the partition.
            final int recordSize = estimateRecordSize(byteSizeStatsMap.get(partitionInfo.index), key, value, headers);
            final int prevProducedBytes = partitionInfo.producedBytes.getAndAdd(recordSize);

            // We need to check if a concurrent thread has raced us and exceeded the "stickiness" limit
            // between the check and update.  For example:
            //  1. Thread1 notices partition1 is under limit, proceeds to use it.
            //  2. Thread2 notices partition1 is under limit, proceeds to use it.
            //  3. Thread1 updates the bookkeeping, drives partition1 over the limit.
            //  4. Thread2 updates the bookkeeping, sees that the partition1 is over the limit, retries.
            if (prevProducedBytes < stickyBatchSize)
                return partitionInfo.index;

            // We've got raced, retry.
        }
    }

    /**
     * Update partition load stats from the queue sizes of each partition.
     * NOTE: queueSizes are modified in place to avoid allocations
     *
     * @param queueSizes The queue sizes
     * @param partitionIds The partition ids for the queues
     * @param length The logical length of the arrays (could be less): we may eliminate some partitions
     *               based on latency, but to avoid reallocation of the arrays, we just decrement
     *               logical length
     * Visible for testing
     */
    public void updatePartitionLoadStats(int[] queueSizes, int[] partitionIds, int length) {
        if (queueSizes == null) {
            partitionLoadStats = null;
            return;
        }
        assert queueSizes.length == partitionIds.length;
        assert length <= queueSizes.length;

        // The queueSizes.length represents the number of all partitions in the topic and if we have
        // less than 2 partitions, there is no need to do adaptive logic.
        // If partitioner.availability.timeout.ms != 0, then partitions that experience high latencies
        // (greater than partitioner.availability.timeout.ms) may be excluded, the length represents
        // partitions that are not excluded.  If some partitions were excluded, we'd still want to
        // go through adaptive logic, even if we have one partition.
        // See also RecordAccumulator#partitionReady where the queueSizes are built.
        if (length < 1 || queueSizes.length < 2) {
            partitionLoadStats = null;
            return;
        }

        // We build cumulative frequency table from the queue sizes in place.  At the beginning
        // each entry contains queue size, then we invert it (so it represents the frequency)
        // and convert to a running sum.  Then a uniformly distributed random variable
        // in the range [0..last) would map to a partition with weighted probability.
        // Example: suppose we have 3 partitions with the corresponding queue sizes:
        //  0 3 1
        // Then we can invert them by subtracting the queue size from the max queue size + 1 = 4:
        //  4 1 3
        // Then we can convert it into a running sum (next value adds previous value):
        //  4 5 8
        // Now if we get a random number in the range [0..8) and find the first value that
        // is strictly greater than the number (e.g. for 4 it would be 5), then the index of
        // the value is the index of the partition we're looking for.  In this example
        // random numbers 0, 1, 2, 3 would map to partition[0], 4 would map to partition[1]
        // and 5, 6, 7 would map to partition[2].

        // Calculate max queue size + 1 and check if all sizes are the same.
        int maxSizePlus1 = queueSizes[0];
        boolean allEqual = true;
        for (int i = 1; i < length; i++) {
            if (queueSizes[i] != maxSizePlus1)
                allEqual = false;
            if (queueSizes[i] > maxSizePlus1)
                maxSizePlus1 = queueSizes[i];
        }
        ++maxSizePlus1;

        if (allEqual && length == queueSizes.length) {
            // No need to have complex probability logic when all queue sizes are the same,
            // and we didn't exclude partitions that experience high latencies (greater than
            // partitioner.availability.timeout.ms).
            partitionLoadStats = null;
            return;
        }

        // Invert and fold the queue size, so that they become separator values in the CFT.
        queueSizes[0] = maxSizePlus1 - queueSizes[0];
        for (int i = 1; i < length; i++) {
            queueSizes[i] = maxSizePlus1 - queueSizes[i] + queueSizes[i - 1];
        }
        partitionLoadStats = new PartitionLoadStats(queueSizes, partitionIds, length);
    }

    /**
     * Estimate the number of bytes a record would take in a batch.
     * Note that this function has side effects.
     * Visible for testing
     */
    public int estimateRecordSize(PartitionByteSizeStats byteSizeStats, byte[] key, byte[] value, Header[] headers) {
        float estimatedCompressionRatio = CompressionRatioEstimator.estimation(topic, compression);
        if (byteSizeStats == null) {
            return MemoryRecordsBuilder.estimateRecordSize(apiVersions.maxUsableProduceMagic(),
                compression, estimatedCompressionRatio, DefaultRecord.MAX_RECORD_OVERHEAD, key, value, headers);
        }
        // We don't really know the record size until it's serialized in a batch (and when compression
        // is used we don't even know until the batch is closed), but the sticky partitioner keeps track
        // of how much data each partition receives, and makes switching decision, before the record comes
        // to the batch.  To do proper estimation we keep track of 3 things:
        //  1. Average record overhead.
        //  2. Expected compression ratio.
        //  3. Batch headers.
        //
        // Even though this is just an estimate, the imprecision should be uniform over time, e.g. a partition that
        // gets more batches because it's on faster broker would have the batch headers properly accounted for
        // etc.
        int recordSize = MemoryRecordsBuilder.estimateRecordSize(apiVersions.maxUsableProduceMagic(),
            compression, estimatedCompressionRatio,
            byteSizeStats.avgRecordOverhead.get().intValue(), key, value, headers);

        // Note that we clear the batch headers when we get them, so this function has side effects.
        int batchHeaderBytes = byteSizeStats.batchHeaderBytes.getAndSet(0);
        return batchHeaderBytes + recordSize;
    }

    /**
     * Info for the current sticky partition.
     */
    private static class StickyPartitionInfo {
        public final int index;
        public final AtomicInteger producedBytes = new AtomicInteger();

        StickyPartitionInfo(int index) {
            this.index = index;
        }
    }

    /**
     * The partition load stats for each topic that are used for adaptive partition distribution.
     */
    private final static class PartitionLoadStats {
        public final int[] cumulativeFrequencyTable;
        public final int[] partitionIds;
        public final int length;
        public PartitionLoadStats(int[] cumulativeFrequencyTable, int[] partitionIds, int length) {
            assert cumulativeFrequencyTable.length == partitionIds.length;
            assert length <= cumulativeFrequencyTable.length;
            this.cumulativeFrequencyTable = cumulativeFrequencyTable;
            this.partitionIds = partitionIds;
            this.length = length;
        }
    }

    /**
     * Per-partition stats that keep track of information needed to calculate accurate byte sizes.
     * There is one stats object per partition.
     */
    public final static class PartitionByteSizeStats {
        private final AtomicInteger batchHeaderBytes = new AtomicInteger(0);
        private final AtomicReference<Double> avgRecordOverhead = new AtomicReference<>((double) DefaultRecord.MAX_RECORD_OVERHEAD);

        /**
         * Update batch stats.
         *
         * @param magic Max usable magic
         * @param compression Compression type
         */
        public void onNewBatch(byte magic, CompressionType compression) {
            batchHeaderBytes.addAndGet(AbstractRecords.recordBatchHeaderSizeInBytes(magic, compression));
        }

        /**
         * Update record overhead stats.
         *
         * @param recordOverhead The overhead of the produced record
         */
        public void updateRecordOverhead(int recordOverhead) {
            // Exponential moving average.
            final double decayCoefficient = 0.5;
            avgRecordOverhead.updateAndGet(v -> v * (1.0 - decayCoefficient) + recordOverhead * decayCoefficient);
        }
    }
}
