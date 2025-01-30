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

package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Semaphore;

/**
 * Used internally by MirrorMaker to write translated offsets into offset-syncs topic, with some buffering logic to limit the number of in-flight records.
 */
class OffsetSyncWriter implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(OffsetSyncWriter.class);
    private static final int MAX_OUTSTANDING_OFFSET_SYNCS = 10;

    private final Map<TopicPartition, OffsetSync> delayedOffsetSyncs = new LinkedHashMap<>();
    private final Map<TopicPartition, OffsetSync> pendingOffsetSyncs = new LinkedHashMap<>();
    private final Semaphore outstandingOffsetSyncs;
    private final KafkaProducer<byte[], byte[]> offsetProducer;
    private final String offsetSyncsTopic;
    private final long maxOffsetLag;
    private final Map<TopicPartition, PartitionState> partitionStates = new HashMap<>();


    public OffsetSyncWriter(MirrorSourceTaskConfig config) {
        outstandingOffsetSyncs = new Semaphore(MAX_OUTSTANDING_OFFSET_SYNCS);
        offsetSyncsTopic = config.offsetSyncsTopic();
        offsetProducer = MirrorUtils.newProducer(config.offsetSyncsTopicProducerConfig());
        maxOffsetLag = config.maxOffsetLag();
    }

    // Visible for testing
    public OffsetSyncWriter(KafkaProducer<byte[], byte[]> producer,
                            String offsetSyncsTopic,
                            Semaphore outstandingOffsetSyncs,
                            long maxOffsetLag) {
        this.offsetProducer = producer;
        this.offsetSyncsTopic = offsetSyncsTopic;
        this.outstandingOffsetSyncs = outstandingOffsetSyncs;
        this.maxOffsetLag = maxOffsetLag;
    }

    public void close() {
        Utils.closeQuietly(offsetProducer, "offset producer");
    }

    public long maxOffsetLag() {
        return maxOffsetLag;
    }

    public Map<TopicPartition, PartitionState> partitionStates() {
        return this.partitionStates;
    }

    // sends OffsetSync record to internal offsets topic
    private void sendOffsetSync(OffsetSync offsetSync) {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(offsetSyncsTopic, 0,
                offsetSync.recordKey(), offsetSync.recordValue());
        offsetProducer.send(record, (x, e) -> {
            if (e != null) {
                LOG.error("Failure sending offset sync.", e);
            } else {
                LOG.trace("Sync'd offsets for {}: {}=={}", offsetSync.topicPartition(),
                        offsetSync.upstreamOffset(), offsetSync.downstreamOffset());
            }
            outstandingOffsetSyncs.release();
        });
    }

    void firePendingOffsetSyncs() {
        while (true) {
            OffsetSync pendingOffsetSync;
            synchronized (this) {
                Iterator<OffsetSync> syncIterator = pendingOffsetSyncs.values().iterator();
                if (!syncIterator.hasNext()) {
                    // Nothing to sync
                    LOG.trace("No more pending offset syncs");
                    return;
                }
                pendingOffsetSync = syncIterator.next();
                if (!outstandingOffsetSyncs.tryAcquire()) {
                    // Too many outstanding syncs
                    LOG.trace("Too many in-flight offset syncs; will try to send remaining offset syncs later");
                    return;
                }
                syncIterator.remove();
            }
            // Publish offset sync outside of synchronized block; we may have to
            // wait for producer metadata to update before Producer::send returns
            sendOffsetSync(pendingOffsetSync);
            LOG.trace("Dispatched offset sync for {}", pendingOffsetSync.topicPartition());
        }
    }

    synchronized void promoteDelayedOffsetSyncs() {
        pendingOffsetSyncs.putAll(delayedOffsetSyncs);
        delayedOffsetSyncs.clear();
    }

    // updates partition state and queues up OffsetSync if necessary
    void maybeQueueOffsetSyncs(TopicPartition topicPartition, long upstreamOffset, long downstreamOffset) {
        PartitionState partitionState =
                partitionStates.computeIfAbsent(topicPartition, x -> new PartitionState(maxOffsetLag));

        OffsetSync offsetSync = new OffsetSync(topicPartition, upstreamOffset, downstreamOffset);
        if (partitionState.update(upstreamOffset, downstreamOffset)) {
            // Queue this sync for an immediate send, as downstream state is sufficiently stale
            synchronized (this) {
                delayedOffsetSyncs.remove(topicPartition);
                pendingOffsetSyncs.put(topicPartition, offsetSync);
            }
            partitionState.reset();
        } else {
            // Queue this sync to be delayed until the next periodic offset commit
            synchronized (this) {
                delayedOffsetSyncs.put(topicPartition, offsetSync);
            }
        }
    }

    // visible for testing
    protected Map<TopicPartition, OffsetSync> getDelayedOffsetSyncs() {
        return delayedOffsetSyncs;
    }

    // visible for testing
    protected Map<TopicPartition, OffsetSync> getPendingOffsetSyncs() {
        return pendingOffsetSyncs;
    }

    static class PartitionState {
        long previousUpstreamOffset = -1L;
        long previousDownstreamOffset = -1L;
        long lastSyncDownstreamOffset = -1L;
        long maxOffsetLag;
        boolean shouldSyncOffsets;

        PartitionState(long maxOffsetLag) {
            this.maxOffsetLag = maxOffsetLag;
        }

        // true if we should emit an offset sync
        boolean update(long upstreamOffset, long downstreamOffset) {
            // Emit an offset sync if any of the following conditions are true
            boolean noPreviousSyncThisLifetime = lastSyncDownstreamOffset == -1L;
            // the OffsetSync::translateDownstream method will translate this offset 1 past the last sync, so add 1.
            // TODO: share common implementation to enforce this relationship
            boolean translatedOffsetTooStale = downstreamOffset - (lastSyncDownstreamOffset + 1) >= maxOffsetLag;
            boolean skippedUpstreamRecord = upstreamOffset - previousUpstreamOffset != 1L;
            boolean truncatedDownstreamTopic = downstreamOffset < previousDownstreamOffset;
            if (noPreviousSyncThisLifetime || translatedOffsetTooStale || skippedUpstreamRecord || truncatedDownstreamTopic) {
                lastSyncDownstreamOffset = downstreamOffset;
                shouldSyncOffsets = true;
            }
            previousUpstreamOffset = upstreamOffset;
            previousDownstreamOffset = downstreamOffset;
            return shouldSyncOffsets;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof PartitionState that)) return false;
            return previousUpstreamOffset == that.previousUpstreamOffset &&
                    previousDownstreamOffset == that.previousDownstreamOffset &&
                    lastSyncDownstreamOffset == that.lastSyncDownstreamOffset &&
                    maxOffsetLag == that.maxOffsetLag &&
                    shouldSyncOffsets == that.shouldSyncOffsets;
        }

        @Override
        public int hashCode() {
            return Objects.hash(previousUpstreamOffset, previousDownstreamOffset, lastSyncDownstreamOffset, maxOffsetLag, shouldSyncOffsets);
        }

        void reset() {
            shouldSyncOffsets = false;
        }
    }

}
