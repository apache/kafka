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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

class OffsetSyncWriter implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(OffsetSyncWriter.class);
    private static final int MAX_OUTSTANDING_OFFSET_SYNCS = 10;

    private final Map<TopicPartition, OffsetSync> delayedOffsetSyncs = new LinkedHashMap<>();
    private final Map<TopicPartition, OffsetSync> pendingOffsetSyncs = new LinkedHashMap<>();
    private Semaphore outstandingOffsetSyncs;
    private KafkaProducer<byte[], byte[]> offsetProducer;
    private String offsetSyncsTopic;
    protected long maxOffsetLag;

    public OffsetSyncWriter(MirrorSourceTaskConfig config) {
        outstandingOffsetSyncs = new Semaphore(MAX_OUTSTANDING_OFFSET_SYNCS);
        offsetSyncsTopic = config.offsetSyncsTopic();
        offsetProducer = MirrorUtils.newProducer(config.offsetSyncsTopicProducerConfig());
        maxOffsetLag = config.maxOffsetLag();
    }

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

    // sends OffsetSync record to internal offsets topic
    private void sendOffsetSync(OffsetSync offsetSync) {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(offsetSyncsTopic, 0,
                offsetSync.recordKey(), offsetSync.recordValue());
        offsetProducer.send(record, (x, e) -> {
            if (e != null) {
                log.error("Failure sending offset sync.", e);
            } else {
                log.trace("Sync'd offsets for {}: {}=={}", offsetSync.topicPartition(),
                        offsetSync.upstreamOffset(), offsetSync.downstreamOffset());
            }
            outstandingOffsetSyncs.release();
        });
    }

    protected void firePendingOffsetSyncs() {
        while (true) {
            OffsetSync pendingOffsetSync;
            synchronized (this) {
                Iterator<OffsetSync> syncIterator = pendingOffsetSyncs.values().iterator();
                if (!syncIterator.hasNext()) {
                    // Nothing to sync
                    log.trace("No more pending offset syncs");
                    return;
                }
                pendingOffsetSync = syncIterator.next();
                if (!outstandingOffsetSyncs.tryAcquire()) {
                    // Too many outstanding syncs
                    log.trace("Too many in-flight offset syncs; will try to send remaining offset syncs later");
                    return;
                }
                syncIterator.remove();
            }
            // Publish offset sync outside of synchronized block; we may have to
            // wait for producer metadata to update before Producer::send returns
            sendOffsetSync(pendingOffsetSync);
            log.trace("Dispatched offset sync for {}", pendingOffsetSync.topicPartition());
        }
    }

    protected void clearPendingOffsetSyncs() {
        pendingOffsetSyncs.clear();
    }

    protected synchronized void promoteDelayedOffsetSyncs() {
        pendingOffsetSyncs.putAll(delayedOffsetSyncs);
        delayedOffsetSyncs.clear();
    }

    // updates partition state and queues up OffsetSync if necessary
    protected void maybeQueueOffsetSyncs(MirrorSourceTask.PartitionState partitionState,
                                       TopicPartition topicPartition,
                                       long upstreamOffset,
                                       long downstreamOffset) {
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
}
