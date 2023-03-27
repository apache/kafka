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

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.apache.kafka.connect.util.TopicAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Used internally by MirrorMaker. Stores offset syncs and performs offset translation.
 * <p>A limited number of offset syncs can be stored per TopicPartition, in a way which provides better translation
 * later in the topic, closer to the live end of the topic.
 * This maintains the following invariants for each topic-partition in the in-memory sync storage:
 * <ul>
 *     <li>syncs[0] is the latest offset sync from the syncs topic</li>
 *     <li>For each i,j, i <= j: syncs[j].upstream <= syncs[i].upstream < syncs[j].upstream + 2^j</li>
 * </ul>
 * <p>Offset translation uses the syncs[i] which most closely precedes the upstream consumer group's current offset.
 * For a fixed in-memory state, translation of variable upstream offsets will be monotonic.
 * For variable in-memory state, translation of a fixed upstream offset will not be monotonic.
 * <p>Translation will be unavailable for all topic-partitions before an initial read-to-end of the offset syncs topic
 * is complete. Translation will be unavailable after that if no syncs are present for a topic-partition, or if relevant
 * offset syncs for the topic were eligible for compaction at the time of the initial read-to-end.
 */
class OffsetSyncStore implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(OffsetSyncStore.class);
    // Store one offset sync for each bit of the topic offset.
    private static final int SYNCS_PER_PARTITION = Long.SIZE;
    private final KafkaBasedLog<byte[], byte[]> backingStore;
    private final Map<TopicPartition, OffsetSync[]> offsetSyncs = new ConcurrentHashMap<>();
    private final TopicAdmin admin;
    protected volatile boolean readToEnd = false;

    OffsetSyncStore(MirrorCheckpointConfig config) {
        Consumer<byte[], byte[]> consumer = null;
        TopicAdmin admin = null;
        KafkaBasedLog<byte[], byte[]> store;
        try {
            consumer = MirrorUtils.newConsumer(config.offsetSyncsTopicConsumerConfig());
            admin = new TopicAdmin(
                    config.offsetSyncsTopicAdminConfig().get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG),
                    config.forwardingAdmin(config.offsetSyncsTopicAdminConfig()));
            store = createBackingStore(config, consumer, admin);
        } catch (Throwable t) {
            Utils.closeQuietly(consumer, "consumer for offset syncs");
            Utils.closeQuietly(admin, "admin client for offset syncs");
            throw t;
        }
        this.admin = admin;
        this.backingStore = store;
    }

    private KafkaBasedLog<byte[], byte[]> createBackingStore(MirrorCheckpointConfig config, Consumer<byte[], byte[]> consumer, TopicAdmin admin) {
        return new KafkaBasedLog<byte[], byte[]>(
                config.offsetSyncsTopic(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                () -> admin,
                (error, record) -> this.handleRecord(record),
                Time.SYSTEM,
                ignored -> {
                }
        ) {
            @Override
            protected Producer<byte[], byte[]> createProducer() {
                return null;
            }

            @Override
            protected Consumer<byte[], byte[]> createConsumer() {
                return consumer;
            }

            @Override
            protected boolean readPartition(TopicPartition topicPartition) {
                return topicPartition.partition() == 0;
            }
        };
    }

    OffsetSyncStore() {
        this.admin = null;
        this.backingStore = null;
    }

    /**
     * Start the OffsetSyncStore, blocking until all previous Offset Syncs have been read from backing storage.
     */
    public void start() {
        backingStore.start();
        readToEnd = true;
    }

    OptionalLong translateDownstream(String group, TopicPartition sourceTopicPartition, long upstreamOffset) {
        if (!readToEnd) {
            // If we have not read to the end of the syncs topic at least once, decline to translate any offsets.
            // This prevents emitting stale offsets while initially reading the offset syncs topic.
            log.debug("translateDownstream({},{},{}): Skipped (initial offset syncs read still in progress)",
                    group, sourceTopicPartition, upstreamOffset);
            return OptionalLong.empty();
        }
        Optional<OffsetSync> offsetSync = latestOffsetSync(sourceTopicPartition, upstreamOffset);
        if (offsetSync.isPresent()) {
            if (offsetSync.get().upstreamOffset() > upstreamOffset) {
                // Offset is too far in the past to translate accurately
                log.debug("translateDownstream({},{},{}): Skipped ({} is ahead of upstream consumer group {})",
                        group, sourceTopicPartition, upstreamOffset,
                        offsetSync.get(), upstreamOffset);
                return OptionalLong.of(-1L);
            }
            // If the consumer group is ahead of the offset sync, we can translate the upstream offset only 1
            // downstream offset past the offset sync itself. This is because we know that future records must appear
            // ahead of the offset sync, but we cannot estimate how many offsets from the upstream topic
            // will be written vs dropped. If we overestimate, then we may skip the correct offset and have data loss.
            // This also handles consumer groups at the end of a topic whose offsets point past the last valid record.
            // This may cause re-reading of records depending on the age of the offset sync.
            // s=offset sync pair, ?=record may or may not be replicated, g=consumer group offset, r=re-read record
            // source |-s?????r???g-|
            //          |  ______/
            //          | /
            //          vv
            // target |-sg----r-----|
            long upstreamStep = upstreamOffset == offsetSync.get().upstreamOffset() ? 0 : 1;
            log.debug("translateDownstream({},{},{}): Translated {} (relative to {})",
                    group, sourceTopicPartition, upstreamOffset,
                    offsetSync.get().downstreamOffset() + upstreamStep,
                    offsetSync.get()
            );
            return OptionalLong.of(offsetSync.get().downstreamOffset() + upstreamStep);
        } else {
            log.debug("translateDownstream({},{},{}): Skipped (offset sync not found)",
                    group, sourceTopicPartition, upstreamOffset);
            return OptionalLong.empty();
        }
    }

    @Override
    public void close() {
        Utils.closeQuietly(backingStore != null ? backingStore::stop : null, "backing store for offset syncs");
        Utils.closeQuietly(admin, "admin client for offset syncs");
    }

    protected void handleRecord(ConsumerRecord<byte[], byte[]> record) {
        OffsetSync offsetSync = OffsetSync.deserializeRecord(record);
        TopicPartition sourceTopicPartition = offsetSync.topicPartition();
        offsetSyncs.compute(sourceTopicPartition, (ignored, syncs) ->
                syncs == null ? createInitialSyncs(offsetSync) : updateExistingSyncs(syncs, offsetSync)
        );
    }

    private OffsetSync[] updateExistingSyncs(OffsetSync[] syncs, OffsetSync offsetSync) {
        // Make a copy of the array before mutating it, so that readers do not see inconsistent data
        // TODO: consider batching updates so that this copy can be performed less often for high-volume sync topics.
        OffsetSync[] mutableSyncs = Arrays.copyOf(syncs, SYNCS_PER_PARTITION);
        updateSyncArray(mutableSyncs, offsetSync);
        if (log.isTraceEnabled()) {
            log.trace("New sync {} applied, new state is {}", offsetSync, offsetArrayToString(mutableSyncs));
        }
        return mutableSyncs;
    }

    private String offsetArrayToString(OffsetSync[] syncs) {
        StringBuilder stateString = new StringBuilder();
        stateString.append("[");
        for (int i = 0; i < SYNCS_PER_PARTITION; i++) {
            if (i == 0 || i == SYNCS_PER_PARTITION - 1 || syncs[i] != syncs[i - 1]) {
                if (i != 0) {
                    stateString.append(",");
                }
                // Print only if the sync is interesting, a series of repeated syncs will appear as ,,,,,
                stateString.append(syncs[i].upstreamOffset());
                stateString.append(":");
                stateString.append(syncs[i].downstreamOffset());
            }
        }
        stateString.append("]");
        return stateString.toString();
    }

    private OffsetSync[] createInitialSyncs(OffsetSync firstSync) {
        OffsetSync[] syncs = new OffsetSync[SYNCS_PER_PARTITION];
        clearSyncArray(syncs, firstSync);
        return syncs;
    }

    private void clearSyncArray(OffsetSync[] syncs, OffsetSync offsetSync) {
        for (int i = 0; i < SYNCS_PER_PARTITION; i++) {
            syncs[i] = offsetSync;
        }
    }

    private void updateSyncArray(OffsetSync[] syncs, OffsetSync offsetSync) {
        long upstreamOffset = offsetSync.upstreamOffset();
        // Old offsets are invalid, so overwrite them all.
        if (!readToEnd || syncs[0].upstreamOffset() > upstreamOffset) {
            clearSyncArray(syncs, offsetSync);
            return;
        }
        syncs[0] = offsetSync;
        for (int i = 1; i < SYNCS_PER_PARTITION; i++) {
            OffsetSync oldValue = syncs[i];
            long mask = Long.MAX_VALUE << i;
            // If the old value is too stale: at least one of the 64-i high bits of the offset value have changed
            // This produces buckets quantized at boundaries of powers of 2
            // Syncs:     a                  b             c       d   e
            // Bucket 0  |a|                |b|           |c|     |d| |e|                   (size    1)
            // Bucket 1 | a|               | b|           |c |    |d | e|                   (size    2)
            // Bucket 2 | a  |           |   b|         |  c |  |  d |   |                  (size    4)
            // Bucket 3 | a      |       |   b   |      |  c    |  d     |                  (size    8)
            // Bucket 4 | a              |   b          |  c             |                  (size   16)
            // Bucket 5 | a                             |  c                             |  (size   32)
            // Bucket 6 | a                                                              |  (size   64)
            // ...        a                                                                 ...
            // Bucket63   a                                                                 (size 2^63)
            // State after a: [a,,,,,,, ... ,a] (all buckets written)
            // State after b: [b,,,,,a,, ... ,a] (buckets 0-4 written)
            // State after c: [c,,,,,,a, ... ,a] (buckets 0-5 written, b expired completely)
            // State after d: [d,,,,c,,a, ... ,a] (buckets 0-3 written)
            // State after e: [e,,d,,c,,a, ... ,a] (buckets 0-1 written)
            if (((oldValue.upstreamOffset() ^ upstreamOffset) & mask) != 0) {
                syncs[i] = offsetSync;
            } else {
                break;
            }
        }
    }

    private Optional<OffsetSync> latestOffsetSync(TopicPartition topicPartition, long upstreamOffset) {
        return Optional.ofNullable(offsetSyncs.get(topicPartition))
                .map(syncs -> lookupLatestSync(syncs, upstreamOffset));
    }


    private OffsetSync lookupLatestSync(OffsetSync[] syncs, long upstreamOffset) {
        // linear search the syncs, effectively a binary search over the topic offsets
        // Search from latest to earliest to find the sync that gives the best accuracy
        for (int i = 0; i < SYNCS_PER_PARTITION; i++) {
            OffsetSync offsetSync = syncs[i];
            if (offsetSync.upstreamOffset() <= upstreamOffset) {
                return offsetSync;
            }
        }
        return syncs[SYNCS_PER_PARTITION - 1];
    }

    // For testing
    OffsetSync syncFor(TopicPartition topicPartition, int syncIdx) {
        OffsetSync[] syncs = offsetSyncs.get(topicPartition);
        if (syncs == null)
            throw new IllegalArgumentException("No syncs present for " + topicPartition);
        if (syncIdx >= syncs.length)
            throw new IllegalArgumentException(
                    "Requested sync " + (syncIdx + 1) + " for " + topicPartition
                            + " but there are only " + syncs.length + " syncs available for that topic partition"
            );
        return syncs[syncIdx];
    }
}
