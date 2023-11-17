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
 *     <li>Invariant A: syncs[0] is the latest offset sync from the syncs topic</li>
 *     <li>Invariant B: For each i,j, i < j, syncs[i] != syncs[j]: syncs[i].upstream <= syncs[j].upstream + 2^j - 2^i</li>
 *     <li>Invariant C: For each i,j, i < j, syncs[i] != syncs[j]: syncs[i].upstream >= syncs[j].upstream + 2^(i-2)</li>
 *     <li>Invariant D: syncs[63] is the earliest offset sync from the syncs topic usable for translation</li>
 * </ul>
 * <p>The above invariants ensure that the store is kept updated upon receipt of each sync, and that distinct
 * offset syncs are separated by approximately exponential space. They can be checked locally (by comparing all adjacent
 * indexes) but hold globally (for all pairs of any distance). This allows updates to the store in linear time.
 * <p>Offset translation uses the syncs[i] which most closely precedes the upstream consumer group's current offset.
 * For a fixed in-memory state, translation of variable upstream offsets will be monotonic.
 * For variable in-memory state, translation of a fixed upstream offset will not be monotonic.
 * <p>Translation will be unavailable for all topic-partitions before an initial read-to-end of the offset syncs topic
 * is complete. Translation will be unavailable after that if no syncs are present for a topic-partition, if replication
 * started after the position of the consumer group, or if relevant offset syncs for the topic were potentially used as
 * for translation in an earlier generation of the sync store.
 */
class OffsetSyncStore implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(OffsetSyncStore.class);
    // Store one offset sync for each bit of the topic offset.
    // Visible for testing
    static final int SYNCS_PER_PARTITION = Long.SIZE;
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
                    config.offsetSyncsTopicAdminConfig(),
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
        updateSyncArray(mutableSyncs, syncs, offsetSync);
        if (log.isTraceEnabled()) {
            log.trace("New sync {} applied, new state is {}", offsetSync, offsetArrayToString(mutableSyncs));
        }
        return mutableSyncs;
    }

    private String offsetArrayToString(OffsetSync[] syncs) {
        StringBuilder stateString = new StringBuilder();
        stateString.append("[");
        for (int i = 0; i < SYNCS_PER_PARTITION; i++) {
            if (i == 0 || syncs[i] != syncs[i - 1]) {
                if (i != 0) {
                    stateString.append(",");
                }
                // Print only if the sync is interesting, a series of repeated syncs will be elided
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
        // If every element of the store is the same, then it satisfies invariants B and C trivially.
        for (int i = 0; i < SYNCS_PER_PARTITION; i++) {
            syncs[i] = offsetSync;
        }
    }

    private void updateSyncArray(OffsetSync[] syncs, OffsetSync[] original, OffsetSync offsetSync) {
        long upstreamOffset = offsetSync.upstreamOffset();
        // While reading to the end of the topic, ensure that our earliest sync is later than
        // any earlier sync that could have been used for translation, to preserve monotonicity
        // If the upstream offset rewinds, all previous offsets are invalid, so overwrite them all.
        if (!readToEnd || syncs[0].upstreamOffset() > upstreamOffset) {
            clearSyncArray(syncs, offsetSync);
            return;
        }
        OffsetSync replacement = offsetSync;
        // Index into the original syncs array to the replacement we'll consider next.
        int replacementIndex = 0;
        // Invariant A is always violated once a new sync appears.
        // Repair Invariant A: the latest sync must always be updated
        syncs[0] = replacement;
        for (int current = 1; current < SYNCS_PER_PARTITION; current++) {
            int previous = current - 1;

            // Try to choose a value from the old array as the replacement
            // This allows us to keep more distinct values stored overall, improving translation.
            boolean skipOldValue;
            do {
                OffsetSync oldValue = original[replacementIndex];
                // If oldValue is not recent enough, then it is not valid to use at the current index.
                // It may still be valid when used in a later index where values are allowed to be older.
                boolean isRecent = invariantB(syncs[previous], oldValue, previous, current);
                // Ensure that this value is sufficiently separated from the previous value
                // We prefer to keep more recent syncs of similar precision (i.e. the value in replacement)
                // If this value is too close to the previous value, it will never be valid in a later position.
                boolean separatedFromPrevious = invariantC(syncs[previous], oldValue, previous);
                // Ensure that this value is sufficiently separated from the next value
                // We prefer to keep existing syncs of lower precision (i.e. the value in syncs[next])
                int next = current + 1;
                boolean separatedFromNext = next >= SYNCS_PER_PARTITION || invariantC(oldValue, syncs[next], current);
                // If the next value in the old array is a duplicate of the current one, then they are equivalent
                // This value will not need to be considered again
                int nextReplacement = replacementIndex + 1;
                boolean duplicate = nextReplacement < SYNCS_PER_PARTITION && oldValue == original[nextReplacement];

                // Promoting the oldValue to the replacement only happens if it satisfies all invariants.
                boolean promoteOldValueToReplacement = isRecent && separatedFromPrevious && separatedFromNext;
                if (promoteOldValueToReplacement) {
                    replacement = oldValue;
                }
                // The index should be skipped without promoting if we know that it will not be used at a later index
                // based only on the observed part of the array so far.
                skipOldValue = duplicate || !separatedFromPrevious;
                if (promoteOldValueToReplacement || skipOldValue) {
                    replacementIndex++;
                }
                // We may need to skip past multiple indices, so keep looping until we're done skipping forward.
                // The index must not get ahead of the current index, as we only promote from low index to high index.
            } while (replacementIndex < current && skipOldValue);

            // The replacement variable always contains a value which satisfies the invariants for this index.
            // This replacement may or may not be used, since the invariants could already be satisfied,
            // and in that case, prefer to keep the existing tail of the syncs array rather than updating it.
            assert invariantB(syncs[previous], replacement, previous, current);
            assert invariantC(syncs[previous], replacement, previous);

            // Test if changes to the previous index affected the invariant for this index
            if (invariantB(syncs[previous], syncs[current], previous, current)) {
                // Invariant B holds for syncs[current]: it must also hold for all later values
                break;
            } else {
                // Invariant B violated for syncs[current]: sync is now too old and must be updated
                // Repair Invariant B: swap in replacement
                syncs[current] = replacement;

                assert invariantB(syncs[previous], syncs[current], previous, current);
                assert invariantC(syncs[previous], syncs[current], previous);
            }
        }
    }

    private boolean invariantB(OffsetSync iSync, OffsetSync jSync, int i, int j) {
        long bound = jSync.upstreamOffset() + (1L << j) - (1L << i);
        return iSync == jSync || bound < 0 || iSync.upstreamOffset() <= bound;
    }

    private boolean invariantC(OffsetSync iSync, OffsetSync jSync, int i) {
        long bound = jSync.upstreamOffset() + (1L << Math.max(i - 2, 0));
        return iSync == jSync || (bound >= 0 && iSync.upstreamOffset() >= bound);
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
