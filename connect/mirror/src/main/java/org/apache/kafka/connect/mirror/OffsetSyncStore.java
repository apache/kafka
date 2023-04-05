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

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;

/** Used internally by MirrorMaker. Stores offset syncs and performs offset translation. */
class OffsetSyncStore implements AutoCloseable {
    private final KafkaBasedLog<byte[], byte[]> backingStore;
    private final Map<TopicPartition, OffsetSync> offsetSyncs = new ConcurrentHashMap<>();
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

    OptionalLong translateDownstream(TopicPartition sourceTopicPartition, long upstreamOffset) {
        if (!readToEnd) {
            // If we have not read to the end of the syncs topic at least once, decline to translate any offsets.
            // This prevents emitting stale offsets while initially reading the offset syncs topic.
            return OptionalLong.empty();
        }
        Optional<OffsetSync> offsetSync = latestOffsetSync(sourceTopicPartition);
        if (offsetSync.isPresent()) {
            if (offsetSync.get().upstreamOffset() > upstreamOffset) {
                // Offset is too far in the past to translate accurately
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
            return OptionalLong.of(offsetSync.get().downstreamOffset() + upstreamStep);
        } else {
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
        offsetSyncs.put(sourceTopicPartition, offsetSync);
    }

    private Optional<OffsetSync> latestOffsetSync(TopicPartition topicPartition) {
        return Optional.ofNullable(offsetSyncs.get(topicPartition));
    }
}
