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
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.apache.kafka.connect.util.TopicAdmin;

import java.util.Map;
import java.util.HashMap;
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Used internally by MirrorMaker. Stores offset syncs and performs offset translation. */
class OffsetSyncStore implements AutoCloseable {
    private final KafkaBasedLog<byte[], byte[]> backingStore;
    private final Map<TopicPartition, OffsetSync> offsetSyncs = new HashMap<>();
    private final TopicAdmin admin;

    OffsetSyncStore(MirrorCheckpointConfig config) {
        String topic = config.offsetSyncsTopic();
        Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(
                config.offsetSyncsTopicConsumerConfig(),
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer());
        this.admin = new TopicAdmin(
                config.offsetSyncsTopicAdminConfig().get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG),
                config.forwardingAdmin(config.offsetSyncsTopicAdminConfig()));
        KafkaBasedLog<byte[], byte[]> store = null;
        try {
            store = KafkaBasedLog.withExistingClients(
                    topic,
                    consumer,
                    null,
                    new TopicAdmin(
                            config.offsetSyncsTopicAdminConfig().get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG),
                            config.forwardingAdmin(config.offsetSyncsTopicAdminConfig())),
                    (error, record) -> this.handleRecord(record),
                    Time.SYSTEM,
                    ignored -> {
                    });
            store.start();
        } catch (Throwable t) {
            Utils.closeQuietly(store != null ? store::stop : null, "backing store");
            Utils.closeQuietly(new TopicAdmin(
                    config.offsetSyncsTopicAdminConfig().get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG),
                    config.forwardingAdmin(config.offsetSyncsTopicAdminConfig())), "admin client");
            throw t;
        }
        this.backingStore = store;
    }

    OffsetSyncStore() {
        this.admin = null;
        this.backingStore = null;
    }

    public void readToEnd(Runnable callback) {
        backingStore.readToEnd((error, result) -> callback.run());
    }

    OptionalLong translateDownstream(TopicPartition sourceTopicPartition, long upstreamOffset) {
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

    // poll and handle records
    synchronized void update(Duration pollTimeout) throws TimeoutException {
        try {
            backingStore.readToEnd().get(pollTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (WakeupException | InterruptedException | ExecutionException e) {
            // swallow
        }
    }

    public synchronized void close() {
        Utils.closeQuietly(backingStore::stop, "offset sync store kafka based log");
        Utils.closeQuietly(admin, "offset sync store admin client");
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
