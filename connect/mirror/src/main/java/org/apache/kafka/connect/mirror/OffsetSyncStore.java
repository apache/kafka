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

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalLong;

/** Used internally by MirrorMaker. Stores offset syncs and performs offset translation. */
class OffsetSyncStore implements AutoCloseable {
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final Map<TopicPartition, OffsetSync> offsetSyncs = new HashMap<>();
    private final TopicPartition offsetSyncTopicPartition;

    OffsetSyncStore(MirrorCheckpointConfig config) {
        consumer = new KafkaConsumer<>(config.offsetSyncsTopicConsumerConfig(),
            new ByteArrayDeserializer(), new ByteArrayDeserializer());
        offsetSyncTopicPartition = new TopicPartition(config.offsetSyncsTopic(), 0);
        consumer.assign(Collections.singleton(offsetSyncTopicPartition));
    }

    // for testing
    OffsetSyncStore(KafkaConsumer<byte[], byte[]> consumer, TopicPartition offsetSyncTopicPartition) {
        this.consumer = consumer;
        this.offsetSyncTopicPartition = offsetSyncTopicPartition;
    }

    OptionalLong translateDownstream(TopicPartition sourceTopicPartition, long upstreamOffset) {
        Optional<OffsetSync> offsetSync = latestOffsetSync(sourceTopicPartition);
        if (offsetSync.isPresent()) {
            if (offsetSync.get().upstreamOffset() > upstreamOffset) {
                // Offset is too far in the past to translate accurately
                return OptionalLong.of(-1L);
            }
            long upstreamStep = upstreamOffset - offsetSync.get().upstreamOffset();
            return OptionalLong.of(offsetSync.get().downstreamOffset() + upstreamStep);
        } else {
            return OptionalLong.empty();
        }
    }

    // poll and handle records
    synchronized void update(Duration pollTimeout) {
        try {
            consumer.poll(pollTimeout).forEach(this::handleRecord);
        } catch (WakeupException e) {
            // swallow
        }
    }

    public synchronized void close() {
        consumer.wakeup();
        Utils.closeQuietly(consumer, "offset sync store consumer");
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
