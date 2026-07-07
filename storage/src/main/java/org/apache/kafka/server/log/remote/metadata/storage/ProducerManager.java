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
package org.apache.kafka.server.log.remote.metadata.storage;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataSerde;
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogSegmentMetadataKeySerde;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataKey;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * This class is responsible for publishing messages into the remote log metadata topic partitions.
 *
 * Caller of this class should take care of not sending messages once the closing of this instance is initiated.
 */
public class ProducerManager implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(ProducerManager.class);

    private final RemoteLogSegmentMetadataKeySerde keySerde = new RemoteLogSegmentMetadataKeySerde();
    private final RemoteLogMetadataSerde serde = new RemoteLogMetadataSerde();
    private final Producer<byte[], byte[]> producer;
    private final RemoteLogMetadataTopicPartitioner topicPartitioner;
    private final TopicBasedRemoteLogMetadataManagerConfig rlmmConfig;
    private final Callback tombstoneRecordsCallback = (metadata, exception) -> {
        if (exception != null) {
            log.error("Failed to publish tombstone records for: {}", metadata, exception);
        }
    };

    public ProducerManager(TopicBasedRemoteLogMetadataManagerConfig rlmmConfig,
                           RemoteLogMetadataTopicPartitioner rlmmTopicPartitioner) {
        this(rlmmConfig, rlmmTopicPartitioner, new KafkaProducer<>(rlmmConfig.producerProperties()));
    }

    public ProducerManager(TopicBasedRemoteLogMetadataManagerConfig rlmmConfig,
                           RemoteLogMetadataTopicPartitioner rlmmTopicPartitioner,
                           Producer<byte[], byte[]> producer) {
        this.rlmmConfig = rlmmConfig;
        this.topicPartitioner = rlmmTopicPartitioner;
        this.producer = producer;
    }

    /**
     * Returns {@link CompletableFuture} which will complete only after publishing of the given {@code remoteLogMetadata}
     * is considered complete.
     *
     * @param remoteLogMetadata RemoteLogMetadata to be published
     * @return a future with acknowledgement.
     */
    CompletableFuture<RecordMetadata> publishMessage(RemoteLogMetadata remoteLogMetadata) {
        CompletableFuture<RecordMetadata> future = new CompletableFuture<>();

        TopicIdPartition topicIdPartition = remoteLogMetadata.topicIdPartition();
        int metadataPartitionNum = topicPartitioner.metadataPartition(topicIdPartition);
        log.debug("Publishing metadata message of partition:[{}] into metadata topic partition:[{}] with payload: [{}]",
                  topicIdPartition, metadataPartitionNum, remoteLogMetadata);
        if (metadataPartitionNum >= rlmmConfig.metadataTopicPartitionsCount()) {
            // This should never occur as long as metadata partitions always remain the same.
            throw new KafkaException("Chosen partition no " + metadataPartitionNum +
                                             " must be less than the partition count: " + rlmmConfig.metadataTopicPartitionsCount());
        }

        try {
            Callback callback = (metadata, exception) -> {
                if (exception != null) {
                    future.completeExceptionally(exception);
                } else {
                    future.complete(metadata);
                }
            };
            String topic = rlmmConfig.remoteLogMetadataTopicName();
            byte[] serializedKey = keySerde.serializer().serialize(topic, remoteLogMetadata.metadataKey());
            byte[] serializedValue = serde.serialize(remoteLogMetadata);
            producer.send(new ProducerRecord<>(topic, metadataPartitionNum, serializedKey, serializedValue), callback);
        } catch (Exception ex) {
            future.completeExceptionally(ex);
        }

        return future;
    }

    /**
     * Publishes tombstone records to mark the deletion of remote log segment metadata when the state of the provided
     * {@link RemoteLogMetadata} instance is {@link RemoteLogSegmentState#DELETE_SEGMENT_FINISHED}.
     *
     * @param remoteLogMetadata The {@link RemoteLogMetadata} instance containing metadata information. If the metadata is an instance
     *                          of {@link RemoteLogSegmentMetadataUpdate} and its state is {@link RemoteLogSegmentState#DELETE_SEGMENT_FINISHED},
     *                          tombstone records are published to indicate its deletion.
     */
    public void maybePublishTombstoneRecords(RemoteLogMetadata remoteLogMetadata) {
        // TODO: Gate sending tombstone records behind a feature flag.
        //       Send the tombstone records only when the consumerTask can handle the null values.
        if (remoteLogMetadata instanceof RemoteLogSegmentMetadataUpdate metadataUpdate) {
            if (metadataUpdate.state() == RemoteLogSegmentState.DELETE_SEGMENT_FINISHED) {
                RemoteLogSegmentId remoteLogSegmentId = metadataUpdate.remoteLogSegmentId();
                int metadataPartition = topicPartitioner.metadataPartition(remoteLogSegmentId.topicIdPartition());
                String topic = rlmmConfig.remoteLogMetadataTopicName();
                try {
                    // Send the tombstone records for all the RemoteLogSegment state to cleanup the expired segment metadata.
                    for (RemoteLogSegmentState state : RemoteLogSegmentState.values()) {
                        RemoteLogSegmentMetadataKey metadataKey = RemoteLogSegmentMetadataKey.of(remoteLogSegmentId, state);
                        byte[] serializedKey = keySerde.serializer().serialize(topic, metadataKey);
                        producer.send(new ProducerRecord<>(topic, metadataPartition, serializedKey, null), tombstoneRecordsCallback);
                    }
                } catch (Exception ex) {
                    log.error("Failed to publish tombstone records for: {}", metadataUpdate, ex);
                }
            }
        }
    }

    public void close() {
        try {
            producer.close(Duration.ofSeconds(30));
        } catch (Exception e) {
            log.error("Error encountered while closing the producer", e);
        }
    }
}
