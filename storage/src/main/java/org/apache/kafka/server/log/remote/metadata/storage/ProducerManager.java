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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataSerde;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadata;

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

    private final RemoteLogMetadataSerde serde = new RemoteLogMetadataSerde();
    private final KafkaProducer<String, byte[]> producer;
    private final RemoteLogMetadataTopicPartitioner topicPartitioner;
    private final TopicBasedRemoteLogMetadataManagerConfig rlmmConfig;

    public ProducerManager(TopicBasedRemoteLogMetadataManagerConfig rlmmConfig,
                           RemoteLogMetadataTopicPartitioner rlmmTopicPartitioner) {
        this.rlmmConfig = rlmmConfig;
        this.producer = new KafkaProducer<>(rlmmConfig.producerProperties());
        topicPartitioner = rlmmTopicPartitioner;
    }

    /**
     * Publishes a tombstone (null value) for the given metadata key.
     * This is used to delete records from the compacted metadata topic.
     *
     * @param topicIdPartition the topic partition
     * @param metadataKey the key to tombstone
     * @return a CompletableFuture containing the record metadata
     */
    public CompletableFuture<RecordMetadata> publishTombstone(
            TopicIdPartition topicIdPartition,
            String metadataKey) {

        log.debug("Publishing tombstone for key: {} to topic: {} partition: {}",
                metadataKey, topicIdPartition.topic(), topicIdPartition.partition());

        CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
        int metadataPartitionNum = topicPartitioner.metadataPartition(topicIdPartition);

        // Send tombstone (null value) to the topic
        producer.send(
                new ProducerRecord<>(
                        rlmmConfig.remoteLogMetadataTopicName(),
                        metadataPartitionNum,
                        metadataKey,
                        null  // ← Tombstone!
                ),
                (metadata, exception) -> {
                    if (exception != null) {
                        future.completeExceptionally(exception);
                    } else {
                        future.complete(metadata);
                    }
                }
        );

        return future;
    }

    /**
     * Returns {@link CompletableFuture} which will complete only after publishing of the given {@code remoteLogMetadata}
     * is considered complete.
     *
     * @param remoteLogMetadata RemoteLogMetadata to be published
     * @return
     */
    public CompletableFuture<RecordMetadata> publishMessage(RemoteLogMetadata remoteLogMetadata) {
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
            producer.send(new ProducerRecord<>(rlmmConfig.remoteLogMetadataTopicName(), metadataPartitionNum, remoteLogMetadata.metadataKey(),
                                               serde.serialize(remoteLogMetadata)), callback);
        } catch (Exception ex) {
            future.completeExceptionally(ex);
        }

        return future;
    }

    public void close() {
        try {
            producer.close(Duration.ofSeconds(30));
        } catch (Exception e) {
            log.error("Error encountered while closing the producer", e);
        }
    }
}
