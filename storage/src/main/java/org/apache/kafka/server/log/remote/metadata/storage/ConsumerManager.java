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

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;

/**
 * This class manages the consumer thread viz {@link ConsumerTask} that polls messages from the assigned metadata topic partitions.
 * It also provides a way to wait until the given record is received by the consumer before it is timed out with an interval of
 * {@link TopicBasedRemoteLogMetadataManagerConfig#consumeWaitMs()}.
 */
public class ConsumerManager implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ConsumerManager.class);
    private static final long CONSUME_RECHECK_INTERVAL_MS = 50L;

    private final TopicBasedRemoteLogMetadataManagerConfig rlmmConfig;
    private final Time time;
    private final ConsumerTask consumerTask;
    private final Thread consumerTaskThread;

    public ConsumerManager(TopicBasedRemoteLogMetadataManagerConfig rlmmConfig,
                           RemotePartitionMetadataEventHandler remotePartitionMetadataEventHandler,
                           RemoteLogMetadataTopicPartitioner topicPartitioner,
                           Time time) {
        this.rlmmConfig = rlmmConfig;
        this.time = time;

        //Create a task to consume messages and submit the respective events to RemotePartitionMetadataEventHandler.
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(rlmmConfig.consumerProperties());
        consumerTask = new ConsumerTask(
            remotePartitionMetadataEventHandler,
            topicPartitioner,
            consumer,
            100L,
            300_000L,
            time
        );
        consumerTaskThread = KafkaThread.nonDaemon("RLMMConsumerTask", consumerTask);
    }

    public void startConsumerThread() {
        try {
            // Start a thread to continuously consume records from topic partitions.
            consumerTaskThread.start();
            log.info("RLMM Consumer task thread is started");
        } catch (Exception e) {
            throw new KafkaException("Error encountered while initializing and scheduling ConsumerTask thread", e);
        }
    }

    /**
     * Waits if necessary for the consumption to reach the {@code offset} of the given record
     * at a certain {@code partition} of the metadata topic.
     *
     * @param recordMetadata record metadata to be checked for consumption.
     * @throws TimeoutException if this method execution did not complete with in the wait time configured with
     *                          property {@code TopicBasedRemoteLogMetadataManagerConfig#REMOTE_LOG_METADATA_CONSUME_WAIT_MS_PROP}.
     */
    public void waitTillConsumptionCatchesUp(RecordMetadata recordMetadata) throws TimeoutException {
        waitTillConsumptionCatchesUp(recordMetadata, rlmmConfig.consumeWaitMs());
    }

    /**
     * Waits if necessary for the consumption to reach the partition/offset of the given {@code RecordMetadata}
     *
     * @param recordMetadata record metadata to be checked for consumption.
     * @param timeoutMs      wait timeout in milliseconds
     * @throws TimeoutException if this method execution did not complete with in the given {@code timeoutMs}.
     */
    public void waitTillConsumptionCatchesUp(RecordMetadata recordMetadata,
                                             long timeoutMs) throws TimeoutException {
        int partition = recordMetadata.partition();
        // If the current assignment does not have the subscription for this partition then return immediately.
        if (!consumerTask.isMetadataPartitionAssigned(partition)) {
            throw new KafkaException("This consumer is not assigned to the target partition " + partition +
                    ". Currently assigned partitions: " + consumerTask.metadataPartitionsAssigned());
        }
        long offset = recordMetadata.offset();
        long startTimeMs = time.milliseconds();
        long consumeCheckIntervalMs = Math.min(CONSUME_RECHECK_INTERVAL_MS, timeoutMs);
        log.info("Wait until the consumer is caught up with the target partition {} up-to offset {}", partition, offset);
        while (true) {
            long readOffset = consumerTask.readOffsetForMetadataPartition(partition).orElse(-1L);
            if (readOffset >= offset) {
                return;
            }
            log.debug("Expected offset for partition {} is {}, but the read offset is {}. " +
                    "Sleeping for {} ms to retry again", partition, offset, readOffset, consumeCheckIntervalMs);
            if (time.milliseconds() - startTimeMs > timeoutMs) {
                log.warn("Expected offset for partition {} is {}, but the read offset is {}",
                        partition, offset, readOffset);
                throw new TimeoutException("Timed out in catching up with the expected offset by consumer.");
            }
            time.sleep(consumeCheckIntervalMs);
        }
    }

    @Override
    public void close() throws IOException {
        // Consumer task will close the task, and it internally closes all the resources including the consumer.
        Utils.closeQuietly(consumerTask, "ConsumerTask");

        // Wait until the consumer thread finishes.
        try {
            consumerTaskThread.join();
        } catch (Exception e) {
            log.error("Encountered error while waiting for consumerTaskThread to finish.", e);
        }
    }

    public void addAssignmentsForPartitions(Set<TopicIdPartition> partitions) {
        consumerTask.addAssignmentsForPartitions(partitions);
    }

    public void removeAssignmentsForPartitions(Set<TopicIdPartition> partitions) {
        consumerTask.removeAssignmentsForPartitions(partitions);
    }

    public Optional<Long> readOffsetForPartition(int metadataPartition) {
        return consumerTask.readOffsetForMetadataPartition(metadataPartition);
    }
}
