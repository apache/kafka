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
                           RemoteLogMetadataTopicPartitioner rlmmTopicPartitioner,
                           Time time) {
        this.rlmmConfig = rlmmConfig;
        this.time = time;

        //Create a task to consume messages and submit the respective events to RemotePartitionMetadataEventHandler.
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(rlmmConfig.consumerProperties());
        consumerTask = new ConsumerTask(consumer, remotePartitionMetadataEventHandler, rlmmTopicPartitioner);
        consumerTaskThread = KafkaThread.nonDaemon("RLMMConsumerTask", consumerTask);
    }

    public void startConsumerThread() {
        try {
            // Start a thread to continuously consume records from topic partitions.
            consumerTaskThread.start();
        } catch (Exception e) {
            throw new KafkaException("Error encountered while initializing and scheduling ConsumerTask thread", e);
        }
    }

    /**
     * Waits if necessary for the consumption to reach the offset of the given {@code recordMetadata}.
     *
     * @param recordMetadata record metadata to be checked for consumption.
     * @throws TimeoutException if this method execution did not complete with in the wait time configured with
     *                          property {@code TopicBasedRemoteLogMetadataManagerConfig#REMOTE_LOG_METADATA_CONSUME_WAIT_MS_PROP}.
     */
    public void waitTillConsumptionCatchesUp(RecordMetadata recordMetadata) throws TimeoutException {
        waitTillConsumptionCatchesUp(recordMetadata, rlmmConfig.consumeWaitMs());
    }

    /**
     * Waits if necessary for the consumption to reach the offset of the given {@code recordMetadata}.
     *
     * @param recordMetadata record metadata to be checked for consumption.
     * @param timeoutMs      wait timeout in milli seconds
     * @throws TimeoutException if this method execution did not complete with in the given {@code timeoutMs}.
     */
    public void waitTillConsumptionCatchesUp(RecordMetadata recordMetadata,
                                             long timeoutMs) throws TimeoutException {
        final int partition = recordMetadata.partition();
        final long consumeCheckIntervalMs = Math.min(CONSUME_RECHECK_INTERVAL_MS, timeoutMs);

        // If the current assignment does not have the subscription for this partition then return immediately.
        if (!consumerTask.isPartitionAssigned(partition)) {
            throw new KafkaException("This consumer is not subscribed to the target partition " + partition + " on which message is produced.");
        }

        final long offset = recordMetadata.offset();
        long startTimeMs = time.milliseconds();
        while (true) {
            long receivedOffset = consumerTask.receivedOffsetForPartition(partition).orElse(-1L);
            if (receivedOffset >= offset) {
                return;
            }

            log.debug("Committed offset [{}] for partition [{}], but the target offset: [{}],  Sleeping for [{}] to retry again",
                      offset, partition, receivedOffset, consumeCheckIntervalMs);

            if (time.milliseconds() - startTimeMs > timeoutMs) {
                log.warn("Committed offset for partition:[{}] is : [{}], but the target offset: [{}] ",
                         partition, receivedOffset, offset);
                throw new TimeoutException("Timed out in catching up with the expected offset by consumer.");
            }

            time.sleep(consumeCheckIntervalMs);
        }
    }

    @Override
    public void close() throws IOException {
        // Consumer task will close the task and it internally closes all the resources including the consumer.
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

    public Optional<Long> receivedOffsetForPartition(int metadataPartition) {
        return consumerTask.receivedOffsetForPartition(metadataPartition);
    }
}
