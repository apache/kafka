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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class manages the consumer thread viz {@link ConsumerTask} that polls messages from the assigned metadata topic partitions.
 * It also provides a way to wait until the given record is received by the consumer before it is timed out with an interval of
 * {@link TopicBasedRemoteLogMetadataManagerConfig#consumeWaitMs()}.
 */
public class ConsumerManager implements Closeable {

    public static final String COMMITTED_OFFSETS_FILE_NAME = "_rlmm_committed_offsets";

    private static final Logger log = LoggerFactory.getLogger(ConsumerManager.class);
    private static final long CONSUME_RECHECK_INTERVAL_MS = 50L;

    private final AdminClient adminClient;
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
        adminClient = AdminClient.create(rlmmConfig.commonProperties());
        Path committedOffsetsPath = new File(rlmmConfig.logDir(), COMMITTED_OFFSETS_FILE_NAME).toPath();
        consumerTask = new ConsumerTask(
                consumer,
                rlmmConfig.remoteLogMetadataTopicName(),
                remotePartitionMetadataEventHandler,
                topicPartitioner,
                committedOffsetsPath,
                time,
                60_000L
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
     * @param partition partition of metadata topic to be checked for consumption
     * @param offset record offset of metadata topic {@code partition} to be checked for consumption
     * @throws TimeoutException if this method execution did not complete with in the wait time configured with
     *                          property {@code TopicBasedRemoteLogMetadataManagerConfig#REMOTE_LOG_METADATA_CONSUME_WAIT_MS_PROP}.
     */
    public void waitTillConsumptionCatchesUp(int partition, long offset) throws TimeoutException {
        waitTillConsumptionCatchesUp(partition, offset, rlmmConfig.consumeWaitMs());
    }

    /**
     * Waits if necessary for the consumption to reach the {@code offset} of the given record
     * at a certain {@code partition} of the metadata topic.
     *
     * @param partition partition of metadata topic to be checked for consumption
     * @param offset record offset of metadata topic {@code partition} to be checked for consumption
     * @param timeoutMs wait timeout in milliseconds
     * @throws TimeoutException if this method execution did not complete with in the given {@code timeoutMs}.
     */
    public void waitTillConsumptionCatchesUp(int partition, long offset, long timeoutMs) throws TimeoutException {
        final long consumeCheckIntervalMs = Math.min(CONSUME_RECHECK_INTERVAL_MS, timeoutMs);

        // If the current assignment does not have the subscription for this partition then return immediately.
        if (!consumerTask.isPartitionAssigned(partition)) {
            throw new KafkaException("This consumer is not assigned to the target partition " + partition + ". " +
                    "Partitions currently assigned: " + consumerTask.metadataPartitionsAssigned());
        }

        long startTimeMs = time.milliseconds();
        while (true) {
            log.debug("Checking if partition [{}] is up to date with offset [{}]", partition, offset);
            long receivedOffset = consumerTask.receivedOffsetForPartition(partition).orElse(-1L);
            if (receivedOffset >= offset) {
                return;
            }

            log.debug("Expected offset [{}] for partition [{}], but the committed offset: [{}],  Sleeping for [{}] to retry again",
                    offset, partition, receivedOffset, consumeCheckIntervalMs);

            if (time.milliseconds() - startTimeMs > timeoutMs) {
                log.warn("Expected offset for partition:[{}] is : [{}], but the committed offset: [{}] ",
                        partition, receivedOffset, offset);
                throw new TimeoutException("Timed out in catching up with the expected offset by consumer.");
            }

            time.sleep(consumeCheckIntervalMs);
        }
    }

    @Override
    public void close() throws IOException {
        // Consumer task will close the task, and it internally closes all the resources including the consumer.
        Utils.closeQuietly(consumerTask, "ConsumerTask");
        Utils.closeQuietly(adminClient, "AdminClient");

        // Wait until the consumer thread finishes.
        try {
            consumerTaskThread.join();
        } catch (Exception e) {
            log.error("Encountered error while waiting for consumerTaskThread to finish.", e);
        }
    }

    public void addAssignmentsForPartitions(Set<TopicIdPartition> partitions) {
        consumerTask.addAssignmentsForPartitions(partitions);
        waitTillMetaPartitionConsumptionCatchesUp();
    }

    /**
     * Whenever there is a change on the remote log metadata topic partitions assigned to the {@link TopicBasedRemoteLogMetadataManager},
     * wait for consumer (therefore the cache as well) to be in-sync before continuing
     */
    private void waitTillMetaPartitionConsumptionCatchesUp() {
        try {
            final Set<TopicPartition> topicPartitions = consumerTask.metadataPartitionsAssigned();
            if (!topicPartitions.isEmpty()) {
                final Map<TopicPartition, OffsetSpec> listOffsetsRequest = topicPartitions.stream()
                        .collect(Collectors.toMap(Function.identity(), tp -> OffsetSpec.latest()));
                final Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets = adminClient.listOffsets(listOffsetsRequest)
                        .all()
                        .get(rlmmConfig.consumeWaitMs(), TimeUnit.MILLISECONDS); // piggybacking on existing timeout

                for (TopicPartition tp : topicPartitions) {
                    waitTillConsumptionCatchesUp(
                            tp.partition(),
                            latestOffsets.get(tp).offset() - 1 // as latest offset is the next latest written record
                    );
                }
            }
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            log.error("Error encountered while consuming from remote log metadata partitions: {}",
                    consumerTask.metadataPartitionsAssigned(), e);
            throw new KafkaException("Error encountered while consuming from remote log metadata partitions", e);
        }
    }

    public void removeAssignmentsForPartitions(Set<TopicIdPartition> partitions) {
        consumerTask.removeAssignmentsForPartitions(partitions);
    }

    public Optional<Long> receivedOffsetForPartition(int metadataPartition) {
        return consumerTask.receivedOffsetForPartition(metadataPartition);
    }
}
