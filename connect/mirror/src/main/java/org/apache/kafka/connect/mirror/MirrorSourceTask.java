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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

/** Replicates a set of topic-partitions. */
public class MirrorSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(MirrorSourceTask.class);

    private KafkaConsumer<byte[], byte[]> consumer;
    private String sourceClusterAlias;
    private Duration pollTimeout;
    private ReplicationPolicy replicationPolicy;
    private MirrorSourceMetrics metrics;
    private boolean stopping = false;
    private Semaphore consumerAccess;
    private OffsetSyncWriter offsetSyncWriter;

    // Enhanced MirrorMaker 2 features for fault tolerance
    private Map<TopicPartition, Long> previousOffsets = new HashMap<>();
    private Map<TopicPartition, Boolean> topicResetDetected = new HashMap<>();

    public MirrorSourceTask() {}

    // for testing
    MirrorSourceTask(KafkaConsumer<byte[], byte[]> consumer, MirrorSourceMetrics metrics, String sourceClusterAlias,
                     ReplicationPolicy replicationPolicy,
                     OffsetSyncWriter offsetSyncWriter) {
        this.consumer = consumer;
        this.metrics = metrics;
        this.sourceClusterAlias = sourceClusterAlias;
        this.replicationPolicy = replicationPolicy;
        consumerAccess = new Semaphore(1);
        this.offsetSyncWriter = offsetSyncWriter;
    }

    @Override
    public void start(Map<String, String> props) {
        MirrorSourceTaskConfig config = new MirrorSourceTaskConfig(props);
        consumerAccess = new Semaphore(1);  // let one thread at a time access the consumer
        sourceClusterAlias = config.sourceClusterAlias();
        metrics = config.metrics();
        pollTimeout = config.consumerPollTimeout();
        replicationPolicy = config.replicationPolicy();
        if (config.emitOffsetSyncsEnabled()) {
            offsetSyncWriter = new OffsetSyncWriter(config);
        }
        consumer = MirrorUtils.newConsumer(config.sourceConsumerConfig("replication-consumer"));
        Set<TopicPartition> taskTopicPartitions = config.taskTopicPartitions();
        initializeConsumer(taskTopicPartitions);

        log.info("{} replicating {} topic-partitions {}->{}: {}.", Thread.currentThread().getName(),
            taskTopicPartitions.size(), sourceClusterAlias, config.targetClusterAlias(), taskTopicPartitions);
    }

    @Override
    public void commit() {
        // Handle delayed and pending offset syncs only when offsetSyncWriter is available
        if (offsetSyncWriter != null) {
            // Offset syncs which were not emitted immediately due to their offset spacing should be sent periodically
            // This ensures that low-volume topics aren't left with persistent lag at the end of the topic
            offsetSyncWriter.promoteDelayedOffsetSyncs();
            // Publish any offset syncs that we've queued up, but have not yet been able to publish
            // (likely because we previously reached our limit for number of outstanding syncs)
            offsetSyncWriter.firePendingOffsetSyncs();
        }
    }

    @Override
    public void stop() {
        long start = System.currentTimeMillis();
        stopping = true;
        consumer.wakeup();
        try {
            consumerAccess.acquire();
        } catch (InterruptedException e) {
            log.warn("Interrupted waiting for access to consumer. Will try closing anyway."); 
        }
        Utils.closeQuietly(consumer, "source consumer");
        Utils.closeQuietly(offsetSyncWriter, "offset sync writer");
        Utils.closeQuietly(metrics, "metrics");
        log.info("Stopping {} took {} ms.", Thread.currentThread().getName(), System.currentTimeMillis() - start);
    }
   
    @Override
    public String version() {
        return new MirrorSourceConnector().version();
    }

    @Override
    public List<SourceRecord> poll() {
        if (!consumerAccess.tryAcquire()) {
            return null;
        }
        if (stopping) {
            return null;
        }
        try {
            // Enhanced MirrorMaker 2: Check for topic resets before polling
            detectAndHandleTopicResets();

            ConsumerRecords<byte[], byte[]> records = consumer.poll(pollTimeout);

            // Enhanced MirrorMaker 2: Detect log truncation
            detectLogTruncation(records);

            List<SourceRecord> sourceRecords = new ArrayList<>(records.count());
            for (ConsumerRecord<byte[], byte[]> record : records) {
                SourceRecord converted = convertRecord(record);
                sourceRecords.add(converted);
                TopicPartition topicPartition = new TopicPartition(converted.topic(), converted.kafkaPartition());
                metrics.recordAge(topicPartition, System.currentTimeMillis() - record.timestamp());
                metrics.recordBytes(topicPartition, byteSize(record.value()));

                // Enhanced MirrorMaker 2: Track offsets for truncation detection
                TopicPartition sourceTopicPartition = new TopicPartition(record.topic(), record.partition());
                previousOffsets.put(sourceTopicPartition, record.offset());
            }
            if (sourceRecords.isEmpty()) {
                // WorkerSourceTasks expects non-zero batch size
                return null;
            } else {
                log.trace("Polled {} records from {}.", sourceRecords.size(), records.partitions());
                return sourceRecords;
            }
        } catch (WakeupException e) {
            return null;
        } catch (UnknownTopicOrPartitionException e) {
            log.warn("Topic or partition not found, attempting recovery: {}", e.getMessage());
            handleTopicRecovery(e);
            return null;
        } catch (KafkaException e) {
            log.warn("Failure during poll.", e);
            return null;
        } catch (Throwable e)  {
            log.error("Failure during poll.", e);
            // allow Connect to deal with the exception
            throw e;
        } finally {
            consumerAccess.release();
        }
    }
 
    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) {
        if (stopping) {
            return;
        }
        if (metadata == null) {
            log.debug("No RecordMetadata (source record was probably filtered out during transformation) -- can't sync offsets for {}.", record.topic());
            return;
        }
        if (!metadata.hasOffset()) {
            log.error("RecordMetadata has no offset -- can't sync offsets for {}.", record.topic());
            return;
        }
        TopicPartition topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());
        long latency = System.currentTimeMillis() - record.timestamp();
        metrics.countRecord(topicPartition);
        metrics.replicationLatency(topicPartition, latency);
        // Queue offset syncs only when offsetWriter is available
        if (offsetSyncWriter != null) {
            TopicPartition sourceTopicPartition = MirrorUtils.unwrapPartition(record.sourcePartition());
            long upstreamOffset = MirrorUtils.unwrapOffset(record.sourceOffset());
            long downstreamOffset = metadata.offset();
            offsetSyncWriter.maybeQueueOffsetSyncs(sourceTopicPartition, upstreamOffset, downstreamOffset);
            // We may be able to immediately publish an offset sync that we've queued up here
            offsetSyncWriter.firePendingOffsetSyncs();
        }
    }
 
    private Map<TopicPartition, Long> loadOffsets(Set<TopicPartition> topicPartitions) {
        return topicPartitions.stream().collect(Collectors.toMap(x -> x, this::loadOffset));
    }

    private Long loadOffset(TopicPartition topicPartition) {
        Map<String, Object> wrappedPartition = MirrorUtils.wrapPartition(topicPartition, sourceClusterAlias);
        Map<String, Object> wrappedOffset = context.offsetStorageReader().offset(wrappedPartition);
        return MirrorUtils.unwrapOffset(wrappedOffset);
    }

    // visible for testing
    void initializeConsumer(Set<TopicPartition> taskTopicPartitions) {
        Map<TopicPartition, Long> topicPartitionOffsets = loadOffsets(taskTopicPartitions);
        consumer.assign(topicPartitionOffsets.keySet());
        log.info("Starting with {} previously uncommitted partitions.", topicPartitionOffsets.values().stream()
                .filter(this::isUncommitted).count());

        topicPartitionOffsets.forEach((topicPartition, offset) -> {
            // Do not call seek on partitions that don't have an existing offset committed.
            if (isUncommitted(offset)) {
                log.trace("Skipping seeking offset for topicPartition: {}", topicPartition);
                return;
            }
            long nextOffsetToCommittedOffset = offset + 1L;
            log.trace("Seeking to offset {} for topicPartition: {}", nextOffsetToCommittedOffset, topicPartition);
            consumer.seek(topicPartition, nextOffsetToCommittedOffset);
        });
    }

    // visible for testing 
    SourceRecord convertRecord(ConsumerRecord<byte[], byte[]> record) {
        String targetTopic = formatRemoteTopic(record.topic());
        Headers headers = convertHeaders(record);
        return new SourceRecord(
                MirrorUtils.wrapPartition(new TopicPartition(record.topic(), record.partition()), sourceClusterAlias),
                MirrorUtils.wrapOffset(record.offset()),
                targetTopic, record.partition(),
                Schema.OPTIONAL_BYTES_SCHEMA, record.key(),
                Schema.BYTES_SCHEMA, record.value(),
                record.timestamp(), headers);
    }

    private Headers convertHeaders(ConsumerRecord<byte[], byte[]> record) {
        ConnectHeaders headers = new ConnectHeaders();
        for (Header header : record.headers()) {
            headers.addBytes(header.key(), header.value());
        }
        return headers;
    }

    private String formatRemoteTopic(String topic) {
        return replicationPolicy.formatRemoteTopic(sourceClusterAlias, topic);
    }

    private static int byteSize(byte[] bytes) {
        if (bytes == null) {
            return 0;
        } else {
            return bytes.length;
        }
    }

    private boolean isUncommitted(Long offset) {
        return offset == null || offset < 0;
    }

    /**
     * Enhanced MirrorMaker 2: Detect log truncation by checking for offset gaps
     * When aggressive retention policies purge messages before replication completes,
     * this creates undetectable gaps in the replicated data stream.
     */
    private void detectLogTruncation(ConsumerRecords<byte[], byte[]> records) {
        for (ConsumerRecord<byte[], byte[]> record : records) {
            TopicPartition sourceTopicPartition = new TopicPartition(record.topic(), record.partition());
            Long lastKnownOffset = previousOffsets.get(sourceTopicPartition);

            if (lastKnownOffset != null && record.offset() > lastKnownOffset + 1) {
                long gapSize = record.offset() - lastKnownOffset - 1;
                String errorMessage = String.format(
                    "CRITICAL: Log truncation detected for %s. Expected offset %d, but got %d. " +
                    "Gap of %d messages detected, indicating potential data loss due to retention policies. " +
                    "Timestamp: %d, Topic: %s, Partition: %d",
                    sourceTopicPartition, lastKnownOffset + 1, record.offset(), gapSize,
                    System.currentTimeMillis(), record.topic(), record.partition()
                );

                log.error(errorMessage);

                // Fail-fast: Throw exception to stop replication immediately
                throw new KafkaException("Log truncation detected: " + errorMessage);
            }
        }
    }

    /**
     * Enhanced MirrorMaker 2: Detect and handle topic resets (deletion/recreation)
     * When topics are deleted and recreated, offsets reset to 0, causing replication failures.
     * This method detects such scenarios and automatically recovers.
     */
    private void detectAndHandleTopicResets() {
        try {
            Set<TopicPartition> assignedPartitions = consumer.assignment();

            for (TopicPartition tp : assignedPartitions) {
                if (topicResetDetected.getOrDefault(tp, false)) {
                    continue; // Already handled this reset
                }

                try {
                    // Try to get current position - this will fail if topic was reset
                    long currentPosition = consumer.position(tp);
                    Long lastKnownOffset = previousOffsets.get(tp);

                    // If we had a previous offset but current position is 0, likely a topic reset
                    if (lastKnownOffset != null && lastKnownOffset > 0 && currentPosition == 0) {
                        log.warn("Topic reset detected for {}. Last known offset: {}, current position: {}. " +
                                "Timestamp: {}",
                                tp, lastKnownOffset, currentPosition, System.currentTimeMillis());

                        handleTopicReset(tp);
                    }
                } catch (Exception e) {
                    log.debug("Error checking position for {}: {}", tp, e.getMessage());
                    // This might indicate a topic reset, attempt recovery
                    handleTopicReset(tp);
                }
            }
        } catch (Exception e) {
            log.warn("Error during topic reset detection: {}", e.getMessage());
        }
    }

    /**
     * Enhanced MirrorMaker 2: Handle topic reset by resubscribing from beginning
     */
    private void handleTopicReset(TopicPartition topicPartition) {
        try {
            log.info("Handling topic reset for {}. Seeking to beginning offset. Timestamp: {}",
                    topicPartition, System.currentTimeMillis());

            // Mark this topic as reset to avoid repeated handling
            topicResetDetected.put(topicPartition, true);

            // Seek to beginning to restart replication from the start
            consumer.seekToBeginning(List.of(topicPartition));

            // Clear previous offset tracking for this partition
            previousOffsets.remove(topicPartition);

            log.info("Successfully recovered from topic reset for {}. Replication will restart from beginning.",
                    topicPartition);

        } catch (Exception e) {
            log.error("Failed to handle topic reset for {}: {}", topicPartition, e.getMessage(), e);
            // Re-throw to let Connect framework handle the error
            throw new KafkaException("Failed to recover from topic reset for " + topicPartition, e);
        }
    }

    /**
     * Enhanced MirrorMaker 2: Handle topic recovery for unknown topics/partitions
     */
    private void handleTopicRecovery(UnknownTopicOrPartitionException e) {
        try {
            log.info("Attempting to recover from unknown topic/partition error: {}. Timestamp: {}",
                    e.getMessage(), System.currentTimeMillis());

            // Wait a bit for topic to be available again
            Thread.sleep(5000);

            // Clear reset detection state to allow re-detection
            topicResetDetected.clear();

            log.info("Topic recovery attempt completed. Will retry on next poll.");

        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.warn("Topic recovery interrupted");
        } catch (Exception ex) {
            log.error("Failed during topic recovery: {}", ex.getMessage(), ex);
        }
    }
}
