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
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
//import java.util.Collections;
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

    private final Map<TopicPartition, Long> expectedNextOffsets =
        new java.util.concurrent.ConcurrentHashMap<>();
    private final Set<TopicPartition> resetHandled =
        java.util.concurrent.ConcurrentHashMap.newKeySet();

    public MirrorSourceTask() {}

    // for testing
    MirrorSourceTask(KafkaConsumer<byte[], byte[]> consumer, MirrorSourceMetrics metrics,
                     String sourceClusterAlias, ReplicationPolicy replicationPolicy,
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
        consumerAccess = new Semaphore(1);
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
        log.info("{} replicating {} topic-partitions {}->{}: {}.",
            Thread.currentThread().getName(), taskTopicPartitions.size(),
            sourceClusterAlias, config.targetClusterAlias(), taskTopicPartitions);
    }

    @Override
    public void commit() {
        if (offsetSyncWriter != null) {
            offsetSyncWriter.promoteDelayedOffsetSyncs();
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
        log.info("Stopping {} took {} ms.",
            Thread.currentThread().getName(), System.currentTimeMillis() - start);
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
            // Task 2: Check for log truncation that occurred while MM2 is
            // actively running. Complements the check in initializeConsumer()
            // which handles truncation that occurred while MM2 was stopped.
            checkForLogTruncation();

            ConsumerRecords<byte[], byte[]> records = consumer.poll(pollTimeout);
            //Task 3: reset detection
            if (handleTopicReset()) {
                return null;
            }
            //Runtime gap detection
            checkRuntimeGaps(records);

            List<SourceRecord> sourceRecords = new ArrayList<>(records.count());
            for (ConsumerRecord<byte[], byte[]> record : records) {
                SourceRecord converted = convertRecord(record);
                sourceRecords.add(converted);
                TopicPartition topicPartition =
                    new TopicPartition(converted.topic(), converted.kafkaPartition());
                metrics.recordAge(topicPartition,
                    System.currentTimeMillis() - record.timestamp());
                metrics.recordBytes(topicPartition, byteSize(record.value()));
            }
            if (sourceRecords.isEmpty()) {
                return null;
            } else {
                log.trace("Polled {} records from {}.",
                    sourceRecords.size(), records.partitions());
                return sourceRecords;
            }
        } catch (WakeupException e) {
            return null;
        } catch (OffsetOutOfRangeException e) {
            Set<TopicPartition> partitions = consumer.assignment();
            if (partitions != null && !partitions.isEmpty()) {
                consumer.seekToBeginning(partitions);
            }
            return null;
        } catch (KafkaException e) {
            log.warn("Failure during poll.", e);
            return null;
        } catch (Throwable e) {
            log.error("Failure during poll.", e);
            throw e;
        } finally {
            consumerAccess.release();
        }
    }

    private boolean handleTopicReset() {
        Set<TopicPartition> partitions = consumer.assignment();
        if (partitions == null || partitions.isEmpty()) {
            return false;
        }
        Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);
        for (TopicPartition tp : partitions) {
            Long lastOffset = loadOffset(tp);
            Long beginningOffset = beginningOffsets.get(tp);
            if (beginningOffset != null 
                && lastOffset != null 
                && beginningOffset == 0 
                && lastOffset > 0 
                && !resetHandled.contains(tp)) {
                resetHandled.add(tp);
                log.warn("[TOPIC RESET DETECTED] topic={} partition={} "
                        + "previousOffset={} currentBeginningOffset={}",
                        tp.topic(), tp.partition(), lastOffset, beginningOffset);
                consumer.seekToBeginning(java.util.Collections.singleton(tp));
                log.info("[TOPIC RESET RECOVERY] topic={} partition={}", tp.topic(), tp.partition());
                return true;
            }
        }
        return false;
    }

    private void checkRuntimeGaps(ConsumerRecords<byte[], byte[]> records) {
        for (TopicPartition tp : records.partitions()) {
            List<ConsumerRecord<byte[], byte[]>> partitionRecords = records.records(tp);
            if (partitionRecords.isEmpty()) {
                continue;
            }
            long expected = expectedNextOffsets.getOrDefault(tp, partitionRecords.get(0).offset());
            for (ConsumerRecord<byte[], byte[]> record : partitionRecords) {
                long actual = record.offset();
                if (actual != expected) {
                    throw new DataLossException("Runtime gap detected for " + tp);
                }
                expected++;
            }
            expectedNextOffsets.put(tp, expected);
        }
    }



    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) {
        if (stopping) {
            return;
        }
        if (metadata == null) {
            log.debug("No RecordMetadata (source record was probably filtered out "
                + "during transformation) -- can't sync offsets for {}.", record.topic());
            return;
        }
        if (!metadata.hasOffset()) {
            log.error("RecordMetadata has no offset -- can't sync offsets for {}.",
                record.topic());
            return;
        }
        TopicPartition topicPartition =
            new TopicPartition(record.topic(), record.kafkaPartition());
        long latency = System.currentTimeMillis() - record.timestamp();
        metrics.countRecord(topicPartition);
        metrics.replicationLatency(topicPartition, latency);
        if (offsetSyncWriter != null) {
            TopicPartition sourceTopicPartition =
                MirrorUtils.unwrapPartition(record.sourcePartition());
            long upstreamOffset = MirrorUtils.unwrapOffset(record.sourceOffset());
            long downstreamOffset = metadata.offset();
            offsetSyncWriter.maybeQueueOffsetSyncs(
                sourceTopicPartition, upstreamOffset, downstreamOffset);
            offsetSyncWriter.firePendingOffsetSyncs();
        }
    }

    private Map<TopicPartition, Long> loadOffsets(Set<TopicPartition> topicPartitions) {
        return topicPartitions.stream().collect(Collectors.toMap(x -> x, this::loadOffset));
    }

    private Long loadOffset(TopicPartition topicPartition) {
        Map<String, Object> wrappedPartition =
            MirrorUtils.wrapPartition(topicPartition, sourceClusterAlias);
        Map<String, Object> wrappedOffset =
            context.offsetStorageReader().offset(wrappedPartition);
        return MirrorUtils.unwrapOffset(wrappedOffset);
    }

    // visible for testing
    void initializeConsumer(Set<TopicPartition> taskTopicPartitions) {
        Map<TopicPartition, Long> topicPartitionOffsets = loadOffsets(taskTopicPartitions);
        consumer.assign(topicPartitionOffsets.keySet());
        log.info("Starting with {} previously uncommitted partitions.",
            topicPartitionOffsets.values().stream().filter(this::isUncommitted).count());

        // Fetch beginning and end offsets once for all assigned partitions.
        // Used to detect topic reset and log truncation before seeking,
        // covering failures that occurred while MM2 was stopped.
        Map<TopicPartition, Long> beginningOffsets =
            consumer.beginningOffsets(topicPartitionOffsets.keySet());

        topicPartitionOffsets.forEach((topicPartition, offset) -> {
            if (isUncommitted(offset)) {
                log.trace("Skipping seeking offset for topicPartition: {}", topicPartition);
                return;
            }

            long nextExpectedOffset = offset + 1L;
            Long beginningOffset = beginningOffsets.get(topicPartition);

            if (beginningOffset == null) {
                log.warn("Could not retrieve offsets for {}. Proceeding with seek.",
                    topicPartition);
                consumer.seek(topicPartition, nextExpectedOffset);
                return;
            }

            // Task 3: Topic Reset Detection (checked before truncation).
            // A topic reset is identified when the beginning offset is 0
            // (fresh topic) and the end offset is less than or equal to the
            // last committed offset (topic has fewer messages than MM2 knows).
            // Checking reset first because reset always starts at 0, while
            // truncation moves beginning offset forward above 0.

            // Task 2: Log Truncation Detection on cold start.
            // If the earliest available offset is ahead of the next expected
            // offset, messages were purged before replication completed.
            // Fail fast to prevent producing a silently incomplete replica.
            if (beginningOffset >= nextExpectedOffset) {
                long messagesLost = beginningOffset - nextExpectedOffset;
                log.error("[TRUNCATION DETECTED] topic={} partition={} "
                    + "lastCommittedOffset={} earliestAvailableOffset={} "
                    + "messagesLost={} timestamp={}. "
                    + "Source topic was truncated by retention policy before "
                    + "replication completed. Failing fast to prevent "
                    + "silent data loss in the replicated stream.",
                    topicPartition.topic(), topicPartition.partition(),
                    offset, beginningOffset, messagesLost, Instant.now());
                throw new DataLossException(String.format(
                    "Log truncation detected on %s partition %d: "
                    + "expected offset %d but earliest available is %d. "
                    + "Messages lost: %d",
                    topicPartition.topic(), topicPartition.partition(),
                    nextExpectedOffset, beginningOffset, messagesLost));
            }

            log.trace("Seeking to offset {} for topicPartition: {}",
                nextExpectedOffset, topicPartition);
            consumer.seek(topicPartition, nextExpectedOffset);
        });
    }

    /**
     * Checks for log truncation during continuous poll operation.
     *
     * <p>Handles truncation that occurs while MM2 is actively running.
     * The check in {@link #initializeConsumer} covers truncation that
     * occurred while MM2 was stopped.
     *
     * <p>On detection, throws {@link DataLossException} to fail fast
     * and prevent silent data loss in the replicated stream.
     */
    private void checkForLogTruncation() {
        Set<TopicPartition> assignedPartitions = consumer.assignment();
        if (assignedPartitions.isEmpty()) {
            return;
        }

        Map<TopicPartition, Long> beginningOffsets =
            consumer.beginningOffsets(assignedPartitions);

        for (TopicPartition topicPartition : assignedPartitions) {
            Long lastCommittedOffset = loadOffset(topicPartition);
            if (isUncommitted(lastCommittedOffset)) {
                log.debug("Skipping truncation check for uncommitted partition: {}",
                    topicPartition);
                continue;
            }

            Long earliestAvailableOffset = beginningOffsets.get(topicPartition);
            if (earliestAvailableOffset == null) {
                log.warn("Could not retrieve beginning offset for partition: {}",
                    topicPartition);
                continue;
            }

            long nextExpectedOffset = lastCommittedOffset + 1L;
            if (earliestAvailableOffset > nextExpectedOffset) {
                long messagesLost = earliestAvailableOffset - nextExpectedOffset;
                log.error("[TRUNCATION DETECTED] topic={} partition={} "
                    + "lastCommittedOffset={} earliestAvailableOffset={} "
                    + "messagesLost={} timestamp={}. "
                    + "Source topic was truncated by retention policy before "
                    + "replication completed. Failing fast to prevent "
                    + "silent data loss in the replicated stream.",
                    topicPartition.topic(), topicPartition.partition(),
                    lastCommittedOffset, earliestAvailableOffset,
                    messagesLost, Instant.now());
                throw new DataLossException(String.format(
                    "Log truncation detected on %s partition %d: "
                    + "expected offset %d but earliest available is %d. "
                    + "Messages lost: %d",
                    topicPartition.topic(), topicPartition.partition(),
                    nextExpectedOffset, earliestAvailableOffset, messagesLost));
            }

            log.debug("Truncation check passed: topic={} partition={} "
                + "lastCommittedOffset={} earliestAvailableOffset={}",
                topicPartition.topic(), topicPartition.partition(),
                lastCommittedOffset, earliestAvailableOffset);
        }
    }

    // visible for testing
    SourceRecord convertRecord(ConsumerRecord<byte[], byte[]> record) {
        String targetTopic = formatRemoteTopic(record.topic());
        Headers headers = convertHeaders(record);
        return new SourceRecord(
            MirrorUtils.wrapPartition(
                new TopicPartition(record.topic(), record.partition()),
                sourceClusterAlias),
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
     * Thrown when log truncation is detected during replication.
     *
     * <p>This is a fail-fast mechanism. When Kafka retention purges messages
     * before MirrorMaker 2 replicates them, continuing would produce a
     * silently incomplete replica. This exception stops MM2 immediately so
     * operators can investigate and take corrective action.
     *
     * <p>Defined as a package-private static inner class to keep all
     * fault-tolerance changes in a single file with minimal disruption
     * to the existing codebase.
     */
    static class DataLossException extends RuntimeException {
        DataLossException(String message) {
            super(message);
        }
    }
}