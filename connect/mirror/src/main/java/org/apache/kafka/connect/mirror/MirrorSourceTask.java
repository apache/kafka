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
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.connect.mirror.MirrorConnectorConfig.METRIC_NAMES_LEGACY;
import static org.apache.kafka.connect.mirror.MirrorConnectorConfig.METRIC_NAMES_NEW;

/** Replicates a set of topic-partitions. */
public class MirrorSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(MirrorSourceTask.class);

    private KafkaConsumer<byte[], byte[]> consumer;
    private String sourceClusterAlias;
    private Duration pollTimeout;
    private ReplicationPolicy replicationPolicy;
    private MirrorSourceLegacyMetrics legacyMetrics;
    private MirrorSourceMetrics metrics;
    private boolean stopping = false;
    private Semaphore consumerAccess;
    private OffsetSyncWriter offsetSyncWriter;
    // When true, the task fails fast on OffsetOutOfRangeException instead of silently
    // resetting to the earliest offset (see DataLossException / TopicResetException).
    private boolean detectOffsetOutOfRange = false;

    public MirrorSourceTask() {}

    // for testing
    MirrorSourceTask(KafkaConsumer<byte[], byte[]> consumer, MirrorSourceLegacyMetrics metrics, String sourceClusterAlias,
                     ReplicationPolicy replicationPolicy,
                     OffsetSyncWriter offsetSyncWriter) {
        this(consumer, metrics, sourceClusterAlias, replicationPolicy, offsetSyncWriter, false);
    }

    // for testing
    MirrorSourceTask(KafkaConsumer<byte[], byte[]> consumer, MirrorSourceLegacyMetrics metrics, String sourceClusterAlias,
                     ReplicationPolicy replicationPolicy,
                     OffsetSyncWriter offsetSyncWriter,
                     boolean detectOffsetOutOfRange) {
        this.consumer = consumer;
        this.legacyMetrics = metrics;
        this.sourceClusterAlias = sourceClusterAlias;
        this.replicationPolicy = replicationPolicy;
        consumerAccess = new Semaphore(1);
        this.offsetSyncWriter = offsetSyncWriter;
        this.detectOffsetOutOfRange = detectOffsetOutOfRange;
    }

    @Override
    public void start(Map<String, String> props) {
        MirrorSourceTaskConfig config = new MirrorSourceTaskConfig(props);
        consumerAccess = new Semaphore(1);  // let one thread at a time access the consumer
        sourceClusterAlias = config.sourceClusterAlias();
        List<String> metricNamesFormats = config.metricNamesFormats();
        legacyMetrics = metricNamesFormats.contains(METRIC_NAMES_LEGACY) ? config.legacyMetrics() : null;
        metrics = metricNamesFormats.contains(METRIC_NAMES_NEW) ? config.metrics(context.pluginMetrics()) : null;
        pollTimeout = config.consumerPollTimeout();
        replicationPolicy = config.replicationPolicy();
        if (config.emitOffsetSyncsEnabled()) {
            offsetSyncWriter = new OffsetSyncWriter(config);
        }
        detectOffsetOutOfRange = config.detectOffsetOutOfRangeEnabled();
        Map<String, Object> consumerConfig = config.sourceConsumerConfig("replication-consumer");
        if (detectOffsetOutOfRange) {
            // Force the consumer to surface OffsetOutOfRangeException instead of silently
            // seeking to the earliest offset, so we can fail fast on data loss / topic reset.
            consumerConfig.put(AUTO_OFFSET_RESET_CONFIG, "none");
        }
        consumer = MirrorUtils.newConsumer(consumerConfig);
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
        Utils.closeQuietly(legacyMetrics, "metrics");
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
            ConsumerRecords<byte[], byte[]> records = consumer.poll(pollTimeout);
            List<SourceRecord> sourceRecords = new ArrayList<>(records.count());
            for (ConsumerRecord<byte[], byte[]> record : records) {
                SourceRecord converted = convertRecord(record);
                sourceRecords.add(converted);
                TopicPartition topicPartition = new TopicPartition(converted.topic(), converted.kafkaPartition());
                long age = System.currentTimeMillis() - record.timestamp();
                long size = byteSize(record.value());
                if (legacyMetrics != null) {
                    legacyMetrics.recordAge(topicPartition, age);
                    legacyMetrics.recordBytes(topicPartition, size);
                }
                if (metrics != null) {
                    metrics.recordAge(topicPartition, age);
                    metrics.recordBytes(topicPartition, size);
                }
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
        } catch (OffsetOutOfRangeException e) {
            // Only reached when auto.offset.reset=none, i.e. when detectOffsetOutOfRange is enabled.
            // Translate the low-level exception into an explicit, fail-fast MM2 exception.
            throw handleOffsetOutOfRange(e);
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
        if (legacyMetrics != null) {
            legacyMetrics.countRecord(topicPartition);
            legacyMetrics.replicationLatency(topicPartition, latency);
        }
        if (metrics != null) {
            metrics.countRecord(topicPartition);
            metrics.replicationLatency(topicPartition, latency);
        }
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
                if (detectOffsetOutOfRange) {
                    // With auto.offset.reset=none the consumer has no fallback for partitions without a
                    // committed offset, so we must position it explicitly. Preserve MM2's default behavior
                    // of starting from the beginning of the topic on first replication.
                    log.trace("Seeking to beginning for uncommitted topicPartition: {}", topicPartition);
                    consumer.seekToBeginning(Set.of(topicPartition));
                    return;
                }
                log.trace("Skipping seeking offset for topicPartition: {}", topicPartition);
                return;
            }
            long nextOffsetToCommittedOffset = offset + 1L;
            log.trace("Seeking to offset {} for topicPartition: {}", nextOffsetToCommittedOffset, topicPartition);
            consumer.seek(topicPartition, nextOffsetToCommittedOffset);
        });
    }

    /**
     * Translate an {@link OffsetOutOfRangeException} raised by the replication consumer into an explicit,
     * fail-fast MirrorMaker 2 exception. For each affected partition we inspect both the earliest and the
     * latest (log end) offset available on the source cluster and compare them against the requested offset:
     * <ul>
     *     <li>requested offset {@code > logEndOffset}: the requested offset points beyond the end of the
     *     log, which can only happen when the topic was deleted and recreated (its log was reset to a
     *     smaller size). A {@link TopicResetException} is thrown.</li>
     *     <li>requested offset {@code < earliestOffset}: earlier records were purged (e.g. by a retention
     *     policy) before they could be replicated, so a {@link DataLossException} is thrown.</li>
     * </ul>
     * Note that an earliest offset of {@code 0} on its own is not sufficient to conclude a topic reset:
     * a brand-new or low-retention topic also starts at {@code 0}. The requested offset must exceed the
     * current log end offset for the partition to be classified as a reset.
     */
    // visible for testing
    RuntimeException handleOffsetOutOfRange(OffsetOutOfRangeException e) {
        Map<TopicPartition, Long> outOfRangeOffsets = e.offsetOutOfRangePartitions();
        Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(outOfRangeOffsets.keySet());
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(outOfRangeOffsets.keySet());
        boolean topicReset = false;
        boolean dataLoss = false;
        StringBuilder details = new StringBuilder();
        for (Map.Entry<TopicPartition, Long> entry : outOfRangeOffsets.entrySet()) {
            TopicPartition tp = entry.getKey();
            long requestedOffset = entry.getValue();
            long earliestOffset = beginningOffsets.getOrDefault(tp, 0L);
            long logEndOffset = endOffsets.getOrDefault(tp, 0L);
            details.append(String.format("[topic=%s, partition=%d, requestedOffset=%d, earliestAvailableOffset=%d, logEndOffset=%d] ",
                    tp.topic(), tp.partition(), requestedOffset, earliestOffset, logEndOffset));
            if (requestedOffset > logEndOffset) {
                // The tracked offset is ahead of the current end of the log. The log must have shrunk,
                // which indicates the topic was deleted and recreated (topic reset).
                topicReset = true;
            } else if (requestedOffset < earliestOffset) {
                // The tracked offset falls below the earliest retained record: earlier records were
                // purged before they could be replicated (data loss).
                dataLoss = true;
            }
        }
        // A topic reset is the more severe / less ambiguous condition, so report it first if detected.
        if (topicReset) {
            String message = "Detected source topic reset (topic deleted and recreated). The previously tracked "
                    + "offset is beyond the current end of the log and is no longer valid: " + details.toString().trim();
            log.error(message);
            return new TopicResetException(message, e);
        } else if (dataLoss) {
            String message = "Detected data loss: source records were purged before they could be replicated. "
                    + "The requested offset is below the earliest available offset: " + details.toString().trim();
            log.error(message);
            return new DataLossException(message, e);
        } else {
            // The offset was out of range but neither classification applied (e.g. a transient race
            // between the failing fetch and the metadata lookup). Surface it as data loss to fail fast.
            String message = "Detected out-of-range offset that could not be conclusively classified. "
                    + "Failing fast to avoid silent data loss: " + details.toString().trim();
            log.error(message);
            return new DataLossException(message, e);
        }
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
}
