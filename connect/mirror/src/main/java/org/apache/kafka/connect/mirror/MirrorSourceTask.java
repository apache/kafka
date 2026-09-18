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
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

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
    private boolean offsetValidationEnabled;

    public MirrorSourceTask() {}

    // for testing
    MirrorSourceTask(KafkaConsumer<byte[], byte[]> consumer, MirrorSourceLegacyMetrics metrics, String sourceClusterAlias,
                     ReplicationPolicy replicationPolicy,
                     OffsetSyncWriter offsetSyncWriter) {
        this(consumer, metrics, sourceClusterAlias, replicationPolicy, offsetSyncWriter,
                MirrorSourceConfig.OFFSET_VALIDATION_ENABLED_DEFAULT);
    }

    // for testing
    MirrorSourceTask(KafkaConsumer<byte[], byte[]> consumer, MirrorSourceLegacyMetrics metrics, String sourceClusterAlias,
                     ReplicationPolicy replicationPolicy,
                     OffsetSyncWriter offsetSyncWriter,
                     boolean offsetValidationEnabled) {
        this.consumer = consumer;
        this.legacyMetrics = metrics;
        this.sourceClusterAlias = sourceClusterAlias;
        this.replicationPolicy = replicationPolicy;
        consumerAccess = new Semaphore(1);
        this.offsetSyncWriter = offsetSyncWriter;
        this.offsetValidationEnabled = offsetValidationEnabled;
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
        offsetValidationEnabled = config.offsetValidationEnabled();
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
            // With auto.offset.reset=none the consumer surfaces invalid offsets instead of silently
            // rewinding. Classify the cause and fail the task so the operator sees it.
            if (!offsetValidationEnabled) {
                log.warn("Failure during poll.", e);
                return null;
            }
            throw classifyOffsetOutOfRange(e);
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
 
    /**
     * Works out why the replication consumer was left holding an out-of-range offset and builds the
     * corresponding fail-fast exception.
     *
     * <p>For each affected partition the log start offset on the source cluster is the deciding
     * signal:
     * <ul>
     *   <li>{@code logStartOffset > 0} -- records ahead of our position were removed by the
     *       retention policy before they could be replicated, so data has been lost.</li>
     *   <li>{@code logStartOffset == 0} -- the log begins at the very start again, meaning the topic
     *       was deleted and recreated (or otherwise truncated to empty) and our tracked offset now
     *       points past the end of the log.</li>
     * </ul>
     *
     * <p>Data loss takes precedence when both conditions are present in a single batch, because it
     * is the condition with unrecoverable consequences for the target cluster.
     *
     * @param cause the exception raised by the consumer
     * @return a {@link DataLossException} or a {@link TopicResetException}, never {@code null}
     */
    // visible for testing
    KafkaException classifyOffsetOutOfRange(OffsetOutOfRangeException cause) {
        // Sort for deterministic logging and error messages.
        Map<TopicPartition, Long> requestedOffsets = new TreeMap<>(
                Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition));
        requestedOffsets.putAll(cause.offsetOutOfRangePartitions());

        Map<TopicPartition, Long> logStartOffsets = beginningOffsets(requestedOffsets.keySet());

        Map<TopicPartition, Long> dataLoss = new LinkedHashMap<>();
        Map<TopicPartition, Long> topicReset = new LinkedHashMap<>();

        requestedOffsets.forEach((topicPartition, requestedOffset) -> {
            Long logStartOffset = logStartOffsets.get(topicPartition);
            if (logStartOffset != null && logStartOffset > 0L) {
                log.error("Detected data loss on {}-{}: MirrorMaker 2 requested offset {} but the "
                                + "earliest available offset on the source cluster is {}. {} record(s) were "
                                + "removed by the retention policy before they could be replicated.",
                        topicPartition.topic(), topicPartition.partition(), requestedOffset,
                        logStartOffset, logStartOffset - requestedOffset);
                dataLoss.put(topicPartition, requestedOffset);
            } else {
                log.error("Detected a topic reset on {}-{}: MirrorMaker 2 requested offset {} but the "
                                + "log now starts at offset 0. The source topic was most likely deleted "
                                + "and recreated.",
                        topicPartition.topic(), topicPartition.partition(), requestedOffset);
                topicReset.put(topicPartition, requestedOffset);
            }
        });

        if (!dataLoss.isEmpty()) {
            return new DataLossException("MirrorMaker 2 cannot replicate " + describe(dataLoss)
                    + " from cluster '" + sourceClusterAlias + "': the requested offsets are no longer "
                    + "available because the source records were removed by the retention policy. "
                    + "Failing the task to avoid silently skipping the missing records. Reset the "
                    + "connector offsets to resume replication and accept the gap.", cause);
        }

        return new TopicResetException("MirrorMaker 2 cannot replicate " + describe(topicReset)
                + " from cluster '" + sourceClusterAlias + "': the source topic appears to have been "
                + "deleted and recreated, so the tracked offsets are no longer valid. Failing the task "
                + "to avoid replicating the new topic on top of the previously mirrored data. Reset the "
                + "connector offsets to resume replication.", cause);
    }

    /**
     * Looks up the log start offset for each partition. Any failure here is non-fatal: we fall back
     * to an empty result, which classifies the failure as a topic reset and still fails the task.
     */
    private Map<TopicPartition, Long> beginningOffsets(Set<TopicPartition> topicPartitions) {
        try {
            return consumer.beginningOffsets(topicPartitions);
        } catch (KafkaException e) {
            log.warn("Unable to look up the earliest offsets for {} while classifying an "
                    + "out-of-range offset.", topicPartitions, e);
            return Map.of();
        }
    }

    private static String describe(Map<TopicPartition, Long> offsets) {
        return offsets.entrySet().stream()
                .map(e -> e.getKey().topic() + "-" + e.getKey().partition() + " at offset " + e.getValue())
                .collect(Collectors.joining(", "));
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

        Set<TopicPartition> uncommittedPartitions = topicPartitionOffsets.entrySet().stream()
                .filter(entry -> isUncommitted(entry.getValue()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

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

        // Offset validation requires auto.offset.reset=none, which means the consumer has no
        // fallback position for partitions we have never replicated before. Seek those explicitly to
        // the beginning so that a first start behaves exactly as it does with the default settings.
        if (offsetValidationEnabled && !uncommittedPartitions.isEmpty()) {
            log.info("Seeking to the beginning of {} partition(s) with no previously committed offset: {}.",
                    uncommittedPartitions.size(), uncommittedPartitions);
            consumer.seekToBeginning(uncommittedPartitions);
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
