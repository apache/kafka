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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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

    // The offset MirrorMaker 2 expects to read next from each source topic-partition.
    // Seeded in initializeConsumer() and advanced after every successfully polled record.
    // Used to detect (a) log truncation -- data purged by retention before replication
    // (fail-fast, Task 2) and (b) source topic reset -- delete/recreate (auto-recover, Task 3).
    // ConcurrentHashMap because poll() and initializeConsumer() may run on different threads.
    // visible for testing
    final Map<TopicPartition, Long> expectedOffsets = new ConcurrentHashMap<>();

    public MirrorSourceTask() {}

    // for testing
    MirrorSourceTask(KafkaConsumer<byte[], byte[]> consumer, MirrorSourceLegacyMetrics metrics, String sourceClusterAlias,
                     ReplicationPolicy replicationPolicy,
                     OffsetSyncWriter offsetSyncWriter) {
        this.consumer = consumer;
        this.legacyMetrics = metrics;
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
        List<String> metricNamesFormats = config.metricNamesFormats();
        legacyMetrics = metricNamesFormats.contains(METRIC_NAMES_LEGACY) ? config.legacyMetrics() : null;
        metrics = metricNamesFormats.contains(METRIC_NAMES_NEW) ? config.metrics(context.pluginMetrics()) : null;
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
            // Before polling, verify no source records have been purged or reset out from
            // under us. Because the source consumer uses auto.offset.reset=earliest, Kafka
            // would otherwise silently skip missing data instead of raising an error.
            // This may throw LogTruncationException (Task 2, fail-fast) or transparently
            // resubscribe a reset topic-partition from the beginning (Task 3, auto-recover).
            detectTruncationAndReset();

            ConsumerRecords<byte[], byte[]> records = consumer.poll(pollTimeout);
            List<SourceRecord> sourceRecords = new ArrayList<>(records.count());
            for (ConsumerRecord<byte[], byte[]> record : records) {
                SourceRecord converted = convertRecord(record);
                sourceRecords.add(converted);
                TopicPartition topicPartition = new TopicPartition(converted.topic(), converted.kafkaPartition());
                // Advance the expected read position for this source partition. We track the
                // *source* topic-partition (record.topic(), not the renamed downstream topic).
                expectedOffsets.put(new TopicPartition(record.topic(), record.partition()), record.offset() + 1L);
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
        } catch (LogTruncationException e) {
            // Task 2: unrecoverable data loss. Re-throw so Kafka Connect fails the task.
            // Must precede the KafkaException clause below, since LogTruncationException
            // is itself a KafkaException and would otherwise be swallowed and logged at WARN.
            log.error("Failing MirrorSourceTask due to detected log truncation / silent data loss.", e);
            throw e;
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
                log.trace("Skipping seeking offset for topicPartition: {}", topicPartition);
                return;
            }
            long nextOffsetToCommittedOffset = offset + 1L;
            log.trace("Seeking to offset {} for topicPartition: {}", nextOffsetToCommittedOffset, topicPartition);
            consumer.seek(topicPartition, nextOffsetToCommittedOffset);
            // Seed truncation/reset detection with the offset we intend to read next.
            expectedOffsets.put(topicPartition, nextOffsetToCommittedOffset);
        });
    }

    /**
     * Detects, for every assigned source topic-partition, whether records that
     * MirrorMaker 2 still needs have been removed from the source log. This handles
     * the two failure scenarios that vanilla MM2 ignores because its consumer uses
     * {@code auto.offset.reset=earliest}:
     *
     * <ul>
     *   <li><b>Log truncation (Task 2):</b> the source retention policy purged records
     *       at offsets we had not yet replicated, but the topic itself still exists and
     *       has grown beyond our expected offset. This is unrecoverable data loss, so we
     *       throw {@link LogTruncationException} to fail fast.</li>
     *   <li><b>Topic reset (Task 3):</b> the source topic was deleted and recreated, so
     *       the log has been rewound to a fresh, smaller log. We treat this as a recoverable
     *       event: log it and resubscribe from the beginning offset.</li>
     * </ul>
     *
     * <p>The distinguishing signal is the source end offset. After a delete/recreate the
     * fresh topic's end offset is smaller than the offset we expected to read next (the
     * log was rewound). Pure retention truncation leaves the end offset at or beyond our
     * expected offset (the log only lost its tail-end, not its head).
     */
    // visible for testing
    void detectTruncationAndReset() {
        if (expectedOffsets.isEmpty()) {
            return;
        }
        Set<TopicPartition> assignment = consumer.assignment();
        if (assignment.isEmpty()) {
            return;
        }
        Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(assignment);
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignment);
        Map<TopicPartition, Long> partitionsToRewind = new HashMap<>();

        for (TopicPartition tp : assignment) {
            Long expected = expectedOffsets.get(tp);
            if (expected == null) {
                // Never read or seeded for this partition yet; nothing to compare against.
                continue;
            }
            long logStartOffset = beginningOffsets.getOrDefault(tp, 0L);
            long logEndOffset = endOffsets.getOrDefault(tp, 0L);

            if (logEndOffset < expected) {
                // The log is shorter than where we expect to read -> the topic was reset
                // (deleted and recreated). Recoverable: resubscribe from the new beginning.
                log.warn("Detected source topic reset on {} at {} ms (epoch). Expected to read "
                                + "from offset {} but the source log now ends at offset {} "
                                + "(begins at {}). The topic was deleted and recreated. "
                                + "Automatically resubscribing from the beginning offset {}.",
                        tp, System.currentTimeMillis(), expected, logEndOffset,
                        logStartOffset, logStartOffset);
                partitionsToRewind.put(tp, logStartOffset);
            } else if (logStartOffset > expected) {
                // The earliest available record is past where we need to read, but the log
                // still extends beyond us -> retention purged un-replicated records.
                // Unrecoverable data loss: fail fast.
                throw new LogTruncationException(tp, expected, logStartOffset);
            }
        }

        // Apply Task 3 recovery after iterating so detection isn't affected by seeks.
        partitionsToRewind.forEach((tp, beginning) -> {
            consumer.seek(tp, beginning);
            expectedOffsets.put(tp, beginning);
            log.info("Resubscribed source topic-partition {} from offset {} after topic reset.",
                    tp, beginning);
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
}
