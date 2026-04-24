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
import org.apache.kafka.connect.mirror.handler.FaultToleranceHandler;
import org.apache.kafka.connect.mirror.tracking.PartitionOffsetTracker;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
// import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;


/**
 * Replicates a set of topic-partitions from a source Kafka cluster to a target cluster.
 *
 * <p>This is the patched version of the vanilla {@code MirrorSourceTask} (Kafka v4.0.0).
 * All custom additions are clearly marked with {@code [CUSTOM]} and delegate to
 * dedicated classes rather than adding logic inline:
 *
 * <ul>
 *   <li><b>Task 2 — Log Truncation Detection</b>: handled by
 *       {@link org.apache.kafka.connect.mirror.detector.TruncationDetector} via
 *       {@link FaultToleranceHandler}. Throws a {@link RuntimeException} on data loss.</li>
 *   <li><b>Task 3 — Topic Reset Recovery</b>: handled by
 *       {@link org.apache.kafka.connect.mirror.detector.TopicResetDetector} via
 *       {@link FaultToleranceHandler}. Auto-seeks to offset 0 on a detected reset.</li>
 * </ul>
 *
 * <p>All other methods are identical to the upstream source.
 */
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

    // [CUSTOM] Tracks the next-expected offset per partition for both fault detectors
    private final PartitionOffsetTracker offsetTracker = new PartitionOffsetTracker();

    // [CUSTOM] Orchestrates Task 2 (truncation) and Task 3 (reset) checks
    private FaultToleranceHandler faultToleranceHandler;

    public MirrorSourceTask() {}

    // Package-private constructor used by unit tests — allows mocking dependencies
    MirrorSourceTask(KafkaConsumer<byte[], byte[]> consumer, MirrorSourceMetrics metrics,
                     String sourceClusterAlias, ReplicationPolicy replicationPolicy,
                     OffsetSyncWriter offsetSyncWriter) {
        this.consumer           = consumer;
        this.metrics            = metrics;
        this.sourceClusterAlias = sourceClusterAlias;
        this.replicationPolicy  = replicationPolicy;
        consumerAccess          = new Semaphore(1);
        this.offsetSyncWriter   = offsetSyncWriter;
        // [CUSTOM] Wire fault tolerance handler with the injected consumer
        this.faultToleranceHandler = new FaultToleranceHandler(consumer, offsetTracker);
    }

    @Override
    public void start(Map<String, String> props) {
        MirrorSourceTaskConfig config = new MirrorSourceTaskConfig(props);
        consumerAccess      = new Semaphore(1);
        sourceClusterAlias  = config.sourceClusterAlias();
        metrics             = config.metrics();
        pollTimeout         = config.consumerPollTimeout();
        replicationPolicy   = config.replicationPolicy();

        if (config.emitOffsetSyncsEnabled()) {
            offsetSyncWriter = new OffsetSyncWriter(config);
        }

        consumer = MirrorUtils.newConsumer(config.sourceConsumerConfig("replication-consumer"));

        // [CUSTOM] Create fault tolerance handler after consumer is initialised
        faultToleranceHandler = new FaultToleranceHandler(consumer, offsetTracker);

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
        log.info("Stopping {} took {} ms.", Thread.currentThread().getName(),
                System.currentTimeMillis() - start);
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
            // [CUSTOM] Run Task 2 + Task 3 checks before every poll
            for (TopicPartition tp : consumer.assignment()) {
                faultToleranceHandler.runChecks(tp, consumer);
            }

            ConsumerRecords<byte[], byte[]> records = consumer.poll(pollTimeout);
            List<SourceRecord> sourceRecords = new ArrayList<>(records.count());

            for (ConsumerRecord<byte[], byte[]> record : records) {
                SourceRecord converted = convertRecord(record);
                sourceRecords.add(converted);

                TopicPartition topicPartition = new TopicPartition(converted.topic(), converted.kafkaPartition());
                metrics.recordAge(topicPartition, System.currentTimeMillis() - record.timestamp());
                metrics.recordBytes(topicPartition, byteSize(record.value()));

                // [CUSTOM] Update offset tracking: record offset so detectors have a valid bookmark
                TopicPartition sourceTp = new TopicPartition(record.topic(), record.partition());
                offsetTracker.recordConsumed(sourceTp, record.offset());
            }

            return sourceRecords.isEmpty() ? null : sourceRecords;

        } catch (WakeupException e) {
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

    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) {
        if (stopping) return;
        if (metadata == null) {
            log.debug("No RecordMetadata (source record was probably filtered out) -- can't sync offsets for {}.", record.topic());
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
        if (offsetSyncWriter != null) {
            TopicPartition sourceTopicPartition = MirrorUtils.unwrapPartition(record.sourcePartition());
            long upstreamOffset   = MirrorUtils.unwrapOffset(record.sourceOffset());
            long downstreamOffset = metadata.offset();
            offsetSyncWriter.maybeQueueOffsetSyncs(sourceTopicPartition, upstreamOffset, downstreamOffset);
            offsetSyncWriter.firePendingOffsetSyncs();
        }
    }

    // ==========================================================================
    // [CUSTOM] Test-helper accessors (package-private — unit tests ONLY)
    //
    // These expose internal state for test arrangement / assertion.
    // They are never called in production.
    // ==========================================================================

    PartitionOffsetTracker getOffsetTracker() {
        return offsetTracker;
    }

    FaultToleranceHandler getFaultToleranceHandler() {
        return faultToleranceHandler;
    }

    // ==========================================================================
    // Standard MM2 helpers (unchanged from vanilla Kafka v4.0.0)
    // ==========================================================================

    private Map<TopicPartition, Long> loadOffsets(Set<TopicPartition> topicPartitions) {
        return topicPartitions.stream().collect(Collectors.toMap(x -> x, this::loadOffset));
    }

    private Long loadOffset(TopicPartition topicPartition) {
        Map<String, Object> wrappedPartition = MirrorUtils.wrapPartition(topicPartition, sourceClusterAlias);
        Map<String, Object> wrappedOffset    = context.offsetStorageReader().offset(wrappedPartition);
        return MirrorUtils.unwrapOffset(wrappedOffset);
    }

    void initializeConsumer(Set<TopicPartition> taskTopicPartitions) {
        Map<TopicPartition, Long> topicPartitionOffsets = loadOffsets(taskTopicPartitions);
        consumer.assign(topicPartitionOffsets.keySet());

        List<TopicPartition> uncommitted = topicPartitionOffsets.entrySet().stream()
                .filter(e -> isUncommitted(e.getValue()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        log.info("Starting with {} previously uncommitted partitions.", uncommitted.size());

        // [CUSTOM] For uncommitted partitions, explicitly seek to the earliest available
        // offset instead of relying on auto.offset.reset. This ensures no messages are
        // skipped due to consumer-group offset configuration or broker-side defaults.
        if (!uncommitted.isEmpty()) {
            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(uncommitted);
            uncommitted.forEach(tp -> {
                long startOffset = beginningOffsets.getOrDefault(tp, 0L);
                log.info("Uncommitted partition {} — explicitly seeking to earliest offset {}.", tp, startOffset);
                consumer.seek(tp, startOffset);
                // [CUSTOM] Seed the tracker so fault detectors have a valid bookmark from the start
                offsetTracker.setNextExpected(tp, startOffset);
            });
        }

        // For committed partitions, seek to stored offset + 1 (resume exactly where we left off)
        topicPartitionOffsets.forEach((topicPartition, offset) -> {
            if (isUncommitted(offset)) {
                return; // already handled above
            }
            long nextOffset = offset + 1L;
            log.trace("Seeking to offset {} for topicPartition: {}", nextOffset, topicPartition);
            consumer.seek(topicPartition, nextOffset);
            // [CUSTOM] Seed the tracker so fault detectors have a valid bookmark from the start
            offsetTracker.setNextExpected(topicPartition, nextOffset);
        });
    }

    SourceRecord convertRecord(ConsumerRecord<byte[], byte[]> record) {
        String targetTopic = formatRemoteTopic(record.topic());
        Headers headers    = convertHeaders(record);
        return new SourceRecord(
            MirrorUtils.wrapPartition(new TopicPartition(record.topic(), record.partition()), sourceClusterAlias),
            MirrorUtils.wrapOffset(record.offset()),
            targetTopic, record.partition(),
            Schema.OPTIONAL_BYTES_SCHEMA, record.key(),
            Schema.BYTES_SCHEMA, record.value(),
            record.timestamp(), headers
        );
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
        return bytes == null ? 0 : bytes.length;
    }

    private boolean isUncommitted(Long offset) {
        return offset == null || offset < 0;
    }
}
