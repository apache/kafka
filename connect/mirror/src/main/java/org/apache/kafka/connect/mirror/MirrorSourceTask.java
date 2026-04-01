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
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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

    // ===== Enhancement State =====
    private final Map<TopicPartition, Long> lastSeenOffsets = new HashMap<>();
    private Set<TopicPartition> previousAssignment = new HashSet<>();

    public MirrorSourceTask() {
    }

    // REQUIRED for Kafka tests (do not remove)
    MirrorSourceTask(KafkaConsumer<byte[], byte[]> consumer,
                     MirrorSourceMetrics metrics,
                     String sourceClusterAlias,
                     ReplicationPolicy replicationPolicy,
                     OffsetSyncWriter offsetSyncWriter) {
        this.consumer = consumer;
        this.metrics = metrics;
        this.sourceClusterAlias = sourceClusterAlias;
        this.replicationPolicy = replicationPolicy;
        this.offsetSyncWriter = offsetSyncWriter;
        this.consumerAccess = new Semaphore(1);
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
                Thread.currentThread().getName(),
                taskTopicPartitions.size(),
                sourceClusterAlias,
                config.targetClusterAlias(),
                taskTopicPartitions);
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
        stopping = true;
        consumer.wakeup();
        try {
            consumerAccess.acquire();
        } catch (InterruptedException e) {
            log.warn("Interrupted while stopping", e);
        }
        Utils.closeQuietly(consumer, "consumer");
        Utils.closeQuietly(offsetSyncWriter, "offset sync writer");
        Utils.closeQuietly(metrics, "metrics");
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
            Set<TopicPartition> assignment = consumer.assignment();

            detectLogTruncation(assignment);
            handleTopicRecreation(assignment);

            List<SourceRecord> sourceRecords = new ArrayList<>(records.count());

            for (ConsumerRecord<byte[], byte[]> record : records) {

                detectTopicReset(record);

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
            }

            log.trace("Polled {} records from {}.",
                    sourceRecords.size(),
                    records.partitions());

            return sourceRecords;

        } catch (OffsetOutOfRangeException e) {
            log.warn("[MM2-FIX] OffsetOutOfRange detected, recovering", e);
            recoverFromTruncation();
            return Collections.emptyList();

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
        if (stopping) {
            return;
        }
        if (metadata == null || !metadata.hasOffset()) {
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
                    sourceTopicPartition,
                    upstreamOffset,
                    downstreamOffset);

            offsetSyncWriter.firePendingOffsetSyncs();
        }
    }

    // ================= FIX: LOG TRUNCATION =================
    private void detectLogTruncation(Set<TopicPartition> assignments) {
        for (TopicPartition tp : assignments) {
            try {
                Map<TopicPartition, OffsetAndMetadata> committedOffsets =
                        consumer.committed(Collections.singleton(tp));

                OffsetAndMetadata committed =
                        committedOffsets != null ? committedOffsets.get(tp) : null;

                if (committed == null) {
                    continue;
                }

                Map<TopicPartition, Long> beginningOffsets =
                        consumer.beginningOffsets(Collections.singleton(tp));

                Long beginning =
                        beginningOffsets != null ? beginningOffsets.get(tp) : null;

                if (beginning != null && committed.offset() < beginning) {
                    log.error("[MM2-FIX][TRUNCATION] topic={}, partition={}",
                            tp.topic(),
                            tp.partition());

                    consumer.seek(tp, beginning);
                }

            } catch (Exception e) {
                log.debug("Skipping truncation check for {}", tp, e);
            }
        }
    }

    // ================= FIX: TOPIC RESET =================
    private void detectTopicReset(ConsumerRecord<byte[], byte[]> record) {
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());

        long currentOffset = record.offset();
        Long previousOffset = lastSeenOffsets.get(tp);

        if (previousOffset != null && currentOffset < previousOffset) {
            log.error("[MM2-FIX][TOPIC-RESET] topic={}, partition={}",
                    tp.topic(),
                    tp.partition());

            try {
                consumer.seekToBeginning(Collections.singleton(tp));
            } catch (Exception e) {
                log.error("[MM2-FIX][RESET-FAILED]", e);
            }
        }

        lastSeenOffsets.put(tp, currentOffset);
    }

    private void recoverFromTruncation() {
        Set<TopicPartition> assignments = consumer.assignment();

        if (assignments == null || assignments.isEmpty()) {
            return;
        }

        try {
            consumer.seekToBeginning(assignments);
            log.warn("[MM2-FIX][RECOVERY]");
        } catch (Exception e) {
            log.error("[MM2-FIX][RECOVERY-FAILED]", e);
        }
    }

    private void handleTopicRecreation(Set<TopicPartition> currentAssignment) {
        if (previousAssignment.isEmpty() && !currentAssignment.isEmpty()) {
            try {
                consumer.seekToBeginning(currentAssignment);
                log.info("[MM2-FIX][TOPIC-RECREATED]");
            } catch (Exception e) {
                log.error("[MM2-FIX][RECREATION-FAILED]", e);
            }
        }

        previousAssignment = new HashSet<>(currentAssignment);
    }

    private Map<TopicPartition, Long> loadOffsets(Set<TopicPartition> topicPartitions) {
        return topicPartitions.stream()
                .collect(Collectors.toMap(x -> x, this::loadOffset));
    }

    private Long loadOffset(TopicPartition topicPartition) {
        Map<String, Object> wrappedPartition =
                MirrorUtils.wrapPartition(topicPartition, sourceClusterAlias);

        Map<String, Object> wrappedOffset =
                context.offsetStorageReader().offset(wrappedPartition);

        return MirrorUtils.unwrapOffset(wrappedOffset);
    }

    void initializeConsumer(Set<TopicPartition> taskTopicPartitions) {
        Map<TopicPartition, Long> offsets = loadOffsets(taskTopicPartitions);
        consumer.assign(offsets.keySet());

        offsets.forEach((tp, offset) -> {
            if (offset != null && offset >= 0) {
                consumer.seek(tp, offset + 1);
            }
        });
    }

    SourceRecord convertRecord(ConsumerRecord<byte[], byte[]> record) {
        String targetTopic = formatRemoteTopic(record.topic());
        Headers headers = convertHeaders(record);

        return new SourceRecord(
                MirrorUtils.wrapPartition(
                        new TopicPartition(record.topic(), record.partition()),
                        sourceClusterAlias),
                MirrorUtils.wrapOffset(record.offset()),
                targetTopic,
                record.partition(),
                Schema.OPTIONAL_BYTES_SCHEMA,
                record.key(),
                Schema.BYTES_SCHEMA,
                record.value(),
                record.timestamp(),
                headers);
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
}
