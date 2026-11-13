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

import org.apache.kafka.clients.consumer.ConsumerConfig;
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
    private boolean failFastOnSourceOffsetLoss;
    private Semaphore consumerAccess;
    private OffsetSyncWriter offsetSyncWriter;

    public MirrorSourceTask() {}

    // for testing
    MirrorSourceTask(KafkaConsumer<byte[], byte[]> consumer, MirrorSourceLegacyMetrics metrics, String sourceClusterAlias,
                     ReplicationPolicy replicationPolicy,
                     OffsetSyncWriter offsetSyncWriter) {
        this(consumer, metrics, sourceClusterAlias, replicationPolicy, offsetSyncWriter,
                MirrorSourceConfig.OFFSET_VALIDATION_ENABLED_DEFAULT);
    }

    // for testing source offset handling
    MirrorSourceTask(KafkaConsumer<byte[], byte[]> consumer, MirrorSourceLegacyMetrics metrics, String sourceClusterAlias,
                     ReplicationPolicy replicationPolicy,
                     OffsetSyncWriter offsetSyncWriter,
                     boolean failFastOnSourceOffsetLoss) {
        this.consumer = consumer;
        this.legacyMetrics = metrics;
        this.sourceClusterAlias = sourceClusterAlias;
        this.replicationPolicy = replicationPolicy;
        this.failFastOnSourceOffsetLoss = failFastOnSourceOffsetLoss;
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
        failFastOnSourceOffsetLoss = config.offsetValidationEnabled();
        if (config.emitOffsetSyncsEnabled()) {
            offsetSyncWriter = new OffsetSyncWriter(config);
        }
        Map<String, Object> consumerConfig = config.sourceConsumerConfig("replication-consumer");
        if (failFastOnSourceOffsetLoss) {
            consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        }
        consumer = MirrorUtils.newConsumer(consumerConfig);
        Set<TopicPartition> taskTopicPartitions = config.taskTopicPartitions();
        initializeConsumer(taskTopicPartitions);

        log.info("{} replicating {} topic-partitions {}->{}: {}.", Thread.currentThread().getName(),
            taskTopicPartitions.size(), sourceClusterAlias, config.targetClusterAlias(), taskTopicPartitions);
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
                return null;
            } else {
                log.trace("Polled {} records from {}.", sourceRecords.size(), records.partitions());
                return sourceRecords;
            }
        } catch (WakeupException e) {
            return null;
        } catch (OffsetOutOfRangeException e) {
            if (failFastOnSourceOffsetLoss) {
                RuntimeException classified = classifyOffsetOutOfRange(e);
                if (classified instanceof TopicResetException) {
                    recoverFromTopicReset(e, (TopicResetException) classified);
                    return null;
                }
                throw classified;
            }
            log.warn("Failure during poll.", e);
            return null;
        } catch (KafkaException e) {
            log.warn("Failure during poll.", e);
            return null;
        } catch (Throwable e)  {
            log.error("Failure during poll.", e);
            throw e;
        } finally {
            consumerAccess.release();
        }
    }

    // visible for testing
    RuntimeException classifyOffsetOutOfRange(OffsetOutOfRangeException exception) {
        Map<TopicPartition, Long> requestedOffsets = exception.offsetOutOfRangePartitions();
        Map<TopicPartition, Long> earliestOffsets = consumer.beginningOffsets(requestedOffsets.keySet());
        List<String> dataLossDetails = new ArrayList<>();
        List<String> topicResetDetails = new ArrayList<>();
        List<String> unknownDetails = new ArrayList<>();

        requestedOffsets.forEach((topicPartition, requestedOffset) -> {
            Long earliestOffset = earliestOffsets.get(topicPartition);
            String detail = topicPartition + " requestedOffset=" + requestedOffset
                    + " earliestOffset=" + earliestOffset;
            if (earliestOffset == null || earliestOffset < 0L) {
                unknownDetails.add(detail);
            } else if (earliestOffset == 0L) {
                topicResetDetails.add(detail);
            } else {
                dataLossDetails.add(detail);
            }
        });

        if (!dataLossDetails.isEmpty() || !unknownDetails.isEmpty()) {
            String message = "Source data loss detected while replicating " + String.join(", ", dataLossDetails)
                    + (unknownDetails.isEmpty() ? "" : "; unable to classify safely: " + String.join(", ", unknownDetails))
                    + (topicResetDetails.isEmpty() ? "" : "; topic reset also detected on: " + String.join(", ", topicResetDetails));
            log.error(message, exception);
            return new DataLossException(message, exception);
        }

        String message = "Source topic reset detected while replicating " + String.join(", ", topicResetDetails);
        return new TopicResetException(message, exception);
    }

    private void recoverFromTopicReset(OffsetOutOfRangeException exception, TopicResetException topicReset) {
        Set<TopicPartition> resetPartitions = exception.offsetOutOfRangePartitions().keySet();
        try {
            consumer.seekToBeginning(resetPartitions);
            log.warn("SOURCE_TOPIC_RESET_RECOVERED: {}. Repositioned {} to the earliest offset; replication will resume "
                    + "on the next poll without restarting the MirrorSourceTask.", topicReset.getMessage(), resetPartitions);
        } catch (KafkaException recoveryFailure) {
            String message = "Source topic reset was detected but automatic recovery failed for " + resetPartitions;
            log.error(message, recoveryFailure);
            throw new TopicResetException(message, recoveryFailure);
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
        if (offsetSyncWriter != null) {
            TopicPartition sourceTopicPartition = MirrorUtils.unwrapPartition(record.sourcePartition());
            long upstreamOffset = MirrorUtils.unwrapOffset(record.sourceOffset());
            long downstreamOffset = metadata.offset();
            offsetSyncWriter.maybeQueueOffsetSyncs(sourceTopicPartition, upstreamOffset, downstreamOffset);
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
            if (isUncommitted(offset)) {
                if (failFastOnSourceOffsetLoss) {
                    log.trace("Seeking uncommitted topicPartition {} to the beginning", topicPartition);
                    consumer.seekToBeginning(Set.of(topicPartition));
                } else {
                    log.trace("Skipping seeking offset for topicPartition: {}", topicPartition);
                }
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
}
