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
import java.util.Collections;
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
    private final Map<TopicPartition, Long> lastSeenOffsets = new HashMap<>();

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
            ConsumerRecords<byte[], byte[]> records = consumer.poll(pollTimeout);
            log.info("✅Polled {} records from {}.", records.count(), records.partitions());
            List<SourceRecord> sourceRecords = new ArrayList<>(records.count());

            for (TopicPartition tp : records.partitions()) {
                List<ConsumerRecord<byte[], byte[]>> partitionRecords = records.records(tp);

                for (ConsumerRecord<byte[], byte[]> record : partitionRecords) {

                    // 🔴 1. Detect forward-gap truncation (only detectable here)
                    detectTruncation(tp, record.offset());

                    // ✅ Update state AFTER validation
                    lastSeenOffsets.put(tp, record.offset());

                    SourceRecord converted = convertRecord(record);
                    sourceRecords.add(converted);

                    TopicPartition targetTp =
                            new TopicPartition(converted.topic(), converted.kafkaPartition());

                    metrics.recordAge(targetTp, System.currentTimeMillis() - record.timestamp());
                    metrics.recordBytes(targetTp, byteSize(record.value()));
                }
            }

            if (sourceRecords.isEmpty()) {
                return null;
            }

            log.trace("Polled {} records from {}.", sourceRecords.size(), records.partitions());
            return sourceRecords;

        } catch (OffsetOutOfRangeException e) {

            log.warn("[OFFSET_OUT_OF_RANGE] Detected 🔴. Evaluating scenario...", e);

            // 🟡 Decide whether truncation OR reset
            for (TopicPartition tp : e.partitions()) {

                Long lastOffset = lastSeenOffsets.get(tp);

                try {
                    long beginningOffset = consumer
                            .beginningOffsets(Collections.singleton(tp))
                            .get(tp);

                    log.warn("[OFFSET CHECK] tp={}, lastOffset={}, beginningOffset={}",
                            tp, lastOffset, beginningOffset);

                    // 🔴 CASE 1: TRUE TRUNCATION (data loss)
                    if (lastOffset != null && lastOffset < beginningOffset) {

                        log.error("[TRUNCATION DETECTED - OFFSET_OUT_OF_RANGE] tp={}, lastOffset={}, beginningOffset={}",
                                tp, lastOffset, beginningOffset);

                        throw new KafkaException(
                                "Fail-fast: Data loss detected due to truncation for " + tp
                                        + " lastOffset=" + lastOffset
                                        + " beginningOffset=" + beginningOffset
                        );
                    }

                    // 🟡 CASE 2: TOPIC RESET (safe recovery)
                    log.warn("[TOPIC RESET DETECTED] tp={}", tp);

                    consumer.seek(tp, beginningOffset);

                    // reset only this partition state
                    lastSeenOffsets.remove(tp);

                } catch (KafkaException ex) {
                    throw ex; // propagate fail-fast
                } catch (Exception ex) {
                    log.error("[RESET RECOVERY FAILED]", ex);
                    throw new KafkaException("Failed during offset recovery", ex);
                }
            }

            log.warn("[RESET RECOVERY SUCCESS] All affected partitions handled");

            return null; // retry next poll

        } catch (WakeupException e) {
            return null;

        } catch (KafkaException e) {
            log.error("[FAIL-FAST] Stopping MM2 due to critical issue", e);
            throw e;

        } catch (Throwable e) {
            log.error("Failure during poll.", e);
            throw e;

        } finally {
            consumerAccess.release();
        }
    }

    private void detectTruncation(TopicPartition tp, long currentOffset) {
        log.info("✅Checking offsets for tp={}, currentOffset={}, lastSeenOffset={}", tp, currentOffset, lastSeenOffsets.get(tp));
        Long lastOffset = lastSeenOffsets.get(tp);

        if (lastOffset == null) {
            return; // first record
        }

        // Normal case.
        if (currentOffset == lastOffset + 1) {
            return;
        }

        // Forward gap indicates data loss/truncation.
        if (currentOffset > lastOffset + 1) {
            log.error(
                "[TRUNCATION DETECTED - GAP] topic-partition={}, expectedOffset={}, actualOffset={}",
                tp, lastOffset + 1, currentOffset
            );

            throw new KafkaException(
                "Fail-fast: Offset gap detected for " + tp
                    + " expected=" + (lastOffset + 1)
                    + " actual=" + currentOffset
            );
        }

        if (currentOffset <= lastOffset) {
            log.warn(
                "[OFFSET REGRESSION] topic-partition={}, lastOffset={}, currentOffset={}",
                tp, lastOffset, currentOffset
            );

            throw new KafkaException(
                "Offset regression detected for " + tp
                    + ". Possible inconsistency."
            );
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
}
