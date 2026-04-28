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

import org.apache.kafka.clients.admin.Admin;
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
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;


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
    private MirrorSourceTaskConfig config;
    private boolean stopping = false;
    private Semaphore consumerAccess;
    private OffsetSyncWriter offsetSyncWriter;
    
    // Track expected offsets to detect log truncation and topic resets
    private final Map<TopicPartition, Long> expectedOffsets = new java.util.HashMap<>();

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
        this.config = new MirrorSourceTaskConfig(props);
        consumerAccess = new Semaphore(1);  // let one thread at a time access the consumer
        sourceClusterAlias = config.sourceClusterAlias();
        legacyMetrics = config.legacyMetrics();
        metrics = config.metrics(context.pluginMetrics());
        pollTimeout = config.consumerPollTimeout();
        replicationPolicy = config.replicationPolicy();
        if (config.emitOffsetSyncsEnabled()) {
            offsetSyncWriter = new OffsetSyncWriter(config);
        }
        Map<String, Object> consumerConfig = config.sourceConsumerConfig("replication-consumer");
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
                TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                // Check for data loss or topic reset
                if (expectedOffsets.containsKey(tp)) {
                    long expectedOffset = expectedOffsets.get(tp);
                    log.info("Checking record on {} at offset {}. Expected next offset: {}", 
                        tp, record.offset(), expectedOffset);

                    if (isDataLoss(record.offset(), expectedOffset)) {
                        log.error("**************************************************");
                        log.error("DATA LOSS DETECTED on {}! Expected offset {}, but got {}.", 
                            tp, expectedOffset, record.offset());
                        log.error("**************************************************");
                        throw new org.apache.kafka.connect.errors.ConnectException("Fail-fast: Data loss detected on " + tp);
                    } else if (isTopicReset(record.offset(), expectedOffset)) {
                        log.error("**************************************************");
                        log.error("TOPIC RESET DETECTED on {}! Expected offset {}, but got {}.", 
                            tp, expectedOffset, record.offset());
                        log.error("**************************************************");
                        log.info("Automatically resubscribing {} from the beginning.", tp);
                        consumer.seekToBeginning(java.util.Collections.singletonList(tp));
                        expectedOffsets.put(tp, 0L);
                        return null; // Force a re-poll from the new position
                    }
                } else {
                    log.info("First record seen for {}. Baselining expected offset at {}", tp, record.offset());
                }

                // Always update the expected offset for the next record
                expectedOffsets.put(tp, record.offset() + 1L);

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
        } catch (org.apache.kafka.clients.consumer.OffsetOutOfRangeException e) {
            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(e.offsetOutOfRangePartitions().keySet());
            log.warn("OffsetOutOfRange detected. Partitions: {}. Beginning offsets: {}", e.offsetOutOfRangePartitions().keySet(), beginningOffsets);
            for (TopicPartition tp : e.offsetOutOfRangePartitions().keySet()) {
                Long expected = expectedOffsets.get(tp);
                Long beginning = beginningOffsets.get(tp);
                if (expected != null && beginning != null && expected < beginning) {
                    log.error("DATA LOSS DETECTED: Log truncation on {}. Expected: {}, Earliest available: {}", tp, expected, beginning);
                    throw new org.apache.kafka.connect.errors.ConnectException("Log truncation detected on " + tp);
                } else {
                    log.warn("TOPIC RESET DETECTED (OffsetOutOfRange). Expected offset {} on {}. Resubscribing from beginning.", expected, tp);
                    consumer.seekToBeginning(java.util.Collections.singletonList(tp));
                    expectedOffsets.put(tp, 0L);
                }
            }
            return null; // Restart poll
        } catch (org.apache.kafka.clients.consumer.NoOffsetForPartitionException e) {
            log.info("No offset for partitions: {}. Seeking to beginning.", e.partitions());
            consumer.seekToBeginning(e.partitions());
            for (TopicPartition tp : e.partitions()) {
                expectedOffsets.put(tp, 0L); // Or beginning offset, but 0 is safe assuming it's the start
            }
            return null; // Restart poll
        } catch (WakeupException e) {
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
        return topicPartitions.stream().collect(Collectors.toMap(
            tp -> tp,
            tp -> {
                Map<String, Object> wrappedPartition = MirrorUtils.wrapPartition(tp, sourceClusterAlias);
                log.info("DIAGNOSTIC: Loading offsets for {} using alias {}. Wrapped: {}", tp.topic(), sourceClusterAlias, wrappedPartition);
                Map<String, Object> offsetMap = context.offsetStorageReader().offset(wrappedPartition);
                long offset = MirrorUtils.unwrapOffset(offsetMap);
                log.info("DIAGNOSTIC: Internal offset for {} is {}", tp.topic(), offset);
                
                if (offset < 0 && tp.topic().toLowerCase().contains("commit-log")) {
                    log.info("DIAGNOSTIC: Topic matches commit-log and offset is missing. Checking target cluster...");
                    long targetOffset = checkTargetOffset(tp);
                    if (targetOffset > 0) {
                        log.info("Found progress on target cluster: {}. Using as baseline.", targetOffset);
                        expectedOffsets.put(tp, targetOffset);
                        return targetOffset;
                    } else {
                        log.info("No progress found on target cluster. Expecting offset 0.");
                        expectedOffsets.put(tp, 0L);
                    }
                }
                return offset;
            }
        ));
    }

    private long checkTargetOffset(TopicPartition tp) {
        try {
            // This is a simplified check for the demonstration.
            // In a real system, we'd use the admin client or a specialized consumer.
            // For now, we'll try to find it via a one-off consumer if possible.
            Map<String, Object> targetProps = new HashMap<>(config.originals());
            // Target cluster is the 'standby' cluster in our setup.
            targetProps.put("bootstrap.servers", "standby-kafka:9094");
            try (KafkaConsumer<byte[], byte[]> targetConsumer = new KafkaConsumer<>(targetProps, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
                TopicPartition targetTp = new TopicPartition(sourceClusterAlias + "." + tp.topic(), tp.partition());
                Map<TopicPartition, Long> endOffsets = targetConsumer.endOffsets(java.util.Collections.singletonList(targetTp));
                return endOffsets.getOrDefault(targetTp, -1L);
            }
        } catch (Exception e) {
            log.warn("Could not check target cluster for offset: {}", e.getMessage());
            return -1L;
        }
    }

    // visible for testing
    void initializeConsumer(Set<TopicPartition> taskTopicPartitions) {
        Map<TopicPartition, Long> topicPartitionOffsets = loadOffsets(taskTopicPartitions);
        consumer.assign(topicPartitionOffsets.keySet());
        log.info("Starting with {} previously uncommitted partitions.", topicPartitionOffsets.values().stream()
                .filter(this::isUncommitted).count());

        topicPartitionOffsets.forEach((topicPartition, offset) -> {
            log.info("Loaded offset {} for topicPartition: {}", offset, topicPartition);
            // Do not call seek on partitions that don't have an existing offset committed.
            if (isUncommitted(offset)) {
                log.trace("Skipping seeking offset for topicPartition: {}", topicPartition);
                return;
            }
            long nextOffsetToCommittedOffset = offset + 1L;
            log.trace("Seeking to offset {} for topicPartition: {}", nextOffsetToCommittedOffset, topicPartition);
            consumer.seek(topicPartition, nextOffsetToCommittedOffset);
            expectedOffsets.put(topicPartition, nextOffsetToCommittedOffset);
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

    private boolean isDataLoss(long currentOffset, long expectedOffset) {
        return currentOffset > expectedOffset;
    }

    private boolean isTopicReset(long currentOffset, long expectedOffset) {
        return currentOffset < expectedOffset;
    }
}
