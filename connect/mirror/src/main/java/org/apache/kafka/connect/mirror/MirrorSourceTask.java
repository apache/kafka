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
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
// import org.apache.kafka.common.errors.OffsetOutOfRangeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
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
    private final java.util.Map<TopicPartition, Long> lastExpectedOffsets = new java.util.HashMap<>();

    private KafkaConsumer<byte[], byte[]> consumer;
    private String sourceClusterAlias;
    private Duration pollTimeout;
    private ReplicationPolicy replicationPolicy;
    private MirrorSourceLegacyMetrics legacyMetrics;
    private MirrorSourceMetrics metrics;
    private boolean stopping = false;
    private Semaphore consumerAccess;
    private OffsetSyncWriter offsetSyncWriter;

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
        if (!consumerAccess.tryAcquire()) return null;

        if (stopping) {
            consumerAccess.release();
            return null;
        }

        try {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(pollTimeout);
            // Validate partitions AFTER poll
            for (TopicPartition tp : consumer.assignment()) {
                long nextOffset = consumer.position(tp);
                Map<TopicPartition, Long> beginningOffsets =
                        consumer.beginningOffsets(Collections.singleton(tp));

                Map<TopicPartition, Long> endOffsets =
                        consumer.endOffsets(Collections.singleton(tp));

                long beginningOffset = beginningOffsets.get(tp);
                long endOffset = endOffsets.get(tp);

                verifyPartitionState(
                        tp,
                        nextOffset,
                        beginningOffset,
                        endOffset
                );
            }

            List<SourceRecord> sourceRecords = new ArrayList<>(records.count());
            for (ConsumerRecord<byte[], byte[]> record : records) {
                sourceRecords.add(convertRecord(record));
            }
            return sourceRecords.isEmpty() ? null : sourceRecords;

        } catch (org.apache.kafka.common.errors.OffsetOutOfRangeException e) {
            log.error("Source log truncation detected", e);
            throw new org.apache.kafka.connect.errors.ConnectException(
                    "Fail-Fast: Source log truncation detected.",
                    e
            );

        } finally {
            consumerAccess.release();
        }
    }

    // // Helper to keep poll() clean
    // private List<SourceRecord> processRecords(ConsumerRecords<byte[], byte[]> records) {
    //     List<SourceRecord> sourceRecords = new ArrayList<>(records.count());
    //     for (ConsumerRecord<byte[], byte[]> record : records) {
    //         sourceRecords.add(convertRecord(record));
    //     }
    //     return sourceRecords.isEmpty() ? null : sourceRecords;
    // }
    
    /*
    private void handleOffsetBreach(Set<TopicPartition> breachedPartitions) {
        if (breachedPartitions == null || breachedPartitions.isEmpty()) return;

        // Query the cluster for the current log boundaries of the affected partitions
        Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(breachedPartitions);
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(breachedPartitions);

        for (TopicPartition tp : breachedPartitions) {
            long beginningOffset = beginningOffsets.getOrDefault(tp, 0L);
            long endOffset = endOffsets.getOrDefault(tp, 0L);
            
            // Look up where our consumer was expecting to read from
            long currentPosition;
            try {
                currentPosition = consumer.position(tp);
            } catch (Exception e) {
                // Fallback if the position cannot be fetched during a heavy breach state
                currentPosition = -1;
            }
            
            // =================================================================
            // TASK 3: ADMINISTRATIVE RESET DETECTION (Topic Deletion & Recreation)
            // =================================================================
            // If the topic was reset, the log starts back at 0, but our 
            // tracking position is stranded in the future (past the new end offset).
            if (beginningOffset == 0 && currentPosition > endOffset) {
                log.warn("CRITICAL - Source topic reset detected for partition {}! (Current position: {}, Log End: {}). Automatically resubscribing from beginning offset (0).", 
                    tp, currentPosition, endOffset); // Satisfies Task 3 logging requirements 
                
                consumer.seek(tp, 0L); // Automatically aligns to offset 0 
                continue;
            }

            // =================================================================
            // TASK 2: LOG TRUNCATION DETECTION (Fail-Fast)
            // =================================================================
            // If the log start offset has moved past 0 and our expected position 
            // falls behind it, data was purged by retention before we could replicate it.
            if (beginningOffset > 0 && currentPosition < beginningOffset) {
                log.error("FATAL - Source log truncation detected for partition {}! Expected position {} is behind source log start offset {}. Failing fast.", 
                    tp, currentPosition, beginningOffset); // Satisfies Task 2 logging requirements 
                
                // Throw exception immediately to crash the container for visibility 
                throw new KafkaException("Source log truncation detected for " + tp + ". Failing fast to prevent silent data loss.");
            }
        }
    }
    */

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

    void initializeConsumer(Set<TopicPartition> taskTopicPartitions) {
        Map<TopicPartition, Long> topicPartitionOffsets = loadOffsets(taskTopicPartitions);
        
        // Use standard assign, no listener here
        consumer.assign(topicPartitionOffsets.keySet());
        
        topicPartitionOffsets.forEach((topicPartition, offset) -> {
            if (!isUncommitted(offset)) {
                consumer.seek(topicPartition, offset + 1L);
            }
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

    private void verifyPartitionState(TopicPartition tp, long nextOffset, long beginningOffset, long endOffset) {
        // 1. True Log Truncation (Scenario 2: Data was chopped out from underneath MM2)
        if (nextOffset < beginningOffset) {
            log.error("CRITICAL: Source log truncation detected for {}! MM2 position is {}, but log starts at {}.", 
                    tp, nextOffset, beginningOffset);
            throw new org.apache.kafka.connect.errors.ConnectException("Fail-Fast: Hard log truncation detected.");
        }

        // 2. True Topic Reset/Purge (Scenario 3: Topic was wiped clean, log reset back to 0)
        if (beginningOffset == 0 && nextOffset > endOffset) {
            log.warn("Detected intentional source topic purge/reset for {}. Re-aligning consumer position to 0L.", tp);
            consumer.seekToBeginning(Collections.singleton(tp));
        }
    }
}

