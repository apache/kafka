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
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
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
    // Admin client used for topic ID lookups (Task 3: topic reset detection)
    private Admin sourceAdminClient;
    // Tracks the last known topic ID per topic name to detect topic reset (deletion + recreation)
    private final Map<String, Uuid> topicIds = new HashMap<>();
    private final Map<TopicPartition, Long> lastSeenOffsets = new HashMap<>();
    private Set<TopicPartition> assignedPartitions;
    private long lastTopicCheckTime = 0;
    private static final long TOPIC_CHECK_INTERVAL_MS = 5000;

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

    // for testing — includes admin client for topic reset detection tests
    MirrorSourceTask(KafkaConsumer<byte[], byte[]> consumer, MirrorSourceMetrics metrics, String sourceClusterAlias,
                     ReplicationPolicy replicationPolicy, 
                     OffsetSyncWriter offsetSyncWriter, Admin sourceAdminClient) {
        this(consumer, metrics, sourceClusterAlias, replicationPolicy, offsetSyncWriter);
        this.sourceAdminClient = sourceAdminClient;
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
        // Create admin client for source cluster topic ID lookups (Task 3: topic reset detection)
        sourceAdminClient = config.forwardingAdmin(config.sourceAdminConfig("replication-source-admin"));
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
        Utils.closeQuietly(sourceAdminClient, "source admin client");
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
            // Task 3: detect topic reset before each poll by comparing current topic IDs
            // against the IDs recorded at initialization time
            long now = System.currentTimeMillis();
            if (now - lastTopicCheckTime > TOPIC_CHECK_INTERVAL_MS) {
                detectAndHandleTopicReset();
                lastTopicCheckTime = now;
            }
            checkLogTruncationDuringPoll(ConsumerRecords.empty());
            detectOffsetResetBeforePoll();
            ConsumerRecords<byte[], byte[]> records = consumer.poll(pollTimeout);
            
            List<SourceRecord> sourceRecords = new ArrayList<>(records.count());
            for (ConsumerRecord<byte[], byte[]> record : records) {
                handleRecord(record, sourceRecords);
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
            log.debug("No RecordMetadata (source record was probably filtered out during transformation)"
                        + " -- can't sync offsets for {}.", record.topic());
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
        this.assignedPartitions = taskTopicPartitions;
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
        // Task 2: detect log truncation — check if retention has purged data that has not yet been replicated.
        // This check covers both the continuous-running case and the restart case, because we read the
        // last committed offset from the Connect offset store on every start/reconfiguration.
        //boolean hasValidCommittedOffsets = topicPartitionOffsets.values().stream().anyMatch(offset -> offset != null && offset >= 0);
        detectLogTruncation(topicPartitionOffsets);
        log.info("No log truncation detected during initialization for {} partitions.", topicPartitionOffsets.size());

        topicPartitionOffsets.forEach((topicPartition, offset) -> {
            if (!isUncommitted(offset)) {
                lastSeenOffsets.put(topicPartition, offset);
            }
        }); 

        // Task 3: record topic IDs at initialization so that poll() can detect topic reset later
        if (sourceAdminClient != null) {
            recordTopicIds(taskTopicPartitions);
        }
    }

    /**
     * Detects log truncation for all partitions that have a previously committed replication offset.
     *
     * Log truncation occurs when Kafka's retention policy purges messages from the source topic
     * before MirrorMaker 2 has had a chance to replicate them. This creates an undetectable gap in
     * the replicated data stream if left unchecked.
     *
     * For each partition with a committed offset, this method compares:
     * 
     *   The last committed replication offset (what MM2 last successfully replicated)</li>
     *   The current log start offset (earliest offset still available on the broker)</li>
     * 
     *
     * If {@code logStartOffset > lastCommittedOffset + 1}, messages in the range
     * {@code [lastCommittedOffset + 1, logStartOffset - 1]} have been purged and are permanently lost.
     *
     * @param topicPartitionOffsets map of topic-partition to last committed replication offset
     * @throws DataLossException if log truncation is detected on any partition
     */
    void detectLogTruncation(Map<TopicPartition, Long> topicPartitionOffsets) {
        // Only check partitions that have a committed offset — uncommitted partitions are starting
        // fresh and there is no gap to detect
        Set<TopicPartition> committedPartitions = topicPartitionOffsets.entrySet().stream()
                .filter(e -> !isUncommitted(e.getValue()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

        if (committedPartitions.isEmpty()) {
            return;
        }

        // beginningOffsets() returns the current logStartOffset for each partition —
        // the earliest offset that is still available on the broker after retention has run
        Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(committedPartitions);

        for (Map.Entry<TopicPartition, Long> entry : beginningOffsets.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            long logStartOffset = entry.getValue();
            long lastCommittedOffset = topicPartitionOffsets.get(topicPartition);
            // The next offset MM2 would attempt to replicate is lastCommittedOffset + 1.
            // If the broker's logStartOffset has advanced past that point, the intervening
            // messages have been permanently purged — this is silent data loss.
            if (logStartOffset > lastCommittedOffset + 1) {
                throw new DataLossException(
                    String.format(
                        "Log truncation detected for %s: last replicated offset was %d, "
                        + "but log start offset is now %d. Messages in range [%d, %d] "
                        + "have been permanently lost due to retention policy. "
                        + "Source cluster: %s.",
                        topicPartition,
                        lastCommittedOffset,
                        logStartOffset,
                        lastCommittedOffset + 1,
                        logStartOffset - 1,
                        sourceClusterAlias
                ));
            }
        }
    }

    private void detectOffsetResetBeforePoll() {
        for (TopicPartition tp : consumer.assignment()) {
            TopicPartition sourceTp = new TopicPartition(tp.topic(), tp.partition());
            Long previousOffset = lastSeenOffsets.get(sourceTp);
            if (previousOffset != null) {
                try {
                    long currentPosition = consumer.position(tp);
                    if (currentPosition < previousOffset) {
                        log.error("Topic reset detected for topic {}: {} -> {}", sourceTp, previousOffset, currentPosition);
                        consumer.seekToBeginning(Set.of(tp));
                        lastSeenOffsets.remove(sourceTp);
                    }
                } catch (KafkaException e) {
                    log.warn("Error checking offset for {}", tp);
                }
            }
        }
    }

    private void handleRecord(ConsumerRecord<byte[], byte[]> record, List<SourceRecord> sourceRecords) {
        String targetTopic = formatRemoteTopic(record.topic());
        TopicPartition targetTp = new TopicPartition(targetTopic, record.partition());
        TopicPartition sourceTp = new TopicPartition(record.topic(), record.partition());
        Long lastSeenOffset = lastSeenOffsets.get(targetTp);
        //RUNTIME TRUNCATION DETECTION
        if (lastSeenOffset != null && record.offset() > lastSeenOffset + 1) {
            log.error("OFFSET GAP DETECTED for {}: expected {} but found {}",
                    targetTp, lastSeenOffset + 1, record.offset());
            throw new DataLossException(
                String.format(
                    "Offset gap detected for %s: expected %d but found %d. "
                        + "This indicates log truncation or missing data.", 
                        targetTp, lastSeenOffset + 1, record.offset()));
        }
        // OFFSET RESET DETECTION
        if (lastSeenOffset != null && record.offset() < lastSeenOffset) {
            if (lastSeenOffsets.get(sourceTp) == null) {
                return;
            }
            log.warn("Topic reset detected for topic '{}' due to offset reset. "
                + "Previous offset: {}, new offset: {}",
                record.topic(), lastSeenOffset, record.offset());
            consumer.seekToBeginning(consumer.assignment().stream().filter(p -> 
                    p.topic().equals(record.topic())).collect(Collectors.toSet()));
            lastSeenOffsets.remove(targetTp);
            lastSeenOffsets.remove(sourceTp);
            return;
        }
        lastSeenOffsets.put(sourceTp, record.offset());
        lastSeenOffsets.put(targetTp, record.offset());
        SourceRecord converted = convertRecord(record);
        sourceRecords.add(converted);
        if (metrics != null) {
            metrics.recordAge(targetTp, System.currentTimeMillis() - record.timestamp());
            metrics.recordBytes(targetTp, byteSize(record.value()));
        }
    }
    /**
     * Records the current topic ID for each unique topic in the given set of topic-partitions.
     *
     * <p>Topic IDs are stable UUIDs assigned by Kafka at topic creation time. When a topic is
     * deleted and recreated, it receives a new UUID. This method seeds the baseline used by
     * {@link #detectAndHandleTopicReset()} to identify such resets during the poll loop.
     *
     * @param taskTopicPartitions the set of topic-partitions assigned to this task
     */
    void recordTopicIds(Set<TopicPartition> taskTopicPartitions) {
        Set<String> topics = taskTopicPartitions.stream()
                .map(TopicPartition::topic)
                .collect(Collectors.toSet());
        try {
            Map<String, KafkaFuture<TopicDescription>> futures =
                    sourceAdminClient.describeTopics(topics).topicNameValues();
            for (Map.Entry<String, KafkaFuture<TopicDescription>> entry : futures.entrySet()) {
                String topic = entry.getKey();
                try {
                    Uuid topicId = entry.getValue().get().topicId();
                    topicIds.put(topic, topicId);
                    log.debug("Recorded topic ID {} for topic {} on cluster {}.",
                            topicId, topic, sourceClusterAlias);
                } catch (ExecutionException e) {
                    log.warn("Could not retrieve topic ID for topic {} on cluster {}: {}",
                            topic, sourceClusterAlias, e.getCause().getMessage());
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while recording topic IDs for cluster {}.", sourceClusterAlias);
        }
    }

    /**
     * Detects topic reset (deletion and recreation) by comparing current topic IDs against
     * the IDs recorded at initialization time.
     *
     * <p>When a Kafka topic is deleted and recreated, Kafka assigns it a new UUID (topic ID).
     * MM2's stored offset refers to the old topic incarnation and is no longer valid for the
     * new topic. Without detection, MM2 would attempt to seek to a stale offset, causing it
     * to crash or stall indefinitely.
     *
     * <p>Upon detecting a topic ID change this method:
     * <ol>
     *   <li>Logs the reset event with timestamp and topic details</li>
     *   <li>Seeks the consumer to the beginning of all affected partitions</li>
     *   <li>Updates the stored topic ID to the new value</li>
     * </ol>
     *
     * <p>This allows MM2 to automatically recover and resume replication from the start of the
     * recreated topic without operator intervention.
     */
    void detectAndHandleTopicReset() {
        if (sourceAdminClient == null || topicIds.isEmpty()) {
            return;
        }
        try {
            Map<String, KafkaFuture<TopicDescription>> futures =
                    sourceAdminClient.describeTopics(topicIds.keySet()).topicNameValues();
            for (Map.Entry<String, KafkaFuture<TopicDescription>> entry : futures.entrySet()) {
                String topic = entry.getKey();
                try {
                    TopicDescription description = entry.getValue().get();
                    Uuid currentTopicId = description.topicId();
                    Uuid knownTopicId = topicIds.get(topic);
                    if (knownTopicId != null && !knownTopicId.equals(currentTopicId)) {
                        log.warn(
                                "Topic reset detected for topic '{}' on cluster '{}' at {}. "
                                + "Previous topic ID: {}, new topic ID: {}. "
                                + "Resubscribing from the beginning offset.",
                                topic,
                                sourceClusterAlias,
                                Instant.now(),
                                knownTopicId,
                                currentTopicId
                        );
                        // Seek all assigned partitions belonging to the reset topic back to the beginning
                        Collection<TopicPartition> assignedPartitions = consumer.assignment().stream()
                                .filter(tp -> tp.topic().equals(topic))
                                .collect(Collectors.toSet());
                        consumer.seekToBeginning(assignedPartitions);
                        // Update the stored topic ID so we don't trigger this again until the next reset
                        topicIds.put(topic, currentTopicId);
                        log.info("Resubscribed {} partition(s) of topic '{}' from offset 0 after topic reset.",
                                assignedPartitions.size(), topic);
                    }
                } catch (ExecutionException e) {
                    log.warn("Could not retrieve topic ID for topic {} on cluster {} during reset check: {}",
                            topic, sourceClusterAlias, e.getCause().getMessage());
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while checking for topic reset on cluster {}.", sourceClusterAlias);
        }
    }
    
    Map<String, Uuid> topicIds() {
        return topicIds;
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

    private void checkLogTruncationDuringPoll(ConsumerRecords<byte[], byte[]> records) {
        Set<TopicPartition> partitions = consumer.assignment();
        if (partitions.isEmpty()) {
            partitions = records.partitions();
        }
        if (partitions.isEmpty()) {
            partitions = lastSeenOffsets.keySet();
        }
        if (partitions.isEmpty()) {
            partitions = assignedPartitions;
        }
        if (partitions == null || partitions.isEmpty()) {
            return;
        }
        Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);
        for (Map.Entry<TopicPartition, Long> entry : beginningOffsets.entrySet()) {
            TopicPartition tp = entry.getKey();
            long logStartOffset = entry.getValue();
            TopicPartition remoteTp = new TopicPartition(formatRemoteTopic(tp.topic()), tp.partition());
            Long lastSeen = lastSeenOffsets.get(remoteTp);
            long compareOffset = (lastSeen != null) ? lastSeen + 1 : 0L;
            if (logStartOffset > compareOffset) {
                TopicPartition targetTp = new TopicPartition(formatRemoteTopic(tp.topic()), tp.partition());
                log.error("LOG TRUNCATION DETECTED during poll for {}: current position {} but log starts at {}",
                            targetTp, compareOffset, logStartOffset);
                throw new DataLossException(
                    String.format(
                            "Log truncation detected for %s during poll: expected offset %d but log starts at %d.",
                            targetTp, compareOffset, logStartOffset));
            }
        }
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
     * Thrown when log truncation is detected during replication offset validation.
     *
     * <p>This exception is raised when Kafka's retention policy has purged messages from the
     * source topic before MirrorMaker 2 has replicated them, creating an irrecoverable gap
     * in the replicated data stream. MM2 fails fast rather than silently producing an
     * incomplete replica.
     */
    public static class DataLossException extends RuntimeException {
        public DataLossException(String message) {
            super(message);
        }
    }
}