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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;

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

    // Tracks the highest leader epoch observed per source partition.
    // A decrease in epoch is the definitive signal that the partition
    // was deleted and recreated — impossible in normal Kafka operation.
    private final Map<TopicPartition, Integer> lastSeenLeaderEpoch = new HashMap<>();
    private final Map<TopicPartition, Long> lastKnownPosition = new HashMap<>();
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

        // ── NEW (Task 2): fail-fast before fetching if truncation detected ──
        try {
            checkForLogTruncation();
        }
        catch(LogTruncationException e){
            consumerAccess.release();
            throw e;
        }
        // ────────────────────────────────────────────────────────────────────

        ConsumerRecords<byte[], byte[]> records;
        try {
            records = consumer.poll(pollTimeout);
        } catch (OffsetOutOfRangeException e) {
            // ── NEW (Task 3): recover from topic reset via OOOR exception ─────
            // In Kafka 4.0.0, OffsetOutOfRangeException doesn't expose partition details.
            // We recover by seeking all assigned partitions to beginning.
            log.warn("OffsetOutOfRangeException caught: {}. Recovering all assigned partitions.",
                     e.getMessage());
            Set<TopicPartition> assignedPartitions = consumer.assignment();
            for (TopicPartition tp : assignedPartitions) {
                handleTopicReset(tp);
            }
            // Return empty list this cycle; next poll() fetches from offset 0
            consumerAccess.release();
            return Collections.emptyList();
            // ──────────────────────────────────────────────────────────────────
        }

        // ── NEW (Task 3): detect topic reset via leader epoch regression ──────
        detectTopicReset(records);
        // ──────────────────────────────────────────────────────────────────────

        List<SourceRecord> sourceRecords = new ArrayList<>(records.count());
        for (ConsumerRecord<byte[], byte[]> record : records) {
            SourceRecord converted = convertRecord(record);
            sourceRecords.add(converted);
            TopicPartition topicPartition = new TopicPartition(converted.topic(), converted.kafkaPartition());
            metrics.recordAge(topicPartition, System.currentTimeMillis() - record.timestamp());
            metrics.recordBytes(topicPartition, byteSize(record.value()));
        }
        if (sourceRecords.isEmpty()) {
            // WorkerSourceTasks expects non-zero batch size
            consumerAccess.release();
            return null;
        } else {
            log.trace("Polled {} records from {}.", sourceRecords.size(), records.partitions());
            consumerAccess.release();
            return sourceRecords;
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

    /**
     * Checks every assigned source partition for log truncation before
     * fetching any records.
     *
     * Algorithm:
     *   For each partition:
     *     nextFetchOffset     = consumer.position(tp)
     *                           (the offset MM2 will request on the next fetch)
     *     brokerBeginOffset   = consumer.beginningOffsets(tp)
     *                           (the earliest offset still available on the broker)
     *
     *     If brokerBeginOffset > nextFetchOffset:
     *       Messages [nextFetchOffset .. brokerBeginOffset - 1] are gone.
     *       Throw LogTruncationException (fail-fast).
     *
     * Why beginningOffsets() and not endOffsets()?
     *   endOffsets() returns the latest written offset — not relevant here.
     *   beginningOffsets() returns the earliest *still available* offset,
     *   which is exactly the boundary we compare against.
     *
     * Performance: beginningOffsets() is a metadata RPC, not a record fetch.
     * Brokers cache log start offsets in memory. This call takes ~1 ms and
     * is safe to issue on every poll() cycle.
     */
    private void checkForLogTruncation() {
        Set<TopicPartition> assignedPartitions = consumer.assignment();
        if (assignedPartitions.isEmpty()) {
            return;
        }

        // Single batched metadata RPC for all partitions
        Map<TopicPartition, Long> beginningOffsets =
                consumer.beginningOffsets(assignedPartitions);

        for (TopicPartition tp : assignedPartitions) {
            long nextFetchOffset   = consumer.position(tp);
            long brokerBeginOffset = beginningOffsets.getOrDefault(tp, 0L);

            // nextFetchOffset == 0 means MM2 has not replicated anything yet
            // for this partition. No gap is possible on the very first run.
            if (nextFetchOffset == 0) {
                continue;
            }

            if (brokerBeginOffset > nextFetchOffset) {
                long lostCount = brokerBeginOffset - nextFetchOffset;
                log.error(
                    "LOG TRUNCATION DETECTED — topic={} partition={} "
                    + "nextFetchOffset={} brokerBeginOffset={} lostMessages={}. "
                    + "Messages purged by retention before replication completed. "
                    + "Failing task to prevent silent data loss on DR cluster.",
                    tp.topic(), tp.partition(),
                    nextFetchOffset, brokerBeginOffset, lostCount
                );
                throw new LogTruncationException(
                    tp.topic(),
                    tp.partition(),
                    nextFetchOffset - 1,  // last offset MM2 successfully handled
                    brokerBeginOffset
                );
            }
        }
    }

    /**
     * Inspects the leader epoch embedded in every record of a polled batch.
     * A leader epoch lower than the last observed epoch for that partition
     * is unambiguous evidence the partition was deleted and recreated.
     *
     * Why check per-record instead of calling AdminClient.describeTopics()?
     *   - The leader epoch is already present in every ConsumerRecord at zero
     *     extra network cost.
     *   - An AdminClient call on every poll() cycle would add latency and
     *     a dependency on admin credentials.
     *   - The per-record check detects the reset on the very first record
     *     returned after MM2 resumes — no delay.
     *
     * Why leader epoch and not offset comparison?
     *   Offsets reset to 0 on topic recreation, but they also start at 0
     *   for a brand-new consumer with no prior checkpoint. Leader epoch is
     *   the only signal that is unambiguously tied to partition recreation.
     *
     * @param records the raw ConsumerRecords returned by consumer.poll()
     */
    private void detectTopicReset(ConsumerRecords<byte[], byte[]> records) {
        for (ConsumerRecord<byte[], byte[]> record : records) {
            TopicPartition tp=new TopicPartition(record.topic(), record.partition());
            // Older brokers (pre-2.4) may omit the leader epoch — skip gracefully
            if(record.leaderEpoch().isPresent()) {
                int currentEpoch = record.leaderEpoch().get();
                Integer knownEpoch = lastSeenLeaderEpoch.get(tp);
                if (knownEpoch != null && currentEpoch < knownEpoch) {
                    // Epoch went backwards — partition was deleted and recreated
                    log.warn(
                            "TOPIC RESET DETECTED — topic={} partition={} "
                                    + "previousEpoch={} currentEpoch={} detectedAt={}. "
                                    + "Partition was deleted and recreated. "
                                    + "Resubscribing from beginning offset.",
                            tp.topic(), tp.partition(),
                            knownEpoch, currentEpoch,
                            Instant.now()
                    );
                    handleTopicReset(tp);
                    lastKnownPosition.put(tp,0L);
                    continue;
                }

                // Always keep track of the highest epoch seen so far
                if (knownEpoch == null || currentEpoch > knownEpoch) {
                    lastSeenLeaderEpoch.put(tp, currentEpoch);
                }
            }
            Long lastPos=lastKnownPosition.get(tp);
            if(lastPos!=null && lastPos>0&&record.offset()==0){
                log.warn(
                        "TOPIC RESET DETECTED (offset reset to 0) - topic={} partition={}"
                        + "lastKnownOffset={} currentRecordOffset=0 detectedAt={}."
                        + "Broker silently reset consumer to beginning (auto.offset.reset)."
                        + "Resubscribing from beginning offset.",
                        tp.topic(), tp.partition(), lastPos, Instant.now()
                );
                handleTopicReset(tp);
                lastKnownPosition.put(tp,0L);
                continue;
            }
            lastKnownPosition.put(tp,record.offset());
        }
    }

    /**
     * Recovers from a confirmed topic reset by seeking the consumer to
     * the beginning of the recreated partition.
     *
     * Recovery steps:
     *  1. Seek the source consumer to offset 0 of the affected partition.
     *     The next poll() cycle will fetch the very first new message.
     *  2. Clear the stale leader-epoch entry for this partition so the
     *     new baseline epoch is learned cleanly from the next batch.
     *  3. Log the recovery action at INFO level so operators can confirm
     *     the event in the log trail.
     *
     * Why NOT fail-fast here (unlike log truncation)?
     *   Log truncation is an unplanned event causing irrecoverable data loss.
     *   A topic reset is a deliberate operational action — the operator
     *   intentionally cleared the source log. Auto-recovering keeps the DR
     *   cluster synchronized with the new source without requiring a manual
     *   task restart.
     *
     * Why clear the epoch tracking entry?
     *   After recovery the consumer position is 0 and the new epoch is
     *   unknown. Keeping the old epoch baseline would cause the next record
     *   (which will have epoch 0) to be misidentified as another reset.
     *   Removing the entry means the first record after recovery establishes
     *   the new baseline cleanly.
     *
     * @param tp the TopicPartition that was reset
     */
    private void handleTopicReset(TopicPartition tp) {
        // Seek the consumer to the very beginning of the recreated partition
        consumer.seekToBeginning(Collections.singletonList(tp));

        // Clear the stale epoch so the new baseline is learned on next poll
        lastSeenLeaderEpoch.remove(tp);

        log.info(
            "Topic reset recovery complete — topic={} partition={}. "
            + "Consumer seeked to offset 0. Replication will resume from start.",
            tp.topic(), tp.partition()
        );
    }
}
