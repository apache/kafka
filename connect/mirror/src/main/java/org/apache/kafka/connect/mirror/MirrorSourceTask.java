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
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import static org.apache.kafka.connect.mirror.MirrorConnectorConfig.METRIC_NAMES_LEGACY;
import static org.apache.kafka.connect.mirror.MirrorConnectorConfig.METRIC_NAMES_NEW;

/** Replicates a set of topic-partitions. */
public class MirrorSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(MirrorSourceTask.class);

    private Consumer<byte[], byte[]> consumer;
    private String sourceClusterAlias;
    private Duration pollTimeout = Duration.ofMillis(100);
    private ReplicationPolicy replicationPolicy;
    private MirrorSourceLegacyMetrics legacyMetrics;
    private MirrorSourceMetrics metrics;
    private boolean stopping = false;
    private Semaphore consumerAccess;
    private OffsetSyncWriter offsetSyncWriter;

    private Admin sourceAdmin;
    private final Map<String, Uuid> knownTopicIds = new HashMap<>();
    private final ReplicationFailureClassifier failureClassifier = new ReplicationFailureClassifier();
    private static final String TOPIC_ID_OFFSET_KEY = "mm2_fault_tolerance_topic_id";

    // Configurable via the "topic.reset.behavior" task property (fail-fast default, self-heal opt-in).
    static final String RESET_BEHAVIOR_CONFIG = "topic.reset.behavior";
    static final String RESET_BEHAVIOR_FAIL_FAST = "fail-fast";
    static final String RESET_BEHAVIOR_SELF_HEAL = "self-heal";
    private String resetBehavior = RESET_BEHAVIOR_FAIL_FAST;

    public MirrorSourceTask() {}

    // for testing
    MirrorSourceTask(Consumer<byte[], byte[]> consumer, MirrorSourceLegacyMetrics metrics, String sourceClusterAlias,
                     ReplicationPolicy replicationPolicy,
                     OffsetSyncWriter offsetSyncWriter) {
        this.consumer = consumer;
        this.legacyMetrics = metrics;
        this.sourceClusterAlias = sourceClusterAlias;
        this.replicationPolicy = replicationPolicy;
        consumerAccess = new Semaphore(1);
        this.offsetSyncWriter = offsetSyncWriter;
    }

    // for testing
    MirrorSourceTask(Consumer<byte[], byte[]> consumer, MirrorSourceLegacyMetrics metrics, String sourceClusterAlias,
                     ReplicationPolicy replicationPolicy, OffsetSyncWriter offsetSyncWriter, Admin sourceAdmin) {
        this(consumer, metrics, sourceClusterAlias, replicationPolicy, offsetSyncWriter);
        this.sourceAdmin = sourceAdmin;
    }

    // visible for testing -- seeds the topic-ID cache directly, bypassing start()/primeKnownTopicIdsAndDetectResets
    void seedKnownTopicId(String topic, Uuid topicId) {
        knownTopicIds.put(topic, topicId);
    }

    // visible for testing -- sets resetBehavior directly, bypassing start()/resolveResetBehavior
    void setResetBehaviorForTesting(String resetBehavior) {
        this.resetBehavior = resetBehavior;
    }

    @Override
    public void start(Map<String, String> props) {
        resetBehavior = resolveResetBehavior(props);

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

        // "earliest" would silently skip purged offsets; "none" makes that throw instead.
        Map<String, Object> consumerConfig = config.sourceConsumerConfig("replication-consumer");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        consumer = MirrorUtils.newConsumer(consumerConfig);

        if (sourceAdmin == null) {
            sourceAdmin = Admin.create(config.sourceAdminConfig("fault-tolerance-admin"));
        }

        Set<TopicPartition> taskTopicPartitions = config.taskTopicPartitions();
        // Check topic identity before seeking to any stored offset, not only reactively
        // on exception -- otherwise a coincidental offset match after a reset never throws.
        Set<TopicPartition> preexistingResets = primeKnownTopicIdsAndDetectResets(taskTopicPartitions);
        initializeConsumer(taskTopicPartitions, preexistingResets);

        log.info("{} replicating {} topic-partitions {}->{}: {}. Reset behavior: {}.", Thread.currentThread().getName(),
                taskTopicPartitions.size(), sourceClusterAlias, config.targetClusterAlias(), taskTopicPartitions, resetBehavior);
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
        Utils.closeQuietly(sourceAdmin, "source admin client");
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
        } catch (NoOffsetForPartitionException e) {
            // First-ever run for this partition, not data loss -- start from the beginning.
            log.info("No previously committed offset for {} -- first replication run for "
                    + "these partitions. Starting from the beginning.", e.partitions());
            consumer.seekToBeginning(e.partitions());
            return null;
        } catch (OffsetOutOfRangeException e) {
            // handleOffsetOutOfRange tells truncation and reset apart, and either throws
            // (DataLossException always; TopicResetException if resetBehavior is fail-fast)
            // or resubscribes from the beginning (reset, only if resetBehavior is self-heal).
            handleOffsetOutOfRange(e);
            return null;
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

    /**
     * Runs at startup, before seeking. Compares each partition's durable topic ID
     * against a fresh one; throws or flags for reset-seeking depending on resetBehavior.
     */
    private Set<TopicPartition> primeKnownTopicIdsAndDetectResets(Set<TopicPartition> topicPartitions) {
        Set<String> allTopics = topicPartitions.stream().map(TopicPartition::topic).collect(Collectors.toSet());
        Map<String, Uuid> currentIds = fetchTopicIds(allTopics);

        Set<TopicPartition> resetPartitions = new HashSet<>();
        for (TopicPartition tp : topicPartitions) {
            Uuid durable = loadKnownTopicId(tp);
            Uuid current = currentIds.get(tp.topic());

            if (durable != null
                    && failureClassifier.classify(durable, current) == ReplicationFailureClassifier.Decision.TOPIC_RESET) {
                if (RESET_BEHAVIOR_SELF_HEAL.equals(resetBehavior)) {
                    log.warn("Detected topic reset for {} at startup: topic ID changed from {} to {}. "
                            + "Resubscribing from the beginning (self-heal mode).", tp, durable, current);
                    resetPartitions.add(tp);
                } else {
                    log.error("Detected topic reset for {} at startup: topic ID changed from {} to {}. "
                                    + "Failing fast (set {}={} to auto-recover instead).",
                            tp, durable, current, RESET_BEHAVIOR_CONFIG, RESET_BEHAVIOR_SELF_HEAL);
                    throw new TopicResetException(String.format(
                            "Topic reset detected on %s at startup: topic ID changed from %s to %s.",
                            tp, durable, current));
                }
            }
            Uuid toCache = current != null ? current : durable;
            if (toCache != null) {
                knownTopicIds.put(tp.topic(), toCache);
            }
        }
        return resetPartitions;
    }

    /** Resolves the topic.reset.behavior property, defaulting to fail-fast. */
    static String resolveResetBehavior(Map<String, String> props) {
        String value = props.getOrDefault(RESET_BEHAVIOR_CONFIG, RESET_BEHAVIOR_FAIL_FAST);
        if (!RESET_BEHAVIOR_FAIL_FAST.equals(value) && !RESET_BEHAVIOR_SELF_HEAL.equals(value)) {
            log.warn("Unrecognized {}='{}', defaulting to '{}'.", RESET_BEHAVIOR_CONFIG, value, RESET_BEHAVIOR_FAIL_FAST);
            return RESET_BEHAVIOR_FAIL_FAST;
        }
        return value;
    }

    private Map<String, Uuid> fetchTopicIds(Set<String> topics) {
        Map<String, Uuid> result = new HashMap<>();
        if (topics.isEmpty()) {
            return result;
        }
        try {
            Map<String, org.apache.kafka.common.KafkaFuture<TopicDescription>> futures =
                    sourceAdmin.describeTopics(topics).topicNameValues();
            for (Map.Entry<String, org.apache.kafka.common.KafkaFuture<TopicDescription>> entry : futures.entrySet()) {
                try {
                    result.put(entry.getKey(), entry.getValue().get().topicId());
                } catch (ExecutionException | InterruptedException e) {
                    log.warn("Could not fetch topic ID for {} while checking for topic reset.", entry.getKey(), e);
                }
            }
        } catch (KafkaException e) {
            log.warn("Could not describe topics {} while checking for topic reset.", topics, e);
        }
        return result;
    }

    /**
     * Distinguishes truncation from topic reset. Truncation always throws
     * {@link DataLossException}. Reset throws {@link TopicResetException} (fail-fast,
     * default) or resubscribes from the beginning (self-heal, opt-in).
     */
    private void handleOffsetOutOfRange(OffsetOutOfRangeException e) {
        Set<TopicPartition> affected = e.offsetOutOfRangePartitions().keySet();
        Set<String> affectedTopics = affected.stream().map(TopicPartition::topic).collect(Collectors.toSet());
        Map<String, Uuid> currentTopicIds = fetchTopicIds(affectedTopics);

        List<TopicPartition> resetPartitions = new ArrayList<>();

        for (TopicPartition tp : affected) {
            long requestedOffset = e.offsetOutOfRangePartitions().get(tp);
            Uuid previousId = knownTopicIds.get(tp.topic());
            Uuid currentId = currentTopicIds.get(tp.topic());

            ReplicationFailureClassifier.Decision decision = failureClassifier.classify(previousId, currentId);

            if (decision == ReplicationFailureClassifier.Decision.TOPIC_RESET) {
                if (RESET_BEHAVIOR_SELF_HEAL.equals(resetBehavior)) {
                    log.warn("Detected topic reset for {}-{}: topic ID changed from {} to {} at {}. "
                                    + "Resubscribing from the beginning offset (self-heal mode).",
                            tp.topic(), tp.partition(), previousId, currentId, Instant.now());
                    resetPartitions.add(tp);
                    knownTopicIds.put(tp.topic(), currentId);
                } else {
                    log.error("Detected topic reset for {}-{}: topic ID changed from {} to {}. "
                                    + "Failing fast (set {}={} to auto-recover instead).",
                            tp.topic(), tp.partition(), previousId, currentId, RESET_BEHAVIOR_CONFIG, RESET_BEHAVIOR_SELF_HEAL);
                    throw new TopicResetException(String.format(
                            "Topic reset detected on %s-%s: topic ID changed from %s to %s.",
                            tp.topic(), tp.partition(), previousId, currentId));
                }
            } else {
                long earliest = safeBeginningOffset(tp);
                log.error("Detected unrecoverable data loss for {}-{}: next required offset {} is no longer "
                                + "available on the source cluster (earliest available offset is now {}). Failing fast.",
                        tp.topic(), tp.partition(), requestedOffset, earliest);
                throw new DataLossException(String.format(
                        "Data loss detected on %s-%s: required offset %d but earliest available is %d. "
                                + "Data was purged by retention before MirrorMaker 2 replicated it.",
                        tp.topic(), tp.partition(), requestedOffset, earliest));
            }
        }

        if (!resetPartitions.isEmpty()) {
            consumer.seekToBeginning(resetPartitions);
        }
    }

    private long safeBeginningOffset(TopicPartition tp) {
        try {
            Map<TopicPartition, Long> beginning = consumer.beginningOffsets(Collections.singletonList(tp));
            Long offset = beginning.get(tp);
            return offset == null ? -1L : offset;
        } catch (KafkaException e) {
            log.warn("Could not fetch beginning offset for {} while reporting data loss.", tp, e);
            return -1L;
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

    /** Reads the topic UUID stamped on the last committed offset, if any -- survives restarts unlike {@link #knownTopicIds}. */
    private Uuid loadKnownTopicId(TopicPartition topicPartition) {
        Map<String, Object> wrappedPartition = MirrorUtils.wrapPartition(topicPartition, sourceClusterAlias);
        Map<String, Object> wrappedOffset = context.offsetStorageReader().offset(wrappedPartition);
        if (wrappedOffset == null) {
            return null;
        }
        Object stored = wrappedOffset.get(TOPIC_ID_OFFSET_KEY);
        if (stored == null) {
            return null;
        }
        try {
            return Uuid.fromString(stored.toString());
        } catch (IllegalArgumentException e) {
            log.warn("Could not parse stored topic ID '{}' for {}; treating as unknown.", stored, topicPartition, e);
            return null;
        }
    }

    // visible for testing -- kept for Kafka's own MirrorSourceTaskTest#testSeekBehaviorDuringStart
    void initializeConsumer(Set<TopicPartition> taskTopicPartitions) {
        initializeConsumer(taskTopicPartitions, Collections.emptySet());
    }

    // visible for testing
    void initializeConsumer(Set<TopicPartition> taskTopicPartitions, Set<TopicPartition> resetPartitions) {
        Map<TopicPartition, Long> topicPartitionOffsets = loadOffsets(taskTopicPartitions);
        consumer.assign(topicPartitionOffsets.keySet());
        log.info("Starting with {} previously uncommitted partitions.", topicPartitionOffsets.values().stream()
                .filter(this::isUncommitted).count());

        topicPartitionOffsets.forEach((topicPartition, offset) -> {
            if (resetPartitions.contains(topicPartition)) {
                log.trace("Seeking {} to the beginning due to a topic reset detected at startup.", topicPartition);
                consumer.seekToBeginning(Collections.singletonList(topicPartition));
                return;
            }
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

        // Stamp the current topic ID onto the committed offset so it survives restarts.
        Map<String, Object> sourceOffset = new HashMap<>(MirrorUtils.wrapOffset(record.offset()));
        Uuid currentTopicId = knownTopicIds.get(record.topic());
        if (currentTopicId != null) {
            sourceOffset.put(TOPIC_ID_OFFSET_KEY, currentTopicId.toString());
        }

        return new SourceRecord(
                MirrorUtils.wrapPartition(new TopicPartition(record.topic(), record.partition()), sourceClusterAlias),
                sourceOffset,
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