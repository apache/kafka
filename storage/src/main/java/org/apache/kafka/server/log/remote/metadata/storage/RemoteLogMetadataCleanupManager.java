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
package org.apache.kafka.server.log.remote.metadata.storage;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class manages periodic cleanup of expired metadata keys from the remote log metadata topic.
 *
 * It runs a background thread that periodically:
 * 1. Consumes all records from __remote_log_metadata_compacted topic
 * 2. For each TopicIdPartition, checks if segment endOffset < current remote/global log start offset
 * 3. Tombstones all keys for expired segments (matching topicId:partition:endOffset:*)
 */
public class RemoteLogMetadataCleanupManager implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(RemoteLogMetadataCleanupManager.class);
    private static final long DEFAULT_CLEANUP_INTERVAL_MS = 60 * 60 * 1000L; // 1 hour

    private final TopicBasedRemoteLogMetadataManagerConfig rlmmConfig;
    private final RemotePartitionMetadataStore metadataStore;
    private final ProducerManager producerManager;
    private final LogStartOffsetProvider logStartOffsetProvider;
    private final Time time;
    private final long cleanupIntervalMs;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private volatile ScheduledExecutorService executorService;

    /**
     * Interface to provide the current log start offset for a TopicIdPartition.
     * This allows the cleanup manager to determine which segments are expired.
     */
    public interface LogStartOffsetProvider {
        /**
         * Returns the current log start offset for the given TopicIdPartition.
         * If the partition is not found or not managed, returns -1.
         */
        long getLogStartOffset(TopicIdPartition topicIdPartition);
    }

    public RemoteLogMetadataCleanupManager(TopicBasedRemoteLogMetadataManagerConfig rlmmConfig,
                                           RemotePartitionMetadataStore metadataStore,
                                           ProducerManager producerManager,
                                           LogStartOffsetProvider logStartOffsetProvider,
                                           Time time) {
        this(rlmmConfig, metadataStore, producerManager, logStartOffsetProvider, time, DEFAULT_CLEANUP_INTERVAL_MS);
    }

    public RemoteLogMetadataCleanupManager(TopicBasedRemoteLogMetadataManagerConfig rlmmConfig,
                                           RemotePartitionMetadataStore metadataStore,
                                           ProducerManager producerManager,
                                           LogStartOffsetProvider logStartOffsetProvider,
                                           Time time,
                                           long cleanupIntervalMs) {
        this.rlmmConfig = rlmmConfig;
        this.metadataStore = metadataStore;
        this.producerManager = producerManager;
        this.logStartOffsetProvider = logStartOffsetProvider;
        this.time = time;
        this.cleanupIntervalMs = cleanupIntervalMs;
    }

    /**
     * Starts the background cleanup task using a scheduled executor.
     */
    public void start() {
        if (executorService == null) {
            executorService = Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "remote-log-metadata-cleanup");
                thread.setDaemon(true);
                return thread;
            });

            log.info("Starting RemoteLogMetadataCleanupManager with interval {} ms", cleanupIntervalMs);

            // Schedule cleanup task at fixed rate
            executorService.scheduleAtFixedRate(
                this::runCleanupWithExceptionHandling,
                cleanupIntervalMs,  // initial delay
                cleanupIntervalMs,  // period
                TimeUnit.MILLISECONDS
            );
        }
    }

    /**
     * Runs cleanup with exception handling to prevent executor from stopping on errors.
     */
    private void runCleanupWithExceptionHandling() {
        if (closed.get()) {
            return;
        }

        try {
            long startTime = time.milliseconds();
            cleanupExpiredMetadata();
            long elapsed = time.milliseconds() - startTime;
            log.info("Completed metadata cleanup in {} ms", elapsed);
        } catch (Exception e) {
            log.error("Error during metadata cleanup, will retry after interval", e);
        }
    }

    /**
     * Main cleanup logic:
     * 1. Consume all records from __remote_log_metadata_compacted
     * 2. Group by TopicIdPartition and endOffset
     * 3. For each partition, check if endOffset < logStartOffset
     * 4. Tombstone all keys for expired segments
     */
    private void cleanupExpiredMetadata() {
        log.info("Starting periodic cleanup of expired metadata keys");

        // Map: TopicIdPartition -> (endOffset -> maxBrokerLeaderEpoch)
        Map<TopicIdPartition, Map<Long, Integer>> partitionToEndOffsetMaxEpoch = new HashMap<>();

        // Consume metadata topic to collect all endOffsets and their max brokerLeaderEpoch
        try (Consumer<String, byte[]> consumer = createCleanupConsumer()) {
            // Subscribe to all partitions of the metadata topic
            int numPartitions = rlmmConfig.metadataTopicPartitionsCount();
            Set<TopicPartition> partitions = new HashSet<>();
            for (int i = 0; i < numPartitions; i++) {
                partitions.add(new TopicPartition(rlmmConfig.remoteLogMetadataTopicName(), i));
            }
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            // Poll until we've consumed all records
            boolean reachedEnd = false;
            int emptyPollCount = 0;
            while (!reachedEnd && !closed.get()) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(1000));

                if (records.isEmpty()) {
                    emptyPollCount++;
                    // If we get 3 consecutive empty polls, assume we've reached the end
                    if (emptyPollCount >= 3) {
                        reachedEnd = true;
                    }
                } else {
                    emptyPollCount = 0;
                    for (ConsumerRecord<String, byte[]> record : records) {
                        // Skip tombstones (null values)
                        if (record.value() == null) {
                            continue;
                        }

                        // Parse the key: topicId:partition:endOffset:brokerLeaderEpoch
                        String key = record.key();
                        if (key == null) {
                            continue;
                        }

                        try {
                            ParsedMetadataKey parsedKey = parseMetadataKey(key);
                            if (parsedKey != null) {
                                TopicIdPartition tip = parsedKey.topicIdPartition;

                                // Track endOffset and its max brokerLeaderEpoch
                                partitionToEndOffsetMaxEpoch
                                    .computeIfAbsent(tip, k -> new HashMap<>())
                                    .merge(parsedKey.endOffset, parsedKey.brokerLeaderEpoch, Math::max);
                            }
                        } catch (Exception e) {
                            log.warn("Failed to parse metadata key: {}", key, e);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed to consume metadata topic for cleanup", e);
            return;
        }

        log.info("Found {} partitions with metadata to check", partitionToEndOffsetMaxEpoch.size());

        // For each partition, get current log start offset and tombstone expired segments
        int totalTombstoned = 0;
        for (Map.Entry<TopicIdPartition, Map<Long, Integer>> entry : partitionToEndOffsetMaxEpoch.entrySet()) {
            if (closed.get()) {
                break;
            }

            TopicIdPartition tip = entry.getKey();
            Map<Long, Integer> endOffsetToMaxEpoch = entry.getValue();

            // Get current log start offset for this partition
            long logStartOffset = logStartOffsetProvider.getLogStartOffset(tip);
            if (logStartOffset < 0) {
                // Partition not found or not managed, skip
                log.debug("Skipping partition {} - not found or not managed", tip);
                continue;
            }

            // Find expired endOffsets (endOffset < logStartOffset)
            for (Map.Entry<Long, Integer> endOffsetEntry : endOffsetToMaxEpoch.entrySet()) {
                long endOffset = endOffsetEntry.getKey();
                int maxBrokerLeaderEpoch = endOffsetEntry.getValue();

                if (endOffset < logStartOffset) {
                    // This segment is expired - tombstone all keys with this endOffset
                    log.info("Found expired segment for partition {} with endOffset {} < logStartOffset {}, maxBrokerLeaderEpoch={}",
                             tip, endOffset, logStartOffset, maxBrokerLeaderEpoch);

                    int tombstoned = tombstoneKeysForEndOffset(tip, endOffset, maxBrokerLeaderEpoch);
                    totalTombstoned += tombstoned;
                }
            }
        }

        log.info("Completed metadata cleanup, tombstoned {} keys", totalTombstoned);
    }

    /**
     * Tombstones all keys matching topicId:partition:endOffset:* with brokerLeaderEpoch <= maxBrokerLeaderEpoch.
     *
     * Uses the in-memory cache to efficiently query all segments with the given endOffset,
     * avoiding the need to scan the metadata topic again.
     *
     * @param tip the topic partition
     * @param endOffset the end offset of the expired segment
     * @param maxBrokerLeaderEpoch the maximum broker leader epoch to tombstone (inclusive)
     * @return number of keys tombstoned
     */
    private int tombstoneKeysForEndOffset(TopicIdPartition tip, long endOffset, int maxBrokerLeaderEpoch) {
        int tombstoned = 0;

        try {
            // Query in-memory cache directly - O(N) where N = segments for this partition with this endOffset
            // This is much faster than scanning the entire metadata topic partition from the beginning
            java.util.Iterator<String> toBeTombstonedKeys =
                metadataStore.listRemoteLogSegmentsByEndoffset(tip, endOffset, maxBrokerLeaderEpoch);

            while (toBeTombstonedKeys.hasNext()) {
                String metadataKey = toBeTombstonedKeys.next();
                log.debug("Tombstoning expired key: {}", metadataKey);

                producerManager.publishTombstone(tip, metadataKey)
                    .whenComplete((metadata, exception) -> {
                        if (exception != null) {
                            log.warn("Failed to tombstone key: {}. Error: {}", metadataKey, exception.getMessage());
                        } else {
                            log.debug("Successfully tombstoned key: {}", metadataKey);
                        }
                    });
                tombstoned++;
            }
        } catch (Exception e) {
            log.warn("Failed to query or tombstone keys for partition {} endOffset {}. Error: {}",
                     tip, endOffset, e.getMessage());
        }

        return tombstoned;
    }

    /**
     * Parses a metadata key in format: topicId:partition:endOffset:brokerLeaderEpoch
     */
    private ParsedMetadataKey parseMetadataKey(String key) {
        String[] parts = key.split(":");
        if (parts.length != 4) {
            return null; // Invalid key format
        }

        try {
            Uuid topicId = Uuid.fromString(parts[0]);
            int partition = Integer.parseInt(parts[1]);
            long endOffset = Long.parseLong(parts[2]);
            int brokerLeaderEpoch = Integer.parseInt(parts[3]);

            TopicIdPartition tip = new TopicIdPartition(topicId, new TopicPartition("", partition));
            return new ParsedMetadataKey(tip, endOffset, brokerLeaderEpoch);
        } catch (Exception e) {
            return null;
        }
    }

    private static class ParsedMetadataKey {
        final TopicIdPartition topicIdPartition;
        final long endOffset;
        final int brokerLeaderEpoch;

        ParsedMetadataKey(TopicIdPartition topicIdPartition, long endOffset, int brokerLeaderEpoch) {
            this.topicIdPartition = topicIdPartition;
            this.endOffset = endOffset;
            this.brokerLeaderEpoch = brokerLeaderEpoch;
        }
    }

    /**
     * Creates a consumer for reading the metadata topic during cleanup.
     * Uses a separate consumer group to avoid interfering with the main consumer.
     */
    private Consumer<String, byte[]> createCleanupConsumer() {
        Map<String, Object> consumerProps = new HashMap<>(rlmmConfig.consumerProperties());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "remote-log-metadata-cleanup-" + Uuid.randomUuid());
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(consumerProps);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            log.info("Closing RemoteLogMetadataCleanupManager");
            if (executorService != null) {
                executorService.shutdown();
                try {
                    if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                        log.warn("Executor did not terminate in time, forcing shutdown");
                        executorService.shutdownNow();
                        if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                            log.error("Executor did not terminate after forced shutdown");
                        }
                    }
                } catch (InterruptedException e) {
                    log.warn("Interrupted while waiting for executor to shutdown");
                    executorService.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
            log.info("RemoteLogMetadataCleanupManager closed");
        }
    }
}
