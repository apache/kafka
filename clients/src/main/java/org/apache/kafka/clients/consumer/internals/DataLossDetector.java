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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.DataLossException;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Utility class for detecting data loss scenarios in Kafka consumers.
 * This class implements various data loss detection mechanisms including:
 * - Offset gap detection
 * - Topic recreation detection  
 * - Out-of-range offset detection
 * - Silent data loss from retention policies
 * - False positive mitigation
 */
public class DataLossDetector {
    
    private final Logger log;
    private final Map<TopicPartition, Long> lastSeenOffsets = new HashMap<>();
    private final Map<TopicPartition, Long> topicGenerations = new HashMap<>();
    private final Map<TopicPartition, Long> lastValidatedTimestamp = new HashMap<>();
    private final Map<TopicPartition, PartitionMetadata> partitionMetadata = new HashMap<>();
    
    // Configuration for detection sensitivity
    private static final long VALIDATION_INTERVAL_MS = 30000; // 30 seconds
    private static final long MAX_OFFSET_GAP_THRESHOLD = 1000; // Configurable threshold
    private static final long TOPIC_RECREATION_GRACE_PERIOD_MS = 5000; // 5 seconds grace period
    
    public DataLossDetector(LogContext logContext) {
        this.log = logContext.logger(DataLossDetector.class);
    }
    
    /**
     * Enhanced metadata tracking for partitions
     */
    private static class PartitionMetadata {
        final long firstKnownOffset;
        final long lastKnownEndOffset;
        final long creationTimestamp;
        final boolean isNewTopic;
        
        PartitionMetadata(long firstKnownOffset, long lastKnownEndOffset, long creationTimestamp, boolean isNewTopic) {
            this.firstKnownOffset = firstKnownOffset;
            this.lastKnownEndOffset = lastKnownEndOffset;
            this.creationTimestamp = creationTimestamp;
            this.isNewTopic = isNewTopic;
        }
    }
    
    /**
     * Continuous validation check for silent data loss during normal operation.
     * This should be called periodically during normal consumption.
     * 
     * @param partition The topic partition to validate
     * @param currentOffset The current consumer position
     * @param beginningOffset Current beginning offset from broker
     * @param endOffset Current end offset from broker
     */
    public void validateContinuousDataIntegrity(TopicPartition partition, long currentOffset, 
                                               long beginningOffset, long endOffset) {
        long currentTime = System.currentTimeMillis();
        Long lastValidated = lastValidatedTimestamp.get(partition);
        
        // Only validate periodically to avoid performance impact
        if (lastValidated != null && (currentTime - lastValidated) < VALIDATION_INTERVAL_MS) {
            return;
        }
        
        lastValidatedTimestamp.put(partition, currentTime);
        
        // Check for silent data loss due to retention
        PartitionMetadata metadata = partitionMetadata.get(partition);
        if (metadata != null) {
            // Detect if beginning offset jumped significantly (retention purged data)
            if (beginningOffset > metadata.firstKnownOffset + MAX_OFFSET_GAP_THRESHOLD) {
                log.warn("Potential silent data loss detected in partition {} due to retention. " +
                        "Beginning offset jumped from {} to {}, gap: {}", 
                        partition, metadata.firstKnownOffset, beginningOffset, 
                        beginningOffset - metadata.firstKnownOffset);
                
                // Update metadata with new baseline
                partitionMetadata.put(partition, new PartitionMetadata(
                    beginningOffset, endOffset, currentTime, false));
            }
            
            // Detect if end offset went backwards (topic truncation/recreation)
            if (endOffset < metadata.lastKnownEndOffset) {
                handleSuspectedTopicRecreation(partition, metadata, beginningOffset, endOffset, currentTime);
            }
        } else {
            // First time seeing this partition - establish baseline
            partitionMetadata.put(partition, new PartitionMetadata(
                beginningOffset, endOffset, currentTime, true));
        }
        
        updateLastSeenOffset(partition, currentOffset);
    }
    
    /**
     * Handle suspected topic recreation with false positive mitigation
     */
    private void handleSuspectedTopicRecreation(TopicPartition partition, PartitionMetadata metadata,
                                              long beginningOffset, long endOffset, long currentTime) {
        // Apply grace period to avoid false positives during normal operations
        if (currentTime - metadata.creationTimestamp < TOPIC_RECREATION_GRACE_PERIOD_MS) {
            log.debug("Ignoring suspected topic recreation for {} within grace period", partition);
            return;
        }
        
        // Detect topic recreation patterns
        boolean likelyRecreation = (beginningOffset == 0 && endOffset < metadata.lastKnownEndOffset) ||
                                  (beginningOffset > metadata.firstKnownOffset);
        
        if (likelyRecreation) {
            log.warn("Topic recreation detected for partition {}. " +
                    "Previous range: [{}, {}], Current range: [{}, {}]",
                    partition, metadata.firstKnownOffset, metadata.lastKnownEndOffset,
                    beginningOffset, endOffset);
            
            // Update metadata for new topic generation
            partitionMetadata.put(partition, new PartitionMetadata(
                beginningOffset, endOffset, currentTime, true));
            
            // Clear old tracking data
            lastSeenOffsets.remove(partition);
            topicGenerations.put(partition, currentTime);
        }
    }
    
    /**
     * Enhanced data loss detection with edge case handling.
     * 
     * @param partition The topic partition being reset
     * @param oldOffset The previous offset (if any)
     * @param newOffset The new offset being set
     * @param beginningOffset The earliest available offset for the partition
     * @param endOffset The latest available offset for the partition
     * @throws DataLossException if data loss is detected
     */
    public void checkForDataLoss(TopicPartition partition, Long oldOffset, long newOffset, 
                                 long beginningOffset, long endOffset) {
        log.debug("Checking for data loss in partition {}: oldOffset={}, newOffset={}, beginningOffset={}, endOffset={}",
                partition, oldOffset, newOffset, beginningOffset, endOffset);
        
        long currentTime = System.currentTimeMillis();
        
        // Check for startup edge cases - be more lenient on first connection
        boolean isStartupScenario = !lastSeenOffsets.containsKey(partition);
        
        // Check for offset gap if we have a previous offset
        if (oldOffset != null) {
            checkOffsetGapWithEdgeCases(partition, oldOffset, newOffset, beginningOffset, endOffset, isStartupScenario);
        }
        
        // Check for topic recreation with false positive mitigation
        checkTopicRecreationWithValidation(partition, beginningOffset, currentTime);
        
        // Validate that the new offset is within reasonable bounds
        validateNewOffsetBounds(partition, newOffset, beginningOffset, endOffset);
        
        // Update tracking information
        lastSeenOffsets.put(partition, newOffset);
        topicGenerations.put(partition, beginningOffset);
        
        // Update partition metadata for continuous monitoring
        partitionMetadata.put(partition, new PartitionMetadata(
            beginningOffset, endOffset, currentTime, isStartupScenario));
    }
    
    /**
     * Enhanced offset gap detection with edge case handling.
     */
    private void checkOffsetGapWithEdgeCases(TopicPartition partition, long oldOffset, long newOffset, 
                                           long beginningOffset, long endOffset, boolean isStartupScenario) {
        // During startup, be more lenient with offset validation
        if (isStartupScenario) {
            log.debug("Startup scenario for partition {}, applying lenient validation", partition);
            
            // Only fail on extreme cases during startup
            if (oldOffset < beginningOffset - MAX_OFFSET_GAP_THRESHOLD) {
                log.warn("Large offset gap detected during startup for partition {}. " +
                        "Previous offset {} is significantly before beginning offset {}", 
                        partition, oldOffset, beginningOffset);
                // Don't throw exception during startup - just warn
                return;
            }
        }
        
        // If old offset is before beginning offset, check if it's due to retention or recreation
        if (oldOffset < beginningOffset) {
            long gap = beginningOffset - oldOffset;
            
            // For small gaps, might be normal retention
            if (gap <= MAX_OFFSET_GAP_THRESHOLD) {
                log.warn("Small offset gap detected for partition {} (gap: {}). " +
                        "Likely due to normal retention policy", partition, gap);
                return;
            }
            
            String details = String.format("Previous offset %d is before beginning offset %d (gap: %d)", 
                                          oldOffset, beginningOffset, gap);
            log.error("Data loss detected due to large offset gap for partition {}: {}", partition, details);
            throw new DataLossException(
                "Data loss detected: large offset gap indicates missing data", 
                Set.of(partition), 
                DataLossException.DataLossType.OFFSET_GAP,
                details
            );
        }
        
        // If old offset is beyond end offset, something is wrong
        if (oldOffset > endOffset) {
            String details = String.format("Previous offset %d is beyond end offset %d", oldOffset, endOffset);
            log.error("Data loss detected due to offset beyond end for partition {}: {}", partition, details);
            throw new DataLossException(
                "Data loss detected: previous offset beyond end offset", 
                Set.of(partition), 
                DataLossException.DataLossType.OUT_OF_RANGE,
                details
            );
        }
        
        // Check for significant gaps in normal operation
        if (newOffset > oldOffset + MAX_OFFSET_GAP_THRESHOLD) {
            String details = String.format("Large offset jump from %d to %d (gap: %d)", 
                                          oldOffset, newOffset, newOffset - oldOffset);
            log.warn("Large offset jump detected for partition {}: {}", partition, details);
            // This might be normal during catch-up, so just warn
        }
    }
    
    /**
     * Enhanced topic recreation detection with false positive mitigation.
     */
    private void checkTopicRecreationWithValidation(TopicPartition partition, long beginningOffset, long currentTime) {
        Long previousGeneration = topicGenerations.get(partition);
        
        if (previousGeneration != null) {
            // If beginning offset reset to 0 or jumped significantly, might be recreation
            boolean suspectedRecreation = (beginningOffset == 0 && previousGeneration > 0) ||
                                        (beginningOffset > previousGeneration + MAX_OFFSET_GAP_THRESHOLD);
            
            if (suspectedRecreation) {
                // Apply grace period to reduce false positives
                Long lastValidated = lastValidatedTimestamp.get(partition);
                if (lastValidated != null && (currentTime - lastValidated) < TOPIC_RECREATION_GRACE_PERIOD_MS) {
                    log.debug("Suspected topic recreation for {} within grace period, ignoring", partition);
                    return;
                }
                
                String details = String.format("Beginning offset changed from %d to %d", 
                                              previousGeneration, beginningOffset);
                log.error("Topic recreation detected for partition {}: {}", partition, details);
                throw new DataLossException(
                    "Data loss detected: topic appears to have been recreated", 
                    Set.of(partition), 
                    DataLossException.DataLossType.TOPIC_RECREATION,
                    details
                );
            }
        }
    }
    
    /**
     * Validate that the new offset is within reasonable bounds.
     */
    private void validateNewOffsetBounds(TopicPartition partition, long newOffset, 
                                       long beginningOffset, long endOffset) {
        if (newOffset < beginningOffset) {
            String details = String.format("New offset %d is before beginning offset %d", 
                                          newOffset, beginningOffset);
            log.error("Invalid new offset for partition {}: {}", partition, details);
            throw new DataLossException(
                "Data loss detected: new offset is out of range", 
                Set.of(partition), 
                DataLossException.DataLossType.OUT_OF_RANGE,
                details
            );
        }
        
        if (newOffset > endOffset) {
            String details = String.format("New offset %d is beyond end offset %d", 
                                          newOffset, endOffset);
            log.error("Invalid new offset for partition {}: {}", partition, details);
            throw new DataLossException(
                "Data loss detected: new offset beyond available data", 
                Set.of(partition), 
                DataLossException.DataLossType.OUT_OF_RANGE,
                details
            );
        }
    }
    
    /**
     * Update the last seen offset for a partition.
     */
    public void updateLastSeenOffset(TopicPartition partition, long offset) {
        lastSeenOffsets.put(partition, offset);
    }
    
    /**
     * Validates that the consumer's committed offset is still valid.
     */
    public void validateCommittedOffset(TopicPartition partition, OffsetAndMetadata offsetAndMetadata,
                                       long beginningOffset, long endOffset) {
        if (offsetAndMetadata == null) {
            return; // No committed offset to validate
        }
        
        long committedOffset = offsetAndMetadata.offset();
        
        if (committedOffset < beginningOffset || committedOffset > endOffset) {
            String details = String.format("Committed offset %d is outside valid range [%d, %d]", 
                                          committedOffset, beginningOffset, endOffset);
            log.error("Data loss detected due to invalid committed offset for partition {}: {}", partition, details);
            throw new DataLossException(
                "Data loss detected: committed offset is out of range", 
                Set.of(partition), 
                DataLossException.DataLossType.OUT_OF_RANGE,
                details
            );
        }
    }
    
    /**
     * Clears tracking information for a partition (e.g., when it's unassigned).
     */
    public void clearPartition(TopicPartition partition) {
        lastSeenOffsets.remove(partition);
        topicGenerations.remove(partition);
        log.debug("Cleared tracking information for partition {}", partition);
    }
    
    /**
     * Gets the last seen offset for a partition.
     */
    public Long getLastSeenOffset(TopicPartition partition) {
        return lastSeenOffsets.get(partition);
    }
}