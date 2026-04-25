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

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Detects log truncation in source topics by tracking expected vs actual offsets.
 *
 * When Kafka retention policies purge messages before MirrorMaker 2 can replicate them,
 * a gap forms between the last replicated offset and the earliest available offset.
 * This detector identifies such gaps and triggers a fail-fast response.
 *
 * Integration: Called from {@link EnhancedMirrorSourceTask} during poll().
 */
public class TruncationDetector {

    private static final Logger log = LoggerFactory.getLogger(TruncationDetector.class);

    // Tracks the next expected offset per TopicPartition
    private final Map<TopicPartition, Long> expectedOffsets = new ConcurrentHashMap<>();

    /**
     * Updates the expected next offset after successfully processing a record.
     *
     * @param tp     the topic partition
     * @param offset the offset of the record just processed
     */
    public void updateExpectedOffset(TopicPartition tp, long offset) {
        expectedOffsets.put(tp, offset + 1);
    }

    /**
     * Checks whether the actual start offset of a partition indicates that log truncation
     * has occurred since the last replicated offset.
     *
     * @param tp                    the topic partition to check
     * @param earliestAvailableOffset the earliest available offset in the source topic
     * @throws LogTruncationException if truncation is detected
     */
    public void checkForTruncation(TopicPartition tp, long earliestAvailableOffset) {
        Long expectedOffset = expectedOffsets.get(tp);

        if (expectedOffset == null) {
            // First time seeing this partition — no baseline to compare
            log.debug("No expected offset tracked for {}. Skipping truncation check.", tp);
            return;
        }

        if (earliestAvailableOffset > expectedOffset) {
            long gapSize = earliestAvailableOffset - expectedOffset;
            String message = String.format(
                    "LOG TRUNCATION DETECTED on %s: expected next offset=%d, but earliest available offset=%d. "
                            + "Gap of %d messages lost due to retention policy. Failing fast to prevent silent data loss.",
                    tp, expectedOffset, earliestAvailableOffset, gapSize
            );
            log.error(message);
            throw new LogTruncationException(message, tp, expectedOffset, earliestAvailableOffset);
        }

        log.trace("Truncation check passed for {}: expectedOffset={}, earliestAvailable={}",
                tp, expectedOffset, earliestAvailableOffset);
    }

    /**
     * Returns the currently tracked expected offset for a partition, or -1 if not tracked.
     */
    public long getExpectedOffset(TopicPartition tp) {
        return expectedOffsets.getOrDefault(tp, -1L);
    }

    /**
     * Resets tracking state for a specific partition (e.g., after topic reset).
     */
    public void resetPartition(TopicPartition tp) {
        expectedOffsets.remove(tp);
        log.info("Reset truncation tracking for {}", tp);
    }

    /**
     * Resets all tracking state.
     */
    public void resetAll() {
        expectedOffsets.clear();
        log.info("Reset all truncation tracking state");
    }
}
