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
package org.apache.kafka.connect.mirror.handler;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.mirror.detector.TopicResetDetector;
import org.apache.kafka.connect.mirror.detector.TruncationDetector;
import org.apache.kafka.connect.mirror.tracking.PartitionOffsetTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * [CUSTOM] Orchestrates the two fault-tolerance checks that run before every poll cycle.
 *
 * <p>This handler is the single integration point between the patched
 * {@code MirrorSourceTask} and the two detector classes. It consults the
 * {@link PartitionOffsetTracker} to decide whether a partition is ready to be
 * inspected, delegates to the appropriate detector, and keeps the tracker in sync
 * when a topic reset is recovered from.
 *
 * <pre>
 * Poll cycle (per partition):
 *   1. checkForTruncation  — Task 2: throws RuntimeException on data loss
 *   2. checkForTopicReset  — Task 3: seeks to offset 0 on reset, returns true
 *      └─ if reset: update tracker to reflect the new starting position
 * </pre>
 */
public class FaultToleranceHandler {

    private static final Logger log = LoggerFactory.getLogger(FaultToleranceHandler.class);

    private final PartitionOffsetTracker tracker;
    private final TruncationDetector     truncationDetector;
    private final TopicResetDetector     resetDetector;

    public FaultToleranceHandler(KafkaConsumer<byte[], byte[]> consumer,
                                 PartitionOffsetTracker tracker) {
        this.tracker            = tracker;
        this.truncationDetector = new TruncationDetector(consumer);
        this.resetDetector      = new TopicResetDetector(consumer);
    }

    /**
     * Runs both fault-tolerance checks for the given partition.
     *
     * <p>This method is called by {@code MirrorSourceTask.poll()} for each assigned
     * partition before the actual {@link KafkaConsumer#poll} call.
     *
     * @param tp              the topic-partition to inspect
     * @param consumer        the live consumer (needed to read current position for reset check)
     * @throws RuntimeException if log truncation is detected (Task 2 fail-fast behaviour)
     */
    public void runChecks(TopicPartition tp, KafkaConsumer<byte[], byte[]> consumer) {
        if (!tracker.isTracked(tp)) {
            // First poll cycle for this partition — no bookmark yet, skip both checks
            return;
        }

        long nextExpected = tracker.getNextExpected(tp);

        // ── Task 2: Log Truncation Detection ──────────────────────────────────
        truncationDetector.check(tp, nextExpected);

        // ── Task 3: Topic Reset Detection + Recovery ───────────────────────────
        long currentPosition;
        try {
            currentPosition = consumer.position(tp);
        } catch (Exception e) {
            log.warn("[FaultToleranceHandler] Could not read consumer position for {}: {}", tp, e.getMessage());
            return;
        }

        boolean resetOccurred = resetDetector.checkAndRecover(tp, currentPosition);
        if (resetOccurred) {
            // The detector already seeked to 0; keep our tracker consistent so
            // the truncation check does not misfire on the next iteration.
            tracker.setNextExpected(tp, 0L);
        }
    }
}
