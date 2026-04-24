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
package org.apache.kafka.connect.mirror.detector;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;

/**
 * [CUSTOM — Task 2] Log Truncation Detector (Fail-Fast).
 *
 * <h3>Problem</h3>
 * Kafka's retention policy may purge messages from the source topic <em>before</em>
 * MirrorMaker 2 has had a chance to replicate them. When this happens the default
 * MM2 behaviour is to silently skip ahead — creating an undetectable gap in the
 * replicated event stream (Write-Ahead Log).
 *
 * <h3>Detection Logic</h3>
 * <pre>
 *   lastLoadedOffset  = next offset MM2 expects to read  (our bookmark)
 *   beginningOffset   = earliest offset still on the broker
 *
 *   gap = beginningOffset − lastLoadedOffset
 *   if gap > 0  →  retention deleted messages we haven't replicated yet
 * </pre>
 *
 * <h3>Response</h3>
 * Log a detailed ERROR with the exact number of lost messages, then throw a
 * {@link RuntimeException} so the Connect framework marks the task FAILED and
 * operators are alerted immediately. Silent data loss is worse than a visible crash.
 */
public class TruncationDetector {

    private static final Logger log = LoggerFactory.getLogger(TruncationDetector.class);

    /** Any positive gap between MM2's bookmark and the broker's beginning offset = data loss. */
    private static final long GAP_THRESHOLD = 0L;

    private final KafkaConsumer<byte[], byte[]> consumer;

    public TruncationDetector(KafkaConsumer<byte[], byte[]> consumer) {
        this.consumer = consumer;
    }

    /**
     * Checks the given partition for evidence of log truncation.
     *
     * @param tp              the topic-partition to inspect
     * @param lastLoadedOffset the next offset MM2 expects to read (offset of last consumed record + 1)
     * @throws RuntimeException if truncation is detected, to halt replication immediately
     */
    public void check(TopicPartition tp, long lastLoadedOffset) {
        Map<TopicPartition, Long> beginningOffsets =
            consumer.beginningOffsets(Collections.singletonList(tp));

        Long beginningOffset = beginningOffsets.get(tp);
        if (beginningOffset == null) {
            log.warn("[Task2-Truncation] Could not fetch beginning offset for {}. Skipping check.", tp);
            return;
        }

        long gap = beginningOffset - lastLoadedOffset;

        if (gap > GAP_THRESHOLD) {
            log.error(
                "[Task2-Truncation] *** LOG TRUNCATION DETECTED — DATA LOSS ***\n" +
                "  Topic-Partition        : {}\n" +
                "  MM2 Expected Offset    : {}\n" +
                "  Topic Beginning Offset : {}\n" +
                "  Messages Lost          : {}\n" +
                "  Detected At            : {}\n" +
                "  Action                 : Failing fast to prevent silent data loss.",
                tp, lastLoadedOffset, beginningOffset, gap, Instant.now()
            );
            throw new RuntimeException(String.format(
                "Log truncation detected on %s: MM2 expected offset %d but topic begins at %d. " +
                "%d messages were lost due to retention policy.",
                tp, lastLoadedOffset, beginningOffset, gap
            ));
        }

        log.debug("[Task2-Truncation] OK — partition={} lastOffset={} beginningOffset={}",
                  tp, lastLoadedOffset, beginningOffset);
    }
}
