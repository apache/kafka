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
 * [CUSTOM — Task 3] Topic Reset Detector.
 *
 * <h3>Problem</h3>
 * Planned maintenance often involves deleting and recreating a Kafka topic
 * (a "topic reset"). After the reset the topic's offsets start from 0 again,
 * but MM2's consumer still holds its old position — a large number far beyond
 * the new topic's end offset. The vanilla MM2 either crashes with
 * {@code OffsetOutOfRangeException} or stalls indefinitely.
 *
 * <h3>Detection Logic</h3>
 * <pre>
 *   currentPosition = where the consumer will read next
 *   endOffset       = latest offset currently available in the (new) topic
 *
 *   if currentPosition > endOffset  →  topic was recreated; positions reset
 * </pre>
 *
 * <h3>Recovery</h3>
 * Log a WARN with full context, seek the consumer back to offset 0, and reset
 * the offset-tracking bookmark so {@link TruncationDetector} does not
 * immediately misfire. Replication continues seamlessly from the beginning of
 * the freshly created topic.
 */
public class TopicResetDetector {

    private static final Logger log = LoggerFactory.getLogger(TopicResetDetector.class);

    private final KafkaConsumer<byte[], byte[]> consumer;

    public TopicResetDetector(KafkaConsumer<byte[], byte[]> consumer) {
        this.consumer = consumer;
    }

    /**
     * Checks whether the topic-partition has been reset and, if so, auto-recovers.
     *
     * @param tp               the topic-partition to inspect
     * @param currentPosition  where the consumer currently expects to read
     * @return {@code true} if a reset was detected and the seek was performed,
     *         {@code false} if everything is normal
     */
    public boolean checkAndRecover(TopicPartition tp, long currentPosition) {
        Map<TopicPartition, Long> endOffsets =
            consumer.endOffsets(Collections.singletonList(tp));

        Long endOffset = endOffsets.get(tp);
        if (endOffset == null) {
            log.warn("[Task3-Reset] Could not fetch end offset for {}. Skipping check.", tp);
            return false;
        }

        if (currentPosition > endOffset) {
            log.warn(
                "[Task3-Reset] *** TOPIC RESET DETECTED ***\n" +
                "  Topic-Partition   : {}\n" +
                "  Consumer Position : {} (where MM2 expected to read next)\n" +
                "  Topic End Offset  : {} (topic has fewer messages than expected)\n" +
                "  Detected At       : {}\n" +
                "  Cause             : Source topic was deleted and recreated\n" +
                "  Action            : Resubscribing from offset 0",
                tp, currentPosition, endOffset, Instant.now()
            );

            consumer.seek(tp, 0L);

            log.info("[Task3-Reset] Successfully resubscribed {} from offset 0. Replication continues.", tp);
            return true;
        }

        log.debug("[Task3-Reset] OK — partition={} position={} endOffset={}",
                  tp, currentPosition, endOffset);
        return false;
    }
}
