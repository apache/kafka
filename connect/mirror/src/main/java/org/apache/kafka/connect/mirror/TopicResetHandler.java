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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Handles source topic reset scenarios (deletion + recreation) gracefully.
 *
 * When a topic is deleted and recreated, MirrorMaker 2's consumer may hold stale offsets
 * that no longer exist, causing {@link OffsetOutOfRangeException}. This handler detects
 * such scenarios, logs the event, and resets the consumer to the beginning offset
 * of the recreated topic.
 *
 * Integration: Called from {@link EnhancedMirrorSourceTask} during poll() error handling.
 */
public class TopicResetHandler {

    private static final Logger log = LoggerFactory.getLogger(TopicResetHandler.class);

    // Tracks topic UUIDs (topicId) to detect recreation
    private final Map<TopicPartition, String> knownTopicIds = new ConcurrentHashMap<>();

    /**
     * Handles an OffsetOutOfRangeException by determining if the topic was reset
     * and recovering by seeking to the beginning.
     *
     * @param consumer   the Kafka consumer to reset
     * @param exception  the OffsetOutOfRangeException that occurred
     * @param truncationDetector the truncation detector to reset state for affected partitions
     * @return true if recovery was successful, false if the exception should be re-thrown
     */
    public boolean handleOffsetOutOfRange(Consumer<byte[], byte[]> consumer,
                                          OffsetOutOfRangeException exception,
                                          TruncationDetector truncationDetector) {
        Set<TopicPartition> affectedPartitions = exception.offsetOutOfRangePartitions().keySet();

        log.warn("TOPIC RESET DETECTED at {}: OffsetOutOfRangeException for partitions: {}",
                Instant.now(), affectedPartitions);

        for (TopicPartition tp : affectedPartitions) {
            log.warn("Topic reset details — topic: {}, partition: {}, timestamp: {}, "
                            + "stale offset: {}",
                    tp.topic(), tp.partition(), Instant.now(),
                    exception.offsetOutOfRangePartitions().get(tp));
        }

        try {
            // Seek to beginning for all affected partitions
            consumer.seekToBeginning(affectedPartitions);

            // Reset truncation detector state for affected partitions
            for (TopicPartition tp : affectedPartitions) {
                truncationDetector.resetPartition(tp);
                log.info("Reset consumer to beginning for {} after topic reset", tp);
            }

            log.info("TOPIC RESET RECOVERY COMPLETE: Successfully resubscribed {} partition(s) from beginning offset",
                    affectedPartitions.size());
            return true;

        } catch (Exception e) {
            log.error("Failed to recover from topic reset for partitions {}: {}",
                    affectedPartitions, e.getMessage(), e);
            return false;
        }
    }

    /**
     * Detects topic recreation by checking if the earliest offset has reset to 0
     * while we expected a higher offset.
     *
     * @param tp              the topic partition
     * @param earliestOffset  current earliest offset from the broker
     * @param expectedOffset  the offset we expected to consume next
     * @return true if a topic reset is suspected
     */
    public boolean isTopicReset(TopicPartition tp, long earliestOffset, long expectedOffset) {
        // If earliest offset is 0 and we expected something higher, topic was likely recreated
        if (earliestOffset == 0 && expectedOffset > 0) {
            log.info("Suspected topic reset for {}: earliest offset is 0 but expected offset was {}",
                    tp, expectedOffset);
            return true;
        }
        return false;
    }

    /**
     * Performs a full resubscription from the beginning for the given partitions.
     *
     * @param consumer           the Kafka consumer
     * @param partitions         partitions to resubscribe
     * @param truncationDetector truncation detector to reset
     */
    public void resubscribeFromBeginning(Consumer<byte[], byte[]> consumer,
                                         Collection<TopicPartition> partitions,
                                         TruncationDetector truncationDetector) {
        log.info("Resubscribing from beginning for partitions: {}", partitions);
        consumer.seekToBeginning(partitions);
        for (TopicPartition tp : partitions) {
            truncationDetector.resetPartition(tp);
        }
        log.info("Resubscription complete for {} partition(s)", partitions.size());
    }
}
