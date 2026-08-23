/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Turns an {@link OffsetOutOfRangeException} into a specific, actionable
 * failure: {@link TopicResetException} if the partition's earliest offset
 * has reset to zero (topic deleted and recreated), or
 * {@link DataLossException} if it has simply advanced past what we were
 * tracking (retention purged records ahead of replication).
 *
 * Pulled out of MirrorSourceTask so it can be unit tested without standing
 * up the whole task.
 */
final class OffsetResetClassifier {

    private static final Logger log = LoggerFactory.getLogger(OffsetResetClassifier.class);

    private OffsetResetClassifier() {
    }

    /**
     * @param exception          the exception thrown by consumer.poll()
     * @param beginningOffsetsOf resolves the current earliest offset for a
     *                           set of partitions, e.g. consumer::beginningOffsets
     */
    static ConnectException classify(OffsetOutOfRangeException exception,
                                      Function<Set<TopicPartition>, Map<TopicPartition, Long>> beginningOffsetsOf) {
        Map<TopicPartition, Long> requestedOffsets = exception.offsetOutOfRangePartitions();
        Map<TopicPartition, Long> earliestOffsets = beginningOffsetsOf.apply(requestedOffsets.keySet());

        ConnectException result = null;
        for (Map.Entry<TopicPartition, Long> entry : requestedOffsets.entrySet()) {
            TopicPartition partition = entry.getKey();
            long requestedOffset = entry.getValue();
            long earliestOffset = earliestOffsets.getOrDefault(partition, 0L);

            ConnectException current = earliestOffset == 0L
                    ? topicReset(partition, requestedOffset)
                    : dataLoss(partition, requestedOffset, earliestOffset);

            if (result == null) {
                result = current;
            }
        }
        return result;
    }

    private static TopicResetException topicReset(TopicPartition partition, long requestedOffset) {
        String message = String.format(
                "Topic reset detected for %s-%d: offset %d is no longer valid and the "
                        + "partition's earliest available offset is now 0. The source topic was "
                        + "most likely deleted and recreated.",
                partition.topic(), partition.partition(), requestedOffset);
        log.error(message);
        return new TopicResetException(message);
    }

    private static DataLossException dataLoss(TopicPartition partition, long requestedOffset, long earliestOffset) {
        String message = String.format(
                "Data loss detected for %s-%d: offset %d is no longer valid; the partition's "
                        + "earliest available offset has advanced to %d. Roughly %d record(s) were "
                        + "purged by the source topic's retention policy before replication.",
                partition.topic(), partition.partition(), requestedOffset, earliestOffset,
                earliestOffset - requestedOffset);
        log.error(message);
        return new DataLossException(message);
    }
}
