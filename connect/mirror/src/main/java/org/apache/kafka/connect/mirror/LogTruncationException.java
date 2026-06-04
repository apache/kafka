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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;

/**
 * Thrown by {@link MirrorSourceTask} when it detects that records on a source
 * topic-partition have been purged (truncated) before MirrorMaker 2 was able to
 * replicate them, resulting in an unrecoverable gap in the replicated stream.
 *
 * <p>Vanilla MirrorMaker 2 runs its source consumer with
 * {@code auto.offset.reset=earliest}, so when the broker's retention policy
 * deletes log segments that have not yet been mirrored, the consumer silently
 * jumps forward to the new log-start offset. The skipped records are never
 * replicated and no error is surfaced &mdash; a silent data-loss event.
 *
 * <p>This exception makes that condition explicit. It is intentionally a
 * subclass of {@link KafkaException} (an unchecked exception) so that it
 * propagates out of {@link MirrorSourceTask#poll()} and is handled by the
 * Kafka Connect framework as a task failure (fail-fast), rather than being
 * swallowed by the {@code KafkaException} catch clause inside {@code poll()}.
 */
public class LogTruncationException extends KafkaException {

    private static final long serialVersionUID = 1L;

    private final TopicPartition topicPartition;
    private final long expectedOffset;
    private final long logStartOffset;

    public LogTruncationException(TopicPartition topicPartition,
                                  long expectedOffset,
                                  long logStartOffset) {
        super(String.format(
                "Detected log truncation on source topic-partition %s: MirrorMaker 2 "
                        + "expected to read from offset %d but the earliest available "
                        + "offset on the source is now %d. %d record(s) were purged by the "
                        + "source retention policy before they could be replicated, "
                        + "creating an unrecoverable gap in the replicated stream. "
                        + "Failing fast to avoid silent data loss.",
                topicPartition, expectedOffset, logStartOffset,
                logStartOffset - expectedOffset));
        this.topicPartition = topicPartition;
        this.expectedOffset = expectedOffset;
        this.logStartOffset = logStartOffset;
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    public long expectedOffset() {
        return expectedOffset;
    }

    public long logStartOffset() {
        return logStartOffset;
    }
}
