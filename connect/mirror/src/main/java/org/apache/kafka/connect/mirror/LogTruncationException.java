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

/**
 * Exception thrown when log truncation is detected during MirrorMaker 2 replication.
 * This indicates that messages have been purged by Kafka retention policies before
 * they could be replicated, resulting in potential silent data loss.
 *
 * This exception triggers a fail-fast behavior, stopping the connector task
 * to prevent further silent data loss.
 */
public class LogTruncationException extends RuntimeException {

    private final TopicPartition topicPartition;
    private final long expectedOffset;
    private final long earliestAvailableOffset;

    public LogTruncationException(String message, TopicPartition topicPartition,
                                  long expectedOffset, long earliestAvailableOffset) {
        super(message);
        this.topicPartition = topicPartition;
        this.expectedOffset = expectedOffset;
        this.earliestAvailableOffset = earliestAvailableOffset;
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    public long getExpectedOffset() {
        return expectedOffset;
    }

    public long getEarliestAvailableOffset() {
        return earliestAvailableOffset;
    }

    public long getGapSize() {
        return earliestAvailableOffset - expectedOffset;
    }
}
