/**
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
package org.apache.kafka.clients.producer;

import org.apache.kafka.common.TopicPartition;

/**
 * The metadata for a record that has been acknowledged by the server
 */
public final class RecordMetadata {

    private final long offset;
    private final TopicPartition topicPartition;

    private RecordMetadata(TopicPartition topicPartition, long offset) {
        super();
        this.offset = offset;
        this.topicPartition = topicPartition;
    }

    public RecordMetadata(TopicPartition topicPartition, long baseOffset, long relativeOffset) {
        // ignore the relativeOffset if the base offset is -1,
        // since this indicates the offset is unknown
        this(topicPartition, baseOffset == -1 ? baseOffset : baseOffset + relativeOffset);
    }

    /**
     * The offset of the record in the topic/partition.
     */
    public long offset() {
        return this.offset;
    }

    /**
     * The topic the record was appended to
     */
    public String topic() {
        return this.topicPartition.topic();
    }

    /**
     * The partition the record was sent to
     */
    public int partition() {
        return this.topicPartition.partition();
    }

    @Override
    public String toString() {
        return topicPartition.toString() + "@" + offset;
    }
}
