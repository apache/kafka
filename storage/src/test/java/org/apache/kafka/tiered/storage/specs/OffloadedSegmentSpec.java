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
package org.apache.kafka.tiered.storage.specs;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Objects;

public final class OffloadedSegmentSpec {

    private final int sourceBrokerId;
    private final TopicPartition topicPartition;
    private final int baseOffset;
    private final List<ProducerRecord<String, String>> records;

    /**
     * Specifies a remote log segment expected to be found in a second-tier storage.
     *
     * @param sourceBrokerId The broker which offloaded (uploaded) the segment to the second-tier storage.
     * @param topicPartition The topic-partition which the remote log segment belongs to.
     * @param baseOffset The base offset of the remote log segment.
     * @param records The records *expected* in the remote log segment.
     */
    public OffloadedSegmentSpec(int sourceBrokerId,
                                TopicPartition topicPartition,
                                int baseOffset,
                                List<ProducerRecord<String, String>> records) {
        this.sourceBrokerId = sourceBrokerId;
        this.topicPartition = topicPartition;
        this.baseOffset = baseOffset;
        this.records = records;
    }

    public int getSourceBrokerId() {
        return sourceBrokerId;
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    public int getBaseOffset() {
        return baseOffset;
    }

    public List<ProducerRecord<String, String>> getRecords() {
        return records;
    }

    @Override
    public String toString() {
        return String.format("Segment[partition=%s offloaded-by-broker-id=%d base-offset=%d record-count=%d]",
                topicPartition, sourceBrokerId, baseOffset, records.size());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OffloadedSegmentSpec that = (OffloadedSegmentSpec) o;
        return sourceBrokerId == that.sourceBrokerId
                && baseOffset == that.baseOffset
                && Objects.equals(topicPartition, that.topicPartition)
                && Objects.equals(records, that.records);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceBrokerId, topicPartition, baseOffset, records);
    }
}
