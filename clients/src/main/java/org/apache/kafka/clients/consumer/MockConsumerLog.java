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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

class MockConsumerLog<K, V> {

    private final TopicPartition tp;
    private long beginningOffset = 0;
    private long endOffset = 0;
    private OffsetAndMetadata committed;
    private List<ConsumerRecord<K, V>> records = new ArrayList<>();

    MockConsumerLog(TopicPartition tp) {
        this.tp = tp;
    }

    long getBeginningOffset() {
        return beginningOffset;
    }

    void removeRecords(long newBeginningOffset) {
        records = records.stream()
                .filter(record -> record.offset() >= newBeginningOffset)
                .collect(Collectors.toList());
        if (records.isEmpty()) {
            beginningOffset = endOffset;
        } else {
            beginningOffset = records.get(0).offset();
        }
    }

    void removeRecords(Set<Long> offsetsToRemove) {
        records = records.stream()
                .filter(record -> !offsetsToRemove.contains(record.offset()))
                .collect(Collectors.toList());
        if (records.isEmpty()) {
            beginningOffset = endOffset;
        } else {
            beginningOffset = records.get(0).offset();
        }
    }

    long getEndOffset() {
        return endOffset;
    }

    void setCommitted(OffsetAndMetadata meta) {
        committed = meta;
    }

    OffsetAndMetadata getCommitted() {
        return committed;
    }

    void addRecord(ConsumerRecord<K, V> record) {
        if (!records.isEmpty()) {
            long previousOffset = records.get(records.size() - 1).offset();
            long newOffset = record.offset();
            if (previousOffset >= newOffset) {
                throw new IllegalArgumentException("Records must be added in offset order, was trying to insert offset " + newOffset + " after offset " + previousOffset);
            }
        } else {
            beginningOffset = record.offset();
        }
        endOffset = record.offset() + 1;
        records.add(record);
    }

    List<ConsumerRecord<K, V>> fetch(long offset) throws OffsetOutOfRangeException {
        if (beginningOffset > offset) {
            throw new OffsetOutOfRangeException(Collections.singletonMap(tp, offset));
        }
        return records.stream()
                .filter(rec -> rec.offset() >= offset)
                .collect(Collectors.toList());
    }
}
