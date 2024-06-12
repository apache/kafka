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

import java.util.List;
import java.util.Objects;

public final class OffloadableSpec {

    private final Integer sourceBrokerId;
    private final Integer baseOffset;
    private final List<ProducerRecord<String, String>> records;

    public OffloadableSpec(Integer sourceBrokerId,
                           Integer baseOffset,
                           List<ProducerRecord<String, String>> records) {
        this.sourceBrokerId = sourceBrokerId;
        this.baseOffset = baseOffset;
        this.records = records;
    }

    public Integer getSourceBrokerId() {
        return sourceBrokerId;
    }

    public Integer getBaseOffset() {
        return baseOffset;
    }

    public List<ProducerRecord<String, String>> getRecords() {
        return records;
    }

    @Override
    public String toString() {
        return "OffloadableSpec{" +
                "sourceBrokerId=" + sourceBrokerId +
                ", baseOffset=" + baseOffset +
                ", records=" + records +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OffloadableSpec that = (OffloadableSpec) o;
        return Objects.equals(sourceBrokerId, that.sourceBrokerId)
                && Objects.equals(baseOffset, that.baseOffset)
                && Objects.equals(records, that.records);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceBrokerId, baseOffset, records);
    }
}
