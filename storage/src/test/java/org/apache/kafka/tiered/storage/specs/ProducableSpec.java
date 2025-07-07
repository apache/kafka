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

public final class ProducableSpec {

    private final List<ProducerRecord<String, String>> records;
    private Integer batchSize;
    private Long earliestLocalLogOffset;

    public ProducableSpec(List<ProducerRecord<String, String>> records,
                          Integer batchSize,
                          Long earliestLocalLogOffset) {
        this.records = records;
        this.batchSize = batchSize;
        this.earliestLocalLogOffset = earliestLocalLogOffset;
    }

    public List<ProducerRecord<String, String>> getRecords() {
        return records;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }

    public Long getEarliestLocalLogOffset() {
        return earliestLocalLogOffset;
    }

    public void setEarliestLocalLogOffset(Long earliestLocalLogOffset) {
        this.earliestLocalLogOffset = earliestLocalLogOffset;
    }

    @Override
    public String toString() {
        return "ProducableSpec{" +
                "records=" + records +
                ", batchSize=" + batchSize +
                ", earliestLocalLogOffset=" + earliestLocalLogOffset +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProducableSpec that = (ProducableSpec) o;
        return Objects.equals(records, that.records)
                && Objects.equals(batchSize, that.batchSize)
                && Objects.equals(earliestLocalLogOffset, that.earliestLocalLogOffset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(records, batchSize, earliestLocalLogOffset);
    }
}
