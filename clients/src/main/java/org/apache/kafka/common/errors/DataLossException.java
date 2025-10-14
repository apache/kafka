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
package org.apache.kafka.common.errors;

import org.apache.kafka.common.TopicPartition;

import java.util.Set;

/**
 * Exception thrown when data loss is detected by the consumer.
 * This can occur due to offset gaps, topic deletion/recreation, or other scenarios
 * where message continuity cannot be guaranteed.
 */
public class DataLossException extends ApiException {

    private static final long serialVersionUID = 1L;

    private final Set<TopicPartition> partitions;
    private final DataLossType lossType;
    private final String details;

    public enum DataLossType {
        OFFSET_GAP("Offset gap detected"),
        TOPIC_RECREATION("Topic deletion/recreation detected"),
        OUT_OF_RANGE("Offset out of range"),
        UNKNOWN("Unknown data loss scenario");

        private final String description;

        DataLossType(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }
    }

    public DataLossException(String message) {
        super(message);
        this.partitions = Set.of();
        this.lossType = DataLossType.UNKNOWN;
        this.details = "";
    }

    public DataLossException(String message, Set<TopicPartition> partitions, DataLossType lossType) {
        super(message);
        this.partitions = partitions;
        this.lossType = lossType;
        this.details = "";
    }

    public DataLossException(String message, Set<TopicPartition> partitions, DataLossType lossType, String details) {
        super(message);
        this.partitions = partitions;
        this.lossType = lossType;
        this.details = details;
    }

    public DataLossException(String message, Throwable cause, Set<TopicPartition> partitions, DataLossType lossType) {
        super(message, cause);
        this.partitions = partitions;
        this.lossType = lossType;
        this.details = "";
    }

    /**
     * @return The set of topic partitions affected by the data loss
     */
    public Set<TopicPartition> partitions() {
        return partitions;
    }

    /**
     * @return The type of data loss detected
     */
    public DataLossType lossType() {
        return lossType;
    }

    /**
     * @return Additional details about the data loss scenario
     */
    public String details() {
        return details;
    }
}