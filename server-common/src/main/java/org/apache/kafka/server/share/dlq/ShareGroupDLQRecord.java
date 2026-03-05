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

package org.apache.kafka.server.share.dlq;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.header.Headers;

import java.util.Optional;

/**
 * Record representing information needed from callers of {@link ShareGroupDLQ#enqueue}
 *
 * @param groupId            The share group id of the message being recorded.
 * @param topicIdPartition   The topic and partition information of the message.
 * @param offset             The offset of the record in the kafka topic partition.
 * @param deliveryCount      If known, the number of times the message was delivered to the share consumer.
 * @param cause              If known, throwable representing the reason for queueing the message.
 * @param originalRecordData If desired, header data and the key/value information in the original message.
 */
public record ShareGroupDLQRecord(
    String groupId,
    TopicIdPartition topicIdPartition,
    long offset,
    Optional<Integer> deliveryCount,
    Optional<Throwable> cause,
    Optional<OriginalRecordData> originalRecordData
) {
    /**
     * Record representing header, key and value information about the original message.
     * If known, this information will be written to the DLQ topic as well.
     *
     * @param headers   The headers of the original record.
     * @param key       A byte array representing the key of the original record.
     * @param value     A byte array representing the value of the original record.
     */
    public record OriginalRecordData(
        Headers headers,
        byte[] key,
        byte[] value
    ) {
    }
}
