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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.common.record.RecordBatch.NO_PARTITION_LEADER_EPOCH;

/**
 * Possible error codes:
 * - {@link Errors#TOPIC_AUTHORIZATION_FAILED} If the user does not have DESCRIBE access to a requested topic
 * - {@link Errors#REPLICA_NOT_AVAILABLE} If the request is received by a broker with version < 2.6 which is not a replica
 * - {@link Errors#NOT_LEADER_OR_FOLLOWER} If the broker is not a leader or follower and either the provided leader epoch
 *     matches the known leader epoch on the broker or is empty
 * - {@link Errors#FENCED_LEADER_EPOCH} If the epoch is lower than the broker's epoch
 * - {@link Errors#UNKNOWN_LEADER_EPOCH} If the epoch is larger than the broker's epoch
 * - {@link Errors#UNKNOWN_TOPIC_OR_PARTITION} If the broker does not have metadata for a topic or partition
 * - {@link Errors#KAFKA_STORAGE_ERROR} If the log directory for one of the requested partitions is offline
 * - {@link Errors#UNKNOWN_SERVER_ERROR} For any unexpected errors
 */
public class OffsetsForLeaderEpochResponse extends AbstractResponse {
    public static final long UNDEFINED_EPOCH_OFFSET = NO_PARTITION_LEADER_EPOCH;
    public static final int UNDEFINED_EPOCH = NO_PARTITION_LEADER_EPOCH;

    private final OffsetForLeaderEpochResponseData data;

    public OffsetsForLeaderEpochResponse(OffsetForLeaderEpochResponseData data) {
        super(ApiKeys.OFFSET_FOR_LEADER_EPOCH);
        this.data = data;
    }

    @Override
    public OffsetForLeaderEpochResponseData data() {
        return data;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        data.topics().forEach(topic ->
            topic.partitions().forEach(partition ->
                updateErrorCounts(errorCounts, Errors.forCode(partition.errorCode()))));
        return errorCounts;
    }

    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    public static OffsetsForLeaderEpochResponse parse(ByteBuffer buffer, short version) {
        return new OffsetsForLeaderEpochResponse(new OffsetForLeaderEpochResponseData(new ByteBufferAccessor(buffer), version));
    }

    @Override
    public String toString() {
        return data.toString();
    }
}
