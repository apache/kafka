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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.types.Type.INT32;

public class ElectPreferredLeadersRequest extends AbstractRequest {

    private static final String TIMEOUT_KEY_NAME = "timeout";
    private static final String TOPIC_PARTITIONS_KEY_NAME = "topic_partitions";
    private static final String PARTITIONS_KEY_NAME = "partitions";

    public static final Schema ELECT_PREFERRED_LEADERS_REQUEST_V0 = new Schema(
            new Field(TOPIC_PARTITIONS_KEY_NAME, ArrayOf.nullable(
                    new Schema(
                            TOPIC_NAME,
                            new Field(PARTITIONS_KEY_NAME,
                                    new ArrayOf(INT32),
                                    "The partitions of this topic whose preferred leader should be elected")))),
            new Field(TIMEOUT_KEY_NAME, INT32, "The time in ms to wait for the elections to be completed.")
    );

    public static Schema[] schemaVersions() {
        return new Schema[]{ELECT_PREFERRED_LEADERS_REQUEST_V0};
    }

    public static class Builder extends AbstractRequest.Builder {

        private final Set<TopicPartition> topicPartitions;
        private final int timeout;

        public Builder(Collection<TopicPartition> topicPartitions, int timeout) {
            super(ApiKeys.ELECT_PREFERRED_LEADERS);
            this.topicPartitions = topicPartitions != null ? new HashSet<>(topicPartitions) : (Set) null;
            this.timeout = timeout;
        }

        @Override
        public ElectPreferredLeadersRequest build(short version) {
            return new ElectPreferredLeadersRequest(version, topicPartitions, timeout);
        }
    }

    private final Set<TopicPartition> topicPartitions;
    private final int timeout;

    public ElectPreferredLeadersRequest(short version, Set<TopicPartition> topicPartitions, int timeout) {
        super(version);
        this.topicPartitions = topicPartitions;
        this.timeout = timeout;
    }

    public ElectPreferredLeadersRequest(Struct struct, short version) {
        super(version);
        Object[] topicPartitionsArray = struct.getArray(TOPIC_PARTITIONS_KEY_NAME);
        if (topicPartitionsArray != null) {
            topicPartitions = new HashSet<>(topicPartitionsArray.length);
            for (Object topicPartitionObj : topicPartitionsArray) {
                Struct topicPartitionStruct = (Struct) topicPartitionObj;
                String topicName = topicPartitionStruct.get(TOPIC_NAME);
                Object[] partitionsArray = topicPartitionStruct.getArray(PARTITIONS_KEY_NAME);
                if (partitionsArray != null) {
                    for (Object partitionObj : partitionsArray) {
                        Integer partition = (Integer) partitionObj;
                        TopicPartition topicPartition = new TopicPartition(topicName, partition);
                        topicPartitions.add(topicPartition);
                    }
                }
            }
        } else {
            topicPartitions = null;
        }
        timeout = struct.getInt(TIMEOUT_KEY_NAME);
    }

    public Set<TopicPartition> topicPartitions() {
        return topicPartitions;
    }

    public int timeout() {
        return timeout;
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.ELECT_PREFERRED_LEADERS.requestSchema(version()));
        Struct[] topicPartitionsArray;
        if (topicPartitions != null) {
            Map<String, List<Integer>> map = CollectionUtils.groupDataByTopic(topicPartitions);
            List<Struct> topicPartitionsList = new ArrayList<>(topicPartitions.size());
            for (Map.Entry<String, List<Integer>> entry : map.entrySet()) {
                Struct partitionStruct = struct.instance(TOPIC_PARTITIONS_KEY_NAME);
                partitionStruct.set(TOPIC_NAME, entry.getKey());
                partitionStruct.set(PARTITIONS_KEY_NAME, entry.getValue().toArray());
                topicPartitionsList.add(partitionStruct);
            }
            topicPartitionsArray = topicPartitionsList.toArray(new Struct[topicPartitionsList.size()]);
        } else {
            topicPartitionsArray = null;
        }
        struct.set(TOPIC_PARTITIONS_KEY_NAME, topicPartitionsArray);
        struct.set(TIMEOUT_KEY_NAME, timeout);
        return struct;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        short version = version();
        switch (version) {
            case 0:
                ApiError error = ApiError.fromThrowable(e);
                Map<TopicPartition, ApiError> errors = new HashMap<>(topicPartitions.size());
                for (TopicPartition partition : topicPartitions)
                    errors.put(partition, error);
                return new ElectPreferredLeadersResponse(throttleTimeMs, errors);
            default:
                throw new IllegalArgumentException(String.format(
                        "Version %d is not valid. Valid versions for %s are 0 to %d",
                        version, this.getClass().getSimpleName(), ApiKeys.ELECT_PREFERRED_LEADERS.latestVersion()));
        }
    }

    public static ElectPreferredLeadersRequest parse(ByteBuffer buffer, short version) {
        return new ElectPreferredLeadersRequest(ApiKeys.ELECT_PREFERRED_LEADERS.parseRequest(version, buffer), version);
    }

}
