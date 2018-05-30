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
import org.apache.kafka.common.requests.DescribeLogDirsResponse.LogDirInfo;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.types.Type.INT32;

public class DescribeLogDirsRequest extends AbstractRequest {

    // request level key names
    private static final String TOPICS_KEY_NAME = "topics";

    // topic level key names
    private static final String PARTITIONS_KEY_NAME = "partitions";

    private static final Schema DESCRIBE_LOG_DIRS_REQUEST_V0 = new Schema(
            new Field(TOPICS_KEY_NAME, ArrayOf.nullable(new Schema(
                    TOPIC_NAME,
                    new Field(PARTITIONS_KEY_NAME, new ArrayOf(INT32), "List of partition ids of the topic.")))));

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema DESCRIBE_LOG_DIRS_REQUEST_V1 = DESCRIBE_LOG_DIRS_REQUEST_V0;

    public static Schema[] schemaVersions() {
        return new Schema[]{DESCRIBE_LOG_DIRS_REQUEST_V0, DESCRIBE_LOG_DIRS_REQUEST_V1};
    }

    private final Set<TopicPartition> topicPartitions;

    public static class Builder extends AbstractRequest.Builder<DescribeLogDirsRequest> {
        private final Set<TopicPartition> topicPartitions;

        // topicPartitions == null indicates requesting all partitions, and an empty list indicates requesting no partitions.
        public Builder(Set<TopicPartition> partitions) {
            super(ApiKeys.DESCRIBE_LOG_DIRS);
            this.topicPartitions = partitions;
        }

        @Override
        public DescribeLogDirsRequest build(short version) {
            return new DescribeLogDirsRequest(topicPartitions, version);
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("(type=DescribeLogDirsRequest")
                .append(", topicPartitions=")
                .append(topicPartitions)
                .append(")");
            return builder.toString();
        }
    }

    public DescribeLogDirsRequest(Struct struct, short version) {
        super(version);

        if (struct.getArray(TOPICS_KEY_NAME) == null) {
            topicPartitions = null;
        } else {
            topicPartitions = new HashSet<>();
            for (Object topicStructObj : struct.getArray(TOPICS_KEY_NAME)) {
                Struct topicStruct = (Struct) topicStructObj;
                String topic = topicStruct.get(TOPIC_NAME);
                for (Object partitionObj : topicStruct.getArray(PARTITIONS_KEY_NAME)) {
                    int partition = (Integer) partitionObj;
                    topicPartitions.add(new TopicPartition(topic, partition));
                }
            }
        }
    }

    // topicPartitions == null indicates requesting all partitions, and an empty list indicates requesting no partitions.
    public DescribeLogDirsRequest(Set<TopicPartition> topicPartitions, short version) {
        super(version);
        this.topicPartitions = topicPartitions;
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.DESCRIBE_LOG_DIRS.requestSchema(version()));
        if (topicPartitions == null) {
            struct.set(TOPICS_KEY_NAME, null);
            return struct;
        }

        Map<String, List<Integer>> partitionsByTopic = new HashMap<>();
        for (TopicPartition tp : topicPartitions) {
            if (!partitionsByTopic.containsKey(tp.topic())) {
                partitionsByTopic.put(tp.topic(), new ArrayList<Integer>());
            }
            partitionsByTopic.get(tp.topic()).add(tp.partition());
        }

        List<Struct> topicStructArray = new ArrayList<>();
        for (Map.Entry<String, List<Integer>> partitionsByTopicEntry : partitionsByTopic.entrySet()) {
            Struct topicStruct = struct.instance(TOPICS_KEY_NAME);
            topicStruct.set(TOPIC_NAME, partitionsByTopicEntry.getKey());
            topicStruct.set(PARTITIONS_KEY_NAME, partitionsByTopicEntry.getValue().toArray());
            topicStructArray.add(topicStruct);
        }
        struct.set(TOPICS_KEY_NAME, topicStructArray.toArray());

        return struct;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        short versionId = version();
        switch (versionId) {
            case 0:
            case 1:
                return new DescribeLogDirsResponse(throttleTimeMs, new HashMap<String, LogDirInfo>());
            default:
                throw new IllegalArgumentException(
                    String.format("Version %d is not valid. Valid versions for %s are 0 to %d", versionId,
                        this.getClass().getSimpleName(), ApiKeys.DESCRIBE_LOG_DIRS.latestVersion()));
        }
    }

    public boolean isAllTopicPartitions() {
        return topicPartitions == null;
    }

    public Set<TopicPartition> topicPartitions() {
        return topicPartitions;
    }

    public static DescribeLogDirsRequest parse(ByteBuffer buffer, short version) {
        return new DescribeLogDirsRequest(ApiKeys.DESCRIBE_LOG_DIRS.parseRequest(version, buffer), version);
    }
}
