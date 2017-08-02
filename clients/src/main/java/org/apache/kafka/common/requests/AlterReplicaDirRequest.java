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
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class AlterReplicaDirRequest extends AbstractRequest {

    // request level key names
    private static final String TOPICS_KEY_NAME = "topics";

    // topic level key names
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITIONS_KEY_NAME = "partitions";

    // partition level key names
    private static final String PARTITION_KEY_NAME = "partition";
    private static final String DIR_KEY_NAME = "dir";

    private final Map<TopicPartition, String> partitionDirs;

    public static class Builder extends AbstractRequest.Builder<AlterReplicaDirRequest> {
        private final Map<TopicPartition, String> partitionDirs;

        public Builder(Map<TopicPartition, String> partitionDirs) {
            super(ApiKeys.ALTER_REPLICA_DIR);
            this.partitionDirs = partitionDirs;
        }

        @Override
        public AlterReplicaDirRequest build(short version) {
            return new AlterReplicaDirRequest(partitionDirs, version);
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("(type=AlterReplicaDirRequest")
                .append(", partitionDirs=")
                .append(partitionDirs)
                .append(")");
            return builder.toString();
        }
    }

    public AlterReplicaDirRequest(Struct struct, short version) {
        super(version);
        partitionDirs = new HashMap<>();
        for (Object topicStructObj : struct.getArray(TOPICS_KEY_NAME)) {
            Struct topicStruct = (Struct) topicStructObj;
            String topic = topicStruct.getString(TOPIC_KEY_NAME);
            for (Object partitionStructObj : topicStruct.getArray(PARTITIONS_KEY_NAME)) {
                Struct partitionStruct = (Struct) partitionStructObj;
                int partition = partitionStruct.getInt(PARTITION_KEY_NAME);
                String dir = partitionStruct.getString(DIR_KEY_NAME);
                partitionDirs.put(new TopicPartition(topic, partition), dir);
            }
        }
    }

    public AlterReplicaDirRequest(Map<TopicPartition, String> partitionDirs, short version) {
        super(version);
        this.partitionDirs = partitionDirs;
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.ALTER_REPLICA_DIR.requestSchema(version()));
        Map<String, Map<Integer, String>> dirsByTopic = CollectionUtils.groupDataByTopic(partitionDirs);
        List<Struct> topicStructArray = new ArrayList<>();
        for (Map.Entry<String, Map<Integer, String>> dirsByTopicEntry : dirsByTopic.entrySet()) {
            Struct topicStruct = struct.instance(TOPICS_KEY_NAME);
            topicStruct.set(TOPIC_KEY_NAME, dirsByTopicEntry.getKey());
            List<Struct> partitionStructArray = new ArrayList<>();
            for (Map.Entry<Integer, String> dirsByPartitionEntry : dirsByTopicEntry.getValue().entrySet()) {
                Struct partitionStruct = topicStruct.instance(PARTITIONS_KEY_NAME);
                partitionStruct.set(PARTITION_KEY_NAME, dirsByPartitionEntry.getKey());
                partitionStruct.set(DIR_KEY_NAME, dirsByPartitionEntry.getValue());
                partitionStructArray.add(partitionStruct);
            }
            topicStruct.set(PARTITIONS_KEY_NAME, partitionStructArray.toArray());
            topicStructArray.add(topicStruct);
        }
        struct.set(TOPICS_KEY_NAME, topicStructArray.toArray());
        return struct;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Map<TopicPartition, Errors> responseMap = new HashMap<>();

        for (Map.Entry<TopicPartition, String> entry : partitionDirs.entrySet()) {
            responseMap.put(entry.getKey(), Errors.forException(e));
        }

        short versionId = version();
        switch (versionId) {
            case 0:
                return new AlterReplicaDirResponse(throttleTimeMs, responseMap);
            default:
                throw new IllegalArgumentException(
                    String.format("Version %d is not valid. Valid versions for %s are 0 to %d", versionId,
                        this.getClass().getSimpleName(), ApiKeys.ALTER_REPLICA_DIR.latestVersion()));
        }
    }

    public Map<TopicPartition, String> partitionDirs() {
        return partitionDirs;
    }

    public static AlterReplicaDirRequest parse(ByteBuffer buffer, short version) {
        return new AlterReplicaDirRequest(ApiKeys.ALTER_REPLICA_DIR.parseRequest(version, buffer), version);
    }
}
