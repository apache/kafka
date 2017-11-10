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
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.types.Type.INT32;
import static org.apache.kafka.common.protocol.types.Type.STRING;

public class AlterReplicaLogDirsRequest extends AbstractRequest {

    // request level key names
    private static final String LOG_DIRS_KEY_NAME = "log_dirs";

    // log dir level key names
    private static final String LOG_DIR_KEY_NAME = "log_dir";
    private static final String TOPICS_KEY_NAME = "topics";

    // topic level key names
    private static final String PARTITIONS_KEY_NAME = "partitions";

    private static final Schema ALTER_REPLICA_LOG_DIRS_REQUEST_V0 = new Schema(
            new Field("log_dirs", new ArrayOf(new Schema(
                    new Field("log_dir", STRING, "The absolute log directory path."),
                    new Field("topics", new ArrayOf(new Schema(
                            TOPIC_NAME,
                            new Field("partitions", new ArrayOf(INT32), "List of partition ids of the topic."))))))));

    public static Schema[] schemaVersions() {
        return new Schema[]{ALTER_REPLICA_LOG_DIRS_REQUEST_V0};
    }

    private final Map<TopicPartition, String> partitionDirs;

    public static class Builder extends AbstractRequest.Builder<AlterReplicaLogDirsRequest> {
        private final Map<TopicPartition, String> partitionDirs;

        public Builder(Map<TopicPartition, String> partitionDirs) {
            super(ApiKeys.ALTER_REPLICA_LOG_DIRS);
            this.partitionDirs = partitionDirs;
        }

        @Override
        public AlterReplicaLogDirsRequest build(short version) {
            return new AlterReplicaLogDirsRequest(partitionDirs, version);
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("(type=AlterReplicaLogDirsRequest")
                .append(", partitionDirs=")
                .append(partitionDirs)
                .append(")");
            return builder.toString();
        }
    }

    public AlterReplicaLogDirsRequest(Struct struct, short version) {
        super(version);
        partitionDirs = new HashMap<>();
        for (Object logDirStructObj : struct.getArray(LOG_DIRS_KEY_NAME)) {
            Struct logDirStruct = (Struct) logDirStructObj;
            String logDir = logDirStruct.getString(LOG_DIR_KEY_NAME);
            for (Object topicStructObj : logDirStruct.getArray(TOPICS_KEY_NAME)) {
                Struct topicStruct = (Struct) topicStructObj;
                String topic = topicStruct.get(TOPIC_NAME);
                for (Object partitionObj : topicStruct.getArray(PARTITIONS_KEY_NAME)) {
                    int partition = (Integer) partitionObj;
                    partitionDirs.put(new TopicPartition(topic, partition), logDir);
                }
            }
        }
    }

    public AlterReplicaLogDirsRequest(Map<TopicPartition, String> partitionDirs, short version) {
        super(version);
        this.partitionDirs = partitionDirs;
    }

    @Override
    protected Struct toStruct() {
        Map<String, List<TopicPartition>> dirPartitions = new HashMap<>();
        for (Map.Entry<TopicPartition, String> entry: partitionDirs.entrySet()) {
            if (!dirPartitions.containsKey(entry.getValue()))
                dirPartitions.put(entry.getValue(), new ArrayList<TopicPartition>());
            dirPartitions.get(entry.getValue()).add(entry.getKey());
        }

        Struct struct = new Struct(ApiKeys.ALTER_REPLICA_LOG_DIRS.requestSchema(version()));
        List<Struct> logDirStructArray = new ArrayList<>();
        for (Map.Entry<String, List<TopicPartition>> logDirEntry: dirPartitions.entrySet()) {
            Struct logDirStruct = struct.instance(LOG_DIRS_KEY_NAME);
            logDirStruct.set(LOG_DIR_KEY_NAME, logDirEntry.getKey());

            List<Struct> topicStructArray = new ArrayList<>();
            for (Map.Entry<String, List<Integer>> topicEntry: CollectionUtils.groupDataByTopic(logDirEntry.getValue()).entrySet()) {
                Struct topicStruct = logDirStruct.instance(TOPICS_KEY_NAME);
                topicStruct.set(TOPIC_NAME, topicEntry.getKey());
                topicStruct.set(PARTITIONS_KEY_NAME, topicEntry.getValue().toArray());
                topicStructArray.add(topicStruct);
            }
            logDirStruct.set(TOPICS_KEY_NAME, topicStructArray.toArray());
            logDirStructArray.add(logDirStruct);
        }
        struct.set(LOG_DIRS_KEY_NAME, logDirStructArray.toArray());
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
                return new AlterReplicaLogDirsResponse(throttleTimeMs, responseMap);
            default:
                throw new IllegalArgumentException(
                    String.format("Version %d is not valid. Valid versions for %s are 0 to %d", versionId,
                        this.getClass().getSimpleName(), ApiKeys.ALTER_REPLICA_LOG_DIRS.latestVersion()));
        }
    }

    public Map<TopicPartition, String> partitionDirs() {
        return partitionDirs;
    }

    public static AlterReplicaLogDirsRequest parse(ByteBuffer buffer, short version) {
        return new AlterReplicaLogDirsRequest(ApiKeys.ALTER_REPLICA_LOG_DIRS.parseRequest(version, buffer), version);
    }
}
