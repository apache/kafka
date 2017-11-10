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

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.types.Type.BOOLEAN;
import static org.apache.kafka.common.protocol.types.Type.INT64;
import static org.apache.kafka.common.protocol.types.Type.STRING;


public class DescribeLogDirsResponse extends AbstractResponse {

    public static final long INVALID_OFFSET_LAG = -1L;

    // request level key names
    private static final String LOG_DIRS_KEY_NAME = "log_dirs";

    // dir level key names
    private static final String LOG_DIR_KEY_NAME = "log_dir";
    private static final String TOPICS_KEY_NAME = "topics";

    // topic level key names
    private static final String PARTITIONS_KEY_NAME = "partitions";

    // partition level key names
    private static final String SIZE_KEY_NAME = "size";
    private static final String OFFSET_LAG_KEY_NAME = "offset_lag";
    private static final String IS_FUTURE_KEY_NAME = "is_future";

    private static final Schema DESCRIBE_LOG_DIRS_RESPONSE_V0 = new Schema(
            THROTTLE_TIME_MS,
            new Field(LOG_DIRS_KEY_NAME, new ArrayOf(new Schema(
                    ERROR_CODE,
                    new Field(LOG_DIR_KEY_NAME, STRING, "The absolute log directory path."),
                    new Field(TOPICS_KEY_NAME, new ArrayOf(new Schema(
                            TOPIC_NAME,
                            new Field(PARTITIONS_KEY_NAME, new ArrayOf(new Schema(
                                    PARTITION_ID,
                                    new Field(SIZE_KEY_NAME, INT64, "The size of the log segments of the partition in bytes."),
                                    new Field(OFFSET_LAG_KEY_NAME, INT64, "The lag of the log's LEO w.r.t. partition's HW " +
                                            "(if it is the current log for the partition) or current replica's LEO " +
                                            "(if it is the future log for the partition)"),
                                    new Field(IS_FUTURE_KEY_NAME, BOOLEAN, "True if this log is created by " +
                                            "AlterReplicaLogDirsRequest and will replace the current log of the replica " +
                                            "in the future.")))))))))));

    public static Schema[] schemaVersions() {
        return new Schema[]{DESCRIBE_LOG_DIRS_RESPONSE_V0};
    }

    private final int throttleTimeMs;
    private final Map<String, LogDirInfo> logDirInfos;

    public DescribeLogDirsResponse(Struct struct) {
        throttleTimeMs = struct.get(THROTTLE_TIME_MS);
        logDirInfos = new HashMap<>();

        for (Object logDirStructObj : struct.getArray(LOG_DIRS_KEY_NAME)) {
            Struct logDirStruct = (Struct) logDirStructObj;
            Errors error = Errors.forCode(logDirStruct.get(ERROR_CODE));
            String logDir = logDirStruct.getString(LOG_DIR_KEY_NAME);
            Map<TopicPartition, ReplicaInfo> replicaInfos = new HashMap<>();

            for (Object topicStructObj : logDirStruct.getArray(TOPICS_KEY_NAME)) {
                Struct topicStruct = (Struct) topicStructObj;
                String topic = topicStruct.get(TOPIC_NAME);

                for (Object partitionStructObj : topicStruct.getArray(PARTITIONS_KEY_NAME)) {
                    Struct partitionStruct = (Struct) partitionStructObj;
                    int partition = partitionStruct.get(PARTITION_ID);
                    long size = partitionStruct.getLong(SIZE_KEY_NAME);
                    long offsetLag = partitionStruct.getLong(OFFSET_LAG_KEY_NAME);
                    boolean isFuture = partitionStruct.getBoolean(IS_FUTURE_KEY_NAME);
                    ReplicaInfo replicaInfo = new ReplicaInfo(size, offsetLag, isFuture);
                    replicaInfos.put(new TopicPartition(topic, partition), replicaInfo);
                }
            }

            logDirInfos.put(logDir, new LogDirInfo(error, replicaInfos));
        }
    }

    /**
     * Constructor for version 0.
     */
    public DescribeLogDirsResponse(int throttleTimeMs, Map<String, LogDirInfo> logDirInfos) {
        this.throttleTimeMs = throttleTimeMs;
        this.logDirInfos = logDirInfos;
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.DESCRIBE_LOG_DIRS.responseSchema(version));
        struct.set(THROTTLE_TIME_MS, throttleTimeMs);
        List<Struct> logDirStructArray = new ArrayList<>();
        for (Map.Entry<String, LogDirInfo> logDirInfosEntry : logDirInfos.entrySet()) {
            LogDirInfo logDirInfo = logDirInfosEntry.getValue();
            Struct logDirStruct = struct.instance(LOG_DIRS_KEY_NAME);
            logDirStruct.set(ERROR_CODE, logDirInfo.error.code());
            logDirStruct.set(LOG_DIR_KEY_NAME, logDirInfosEntry.getKey());

            Map<String, Map<Integer, ReplicaInfo>> replicaInfosByTopic = CollectionUtils.groupDataByTopic(logDirInfo.replicaInfos);
            List<Struct> topicStructArray = new ArrayList<>();
            for (Map.Entry<String, Map<Integer, ReplicaInfo>> replicaInfosByTopicEntry : replicaInfosByTopic.entrySet()) {
                Struct topicStruct = logDirStruct.instance(TOPICS_KEY_NAME);
                topicStruct.set(TOPIC_NAME, replicaInfosByTopicEntry.getKey());
                List<Struct> partitionStructArray = new ArrayList<>();

                for (Map.Entry<Integer, ReplicaInfo> replicaInfosByPartitionEntry : replicaInfosByTopicEntry.getValue().entrySet()) {
                    Struct partitionStruct = topicStruct.instance(PARTITIONS_KEY_NAME);
                    ReplicaInfo replicaInfo = replicaInfosByPartitionEntry.getValue();
                    partitionStruct.set(PARTITION_ID, replicaInfosByPartitionEntry.getKey());
                    partitionStruct.set(SIZE_KEY_NAME, replicaInfo.size);
                    partitionStruct.set(OFFSET_LAG_KEY_NAME, replicaInfo.offsetLag);
                    partitionStruct.set(IS_FUTURE_KEY_NAME, replicaInfo.isFuture);
                    partitionStructArray.add(partitionStruct);
                }
                topicStruct.set(PARTITIONS_KEY_NAME, partitionStructArray.toArray());
                topicStructArray.add(topicStruct);
            }
            logDirStruct.set(TOPICS_KEY_NAME, topicStructArray.toArray());
            logDirStructArray.add(logDirStruct);
        }
        struct.set(LOG_DIRS_KEY_NAME, logDirStructArray.toArray());
        return struct;
    }

    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        for (LogDirInfo logDirInfo : logDirInfos.values())
            updateErrorCounts(errorCounts, logDirInfo.error);
        return errorCounts;
    }

    public Map<String, LogDirInfo> logDirInfos() {
        return logDirInfos;
    }

    public static DescribeLogDirsResponse parse(ByteBuffer buffer, short version) {
        return new DescribeLogDirsResponse(ApiKeys.DESCRIBE_LOG_DIRS.responseSchema(version).read(buffer));
    }

    /**
     * Possible error code:
     *
     * KAFKA_STORAGE_ERROR (56)
     * UNKNOWN (-1)
     */
    static public class LogDirInfo {
        public final Errors error;
        public final Map<TopicPartition, ReplicaInfo> replicaInfos;

        public LogDirInfo(Errors error, Map<TopicPartition, ReplicaInfo> replicaInfos) {
            this.error = error;
            this.replicaInfos = replicaInfos;
        }
    }

    static public class ReplicaInfo {

        public final long size;
        public final long offsetLag;
        public final boolean isFuture;

        public ReplicaInfo(long size, long offsetLag, boolean isFuture) {
            this.size = size;
            this.offsetLag = offsetLag;
            this.isFuture = isFuture;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("(size=")
                .append(size)
                .append(", offsetLag=")
                .append(offsetLag)
                .append(", isFuture=")
                .append(isFuture)
                .append(")");
            return builder.toString();
        }
    }
}
