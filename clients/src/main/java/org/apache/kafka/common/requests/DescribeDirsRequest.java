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
import org.apache.kafka.common.requests.DescribeDirsResponse.LogDirInfo;
import org.apache.kafka.common.requests.DescribeDirsResponse.ReplicaInfo;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class DescribeDirsRequest extends AbstractRequest {

    // request level key names
    private static final String LOG_DIRS_KEY_NAME = "log_dirs";
    private static final String TOPICS_KEY_NAME = "topics";

    // topic level key names
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITIONS_KEY_NAME = "partitions";

    private final Set<String> logDirs;
    private final Set<TopicPartition> topicPartitions;

    public static class Builder extends AbstractRequest.Builder<DescribeDirsRequest> {
        private final Set<String> logDirs;
        private final Set<TopicPartition> topicPartitions;

        public Builder(Set<String> logDirs, Set<TopicPartition> partitions) {
            super(ApiKeys.DESCRIBE_DIRS);
            this.logDirs = logDirs;
            this.topicPartitions = partitions;
        }

        @Override
        public DescribeDirsRequest build(short version) {
            return new DescribeDirsRequest(logDirs, topicPartitions, version);
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("(type=DescribeDirsRequest")
                .append(", logDirs=")
                .append(logDirs)
                .append(", topicPartitions=")
                .append(topicPartitions)
                .append(")");
            return builder.toString();
        }
    }

    public DescribeDirsRequest(Struct struct, short version) {
        super(version);
        logDirs = new HashSet<>();
        topicPartitions = new HashSet<>();
        for (Object logDir : struct.getArray(LOG_DIRS_KEY_NAME)) {
            logDirs.add((String) logDir);
        }
        for (Object topicStructObj : struct.getArray(TOPICS_KEY_NAME)) {
            Struct topicStruct = (Struct) topicStructObj;
            String topic = topicStruct.getString(TOPIC_KEY_NAME);
            for (Object partitionObj : topicStruct.getArray(PARTITIONS_KEY_NAME)) {
                int partition = (Integer) partitionObj;
                topicPartitions.add(new TopicPartition(topic, partition));
            }
        }
    }

    public DescribeDirsRequest(Set<String> logDirs, Set<TopicPartition> topicPartitions, short version) {
        super(version);
        this.logDirs = logDirs;
        this.topicPartitions = topicPartitions;
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.DESCRIBE_DIRS.requestSchema(version()));
        struct.set(LOG_DIRS_KEY_NAME, logDirs.toArray());

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
            topicStruct.set(TOPIC_KEY_NAME, partitionsByTopicEntry.getKey());
            topicStruct.set(PARTITIONS_KEY_NAME, partitionsByTopicEntry.getValue().toArray());
            topicStructArray.add(topicStruct);
        }
        struct.set(TOPICS_KEY_NAME, topicStructArray.toArray());

        return struct;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Map<String, LogDirInfo> logDirInfos = new HashMap<>();

        for (String logDir : logDirs) {
            logDirInfos.put(logDir, new LogDirInfo(Errors.forException(e), new HashMap<TopicPartition, ReplicaInfo>()));
        }

        short versionId = version();
        switch (versionId) {
            case 0:
                return new DescribeDirsResponse(throttleTimeMs, logDirInfos);
            default:
                throw new IllegalArgumentException(
                    String.format("Version %d is not valid. Valid versions for %s are 0 to %d", versionId,
                        this.getClass().getSimpleName(), ApiKeys.DESCRIBE_DIRS.latestVersion()));
        }
    }

    public Set<String> logDirs() {
        return logDirs;
    }

    public Set<TopicPartition> topicPartitions() {
        return topicPartitions;
    }

    public static DescribeDirsRequest parse(ByteBuffer buffer, short version) {
        return new DescribeDirsRequest(ApiKeys.DESCRIBE_DIRS.parseRequest(version, buffer), version);
    }
}
