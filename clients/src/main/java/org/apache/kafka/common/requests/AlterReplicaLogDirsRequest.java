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
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData;
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData;
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData.AlterReplicaLogDirTopicResult;

public class AlterReplicaLogDirsRequest extends AbstractRequest {

    private final AlterReplicaLogDirsRequestData data;

    public static class Builder extends AbstractRequest.Builder<AlterReplicaLogDirsRequest> {
        private final AlterReplicaLogDirsRequestData data;

        public Builder(AlterReplicaLogDirsRequestData data) {
            super(ApiKeys.ALTER_REPLICA_LOG_DIRS);
            this.data = data;
        }

        @Override
        public AlterReplicaLogDirsRequest build(short version) {
            return new AlterReplicaLogDirsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    public AlterReplicaLogDirsRequest(AlterReplicaLogDirsRequestData data, short version) {
        super(ApiKeys.ALTER_REPLICA_LOG_DIRS, version);
        this.data = data;
    }

    @Override
    public AlterReplicaLogDirsRequestData data() {
        return data;
    }

    public AlterReplicaLogDirsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        AlterReplicaLogDirsResponseData data = new AlterReplicaLogDirsResponseData();
        data.setResults(this.data.dirs().stream().flatMap(alterDir ->
            alterDir.topics().stream().map(topic ->
                new AlterReplicaLogDirTopicResult()
                    .setTopicName(topic.name())
                    .setPartitions(topic.partitions().stream().map(partitionId ->
                        new AlterReplicaLogDirsResponseData.AlterReplicaLogDirPartitionResult()
                            .setErrorCode(Errors.forException(e).code())
                            .setPartitionIndex(partitionId)).collect(Collectors.toList())))).collect(Collectors.toList()));
        return new AlterReplicaLogDirsResponse(data.setThrottleTimeMs(throttleTimeMs));
    }

    public Map<TopicPartition, String> partitionDirs() {
        Map<TopicPartition, String> result = new HashMap<>();
        data.dirs().forEach(alterDir ->
            alterDir.topics().forEach(topic ->
                topic.partitions().forEach(partition ->
                    result.put(new TopicPartition(topic.name(), partition), alterDir.path())))
        );
        return result;
    }

    public static AlterReplicaLogDirsRequest parse(ByteBuffer buffer, short version) {
        return new AlterReplicaLogDirsRequest(new AlterReplicaLogDirsRequestData(new ByteBufferAccessor(buffer), version), version);
    }
}
