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
import org.apache.kafka.common.message.DescribeLogDirsResponseData;
import org.apache.kafka.common.message.DescribeLogDirsResponseData.DescribeLogDirsPartition;
import org.apache.kafka.common.message.DescribeLogDirsResponseData.DescribeLogDirsResult;
import org.apache.kafka.common.message.DescribeLogDirsResponseData.DescribeLogDirsTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;


public class DescribeLogDirsResponse extends AbstractResponse {

    public static final long INVALID_OFFSET_LAG = -1L;

    private final DescribeLogDirsResponseData data;

    public DescribeLogDirsResponse(Struct struct, short version) {
        this.data = new DescribeLogDirsResponseData(struct, version);
    }

    public DescribeLogDirsResponse(DescribeLogDirsResponseData data) {
        this.data = data;
    }

    public DescribeLogDirsResponseData data() {
        return data;
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        data.results().forEach(result -> {
            updateErrorCounts(errorCounts, Errors.forCode(result.errorCode()));
        });
        return errorCounts;
    }

    public Map<String, LogDirInfo> logDirInfos() {
        HashMap<String, LogDirInfo> result = new HashMap<>(data.results().size());
        for (DescribeLogDirsResult logDirResult : data.results()) {
            Map<TopicPartition, ReplicaInfo> replicaInfoMap = new HashMap<>();
            for (DescribeLogDirsTopic t : logDirResult.topics()) {
                for (DescribeLogDirsPartition p : t.partitions()) {
                    replicaInfoMap.put(
                            new TopicPartition(t.name(), p.partitionIndex()),
                            new ReplicaInfo(p.partitionSize(), p.offsetLag(), p.isFutureKey()));
                }
            }
            result.put(logDirResult.logDir(), new LogDirInfo(Errors.forCode(logDirResult.errorCode()), replicaInfoMap));
        }
        return result;
    }

    public static DescribeLogDirsResponse parse(ByteBuffer buffer, short version) {
        return new DescribeLogDirsResponse(ApiKeys.DESCRIBE_LOG_DIRS.responseSchema(version).read(buffer), version);
    }

    // Note this class is part of the public API, reachable from Admin.describeLogDirs()
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

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("(error=")
                    .append(error)
                    .append(", replicas=")
                    .append(replicaInfos)
                    .append(")");
            return builder.toString();
        }
    }

    // Note this class is part of the public API, reachable from Admin.describeLogDirs()
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

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }
}
