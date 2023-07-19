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
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;


public class DescribeLogDirsResponse extends AbstractResponse {

    public static final long INVALID_OFFSET_LAG = -1L;
    public static final long UNKNOWN_VOLUME_BYTES = -1L;

    private final DescribeLogDirsResponseData data;

    public DescribeLogDirsResponse(DescribeLogDirsResponseData data) {
        super(ApiKeys.DESCRIBE_LOG_DIRS);
        this.data = data;
    }

    @Override
    public DescribeLogDirsResponseData data() {
        return data;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        errorCounts.put(Errors.forCode(data.errorCode()), 1);
        data.results().forEach(result -> {
            updateErrorCounts(errorCounts, Errors.forCode(result.errorCode()));
        });
        return errorCounts;
    }

    public static DescribeLogDirsResponse parse(ByteBuffer buffer, short version) {
        return new DescribeLogDirsResponse(new DescribeLogDirsResponseData(new ByteBufferAccessor(buffer), version));
    }

    // Note this class is part of the public API, reachable from Admin.describeLogDirs()
    /**
     * Possible error code:
     *
     * KAFKA_STORAGE_ERROR (56)
     * UNKNOWN (-1)
     *
     * @deprecated Deprecated Since Kafka 2.7.
     * Use {@link org.apache.kafka.clients.admin.DescribeLogDirsResult#descriptions()}
     * and {@link org.apache.kafka.clients.admin.DescribeLogDirsResult#allDescriptions()} to access the replacement
     * class {@link org.apache.kafka.clients.admin.LogDirDescription}.
     */
    @Deprecated
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

    /**
     * @deprecated Deprecated Since Kafka 2.7.
     * Use {@link org.apache.kafka.clients.admin.DescribeLogDirsResult#descriptions()}
     * and {@link org.apache.kafka.clients.admin.DescribeLogDirsResult#allDescriptions()} to access the replacement
     * class {@link org.apache.kafka.clients.admin.ReplicaInfo}.
     */
    @Deprecated
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
