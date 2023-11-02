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
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ApiException;

import java.util.Map;
import java.util.OptionalLong;

import static java.util.Collections.unmodifiableMap;
import static org.apache.kafka.common.requests.DescribeLogDirsResponse.UNKNOWN_VOLUME_BYTES;

/**
 * A description of a log directory on a particular broker.
 */
public class LogDirDescription {
    private final Map<TopicPartition, ReplicaInfo> replicaInfos;
    private final ApiException error;
    private final OptionalLong totalBytes;
    private final OptionalLong usableBytes;

    public LogDirDescription(ApiException error, Map<TopicPartition, ReplicaInfo> replicaInfos) {
        this(error, replicaInfos, UNKNOWN_VOLUME_BYTES, UNKNOWN_VOLUME_BYTES);
    }

    public LogDirDescription(ApiException error, Map<TopicPartition, ReplicaInfo> replicaInfos, long totalBytes, long usableBytes) {
        this.error = error;
        this.replicaInfos = replicaInfos;
        this.totalBytes = (totalBytes == UNKNOWN_VOLUME_BYTES) ? OptionalLong.empty() : OptionalLong.of(totalBytes);
        this.usableBytes = (usableBytes == UNKNOWN_VOLUME_BYTES) ? OptionalLong.empty() : OptionalLong.of(usableBytes);
    }

    /**
     * Returns `ApiException` if the log directory is offline or an error occurred, otherwise returns null.
     * <ul>
     * <li> KafkaStorageException - The log directory is offline.
     * <li> UnknownServerException - The server experienced an unexpected error when processing the request.
     * </ul>
     */
    public ApiException error() {
        return error;
    }

    /**
     * A map from topic partition to replica information for that partition
     * in this log directory.
     */
    public Map<TopicPartition, ReplicaInfo> replicaInfos() {
        return unmodifiableMap(replicaInfos);
    }

    /**
     * The total size of the volume this log directory is on or empty if the broker did not return a value.
     * For volumes larger than Long.MAX_VALUE, Long.MAX_VALUE is returned.
     */
    public OptionalLong totalBytes() {
        return totalBytes;
    }

    /**
     * The usable size on the volume this log directory is on or empty if the broker did not return a value.
     * For usable sizes larger than Long.MAX_VALUE, Long.MAX_VALUE is returned.
     */
    public OptionalLong usableBytes() {
        return usableBytes;
    }

    @Override
    public String toString() {
        return "LogDirDescription(" +
                "replicaInfos=" + replicaInfos +
                ", error=" + error +
                ", totalBytes=" + totalBytes +
                ", usableBytes=" + usableBytes +
                ')';
    }
}
