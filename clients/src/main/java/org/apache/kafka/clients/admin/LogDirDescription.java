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

import static java.util.Collections.unmodifiableMap;

/**
 * A description of a log directory on a particular broker.
 */
public class LogDirDescription {
    private final Map<TopicPartition, ReplicaInfo> replicaInfos;
    private final ApiException error;

    public LogDirDescription(ApiException error, Map<TopicPartition, ReplicaInfo> replicaInfos) {
        this.error = error;
        this.replicaInfos = replicaInfos;
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

    @Override
    public String toString() {
        return "LogDirDescription(" +
                "replicaInfos=" + replicaInfos +
                ", error=" + error +
                ')';
    }
}
