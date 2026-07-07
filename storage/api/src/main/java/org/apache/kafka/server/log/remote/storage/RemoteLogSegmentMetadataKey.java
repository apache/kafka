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
package org.apache.kafka.server.log.remote.storage;

import org.apache.kafka.common.Uuid;

import java.util.Objects;

/**
 * Structured key for remote log segment metadata Kafka records.
 */
public record RemoteLogSegmentMetadataKey(Uuid topicId, int partition, Uuid segmentId, byte stateId) {

    public RemoteLogSegmentMetadataKey(Uuid topicId, int partition, Uuid segmentId, byte stateId) {
        this.topicId = Objects.requireNonNull(topicId, "topicId cannot be null");
        this.partition = partition;
        this.segmentId = Objects.requireNonNull(segmentId, "segmentId cannot be null");
        this.stateId = stateId;
    }

    public static RemoteLogSegmentMetadataKey of(RemoteLogSegmentId segmentId, RemoteLogSegmentState state) {
        return new RemoteLogSegmentMetadataKey(
                segmentId.topicIdPartition().topicId(),
                segmentId.topicIdPartition().partition(),
                segmentId.id(),
                state.id()
        );
    }
}
