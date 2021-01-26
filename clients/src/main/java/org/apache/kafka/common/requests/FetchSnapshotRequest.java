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
import org.apache.kafka.common.message.FetchSnapshotRequestData;
import org.apache.kafka.common.message.FetchSnapshotResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Optional;
import java.util.function.UnaryOperator;

final public class FetchSnapshotRequest extends AbstractRequest {
    private final FetchSnapshotRequestData data;

    public FetchSnapshotRequest(FetchSnapshotRequestData data, short version) {
        super(ApiKeys.FETCH_SNAPSHOT, version);
        this.data = data;
    }

    @Override
    public FetchSnapshotResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new FetchSnapshotResponse(
            new FetchSnapshotResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(Errors.forException(e).code())
        );
    }

    @Override
    public FetchSnapshotRequestData data() {
        return data;
    }

    /**
     * Creates a FetchSnapshotRequestData with a single PartitionSnapshot for the topic partition.
     *
     * The partition index will already be populated when calling operator.
     *
     * @param topicPartition the topic partition to include
     * @param operator unary operator responsible for populating all the appropriate fields
     * @return the created fetch snapshot request data
     */
    public static FetchSnapshotRequestData singleton(
        TopicPartition topicPartition,
        UnaryOperator<FetchSnapshotRequestData.PartitionSnapshot> operator
    ) {
        FetchSnapshotRequestData.PartitionSnapshot partitionSnapshot = operator.apply(
            new FetchSnapshotRequestData.PartitionSnapshot().setPartition(topicPartition.partition())
        );

        return new FetchSnapshotRequestData()
            .setTopics(
                Collections.singletonList(
                    new FetchSnapshotRequestData.TopicSnapshot()
                        .setName(topicPartition.topic())
                        .setPartitions(Collections.singletonList(partitionSnapshot))
                )
            );
    }

    /**
     * Finds the PartitionSnapshot for a given topic partition.
     *
     * @param data the fetch snapshot request data
     * @param topicPartition the topic partition to find
     * @return the request partition snapshot if found, otherwise an empty Optional
     */
    public static Optional<FetchSnapshotRequestData.PartitionSnapshot> forTopicPartition(
        FetchSnapshotRequestData data,
        TopicPartition topicPartition
    ) {
        return data
            .topics()
            .stream()
            .filter(topic -> topic.name().equals(topicPartition.topic()))
            .flatMap(topic -> topic.partitions().stream())
            .filter(partition -> partition.partition() == topicPartition.partition())
            .findAny();
    }

    public static FetchSnapshotRequest parse(ByteBuffer buffer, short version) {
        return new FetchSnapshotRequest(new FetchSnapshotRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    public static class Builder extends AbstractRequest.Builder<FetchSnapshotRequest> {
        private final FetchSnapshotRequestData data;

        public Builder(FetchSnapshotRequestData  data) {
            super(ApiKeys.FETCH_SNAPSHOT);
            this.data = data;
        }

        @Override
        public FetchSnapshotRequest build(short version) {
            return new FetchSnapshotRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }
}
