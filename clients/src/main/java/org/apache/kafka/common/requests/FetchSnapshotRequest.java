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

import java.util.Collections;
import java.util.Optional;
import java.util.function.UnaryOperator;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.FetchSnapshotRequestData;
import org.apache.kafka.common.message.FetchSnapshotResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

final public class FetchSnapshotRequest extends AbstractRequest {
    public final FetchSnapshotRequestData data;

    public FetchSnapshotRequest(FetchSnapshotRequestData data) {
        super(ApiKeys.FETCH_SNAPSHOT, (short) (FetchSnapshotRequestData.SCHEMAS.length - 1));
        this.data = data;
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }

    @Override
    public FetchSnapshotResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        // TODO: we need to handle throttleTimeMs
        return new FetchSnapshotResponse(new FetchSnapshotResponseData().setErrorCode(Errors.forException(e).code()));
    }

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

    // TODO: write documentation. This function assumes that topic partitions are unique in `data`
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
}
