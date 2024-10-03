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

package org.apache.kafka.server.share.persister;

import org.apache.kafka.common.message.WriteShareGroupStateRequestData;

import java.util.stream.Collectors;

/**
 * This class contains the parameters for {@link Persister#writeState(WriteShareGroupStateParameters)}.
 */
public class WriteShareGroupStateParameters implements PersisterParameters {
    private final GroupTopicPartitionData<PartitionStateBatchData> groupTopicPartitionData;

    private WriteShareGroupStateParameters(GroupTopicPartitionData<PartitionStateBatchData> groupTopicPartitionData) {
        this.groupTopicPartitionData = groupTopicPartitionData;
    }

    public GroupTopicPartitionData<PartitionStateBatchData> groupTopicPartitionData() {
        return groupTopicPartitionData;
    }

    public static WriteShareGroupStateParameters from(WriteShareGroupStateRequestData data) {
        return new Builder()
                .setGroupTopicPartitionData(new GroupTopicPartitionData<>(data.groupId(), data.topics().stream()
                        .map(writeStateData -> new TopicData<>(writeStateData.topicId(),
                                writeStateData.partitions().stream()
                                        .map(partitionData -> PartitionFactory.newPartitionStateBatchData(partitionData.partition(), partitionData.stateEpoch(), partitionData.startOffset(),
                                                partitionData.leaderEpoch(),
                                                partitionData.stateBatches().stream()
                                                        .map(PersisterStateBatch::from)
                                                        .collect(Collectors.toList())))
                                        .collect(Collectors.toList())))
                        .collect(Collectors.toList())))
                .build();
    }

    public static class Builder {
        private GroupTopicPartitionData<PartitionStateBatchData> groupTopicPartitionData;

        public Builder setGroupTopicPartitionData(GroupTopicPartitionData<PartitionStateBatchData> groupTopicPartitionData) {
            this.groupTopicPartitionData = groupTopicPartitionData;
            return this;
        }

        public WriteShareGroupStateParameters build() {
            return new WriteShareGroupStateParameters(groupTopicPartitionData);
        }
    }

    @Override
    public String toString() {
        return "WriteShareGroupStateParameters(" + groupTopicPartitionData + ")";
    }
}
