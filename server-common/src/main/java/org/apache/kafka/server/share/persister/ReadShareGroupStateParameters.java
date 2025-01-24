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

import org.apache.kafka.common.message.ReadShareGroupStateRequestData;

import java.util.stream.Collectors;

/**
 * This class contains the parameters for {@link Persister#readState(ReadShareGroupStateParameters)}.
 */
public class ReadShareGroupStateParameters implements PersisterParameters {
    private final GroupTopicPartitionData<PartitionIdLeaderEpochData> groupTopicPartitionData;

    private ReadShareGroupStateParameters(GroupTopicPartitionData<PartitionIdLeaderEpochData> groupTopicPartitionData) {
        this.groupTopicPartitionData = groupTopicPartitionData;
    }

    public GroupTopicPartitionData<PartitionIdLeaderEpochData> groupTopicPartitionData() {
        return groupTopicPartitionData;
    }

    public static ReadShareGroupStateParameters from(ReadShareGroupStateRequestData data) {
        return new Builder()
                .setGroupTopicPartitionData(new GroupTopicPartitionData<>(data.groupId(), data.topics().stream()
                        .map(readStateData -> new TopicData<>(readStateData.topicId(),
                                readStateData.partitions().stream()
                                        .map(partitionData -> PartitionFactory.newPartitionIdLeaderEpochData(partitionData.partition(), partitionData.leaderEpoch()))
                                        .collect(Collectors.toList())))
                        .collect(Collectors.toList())))
                .build();
    }

    public static class Builder {
        private GroupTopicPartitionData<PartitionIdLeaderEpochData> groupTopicPartitionData;

        public Builder setGroupTopicPartitionData(GroupTopicPartitionData<PartitionIdLeaderEpochData> groupTopicPartitionData) {
            this.groupTopicPartitionData = groupTopicPartitionData;
            return this;
        }

        public ReadShareGroupStateParameters build() {
            return new ReadShareGroupStateParameters(groupTopicPartitionData);
        }
    }

    @Override
    public String toString() {
        return "ReadShareGroupStateParameters(" + groupTopicPartitionData + ")";
    }
}
