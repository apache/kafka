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

import org.apache.kafka.common.message.DeleteShareGroupStateRequestData;

import java.util.stream.Collectors;

/**
 * This class contains the parameters for {@link Persister#deleteState(DeleteShareGroupStateParameters)}.
 */
public class DeleteShareGroupStateParameters implements PersisterParameters {
    private final GroupTopicPartitionData<PartitionIdData> groupTopicPartitionData;

    private DeleteShareGroupStateParameters(GroupTopicPartitionData<PartitionIdData> groupTopicPartitionData) {
        this.groupTopicPartitionData = groupTopicPartitionData;
    }

    public static DeleteShareGroupStateParameters from(DeleteShareGroupStateRequestData data) {
        return new Builder()
                .setGroupTopicPartitionData(new GroupTopicPartitionData<>(data.groupId(), data.topics().stream()
                        .map(deleteStateData -> new TopicData<>(deleteStateData.topicId(), deleteStateData.partitions().stream()
                                .map(partitionData -> PartitionFactory.newPartitionIdData(partitionData.partition()))
                                .collect(Collectors.toList())))
                        .collect(Collectors.toList())))
                .build();
    }

    public GroupTopicPartitionData<PartitionIdData> groupTopicPartitionData() {
        return groupTopicPartitionData;
    }

    public static class Builder {
        private GroupTopicPartitionData<PartitionIdData> groupTopicPartitionData;

        public Builder setGroupTopicPartitionData(GroupTopicPartitionData<PartitionIdData> groupTopicPartitionData) {
            this.groupTopicPartitionData = groupTopicPartitionData;
            return this;
        }

        public DeleteShareGroupStateParameters build() {
            return new DeleteShareGroupStateParameters(groupTopicPartitionData);
        }
    }
}
