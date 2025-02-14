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

import org.apache.kafka.common.message.ReadShareGroupStateResponseData;

import java.util.List;

/**
 * This class contains the result from {@link Persister#readState(ReadShareGroupStateParameters)}.
 */
public class ReadShareGroupStateResult implements PersisterResult {
    private final List<TopicData<PartitionAllData>> topicsData;

    private ReadShareGroupStateResult(List<TopicData<PartitionAllData>> topicsData) {
        this.topicsData = topicsData;
    }

    public List<TopicData<PartitionAllData>> topicsData() {
        return topicsData;
    }

    public static ReadShareGroupStateResult from(ReadShareGroupStateResponseData data) {
        return new Builder()
                .setTopicsData(data.results().stream()
                        .map(topicData -> new TopicData<>(topicData.topicId(),
                                topicData.partitions().stream()
                                        .map(partitionResult -> PartitionFactory.newPartitionAllData(
                                                partitionResult.partition(),
                                                partitionResult.stateEpoch(),
                                                partitionResult.startOffset(),
                                                partitionResult.errorCode(),
                                                partitionResult.errorMessage(),
                                                partitionResult.stateBatches().stream()
                                                        .map(PersisterStateBatch::from)
                                                        .toList()
                                        ))
                                        .toList()))
                        .toList())
                .build();
    }

    public static class Builder {

        private List<TopicData<PartitionAllData>> topicsData;

        public Builder setTopicsData(List<TopicData<PartitionAllData>> topicsData) {
            this.topicsData = topicsData;
            return this;
        }

        public ReadShareGroupStateResult build() {
            return new ReadShareGroupStateResult(topicsData);
        }
    }
}
