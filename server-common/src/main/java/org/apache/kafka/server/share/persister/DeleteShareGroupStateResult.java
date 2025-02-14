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

import org.apache.kafka.common.message.DeleteShareGroupStateResponseData;

import java.util.List;

/**
 * This class contains the result from {@link Persister#deleteState(DeleteShareGroupStateParameters)}.
 */
public class DeleteShareGroupStateResult implements PersisterResult {
    private final List<TopicData<PartitionErrorData>> topicsData;

    private DeleteShareGroupStateResult(List<TopicData<PartitionErrorData>> topicsData) {
        this.topicsData = topicsData;
    }

    public List<TopicData<PartitionErrorData>> topicsData() {
        return topicsData;
    }

    public static DeleteShareGroupStateResult from(DeleteShareGroupStateResponseData data) {
        return new Builder()
                .setTopicsData(data.results().stream()
                        .map(deleteStateResult -> new TopicData<>(deleteStateResult.topicId(), deleteStateResult.partitions().stream()
                                .map(partitionResult -> PartitionFactory.newPartitionErrorData(partitionResult.partition(), partitionResult.errorCode(), partitionResult.errorMessage()))
                                .toList()))
                        .toList())
                .build();
    }

    public static class Builder {
        private List<TopicData<PartitionErrorData>> topicsData;

        public Builder setTopicsData(List<TopicData<PartitionErrorData>> topicsData) {
            this.topicsData = topicsData;
            return this;
        }

        public DeleteShareGroupStateResult build() {
            return new DeleteShareGroupStateResult(topicsData);
        }
    }
}
