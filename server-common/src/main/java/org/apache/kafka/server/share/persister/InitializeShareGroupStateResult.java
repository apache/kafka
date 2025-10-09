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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.InitializeShareGroupStateResponseData;
import org.apache.kafka.common.protocol.Errors;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class contains the result from {@link Persister#initializeState(InitializeShareGroupStateParameters)}.
 */
public class InitializeShareGroupStateResult implements PersisterResult {
    private final List<TopicData<PartitionErrorData>> topicsData;

    private InitializeShareGroupStateResult(List<TopicData<PartitionErrorData>> topicsData) {
        this.topicsData = topicsData;
    }

    public List<TopicData<PartitionErrorData>> topicsData() {
        return topicsData;
    }

    public static InitializeShareGroupStateResult from(InitializeShareGroupStateResponseData data) {
        return new Builder()
                .setTopicsData(data.results().stream()
                        .map(initializeStateResult -> new TopicData<>(initializeStateResult.topicId(),
                                initializeStateResult.partitions().stream()
                                        .map(partitionResult -> PartitionFactory.newPartitionErrorData(partitionResult.partition(), partitionResult.errorCode(), partitionResult.errorMessage()))
                                        .collect(Collectors.toList())))
                        .collect(Collectors.toList()))
                .build();
    }

    public Map<Errors, Integer> errorCounts() {
        return topicsData.stream()
            .flatMap(topicData -> topicData.partitions().stream())
            .filter(e -> e.errorCode() != Errors.NONE.code())
            .collect(Collectors.groupingBy(
                partitionError -> Errors.forCode(partitionError.errorCode()),
                Collectors.summingInt(partitionError -> 1)
            ));
    }

    public Map<Uuid, Map<Integer, PartitionErrorData>> getErrors() {
        return topicsData.stream()
            .collect(Collectors.toMap(
                TopicData::topicId,
                topicData -> topicData.partitions().stream()
                    .collect(Collectors.toMap(
                        PartitionIdData::partition,
                        partitionErrorData -> partitionErrorData
                    ))
            ));
    }

    public static class Builder {
        private List<TopicData<PartitionErrorData>> topicsData;

        public Builder setTopicsData(List<TopicData<PartitionErrorData>> topicsData) {
            this.topicsData = topicsData;
            return this;
        }

        public InitializeShareGroupStateResult build() {
            return new InitializeShareGroupStateResult(topicsData);
        }
    }
}
