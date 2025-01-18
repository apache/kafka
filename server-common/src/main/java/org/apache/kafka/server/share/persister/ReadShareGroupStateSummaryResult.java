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

import org.apache.kafka.common.message.ReadShareGroupStateSummaryResponseData;

import java.util.List;
import java.util.stream.Collectors;

/**
 * This class contains the result from {@link Persister#readSummary(ReadShareGroupStateSummaryParameters)}.
 */
public class ReadShareGroupStateSummaryResult implements PersisterResult {
    private final List<TopicData<PartitionStateErrorData>> topicsData;

    private ReadShareGroupStateSummaryResult(List<TopicData<PartitionStateErrorData>> topicsData) {
        this.topicsData = topicsData;
    }

    public static ReadShareGroupStateSummaryResult from(ReadShareGroupStateSummaryResponseData data) {
        return new Builder()
                .setTopicsData(data.results().stream()
                        .map(readStateSummaryResult -> new TopicData<>(readStateSummaryResult.topicId(),
                                readStateSummaryResult.partitions().stream()
                                        .map(partitionResult -> PartitionFactory.newPartitionStateErrorData(
                                                partitionResult.partition(), partitionResult.stateEpoch(), partitionResult.startOffset(), partitionResult.errorCode(), partitionResult.errorMessage()))
                                        .collect(Collectors.toList())))
                        .collect(Collectors.toList()))
                .build();
    }

    public static class Builder {
        private List<TopicData<PartitionStateErrorData>> topicsData;

        public Builder setTopicsData(List<TopicData<PartitionStateErrorData>> topicsData) {
            this.topicsData = topicsData;
            return this;
        }

        public ReadShareGroupStateSummaryResult build() {
            return new ReadShareGroupStateSummaryResult(topicsData);
        }
    }
}
