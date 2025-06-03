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

import org.apache.kafka.common.message.GetReplicaLogInfoRequestData;
import org.apache.kafka.common.message.GetReplicaLogInfoResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.ArrayList;
import java.util.List;

public class GetReplicaLogInfoRequest extends AbstractRequest {
    public static final int MAX_PARTITIONS_PER_REQUEST = 1000;

    public static class Builder extends AbstractRequest.Builder<GetReplicaLogInfoRequest> {

        private final GetReplicaLogInfoRequestData data;

        public Builder(GetReplicaLogInfoRequestData data) {
            super(ApiKeys.GET_REPLICA_LOG_INFO);
            this.data = data;
        }

        public Builder(List<GetReplicaLogInfoRequestData.TopicPartitions> topicPartitions) {
            super(ApiKeys.GET_REPLICA_LOG_INFO, ApiKeys.GET_REPLICA_LOG_INFO.oldestVersion(),
                  ApiKeys.GET_REPLICA_LOG_INFO.latestVersion());
            GetReplicaLogInfoRequestData data = new GetReplicaLogInfoRequestData();
            data.setTopicPartitions(topicPartitions);
            this.data = data;
        }

        @Override
        public GetReplicaLogInfoRequest build(short version) {
            return new GetReplicaLogInfoRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final GetReplicaLogInfoRequestData data;

    public GetReplicaLogInfoRequest(GetReplicaLogInfoRequestData data, short version) {
        super(ApiKeys.GET_REPLICA_LOG_INFO, version);
        this.data = data;
    }

    @Override
    public GetReplicaLogInfoRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        GetReplicaLogInfoResponseData responseData = new GetReplicaLogInfoResponseData();
        for (GetReplicaLogInfoRequestData.TopicPartitions topicPartition : data().topicPartitions()) {
            ArrayList<GetReplicaLogInfoResponseData.PartitionLogInfo> partitionLogInfos = new ArrayList<>(topicPartition.partitions().size());
            for (Integer partition: topicPartition.partitions()) {
                partitionLogInfos.add(new GetReplicaLogInfoResponseData.PartitionLogInfo()
                    .setPartition(partition)
                    .setErrorCode(error.code())
                );
            }
            responseData.topicPartitionLogInfoList().add(new GetReplicaLogInfoResponseData.TopicPartitionLogInfo()
                    .setTopicId(topicPartition.topicId())
                    .setPartitionLogInfo(partitionLogInfos));
        }
        return new GetReplicaLogInfoResponse(responseData);
    }

    public static GetReplicaLogInfoRequest parse(Readable readable, short version) {
        return new GetReplicaLogInfoRequest(
                new GetReplicaLogInfoRequestData(readable, version),
                version
        );
    }

}
