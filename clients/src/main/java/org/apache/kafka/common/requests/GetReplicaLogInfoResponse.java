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

import org.apache.kafka.common.message.GetReplicaLogInfoResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetReplicaLogInfoResponse extends AbstractResponse {
    private final GetReplicaLogInfoResponseData data;

    public GetReplicaLogInfoResponse(GetReplicaLogInfoResponseData data) {
        super(ApiKeys.GET_REPLICA_LOG_INFO);
        this.data = data;
    }

    @Override
    public GetReplicaLogInfoResponseData data() {
        return data;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        data.topicPartitionLogInfoList().forEach(topicPartitionLogInfo -> {
            topicPartitionLogInfo.partitionLogInfo().forEach(partitionLogInfo -> {
                updateErrorCounts(errorCounts, Errors.forCode(partitionLogInfo.errorCode()));
            });
        });
        return errorCounts;
    }


    public static GetReplicaLogInfoResponse prepareResponse(List<GetReplicaLogInfoResponseData.TopicPartitionLogInfo> topicPartitionLogInfoList) {
        GetReplicaLogInfoResponseData responseData = new GetReplicaLogInfoResponseData();
        topicPartitionLogInfoList.forEach(topicPartitionLogInfo -> {
            responseData.topicPartitionLogInfoList().add(topicPartitionLogInfo);
        });
        return new GetReplicaLogInfoResponse(responseData);
    }

    public static GetReplicaLogInfoResponse parse(Readable readable, short version) {
        return new GetReplicaLogInfoResponse(new GetReplicaLogInfoResponseData(readable, version));
    }
}
