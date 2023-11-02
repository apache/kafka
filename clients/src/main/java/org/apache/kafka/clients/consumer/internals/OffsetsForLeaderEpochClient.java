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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse;
import org.apache.kafka.common.utils.LogContext;

import java.util.Map;

/**
 * Convenience class for making asynchronous requests to the OffsetsForLeaderEpoch API
 */
public class OffsetsForLeaderEpochClient extends AsyncClient<
        Map<TopicPartition, SubscriptionState.FetchPosition>,
        OffsetsForLeaderEpochRequest,
        OffsetsForLeaderEpochResponse,
        OffsetsForLeaderEpochUtils.OffsetForEpochResult> {

    OffsetsForLeaderEpochClient(ConsumerNetworkClient client, LogContext logContext) {
        super(client, logContext);
    }

    @Override
    protected AbstractRequest.Builder<OffsetsForLeaderEpochRequest> prepareRequest(
            Node node, Map<TopicPartition, SubscriptionState.FetchPosition> requestData) {
        return OffsetsForLeaderEpochUtils.prepareRequest(requestData);
    }

    @Override
    protected OffsetsForLeaderEpochUtils.OffsetForEpochResult handleResponse(
            Node node,
            Map<TopicPartition, SubscriptionState.FetchPosition> requestData,
            OffsetsForLeaderEpochResponse response) {

        return OffsetsForLeaderEpochUtils.handleResponse(requestData, response);
    }
}