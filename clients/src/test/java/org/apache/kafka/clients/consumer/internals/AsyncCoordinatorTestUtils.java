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

import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class AsyncCoordinatorTestUtils {
    static class MockCommitCallback implements OffsetCommitCallback {
        public int invoked = 0;
        public Exception exception = null;

        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            invoked++;
            this.exception = exception;
        }
    }

    static GroupRebalanceConfig buildRebalanceConfig(
            final int sessionTimeoutMs,
            final int rebalanceTimeoutMs,
            final int heartbeatIntervalMs,
            final String groupId,
            final Optional<String> groupInstanceId,
            final long retryBackoffMs) {
        return new GroupRebalanceConfig(sessionTimeoutMs,
                rebalanceTimeoutMs,
                heartbeatIntervalMs,
                groupId,
                groupInstanceId,
                retryBackoffMs,
                !groupInstanceId.isPresent());
    }


    static HeartbeatResponse heartbeatResponse(Errors error) {
        return new HeartbeatResponse(new HeartbeatResponseData().setErrorCode(error.code()));
    }

    static FindCoordinatorResponse groupCoordinatorResponse(Node node, Errors error, String groupId) {
        return FindCoordinatorResponse.prepareResponse(error, groupId, node);
    }

    static void prepareOffsetCommitRequest(
            Map<TopicPartition, Long> expectedOffsets,
            Errors error,
            MockClient client) {
        prepareOffsetCommitRequest(expectedOffsets, error, false, client);
    }

    static void prepareOffsetCommitRequestDisconnect(
            Map<TopicPartition, Long> expectedOffsets,
            MockClient client) {
        prepareOffsetCommitRequest(expectedOffsets, Errors.NONE, true, client);
    }

    static Map<TopicPartition, Errors> partitionErrors(
            Collection<TopicPartition> partitions,
            Errors error) {
        final Map<TopicPartition, Errors> errors = new HashMap<>();
        for (TopicPartition partition : partitions) {
            errors.put(partition, error);
        }
        return errors;
    }

    static void prepareOffsetCommitRequest(
            final Map<TopicPartition, Long> expectedOffsets,
            Errors error,
            boolean disconnected,
            MockClient client) {
        Map<TopicPartition, Errors> errors = partitionErrors(expectedOffsets.keySet(), error);
        client.prepareResponse(offsetCommitRequestMatcher(expectedOffsets), offsetCommitResponse(errors), disconnected);
    }

    static OffsetCommitResponse offsetCommitResponse(Map<TopicPartition, Errors> responseData) {
        return new OffsetCommitResponse(responseData);
    }

    static MockClient.RequestMatcher offsetCommitRequestMatcher(final Map<TopicPartition,
            Long> expectedOffsets) {
        return body -> {
            OffsetCommitRequest req = (OffsetCommitRequest) body;
            Map<TopicPartition, Long> offsets = req.offsets();
            if (offsets.size() != expectedOffsets.size()) {
                return false;
            }

            for (Map.Entry<TopicPartition, Long> expectedOffset : expectedOffsets.entrySet()) {
                if (!offsets.containsKey(expectedOffset.getKey())) {

                    return false;
                } else {
                    Long actualOffset = offsets.get(expectedOffset.getKey());
                    if (!actualOffset.equals(expectedOffset.getValue())) {
                        return false;
                    }
                }
            }
            return true;
        };
    }
}
