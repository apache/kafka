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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState;
import org.apache.kafka.common.message.LeaderAndIsrResponseData;
import org.apache.kafka.common.message.LeaderAndIsrResponseData.LeaderAndIsrTopicError;
import org.apache.kafka.common.message.LeaderAndIsrResponseData.LeaderAndIsrPartitionError;
import org.apache.kafka.common.message.LeaderAndIsrResponseData.LeaderAndIsrTopicErrorCollection;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.protocol.ApiKeys.LEADER_AND_ISR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LeaderAndIsrResponseTest {

    @Test
    public void testErrorCountsFromGetErrorResponse() {
        List<LeaderAndIsrPartitionState> partitionStates = new ArrayList<>();
        partitionStates.add(new LeaderAndIsrPartitionState()
            .setTopicName("foo")
            .setPartitionIndex(0)
            .setControllerEpoch(15)
            .setLeader(1)
            .setLeaderEpoch(10)
            .setIsr(Collections.singletonList(10))
            .setPartitionEpoch(20)
            .setReplicas(Collections.singletonList(10))
            .setIsNew(false));
        partitionStates.add(new LeaderAndIsrPartitionState()
            .setTopicName("foo")
            .setPartitionIndex(1)
            .setControllerEpoch(15)
            .setLeader(1)
            .setLeaderEpoch(10)
            .setIsr(Collections.singletonList(10))
            .setPartitionEpoch(20)
            .setReplicas(Collections.singletonList(10))
            .setIsNew(false));
        Map<String, Uuid> topicIds = Collections.singletonMap("foo", Uuid.randomUuid());

        LeaderAndIsrRequest request = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion(),
                15, 20, 0, partitionStates, topicIds, Collections.emptySet()).build();
        LeaderAndIsrResponse response = request.getErrorResponse(0, Errors.CLUSTER_AUTHORIZATION_FAILED.exception());
        assertEquals(Collections.singletonMap(Errors.CLUSTER_AUTHORIZATION_FAILED, 3), response.errorCounts());
    }

    @Test
    public void testErrorCountsWithTopLevelError() {
        for (short version : LEADER_AND_ISR.allVersions()) {
            LeaderAndIsrResponse response;
            if (version < 5) {
                List<LeaderAndIsrPartitionError> partitions = createPartitions("foo",
                        asList(Errors.NONE, Errors.NOT_LEADER_OR_FOLLOWER));
                response = new LeaderAndIsrResponse(new LeaderAndIsrResponseData()
                        .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                        .setPartitionErrors(partitions), version);
            } else {
                Uuid id = Uuid.randomUuid();
                LeaderAndIsrTopicErrorCollection topics = createTopic(id, asList(Errors.NONE, Errors.NOT_LEADER_OR_FOLLOWER));
                response = new LeaderAndIsrResponse(new LeaderAndIsrResponseData()
                        .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                        .setTopics(topics), version); 
            }
            assertEquals(Collections.singletonMap(Errors.UNKNOWN_SERVER_ERROR, 3), response.errorCounts());
        }
    }

    @Test
    public void testErrorCountsNoTopLevelError() {
        for (short version : LEADER_AND_ISR.allVersions()) {
            LeaderAndIsrResponse response;
            if (version < 5) {
                List<LeaderAndIsrPartitionError> partitions = createPartitions("foo",
                        asList(Errors.NONE, Errors.CLUSTER_AUTHORIZATION_FAILED));
                response = new LeaderAndIsrResponse(new LeaderAndIsrResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setPartitionErrors(partitions), version);
            } else {
                Uuid id = Uuid.randomUuid();
                LeaderAndIsrTopicErrorCollection topics = createTopic(id, asList(Errors.NONE, Errors.CLUSTER_AUTHORIZATION_FAILED));
                response = new LeaderAndIsrResponse(new LeaderAndIsrResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setTopics(topics), version);
            }
            Map<Errors, Integer> errorCounts = response.errorCounts();
            assertEquals(2, errorCounts.size());
            assertEquals(2, errorCounts.get(Errors.NONE).intValue());
            assertEquals(1, errorCounts.get(Errors.CLUSTER_AUTHORIZATION_FAILED).intValue());
        }
    }

    @Test
    public void testToString() {
        for (short version : LEADER_AND_ISR.allVersions()) {
            LeaderAndIsrResponse response;
            if (version < 5) {
                List<LeaderAndIsrPartitionError> partitions = createPartitions("foo",
                        asList(Errors.NONE, Errors.CLUSTER_AUTHORIZATION_FAILED));
                response = new LeaderAndIsrResponse(new LeaderAndIsrResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setPartitionErrors(partitions), version);
                String responseStr = response.toString();
                assertTrue(responseStr.contains(LeaderAndIsrResponse.class.getSimpleName()));
                assertTrue(responseStr.contains(partitions.toString()));
                assertTrue(responseStr.contains("errorCode=" + Errors.NONE.code()));

            } else {
                Uuid id = Uuid.randomUuid();
                LeaderAndIsrTopicErrorCollection topics = createTopic(id, asList(Errors.NONE, Errors.CLUSTER_AUTHORIZATION_FAILED));
                response = new LeaderAndIsrResponse(new LeaderAndIsrResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setTopics(topics), version);
                String responseStr = response.toString();
                assertTrue(responseStr.contains(LeaderAndIsrResponse.class.getSimpleName()));
                assertTrue(responseStr.contains(topics.toString()));
                assertTrue(responseStr.contains(id.toString()));
                assertTrue(responseStr.contains("errorCode=" + Errors.NONE.code()));
            }
        }
    }

    private List<LeaderAndIsrPartitionError> createPartitions(String topicName, List<Errors> errors) {
        List<LeaderAndIsrPartitionError> partitions = new ArrayList<>();
        int partitionIndex = 0;
        for (Errors error : errors) {
            partitions.add(new LeaderAndIsrPartitionError()
                    .setTopicName(topicName)
                    .setPartitionIndex(partitionIndex++)
                    .setErrorCode(error.code()));
        }
        return partitions;
    }

    private LeaderAndIsrTopicErrorCollection createTopic(Uuid id, List<Errors> errors) {
        LeaderAndIsrTopicErrorCollection topics = new LeaderAndIsrTopicErrorCollection();
        LeaderAndIsrTopicError topic = new LeaderAndIsrTopicError();
        topic.setTopicId(id);
        List<LeaderAndIsrPartitionError> partitions = new ArrayList<>();
        int partitionIndex = 0;
        for (Errors error : errors) {
            partitions.add(new LeaderAndIsrPartitionError()
                .setPartitionIndex(partitionIndex++)
                .setErrorCode(error.code()));
        }
        topic.setPartitionErrors(partitions);
        topics.add(topic);
        return topics;
    }

}
