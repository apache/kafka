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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrRequestPartition;
import org.apache.kafka.common.message.LeaderAndIsrResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LeaderAndIsrResponseTest {

    @Test
    public void testErrorCountsFromGetErrorResponse() {
        HashMap<TopicPartition, LeaderAndIsrRequestPartition> partitionStates = new HashMap<>();
        partitionStates.put(new TopicPartition("foo", 0), new LeaderAndIsrRequestPartition()
            .setControllerEpoch(15)
            .setLeaderKey(1)
            .setLeaderEpoch(10)
            .setIsrReplicas(Collections.singletonList(10))
            .setZkVersion(20)
            .setReplicas(Collections.singletonList(10))
            .setIsNew(false));
        partitionStates.put(new TopicPartition("foo", 1), new LeaderAndIsrRequestPartition()
            .setControllerEpoch(15)
            .setLeaderKey(1)
            .setLeaderEpoch(10)
            .setIsrReplicas(Collections.singletonList(10))
            .setZkVersion(20)
            .setReplicas(Collections.singletonList(10))
            .setIsNew(false));
        LeaderAndIsrRequest request = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion(),
                15, 20, 0, partitionStates, Collections.emptySet()).build();
        LeaderAndIsrResponse response = request.getErrorResponse(0, Errors.CLUSTER_AUTHORIZATION_FAILED.exception());
        assertEquals(Collections.singletonMap(Errors.CLUSTER_AUTHORIZATION_FAILED, 2), response.errorCounts());
    }

    @Test
    public void testErrorCountsWithTopLevelError() {
        List<LeaderAndIsrResponseData.LeaderAndIsrResponsePartition> partitions = createPartitions("foo",
            asList(Errors.NONE, Errors.NOT_LEADER_FOR_PARTITION));
        LeaderAndIsrResponse response = new LeaderAndIsrResponse(new LeaderAndIsrResponseData()
            .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
            .setPartitions(partitions));
        assertEquals(Collections.singletonMap(Errors.UNKNOWN_SERVER_ERROR, 2), response.errorCounts());
    }

    @Test
    public void testErrorCountsNoTopLevelError() {
        List<LeaderAndIsrResponseData.LeaderAndIsrResponsePartition> partitions = createPartitions("foo",
            asList(Errors.NONE, Errors.CLUSTER_AUTHORIZATION_FAILED));
        LeaderAndIsrResponse response = new LeaderAndIsrResponse(new LeaderAndIsrResponseData()
                .setErrorCode(Errors.NONE.code())
                .setPartitions(partitions));
        Map<Errors, Integer> errorCounts = response.errorCounts();
        assertEquals(2, errorCounts.size());
        assertEquals(1, errorCounts.get(Errors.NONE).intValue());
        assertEquals(1, errorCounts.get(Errors.CLUSTER_AUTHORIZATION_FAILED).intValue());
    }

    @Test
    public void testToString() {
        List<LeaderAndIsrResponseData.LeaderAndIsrResponsePartition> partitions = createPartitions("foo",
                asList(Errors.NONE, Errors.CLUSTER_AUTHORIZATION_FAILED));
        LeaderAndIsrResponse response = new LeaderAndIsrResponse(new LeaderAndIsrResponseData()
                .setErrorCode(Errors.NONE.code())
                .setPartitions(partitions));
        String responseStr = response.toString();
        assertTrue(responseStr.contains(LeaderAndIsrResponse.class.getSimpleName()));
        assertTrue(responseStr.contains(partitions.toString()));
        assertTrue(responseStr.contains("errorCode=" + Errors.NONE.code()));
    }

    private List<LeaderAndIsrResponseData.LeaderAndIsrResponsePartition> createPartitions(String topicName, List<Errors> errors) {
        List<LeaderAndIsrResponseData.LeaderAndIsrResponsePartition> partitions = new ArrayList<>();
        int partitionIndex = 0;
        for (Errors error : errors) {
            partitions.add(new LeaderAndIsrResponseData.LeaderAndIsrResponsePartition()
                    .setTopicName(topicName)
                    .setPartitionIndex(partitionIndex++)
                    .setErrorCode(error.code()));
        }
        return partitions;
    }

}
