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

import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaPartitionState;
import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaTopicState;
import org.apache.kafka.common.message.StopReplicaResponseData;
import org.apache.kafka.common.message.StopReplicaResponseData.StopReplicaPartitionError;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.protocol.ApiKeys.STOP_REPLICA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StopReplicaResponseTest {

    @Test
    public void testErrorCountsFromGetErrorResponse() {
        List<StopReplicaTopicState> topicStates = new ArrayList<>();
        topicStates.add(new StopReplicaTopicState()
            .setTopicName("foo")
            .setPartitionStates(Arrays.asList(
                new StopReplicaPartitionState().setPartitionIndex(0),
                new StopReplicaPartitionState().setPartitionIndex(1))));

        for (short version : STOP_REPLICA.allVersions()) {
            StopReplicaRequest request = new StopReplicaRequest.Builder(version,
                15, 20, 0, false, topicStates).build(version);
            StopReplicaResponse response = request
                .getErrorResponse(0, Errors.CLUSTER_AUTHORIZATION_FAILED.exception());
            assertEquals(Collections.singletonMap(Errors.CLUSTER_AUTHORIZATION_FAILED, 3),
                response.errorCounts());
        }
    }

    @Test
    public void testErrorCountsWithTopLevelError() {
        List<StopReplicaPartitionError> errors = new ArrayList<>();
        errors.add(new StopReplicaPartitionError().setTopicName("foo").setPartitionIndex(0));
        errors.add(new StopReplicaPartitionError().setTopicName("foo").setPartitionIndex(1)
            .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code()));
        StopReplicaResponse response = new StopReplicaResponse(new StopReplicaResponseData()
            .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
            .setPartitionErrors(errors));
        assertEquals(Collections.singletonMap(Errors.UNKNOWN_SERVER_ERROR, 3), response.errorCounts());
    }

    @Test
    public void testErrorCountsNoTopLevelError() {
        List<StopReplicaPartitionError> errors = new ArrayList<>();
        errors.add(new StopReplicaPartitionError().setTopicName("foo").setPartitionIndex(0));
        errors.add(new StopReplicaPartitionError().setTopicName("foo").setPartitionIndex(1)
            .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code()));
        StopReplicaResponse response = new StopReplicaResponse(new StopReplicaResponseData()
            .setErrorCode(Errors.NONE.code())
            .setPartitionErrors(errors));
        Map<Errors, Integer> errorCounts = response.errorCounts();
        assertEquals(2, errorCounts.size());
        assertEquals(2, errorCounts.get(Errors.NONE).intValue());
        assertEquals(1, errorCounts.get(Errors.CLUSTER_AUTHORIZATION_FAILED).intValue());
    }

    @Test
    public void testToString() {
        List<StopReplicaPartitionError> errors = new ArrayList<>();
        errors.add(new StopReplicaPartitionError().setTopicName("foo").setPartitionIndex(0));
        errors.add(new StopReplicaPartitionError().setTopicName("foo").setPartitionIndex(1)
            .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code()));
        StopReplicaResponse response = new StopReplicaResponse(new StopReplicaResponseData().setPartitionErrors(errors));
        String responseStr = response.toString();
        assertTrue(responseStr.contains(StopReplicaResponse.class.getSimpleName()));
        assertTrue(responseStr.contains(errors.toString()));
        assertTrue(responseStr.contains("errorCode=" + Errors.NONE.code()));
    }

}
