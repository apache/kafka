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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LeaderAndIsrResponseTest {

    @Test
    public void testErrorCountsFromGetErrorResponse() {
        HashMap<TopicPartition, LeaderAndIsrRequest.PartitionState> partitionStates = new HashMap<>();
        partitionStates.put(new TopicPartition("foo", 0), new LeaderAndIsrRequest.PartitionState(15, 1, 10,
                Collections.singletonList(10), 20, Collections.singletonList(10), false));
        partitionStates.put(new TopicPartition("foo", 1), new LeaderAndIsrRequest.PartitionState(15, 1, 10,
                Collections.singletonList(10), 20, Collections.singletonList(10), false));
        LeaderAndIsrRequest request = new LeaderAndIsrRequest.Builder(ApiKeys.LEADER_AND_ISR.latestVersion(),
                15, 20, 0, partitionStates, Collections.<Node>emptySet()).build();
        LeaderAndIsrResponse response = request.getErrorResponse(0, Errors.CLUSTER_AUTHORIZATION_FAILED.exception());
        assertEquals(Collections.singletonMap(Errors.CLUSTER_AUTHORIZATION_FAILED, 2), response.errorCounts());
    }

    @Test
    public void testErrorCountsWithTopLevelError() {
        Map<TopicPartition, Errors> errors = new HashMap<>();
        errors.put(new TopicPartition("foo", 0), Errors.NONE);
        errors.put(new TopicPartition("foo", 1), Errors.NOT_LEADER_FOR_PARTITION);
        LeaderAndIsrResponse response = new LeaderAndIsrResponse(Errors.UNKNOWN_SERVER_ERROR, errors);
        assertEquals(Collections.singletonMap(Errors.UNKNOWN_SERVER_ERROR, 2), response.errorCounts());
    }

    @Test
    public void testErrorCountsNoTopLevelError() {
        Map<TopicPartition, Errors> errors = new HashMap<>();
        errors.put(new TopicPartition("foo", 0), Errors.NONE);
        errors.put(new TopicPartition("foo", 1), Errors.CLUSTER_AUTHORIZATION_FAILED);
        LeaderAndIsrResponse response = new LeaderAndIsrResponse(Errors.NONE, errors);
        Map<Errors, Integer> errorCounts = response.errorCounts();
        assertEquals(2, errorCounts.size());
        assertEquals(1, errorCounts.get(Errors.NONE).intValue());
        assertEquals(1, errorCounts.get(Errors.CLUSTER_AUTHORIZATION_FAILED).intValue());
    }

    @Test
    public void testToString() {
        Map<TopicPartition, Errors> errors = new HashMap<>();
        errors.put(new TopicPartition("foo", 0), Errors.NONE);
        errors.put(new TopicPartition("foo", 1), Errors.CLUSTER_AUTHORIZATION_FAILED);
        LeaderAndIsrResponse response = new LeaderAndIsrResponse(Errors.NONE, errors);
        String responseStr = response.toString();
        assertTrue(responseStr.contains(LeaderAndIsrResponse.class.getSimpleName()));
        assertTrue(responseStr.contains(errors.toString()));
        assertTrue(responseStr.contains(Errors.NONE.name()));
    }

}
