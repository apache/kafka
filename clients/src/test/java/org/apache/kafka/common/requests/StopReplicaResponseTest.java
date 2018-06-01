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
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Utils;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StopReplicaResponseTest {

    @Test
    public void testErrorCountsFromGetErrorResponse() {
        StopReplicaRequest request = new StopReplicaRequest.Builder(15, 20, false,
                Utils.mkSet(new TopicPartition("foo", 0), new TopicPartition("foo", 1))).build();
        StopReplicaResponse response = request.getErrorResponse(0, Errors.CLUSTER_AUTHORIZATION_FAILED.exception());
        assertEquals(Collections.singletonMap(Errors.CLUSTER_AUTHORIZATION_FAILED, 2), response.errorCounts());
    }

    @Test
    public void testErrorCountsWithTopLevelError() {
        Map<TopicPartition, Errors> errors = new HashMap<>();
        errors.put(new TopicPartition("foo", 0), Errors.NONE);
        errors.put(new TopicPartition("foo", 1), Errors.NOT_LEADER_FOR_PARTITION);
        StopReplicaResponse response = new StopReplicaResponse(Errors.UNKNOWN_SERVER_ERROR, errors);
        assertEquals(Collections.singletonMap(Errors.UNKNOWN_SERVER_ERROR, 2), response.errorCounts());
    }

    @Test
    public void testErrorCountsNoTopLevelError() {
        Map<TopicPartition, Errors> errors = new HashMap<>();
        errors.put(new TopicPartition("foo", 0), Errors.NONE);
        errors.put(new TopicPartition("foo", 1), Errors.CLUSTER_AUTHORIZATION_FAILED);
        StopReplicaResponse response = new StopReplicaResponse(Errors.NONE, errors);
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
        StopReplicaResponse response = new StopReplicaResponse(Errors.NONE, errors);
        String responseStr = response.toString();
        assertTrue(responseStr.contains(StopReplicaResponse.class.getSimpleName()));
        assertTrue(responseStr.contains(errors.toString()));
        assertTrue(responseStr.contains(Errors.NONE.name()));
    }

}
