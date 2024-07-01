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

import org.apache.kafka.common.message.ConsumerGroupDescribeRequestData;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.protocol.Errors;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.kafka.common.requests.ConsumerGroupDescribeRequest.getErrorDescribedGroupList;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConsumerGroupRequestTest {
    private static final int THROTTLE_TIME_MS = 1000;

    @Test
    void testGetErrorConsumerGroupHeartbeatResponse() {
        ConsumerGroupHeartbeatRequestData data = new ConsumerGroupHeartbeatRequestData();
        ConsumerGroupHeartbeatRequest request = new ConsumerGroupHeartbeatRequest.Builder(data).build();
        Throwable e = Errors.UNSUPPORTED_VERSION.exception();
        ConsumerGroupHeartbeatResponse response = request.getErrorResponse(THROTTLE_TIME_MS, e);

        assertEquals(THROTTLE_TIME_MS, response.throttleTimeMs());
        ApiError apiError = ApiError.fromThrowable(e);
        assertEquals(apiError.code(), response.data().errorCode());
        assertEquals(e.getMessage(), response.data().errorMessage());
    }

    @Test
    void testGetErrorConsumerGroupDescribeResponse() {
        List<String> groupIds = Arrays.asList("group0", "group1");
        ConsumerGroupDescribeRequestData data = new ConsumerGroupDescribeRequestData();
        data.groupIds().addAll(groupIds);
        ConsumerGroupDescribeRequest request = new ConsumerGroupDescribeRequest.Builder(data, true)
            .build();
        Throwable e = Errors.GROUP_AUTHORIZATION_FAILED.exception();
        ConsumerGroupDescribeResponse response = request.getErrorResponse(THROTTLE_TIME_MS, e);

        assertEquals(THROTTLE_TIME_MS, response.throttleTimeMs());
        ApiError apiError = ApiError.fromThrowable(e);
        for (int i = 0; i < groupIds.size(); i++) {
            ConsumerGroupDescribeResponseData.DescribedGroup group = response.data().groups().get(i);
            assertEquals(groupIds.get(i), group.groupId());
            assertEquals(apiError.code(), group.errorCode());
            assertEquals(e.getMessage(), group.errorMessage());
        }
    }

    @Test
    public void testGetErrorDescribedGroupListResponse() {
        List<ConsumerGroupDescribeResponseData.DescribedGroup> expectedDescribedGroupList = Arrays.asList(
            new ConsumerGroupDescribeResponseData.DescribedGroup()
                .setGroupId("group-id-1")
                .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code())
                .setErrorMessage(Errors.COORDINATOR_LOAD_IN_PROGRESS.message()),
            new ConsumerGroupDescribeResponseData.DescribedGroup()
                .setGroupId("group-id-2")
                .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code())
                .setErrorMessage(Errors.COORDINATOR_LOAD_IN_PROGRESS.message()),
            new ConsumerGroupDescribeResponseData.DescribedGroup()
                .setGroupId("group-id-3")
                .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code())
                .setErrorMessage(Errors.COORDINATOR_LOAD_IN_PROGRESS.message())
        );

        List<ConsumerGroupDescribeResponseData.DescribedGroup> describedGroupList = getErrorDescribedGroupList(
            Arrays.asList("group-id-1", "group-id-2", "group-id-3"),
            Errors.COORDINATOR_LOAD_IN_PROGRESS
        );

        assertEquals(expectedDescribedGroupList, describedGroupList);
    }
}
