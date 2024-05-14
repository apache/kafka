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

import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.kafka.common.requests.DescribeGroupsRequest.getErrorDescribedGroupList;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DescribeGroupsRequestTest {

    @Test
    public void testGetErrorDescribedGroupList() {
        List<DescribeGroupsResponseData.DescribedGroup> expectedDescribedGroupList = Arrays.asList(
            new DescribeGroupsResponseData.DescribedGroup()
                .setGroupId("group-id-1")
                .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code()),
            new DescribeGroupsResponseData.DescribedGroup()
                .setGroupId("group-id-2")
                .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code()),
            new DescribeGroupsResponseData.DescribedGroup()
                .setGroupId("group-id-3")
                .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code())
        );

        List<DescribeGroupsResponseData.DescribedGroup> describedGroupList = getErrorDescribedGroupList(
            Arrays.asList("group-id-1", "group-id-2", "group-id-3"),
            Errors.COORDINATOR_LOAD_IN_PROGRESS
        );

        assertEquals(expectedDescribedGroupList, describedGroupList);
    }
}
