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

import org.apache.kafka.common.message.DeleteGroupsRequestData;
import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.apache.kafka.common.requests.DeleteGroupsRequest.getErrorResultCollection;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DeleteGroupsRequestTest {

    @Test
    public void testGetErrorResultCollection() {
        String groupId1 = "group-id-1";
        String groupId2 = "group-id-2";
        DeleteGroupsRequestData data = new DeleteGroupsRequestData()
            .setGroupsNames(Arrays.asList(groupId1, groupId2));
        DeleteGroupsResponseData.DeletableGroupResultCollection expectedResultCollection =
            new DeleteGroupsResponseData.DeletableGroupResultCollection(Arrays.asList(
                new DeleteGroupsResponseData.DeletableGroupResult()
                    .setGroupId(groupId1)
                    .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code()),
                new DeleteGroupsResponseData.DeletableGroupResult()
                    .setGroupId(groupId2)
                    .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code())
            ).iterator());

        assertEquals(expectedResultCollection, getErrorResultCollection(data.groupsNames(), Errors.COORDINATOR_LOAD_IN_PROGRESS));
    }
}
