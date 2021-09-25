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

import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.message.DeleteGroupsResponseData.DeletableGroupResult;
import org.apache.kafka.common.message.DeleteGroupsResponseData.DeletableGroupResultCollection;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DeleteGroupsResponseTest {

    private static final String GROUP_ID_1 = "groupId1";
    private static final String GROUP_ID_2 = "groupId2";
    private static final int THROTTLE_TIME_MS = 10;
    private static DeleteGroupsResponse deleteGroupsResponse;

    static {
        deleteGroupsResponse = new DeleteGroupsResponse(
            new DeleteGroupsResponseData()
                .setResults(
                    new DeletableGroupResultCollection(Arrays.asList(
                        new DeletableGroupResult()
                            .setGroupId(GROUP_ID_1)
                            .setErrorCode(Errors.NONE.code()),
                        new DeletableGroupResult()
                            .setGroupId(GROUP_ID_2)
                            .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code())).iterator()
                    )
                )
                .setThrottleTimeMs(THROTTLE_TIME_MS));
    }

    @Test
    public void testGetErrorWithExistingGroupIds() {
        assertEquals(Errors.NONE, deleteGroupsResponse.get(GROUP_ID_1));
        assertEquals(Errors.GROUP_AUTHORIZATION_FAILED, deleteGroupsResponse.get(GROUP_ID_2));

        Map<String, Errors> expectedErrors = new HashMap<>();
        expectedErrors.put(GROUP_ID_1, Errors.NONE);
        expectedErrors.put(GROUP_ID_2, Errors.GROUP_AUTHORIZATION_FAILED);
        assertEquals(expectedErrors, deleteGroupsResponse.errors());

        Map<Errors, Integer> expectedErrorCounts = new HashMap<>();
        expectedErrorCounts.put(Errors.NONE, 1);
        expectedErrorCounts.put(Errors.GROUP_AUTHORIZATION_FAILED, 1);
        assertEquals(expectedErrorCounts, deleteGroupsResponse.errorCounts());
    }

    @Test
    public void testGetErrorWithInvalidGroupId() {
        assertThrows(IllegalArgumentException.class, () -> deleteGroupsResponse.get("invalid-group-id"));
    }

    @Test
    public void testGetThrottleTimeMs() {
        assertEquals(THROTTLE_TIME_MS, deleteGroupsResponse.throttleTimeMs());
    }
}
