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
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConsumerGroupDescribeRequestTest {

    @Test
    void testGetErrorResponse() {
        List<String> groupIds = Arrays.asList("group0", "group1");
        ConsumerGroupDescribeRequestData data = new ConsumerGroupDescribeRequestData();
        data.groupIds().addAll(groupIds);
        ConsumerGroupDescribeRequest request = new ConsumerGroupDescribeRequest.Builder(data, true)
            .build();
        Throwable e = Errors.GROUP_AUTHORIZATION_FAILED.exception();
        int throttleTimeMs = 1000;

        ConsumerGroupDescribeResponse response = request.getErrorResponse(throttleTimeMs, e);

        assertEquals(throttleTimeMs, response.throttleTimeMs());
        for (int i = 0; i < groupIds.size(); i++) {
            ConsumerGroupDescribeResponseData.DescribedGroup group = response.data().groups().get(i);
            assertEquals(groupIds.get(i), group.groupId());
            assertEquals(Errors.forException(e).code(), group.errorCode());
        }
    }
}
