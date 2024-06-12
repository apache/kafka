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

import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ConsumerGroupDescribeResponseTest {

    @Test
    void testErrorCounts() {
        Errors e = Errors.INVALID_GROUP_ID;
        int errorCount = 2;
        ConsumerGroupDescribeResponseData data = new ConsumerGroupDescribeResponseData();
        for (int i = 0; i < errorCount; i++) {
            data.groups().add(
                new ConsumerGroupDescribeResponseData.DescribedGroup()
                    .setErrorCode(e.code())
            );
        }
        ConsumerGroupDescribeResponse response = new ConsumerGroupDescribeResponse(data);

        Map<Errors, Integer> counts = response.errorCounts();

        assertEquals(errorCount, counts.get(e));
        assertNull(counts.get(Errors.COORDINATOR_NOT_AVAILABLE));
    }
}
