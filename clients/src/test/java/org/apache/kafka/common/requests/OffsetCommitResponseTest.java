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
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponsePartition;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.MessageUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.common.requests.AbstractResponse.DEFAULT_THROTTLE_TIME;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class OffsetCommitResponseTest {

    protected final int throttleTimeMs = 10;

    protected final String topicOne = "topic1";
    protected final int partitionOne = 1;
    protected final Errors errorOne = Errors.COORDINATOR_NOT_AVAILABLE;
    protected final Errors errorTwo = Errors.NOT_COORDINATOR;
    protected final String topicTwo = "topic2";
    protected final int partitionTwo = 2;

    protected TopicPartition tp1 = new TopicPartition(topicOne, partitionOne);
    protected TopicPartition tp2 = new TopicPartition(topicTwo, partitionTwo);
    protected Map<Errors, Integer> expectedErrorCounts;
    protected Map<TopicPartition, Errors> errorsMap;

    @BeforeEach
    public void setUp() {
        expectedErrorCounts = new HashMap<>();
        expectedErrorCounts.put(errorOne, 1);
        expectedErrorCounts.put(errorTwo, 1);

        errorsMap = new HashMap<>();
        errorsMap.put(tp1, errorOne);
        errorsMap.put(tp2, errorTwo);
    }

    @Test
    public void testConstructorWithErrorResponse() {
        OffsetCommitResponse response = new OffsetCommitResponse(throttleTimeMs, errorsMap);

        assertEquals(expectedErrorCounts, response.errorCounts());
        assertEquals(throttleTimeMs, response.throttleTimeMs());
    }

    @Test
    public void testParse() {
        OffsetCommitResponseData data = new OffsetCommitResponseData()
            .setTopics(Arrays.asList(
                new OffsetCommitResponseTopic().setPartitions(
                    Collections.singletonList(new OffsetCommitResponsePartition()
                        .setPartitionIndex(partitionOne)
                        .setErrorCode(errorOne.code()))),
                new OffsetCommitResponseTopic().setPartitions(
                    Collections.singletonList(new OffsetCommitResponsePartition()
                        .setPartitionIndex(partitionTwo)
                        .setErrorCode(errorTwo.code())))
            ))
            .setThrottleTimeMs(throttleTimeMs);

        for (short version = 0; version <= ApiKeys.OFFSET_COMMIT.latestVersion(); version++) {
            ByteBuffer buffer = MessageUtil.toByteBuffer(data, version);
            OffsetCommitResponse response = OffsetCommitResponse.parse(buffer, version);
            assertEquals(expectedErrorCounts, response.errorCounts());

            if (version >= 3) {
                assertEquals(throttleTimeMs, response.throttleTimeMs());
            } else {
                assertEquals(DEFAULT_THROTTLE_TIME, response.throttleTimeMs());
            }

            assertEquals(version >= 4, response.shouldClientThrottle(version));
        }
    }

}
