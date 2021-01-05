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
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnPartitionResult;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResult;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResultCollection;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class AddPartitionsToTxnResponseTest {

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

    @Before
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
        AddPartitionsToTxnResponse response = new AddPartitionsToTxnResponse(throttleTimeMs, errorsMap);

        assertEquals(expectedErrorCounts, response.errorCounts());
        assertEquals(throttleTimeMs, response.throttleTimeMs());
    }

    @Test
    public void testParse() {

        AddPartitionsToTxnTopicResultCollection topicCollection = new AddPartitionsToTxnTopicResultCollection();

        AddPartitionsToTxnTopicResult topicResult = new AddPartitionsToTxnTopicResult();
        topicResult.setName(topicOne);

        topicResult.results().add(new AddPartitionsToTxnPartitionResult()
                                      .setErrorCode(errorOne.code())
                                      .setPartitionIndex(partitionOne));

        topicResult.results().add(new AddPartitionsToTxnPartitionResult()
                                      .setErrorCode(errorTwo.code())
                                      .setPartitionIndex(partitionTwo));

        topicCollection.add(topicResult);

        AddPartitionsToTxnResponseData data = new AddPartitionsToTxnResponseData()
                                                  .setResults(topicCollection)
                                                  .setThrottleTimeMs(throttleTimeMs);
        AddPartitionsToTxnResponse response = new AddPartitionsToTxnResponse(data);

        for (short version = 0; version <= ApiKeys.ADD_PARTITIONS_TO_TXN.latestVersion(); version++) {
            AddPartitionsToTxnResponse parsedResponse = AddPartitionsToTxnResponse.parse(response.serialize(version), version);
            assertEquals(expectedErrorCounts, parsedResponse.errorCounts());
            assertEquals(throttleTimeMs, parsedResponse.throttleTimeMs());
            assertEquals(version >= 1, parsedResponse.shouldClientThrottle(version));
        }
    }
}
