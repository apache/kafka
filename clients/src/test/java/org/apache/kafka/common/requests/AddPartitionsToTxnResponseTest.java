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
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnResult;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnResultCollection;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnPartitionResult;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResult;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResultCollection;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.common.requests.AddPartitionsToTxnResponse.errorsForTransaction;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AddPartitionsToTxnResponseTest {

    protected final int throttleTimeMs = 10;

    protected final String topicOne = "topic1";
    protected final int partitionOne = 1;
    protected final Errors errorOne = Errors.COORDINATOR_NOT_AVAILABLE;
    protected final Errors errorTwo = Errors.NOT_COORDINATOR;
    protected final String topicTwo = "topic2";
    protected final int partitionTwo = 2;
    protected final TopicPartition tp1 = new TopicPartition(topicOne, partitionOne);
    protected final TopicPartition tp2 = new TopicPartition(topicTwo, partitionTwo);

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

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.ADD_PARTITIONS_TO_TXN)
    public void testParse(short version) {
        AddPartitionsToTxnTopicResultCollection topicCollection = new AddPartitionsToTxnTopicResultCollection();

        AddPartitionsToTxnTopicResult topicResult = new AddPartitionsToTxnTopicResult();
        topicResult.setName(topicOne);

        topicResult.resultsByPartition().add(new AddPartitionsToTxnPartitionResult()
                                      .setPartitionErrorCode(errorOne.code())
                                      .setPartitionIndex(partitionOne));

        topicResult.resultsByPartition().add(new AddPartitionsToTxnPartitionResult()
                                      .setPartitionErrorCode(errorTwo.code())
                                      .setPartitionIndex(partitionTwo));

        topicCollection.add(topicResult);
            
        if (version < 4) {
            AddPartitionsToTxnResponseData data = new AddPartitionsToTxnResponseData()
                    .setResultsByTopicV3AndBelow(topicCollection)
                    .setThrottleTimeMs(throttleTimeMs);
            AddPartitionsToTxnResponse response = new AddPartitionsToTxnResponse(data);

            AddPartitionsToTxnResponse parsedResponse = AddPartitionsToTxnResponse.parse(response.serialize(version), version);
            assertEquals(expectedErrorCounts, parsedResponse.errorCounts());
            assertEquals(throttleTimeMs, parsedResponse.throttleTimeMs());
            assertEquals(version >= 1, parsedResponse.shouldClientThrottle(version));
        } else {
            AddPartitionsToTxnResultCollection results = new AddPartitionsToTxnResultCollection();
            results.add(new AddPartitionsToTxnResult().setTransactionalId("txn1").setTopicResults(topicCollection));
            
            // Create another transaction with new name and errorOne for a single partition.
            Map<TopicPartition, Errors> txnTwoExpectedErrors = Collections.singletonMap(tp2, errorOne);
            results.add(AddPartitionsToTxnResponse.resultForTransaction("txn2", txnTwoExpectedErrors));

            AddPartitionsToTxnResponseData data = new AddPartitionsToTxnResponseData()
                    .setResultsByTransaction(results)
                    .setThrottleTimeMs(throttleTimeMs);
            AddPartitionsToTxnResponse response = new AddPartitionsToTxnResponse(data);

            Map<Errors, Integer> newExpectedErrorCounts = new HashMap<>();
            newExpectedErrorCounts.put(Errors.NONE, 1); // top level error
            newExpectedErrorCounts.put(errorOne, 2);
            newExpectedErrorCounts.put(errorTwo, 1);
            
            AddPartitionsToTxnResponse parsedResponse = AddPartitionsToTxnResponse.parse(response.serialize(version), version);
            assertEquals(txnTwoExpectedErrors, errorsForTransaction(response.getTransactionTopicResults("txn2")));
            assertEquals(newExpectedErrorCounts, parsedResponse.errorCounts());
            assertEquals(throttleTimeMs, parsedResponse.throttleTimeMs());
            assertTrue(parsedResponse.shouldClientThrottle(version));
        }
    }
    
    @Test
    public void testBatchedErrors() {
        Map<TopicPartition, Errors> txn1Errors = Collections.singletonMap(tp1, errorOne);
        Map<TopicPartition, Errors> txn2Errors = Collections.singletonMap(tp1, errorOne);
        
        AddPartitionsToTxnResult transaction1 = AddPartitionsToTxnResponse.resultForTransaction("txn1", txn1Errors);
        AddPartitionsToTxnResult transaction2 = AddPartitionsToTxnResponse.resultForTransaction("txn2", txn2Errors);
        
        AddPartitionsToTxnResultCollection results = new AddPartitionsToTxnResultCollection();
        results.add(transaction1);
        results.add(transaction2);
        
        AddPartitionsToTxnResponse response = new AddPartitionsToTxnResponse(new AddPartitionsToTxnResponseData().setResultsByTransaction(results));
        
        assertEquals(txn1Errors, errorsForTransaction(response.getTransactionTopicResults("txn1")));
        assertEquals(txn2Errors, errorsForTransaction(response.getTransactionTopicResults("txn2")));
        
        Map<String, Map<TopicPartition, Errors>> expectedErrors = new HashMap<>();
        expectedErrors.put("txn1", txn1Errors);
        expectedErrors.put("txn2", txn2Errors);
        assertEquals(expectedErrors, response.errors());
    }
}
