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
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTransaction;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTransactionCollection;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTopicCollection;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData;
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import static org.apache.kafka.common.requests.AddPartitionsToTxnResponse.errorsForTransaction;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class AddPartitionsToTxnRequestTest {
    private final String transactionalId1 = "transaction1";
    private final String transactionalId2 = "transaction2";
    private static int producerId = 10;
    private static short producerEpoch = 1;
    private static int throttleTimeMs = 10;
    private static TopicPartition tp0 = new TopicPartition("topic", 0);
    private static TopicPartition tp1 = new TopicPartition("topic", 1);

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.ADD_PARTITIONS_TO_TXN)
    public void testConstructor(short version) {
        
        AddPartitionsToTxnRequest request;

        if (version < 4) {
            List<TopicPartition> partitions = new ArrayList<>();
            partitions.add(tp0);
            partitions.add(tp1);

            AddPartitionsToTxnRequest.Builder builder = AddPartitionsToTxnRequest.Builder.forClient(transactionalId1, producerId, producerEpoch, partitions);
            request = builder.build(version);

            assertEquals(transactionalId1, request.data().v3AndBelowTransactionalId());
            assertEquals(producerId, request.data().v3AndBelowProducerId());
            assertEquals(producerEpoch, request.data().v3AndBelowProducerEpoch());
            assertEquals(partitions, AddPartitionsToTxnRequest.getPartitions(request.data().v3AndBelowTopics()));
        } else {
            AddPartitionsToTxnTransactionCollection transactions = createTwoTransactionCollection();

            AddPartitionsToTxnRequest.Builder builder = AddPartitionsToTxnRequest.Builder.forBroker(transactions);
            request = builder.build(version);
            
            AddPartitionsToTxnTransaction reqTxn1 = request.data().transactions().find(transactionalId1);
            AddPartitionsToTxnTransaction reqTxn2 = request.data().transactions().find(transactionalId2);

            assertEquals(transactions.find(transactionalId1), reqTxn1);
            assertEquals(transactions.find(transactionalId2), reqTxn2);
        }
        AddPartitionsToTxnResponse response = request.getErrorResponse(throttleTimeMs, Errors.UNKNOWN_TOPIC_OR_PARTITION.exception());

        assertEquals(throttleTimeMs, response.throttleTimeMs());
        
        if (version >= 4) {
            assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), response.data().errorCode());
            // Since the error is top level, we count it as one error in the counts.
            assertEquals(Collections.singletonMap(Errors.UNKNOWN_TOPIC_OR_PARTITION, 1), response.errorCounts());
        } else {
            assertEquals(Collections.singletonMap(Errors.UNKNOWN_TOPIC_OR_PARTITION, 2), response.errorCounts());  
        }
    }
    
    @Test
    public void testBatchedRequests() {
        AddPartitionsToTxnTransactionCollection transactions = createTwoTransactionCollection();

        AddPartitionsToTxnRequest.Builder builder = AddPartitionsToTxnRequest.Builder.forBroker(transactions);
        AddPartitionsToTxnRequest request = builder.build(ApiKeys.ADD_PARTITIONS_TO_TXN.latestVersion());
        
        Map<String, List<TopicPartition>> expectedMap = new HashMap<>();
        expectedMap.put(transactionalId1, Collections.singletonList(tp0));
        expectedMap.put(transactionalId2, Collections.singletonList(tp1));
        
        assertEquals(expectedMap, request.partitionsByTransaction());

        AddPartitionsToTxnResponseData.AddPartitionsToTxnResultCollection results = new AddPartitionsToTxnResponseData.AddPartitionsToTxnResultCollection();
        
        results.add(request.errorResponseForTransaction(transactionalId1, Errors.UNKNOWN_TOPIC_OR_PARTITION));
        results.add(request.errorResponseForTransaction(transactionalId2, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED));
        
        AddPartitionsToTxnResponse response = new AddPartitionsToTxnResponse(new AddPartitionsToTxnResponseData()
                .setResultsByTransaction(results)
                .setThrottleTimeMs(throttleTimeMs));
        
        assertEquals(Collections.singletonMap(tp0, Errors.UNKNOWN_TOPIC_OR_PARTITION), errorsForTransaction(response.getTransactionTopicResults(transactionalId1)));
        assertEquals(Collections.singletonMap(tp1, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED), errorsForTransaction(response.getTransactionTopicResults(transactionalId2)));
    }
    
    @Test
    public void testNormalizeRequest() {
        List<TopicPartition> partitions = new ArrayList<>();
        partitions.add(tp0);
        partitions.add(tp1);

        AddPartitionsToTxnRequest.Builder builder = AddPartitionsToTxnRequest.Builder.forClient(transactionalId1, producerId, producerEpoch, partitions);
        AddPartitionsToTxnRequest request = builder.build((short) 3);

        AddPartitionsToTxnRequest singleton = request.normalizeRequest();
        assertEquals(partitions, singleton.partitionsByTransaction().get(transactionalId1));
        
        AddPartitionsToTxnTransaction transaction = singleton.data().transactions().find(transactionalId1);
        assertEquals(producerId, transaction.producerId());
        assertEquals(producerEpoch, transaction.producerEpoch());
    }
    
    private AddPartitionsToTxnTransactionCollection createTwoTransactionCollection() {
        AddPartitionsToTxnTopicCollection topics0 = new AddPartitionsToTxnTopicCollection();
        topics0.add(new AddPartitionsToTxnTopic()
                .setName(tp0.topic())
                .setPartitions(Collections.singletonList(tp0.partition())));
        AddPartitionsToTxnTopicCollection topics1 = new AddPartitionsToTxnTopicCollection();
        topics1.add(new AddPartitionsToTxnTopic()
                .setName(tp1.topic())
                .setPartitions(Collections.singletonList(tp1.partition())));

        AddPartitionsToTxnTransactionCollection transactions = new AddPartitionsToTxnTransactionCollection();
        transactions.add(new AddPartitionsToTxnTransaction()
                .setTransactionalId(transactionalId1)
                .setProducerId(producerId)
                .setProducerEpoch(producerEpoch)
                .setVerifyOnly(true)
                .setTopics(topics0));
        transactions.add(new AddPartitionsToTxnTransaction()
                .setTransactionalId(transactionalId2)
                .setProducerId(producerId + 1)
                .setProducerEpoch((short) (producerEpoch + 1))
                .setVerifyOnly(false)
                .setTopics(topics1));
        return transactions;
    }
}
