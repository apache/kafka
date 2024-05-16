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
    private static final int PRODUCER_ID = 10;
    private static final short PRODUCER_EPOCH = 1;
    private static final int THROTTLE_TIME_MS = 10;
    private static final TopicPartition TP_0 = new TopicPartition("topic", 0);
    private static final TopicPartition TP_1 = new TopicPartition("topic", 1);
    private final String transactionalId1 = "transaction1";
    private final String transactionalId2 = "transaction2";

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.ADD_PARTITIONS_TO_TXN)
    public void testConstructor(short version) {
        
        AddPartitionsToTxnRequest request;

        if (version < 4) {
            List<TopicPartition> partitions = new ArrayList<>();
            partitions.add(TP_0);
            partitions.add(TP_1);

            AddPartitionsToTxnRequest.Builder builder = AddPartitionsToTxnRequest.Builder.forClient(transactionalId1, PRODUCER_ID, PRODUCER_EPOCH, partitions);
            request = builder.build(version);

            assertEquals(transactionalId1, request.data().v3AndBelowTransactionalId());
            assertEquals(PRODUCER_ID, request.data().v3AndBelowProducerId());
            assertEquals(PRODUCER_EPOCH, request.data().v3AndBelowProducerEpoch());
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
        AddPartitionsToTxnResponse response = request.getErrorResponse(THROTTLE_TIME_MS, Errors.UNKNOWN_TOPIC_OR_PARTITION.exception());

        assertEquals(THROTTLE_TIME_MS, response.throttleTimeMs());
        
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
        expectedMap.put(transactionalId1, Collections.singletonList(TP_0));
        expectedMap.put(transactionalId2, Collections.singletonList(TP_1));
        
        assertEquals(expectedMap, request.partitionsByTransaction());

        AddPartitionsToTxnResponseData.AddPartitionsToTxnResultCollection results = new AddPartitionsToTxnResponseData.AddPartitionsToTxnResultCollection();
        
        results.add(request.errorResponseForTransaction(transactionalId1, Errors.UNKNOWN_TOPIC_OR_PARTITION));
        results.add(request.errorResponseForTransaction(transactionalId2, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED));
        
        AddPartitionsToTxnResponse response = new AddPartitionsToTxnResponse(new AddPartitionsToTxnResponseData()
                .setResultsByTransaction(results)
                .setThrottleTimeMs(THROTTLE_TIME_MS));
        
        assertEquals(Collections.singletonMap(TP_0, Errors.UNKNOWN_TOPIC_OR_PARTITION), errorsForTransaction(response.getTransactionTopicResults(transactionalId1)));
        assertEquals(Collections.singletonMap(TP_1, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED), errorsForTransaction(response.getTransactionTopicResults(transactionalId2)));
    }
    
    @Test
    public void testNormalizeRequest() {
        List<TopicPartition> partitions = new ArrayList<>();
        partitions.add(TP_0);
        partitions.add(TP_1);

        AddPartitionsToTxnRequest.Builder builder = AddPartitionsToTxnRequest.Builder.forClient(transactionalId1, PRODUCER_ID, PRODUCER_EPOCH, partitions);
        AddPartitionsToTxnRequest request = builder.build((short) 3);

        AddPartitionsToTxnRequest singleton = request.normalizeRequest();
        assertEquals(partitions, singleton.partitionsByTransaction().get(transactionalId1));
        
        AddPartitionsToTxnTransaction transaction = singleton.data().transactions().find(transactionalId1);
        assertEquals(PRODUCER_ID, transaction.producerId());
        assertEquals(PRODUCER_EPOCH, transaction.producerEpoch());
    }
    
    private AddPartitionsToTxnTransactionCollection createTwoTransactionCollection() {
        AddPartitionsToTxnTopicCollection topics0 = new AddPartitionsToTxnTopicCollection();
        topics0.add(new AddPartitionsToTxnTopic()
                .setName(TP_0.topic())
                .setPartitions(Collections.singletonList(TP_0.partition())));
        AddPartitionsToTxnTopicCollection topics1 = new AddPartitionsToTxnTopicCollection();
        topics1.add(new AddPartitionsToTxnTopic()
                .setName(TP_1.topic())
                .setPartitions(Collections.singletonList(TP_1.partition())));

        AddPartitionsToTxnTransactionCollection transactions = new AddPartitionsToTxnTransactionCollection();
        transactions.add(new AddPartitionsToTxnTransaction()
                .setTransactionalId(transactionalId1)
                .setProducerId(PRODUCER_ID)
                .setProducerEpoch(PRODUCER_EPOCH)
                .setVerifyOnly(true)
                .setTopics(topics0));
        transactions.add(new AddPartitionsToTxnTransaction()
                .setTransactionalId(transactionalId2)
                .setProducerId(PRODUCER_ID + 1)
                .setProducerEpoch((short) (PRODUCER_EPOCH + 1))
                .setVerifyOnly(false)
                .setTopics(topics1));
        return transactions;
    }
}
