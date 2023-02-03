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
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;

import java.util.ArrayList;

import java.util.Collections;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AddPartitionsToTxnRequestTest {

    private static String transactionalId = "transactionalId";
    private static int producerId = 10;
    private static short producerEpoch = 1;
    private static int throttleTimeMs = 10;

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.ADD_PARTITIONS_TO_TXN)
    public void testConstructor(short version) {
        TopicPartition tp0 = new TopicPartition("topic", 0);
        TopicPartition tp1 = new TopicPartition("topic", 1);

        if (version < 4) {
            List<TopicPartition> partitions = new ArrayList<>();
            partitions.add(tp0);
            partitions.add(tp1);

            AddPartitionsToTxnRequest.Builder builder = new AddPartitionsToTxnRequest.Builder(transactionalId, producerId, producerEpoch, partitions);
            AddPartitionsToTxnRequest request = builder.build(version);

            assertEquals(transactionalId, request.data().transactionalId());
            assertEquals(producerId, request.data().producerId());
            assertEquals(producerEpoch, request.data().producerEpoch());
            assertEquals(partitions, request.partitions());

            AddPartitionsToTxnResponse response = request.getErrorResponse(throttleTimeMs, Errors.UNKNOWN_TOPIC_OR_PARTITION.exception());

            assertEquals(Collections.singletonMap(Errors.UNKNOWN_TOPIC_OR_PARTITION, 2), response.errorCounts());
            assertEquals(throttleTimeMs, response.throttleTimeMs());
        } else {
            String transaction1 = "transaction1";
            String transaction2 = "transaction2";

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
                    .setTransactionalId(transaction1)
                    .setProducerId(producerId)
                    .setProducerEpoch(producerEpoch)
                    .setTopics(topics0));
            transactions.add(new AddPartitionsToTxnTransaction()
                    .setTransactionalId(transaction2)
                    .setProducerId(producerId + 1)
                    .setProducerEpoch((short) (producerEpoch + 1))
                    .setTopics(topics1));

            boolean verifyOnly = true;

            AddPartitionsToTxnRequest.Builder builder = new AddPartitionsToTxnRequest.Builder(transactions, verifyOnly);
            AddPartitionsToTxnRequest request = builder.build(version);
            
            AddPartitionsToTxnTransaction reqTxn1 = request.data().transactions().find(transaction1);
            AddPartitionsToTxnTransaction reqTxn2 = request.data().transactions().find(transaction2);

            assertEquals(verifyOnly, request.data().verifyOnly());
            assertEquals(transactions.find(transaction1), reqTxn1);
            assertEquals(transactions.find(transaction2), reqTxn2);
        }
    }
}
