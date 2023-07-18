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
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTransaction;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTransactionCollection;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTopicCollection;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnPartitionResult;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnPartitionResultCollection;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnResult;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResult;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResultCollection;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AddPartitionsToTxnRequest extends AbstractRequest {

    private static final short LAST_CLIENT_VERSION = (short) 3;
    // Note: earliest broker version is also the first version to support verification requests.
    private static final short EARLIEST_BROKER_VERSION = (short) 4;

    private final AddPartitionsToTxnRequestData data;

    public static class Builder extends AbstractRequest.Builder<AddPartitionsToTxnRequest> {
        public final AddPartitionsToTxnRequestData data;
        
        public static Builder forClient(String transactionalId,
                                        long producerId,
                                        short producerEpoch,
                                        List<TopicPartition> partitions) {

            AddPartitionsToTxnTopicCollection topics = buildTxnTopicCollection(partitions);
            
            return new Builder(ApiKeys.ADD_PARTITIONS_TO_TXN.oldestVersion(), LAST_CLIENT_VERSION,
                new AddPartitionsToTxnRequestData()
                    .setV3AndBelowTransactionalId(transactionalId)
                    .setV3AndBelowProducerId(producerId)
                    .setV3AndBelowProducerEpoch(producerEpoch)
                    .setV3AndBelowTopics(topics));
        }
        
        public static Builder forBroker(AddPartitionsToTxnTransactionCollection transactions) {
            return new Builder(EARLIEST_BROKER_VERSION, ApiKeys.ADD_PARTITIONS_TO_TXN.latestVersion(),
                new AddPartitionsToTxnRequestData()
                    .setTransactions(transactions));
        }
        
        private Builder(short minVersion, short maxVersion, AddPartitionsToTxnRequestData data) {
            super(ApiKeys.ADD_PARTITIONS_TO_TXN, minVersion, maxVersion);

            this.data = data;
        }

        private static AddPartitionsToTxnTopicCollection buildTxnTopicCollection(final List<TopicPartition> partitions) {
            Map<String, List<Integer>> partitionMap = new HashMap<>();
            for (TopicPartition topicPartition : partitions) {
                String topicName = topicPartition.topic();

                partitionMap.compute(topicName, (key, subPartitions) -> {
                    if (subPartitions == null) {
                        subPartitions = new ArrayList<>();
                    }
                    subPartitions.add(topicPartition.partition());
                    return subPartitions;
                });
            }

            AddPartitionsToTxnTopicCollection topics = new AddPartitionsToTxnTopicCollection();
            for (Map.Entry<String, List<Integer>> partitionEntry : partitionMap.entrySet()) {
                topics.add(new AddPartitionsToTxnTopic()
                    .setName(partitionEntry.getKey())
                    .setPartitions(partitionEntry.getValue()));
            }
            return topics;
        }

        @Override
        public AddPartitionsToTxnRequest build(short version) {
            return new AddPartitionsToTxnRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    public AddPartitionsToTxnRequest(final AddPartitionsToTxnRequestData data, short version) {
        super(ApiKeys.ADD_PARTITIONS_TO_TXN, version);
        this.data = data;
    }

    @Override
    public AddPartitionsToTxnRequestData data() {
        return data;
    }

    @Override
    public AddPartitionsToTxnResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        AddPartitionsToTxnResponseData response = new AddPartitionsToTxnResponseData();
        if (version() < EARLIEST_BROKER_VERSION) {
            response.setResultsByTopicV3AndBelow(errorResponseForTopics(data.v3AndBelowTopics(), error));
        } else {
            response.setErrorCode(error.code());
        }
        response.setThrottleTimeMs(throttleTimeMs);
        return new AddPartitionsToTxnResponse(response);
    }

    public static List<TopicPartition> getPartitions(AddPartitionsToTxnTopicCollection topics) {
        List<TopicPartition> partitions = new ArrayList<>();

        for (AddPartitionsToTxnTopic topicCollection : topics) {
            for (Integer partition : topicCollection.partitions()) {
                partitions.add(new TopicPartition(topicCollection.name(), partition));
            }
        }
        return partitions;
    }

    public Map<String, List<TopicPartition>> partitionsByTransaction() {
        Map<String, List<TopicPartition>> partitionsByTransaction = new HashMap<>();
        for (AddPartitionsToTxnTransaction transaction : data.transactions()) {
            List<TopicPartition> partitions = getPartitions(transaction.topics());
            partitionsByTransaction.put(transaction.transactionalId(), partitions);
        }
        return partitionsByTransaction;
    }

    // Takes a version 3 or below request (client request) and returns a v4+ singleton (one transaction ID) request.
    public AddPartitionsToTxnRequest normalizeRequest() {
        return new AddPartitionsToTxnRequest(new AddPartitionsToTxnRequestData().setTransactions(singletonTransaction()), version());
    }

    // This method returns true if all the transactions in it are verify only. One reason to distinguish is to separate
    // requests that will need to write to log in the non error case (adding partitions) from ones that will not (verify only).
    public boolean allVerifyOnlyRequest() {
        return version() > LAST_CLIENT_VERSION &&
            data.transactions().stream().allMatch(AddPartitionsToTxnTransaction::verifyOnly);
    }

    private AddPartitionsToTxnTransactionCollection singletonTransaction() {
        AddPartitionsToTxnTransactionCollection singleTxn = new AddPartitionsToTxnTransactionCollection();
        singleTxn.add(new AddPartitionsToTxnTransaction()
            .setTransactionalId(data.v3AndBelowTransactionalId())
            .setProducerId(data.v3AndBelowProducerId())
            .setProducerEpoch(data.v3AndBelowProducerEpoch())
            .setTopics(data.v3AndBelowTopics()));
        return singleTxn;
    }
    
    public AddPartitionsToTxnResult errorResponseForTransaction(String transactionalId, Errors e) {
        AddPartitionsToTxnResult txnResult = new AddPartitionsToTxnResult().setTransactionalId(transactionalId);
        AddPartitionsToTxnTopicResultCollection topicResults = errorResponseForTopics(data.transactions().find(transactionalId).topics(), e);
        txnResult.setTopicResults(topicResults);
        return txnResult;
    }
    
    private AddPartitionsToTxnTopicResultCollection errorResponseForTopics(AddPartitionsToTxnTopicCollection topics, Errors e) {
        AddPartitionsToTxnTopicResultCollection topicResults = new AddPartitionsToTxnTopicResultCollection();
        for (AddPartitionsToTxnTopic topic : topics) {
            AddPartitionsToTxnTopicResult topicResult = new AddPartitionsToTxnTopicResult().setName(topic.name());
            AddPartitionsToTxnPartitionResultCollection partitionResult = new AddPartitionsToTxnPartitionResultCollection();
            for (Integer partition : topic.partitions()) {
                partitionResult.add(new AddPartitionsToTxnPartitionResult()
                    .setPartitionIndex(partition)
                    .setPartitionErrorCode(e.code()));
            }
            topicResult.setResultsByPartition(partitionResult);
            topicResults.add(topicResult);
        }
        return topicResults;
    }

    public static AddPartitionsToTxnRequest parse(ByteBuffer buffer, short version) {
        return new AddPartitionsToTxnRequest(new AddPartitionsToTxnRequestData(new ByteBufferAccessor(buffer), version), version);
    }
}
