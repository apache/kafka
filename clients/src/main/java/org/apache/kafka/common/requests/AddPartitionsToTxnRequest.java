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
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnResultCollection;
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

    private final AddPartitionsToTxnRequestData data;

    private List<TopicPartition> cachedPartitions = null;
    
    private Map<String, List<TopicPartition>> cachedPartitionsByTransaction = null;

    private final short version;

    public static class Builder extends AbstractRequest.Builder<AddPartitionsToTxnRequest> {
        public final AddPartitionsToTxnRequestData data;
        
        public static Builder forClient(String transactionalId,
                                        long producerId,
                                        short producerEpoch,
                                        List<TopicPartition> partitions) {

            AddPartitionsToTxnTopicCollection topics = buildTxnTopicCollection(partitions);
            
            return new Builder(ApiKeys.ADD_PARTITIONS_TO_TXN.oldestVersion(),
                    (short) 3, 
                new AddPartitionsToTxnRequestData()
                    .setV3AndBelowTransactionalId(transactionalId)
                    .setV3AndBelowProducerId(producerId)
                    .setV3AndBelowProducerEpoch(producerEpoch)
                    .setV3AndBelowTopics(topics));
        }
        
        public static Builder forBroker(AddPartitionsToTxnTransactionCollection transactions) {
            return new Builder((short) 4,
                    ApiKeys.ADD_PARTITIONS_TO_TXN.latestVersion(),
                    new AddPartitionsToTxnRequestData()
                        .setTransactions(transactions));
        }
        
        public Builder(short minVersion, short maxVersion, AddPartitionsToTxnRequestData data) {
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
        this.version = version;
    }
    
    // Only used for versions < 4
    public List<TopicPartition> partitions() {
        if (cachedPartitions != null) {
            return cachedPartitions;
        }
        cachedPartitions = getPartitions(data);
        return cachedPartitions;
    }

    // Only used for versions < 4
    static List<TopicPartition> getPartitions(AddPartitionsToTxnRequestData data) {
        List<TopicPartition> partitions = new ArrayList<>();

        for (AddPartitionsToTxnTopic topicCollection : data.v3AndBelowTopics()) {
            for (Integer partition : topicCollection.partitions()) {
                partitions.add(new TopicPartition(topicCollection.name(), partition));
            }
        }
        return partitions;
    }
    
    private List<TopicPartition> partitionsForTransaction(String transaction) {
        if (cachedPartitionsByTransaction == null) {
            cachedPartitionsByTransaction = new HashMap<>();
        }
        
        return cachedPartitionsByTransaction.computeIfAbsent(transaction, txn -> {
            List<TopicPartition> partitions = new ArrayList<>();
            for (AddPartitionsToTxnTopic topicCollection : data.transactions().find(txn).topics()) {
                for (Integer partition : topicCollection.partitions()) {
                    partitions.add(new TopicPartition(topicCollection.name(), partition));
                }
            }
            return partitions;
        });
    }
    
    public Map<String, List<TopicPartition>> partitionsByTransaction() {
        if (cachedPartitionsByTransaction != null && cachedPartitionsByTransaction.size() == data.transactions().size()) {
            return cachedPartitionsByTransaction;
        }
        
        for (AddPartitionsToTxnTransaction transaction : data.transactions()) {
            if (cachedPartitionsByTransaction == null || !cachedPartitionsByTransaction.containsKey(transaction.transactionalId())) {
                partitionsForTransaction(transaction.transactionalId());
            }
        }
        return cachedPartitionsByTransaction;
    }
    
    // Takes a version 3 or below request and returns a v4+ singleton (one transaction ID) request.
    public AddPartitionsToTxnRequest normalizeRequest() {
        return new AddPartitionsToTxnRequest(new AddPartitionsToTxnRequestData().setTransactions(singletonTransaction()), version);
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

    @Override
    public AddPartitionsToTxnRequestData data() {
        return data;
    }

    @Override
    public AddPartitionsToTxnResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        if (version < 4) {
            final HashMap<TopicPartition, Errors> errors = new HashMap<>();
            for (TopicPartition partition : partitions()) {
                errors.put(partition, error);
            }
            return new AddPartitionsToTxnResponse(throttleTimeMs, errors);
        } else {
            AddPartitionsToTxnResponseData response = new AddPartitionsToTxnResponseData();
            AddPartitionsToTxnResultCollection results = new AddPartitionsToTxnResultCollection();
            for (AddPartitionsToTxnTransaction transaction : data().transactions()) {
                results.add(errorResponseForTransaction(transaction.transactionalId(), error));
            }
            response.setResultsByTransaction(results);
            response.setThrottleTimeMs(throttleTimeMs);
            return new AddPartitionsToTxnResponse(response);
        }
    }
    
    public AddPartitionsToTxnResult errorResponseForTransaction(String transactionalId, Errors e) {
        AddPartitionsToTxnResult txnResult = new AddPartitionsToTxnResult().setTransactionalId(transactionalId);
        AddPartitionsToTxnTopicResultCollection topicResults = new AddPartitionsToTxnTopicResultCollection();
        for (AddPartitionsToTxnTopic topic : data.transactions().find(transactionalId).topics()) {
            AddPartitionsToTxnTopicResult topicResult = new AddPartitionsToTxnTopicResult().setName(topic.name());
            AddPartitionsToTxnPartitionResultCollection partitionResult = new AddPartitionsToTxnPartitionResultCollection();
            for (Integer partition : topic.partitions()) {
                partitionResult.add(new AddPartitionsToTxnPartitionResult()
                        .setPartitionIndex(partition)
                        .setPartitionErrorCode(e.code()));
            }
            topicResult.setResults(partitionResult);
            topicResults.add(topicResult);
        }
        txnResult.setTopicResults(topicResults);
        return txnResult;
    }

    public static AddPartitionsToTxnRequest parse(ByteBuffer buffer, short version) {
        return new AddPartitionsToTxnRequest(new AddPartitionsToTxnRequestData(new ByteBufferAccessor(buffer), version), version);
    }
}
