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
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnPartitionResult;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnPartitionResultCollection;
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

/**
 * Possible error codes:
 *
 *   - {@link Errors#NOT_COORDINATOR}
 *   - {@link Errors#COORDINATOR_NOT_AVAILABLE}
 *   - {@link Errors#COORDINATOR_LOAD_IN_PROGRESS}
 *   - {@link Errors#INVALID_TXN_STATE}
 *   - {@link Errors#INVALID_PRODUCER_ID_MAPPING}
 *   - {@link Errors#INVALID_PRODUCER_EPOCH} // for version <=1
 *   - {@link Errors#PRODUCER_FENCED}
 *   - {@link Errors#TOPIC_AUTHORIZATION_FAILED}
 *   - {@link Errors#TRANSACTIONAL_ID_AUTHORIZATION_FAILED}
 *   - {@link Errors#UNKNOWN_TOPIC_OR_PARTITION}
 */
public class AddPartitionsToTxnResponse extends AbstractResponse {

    private final AddPartitionsToTxnResponseData data;

    public static final String V3_AND_BELOW_TXN_ID = "";

    public AddPartitionsToTxnResponse(AddPartitionsToTxnResponseData data) {
        super(ApiKeys.ADD_PARTITIONS_TO_TXN);
        this.data = data;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    public Map<String, Map<TopicPartition, Errors>> errors() {
        Map<String, Map<TopicPartition, Errors>> errorsMap = new HashMap<>();

        if (!this.data.resultsByTopicV3AndBelow().isEmpty()) {
            errorsMap.put(V3_AND_BELOW_TXN_ID, errorsForTransaction(this.data.resultsByTopicV3AndBelow()));
        }

        for (AddPartitionsToTxnResult result : this.data.resultsByTransaction()) {
            errorsMap.put(result.transactionalId(), errorsForTransaction(result.topicResults()));
        }
        
        return errorsMap;
    }

    private static AddPartitionsToTxnTopicResultCollection topicCollectionForErrors(Map<TopicPartition, Errors> errors) {
        Map<String, AddPartitionsToTxnPartitionResultCollection> resultMap = new HashMap<>();

        for (Map.Entry<TopicPartition, Errors> entry : errors.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            String topicName = topicPartition.topic();

            AddPartitionsToTxnPartitionResult partitionResult =
                    new AddPartitionsToTxnPartitionResult()
                        .setPartitionErrorCode(entry.getValue().code())
                        .setPartitionIndex(topicPartition.partition());

            AddPartitionsToTxnPartitionResultCollection partitionResultCollection = resultMap.getOrDefault(
                    topicName, new AddPartitionsToTxnPartitionResultCollection()
            );

            partitionResultCollection.add(partitionResult);
            resultMap.put(topicName, partitionResultCollection);
        }

        AddPartitionsToTxnTopicResultCollection topicCollection = new AddPartitionsToTxnTopicResultCollection();
        for (Map.Entry<String, AddPartitionsToTxnPartitionResultCollection> entry : resultMap.entrySet()) {
            topicCollection.add(new AddPartitionsToTxnTopicResult()
                .setName(entry.getKey())
                .setResultsByPartition(entry.getValue()));
        }
        return topicCollection;
    }

    public static AddPartitionsToTxnResult resultForTransaction(String transactionalId, Map<TopicPartition, Errors> errors) {
        return new AddPartitionsToTxnResult().setTransactionalId(transactionalId).setTopicResults(topicCollectionForErrors(errors));
    }

    public AddPartitionsToTxnTopicResultCollection getTransactionTopicResults(String transactionalId) {
        return data.resultsByTransaction().find(transactionalId).topicResults();
    }

    public static Map<TopicPartition, Errors> errorsForTransaction(AddPartitionsToTxnTopicResultCollection topicCollection) {
        Map<TopicPartition, Errors> topicResults = new HashMap<>();
        for (AddPartitionsToTxnTopicResult topicResult : topicCollection) {
            for (AddPartitionsToTxnPartitionResult partitionResult : topicResult.resultsByPartition()) {
                topicResults.put(
                    new TopicPartition(topicResult.name(), partitionResult.partitionIndex()), Errors.forCode(partitionResult.partitionErrorCode()));
            }
        }
        return topicResults;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        List<Errors> allErrors = new ArrayList<>();

        // If we are not using this field, we have request 4 or later
        if (this.data.resultsByTopicV3AndBelow().isEmpty()) {
            allErrors.add(Errors.forCode(data.errorCode()));
        }
        
        errors().forEach((txnId, errors) -> 
            allErrors.addAll(errors.values())
        );
        return errorCounts(allErrors);
    }

    @Override
    public AddPartitionsToTxnResponseData data() {
        return data;
    }

    public static AddPartitionsToTxnResponse parse(ByteBuffer buffer, short version) {
        return new AddPartitionsToTxnResponse(new AddPartitionsToTxnResponseData(new ByteBufferAccessor(buffer), version));
    }

    @Override
    public String toString() {
        return data.toString();
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }
}
