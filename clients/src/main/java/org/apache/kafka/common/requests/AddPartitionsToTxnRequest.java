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
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTopicCollection;
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

    public static class Builder extends AbstractRequest.Builder<AddPartitionsToTxnRequest> {
        public final AddPartitionsToTxnRequestData data;

        public Builder(final AddPartitionsToTxnRequestData data) {
            super(ApiKeys.ADD_PARTITIONS_TO_TXN);
            this.data = data;
        }

        public Builder(final String transactionalId,
                       final long producerId,
                       final short producerEpoch,
                       final List<TopicPartition> partitions) {
            super(ApiKeys.ADD_PARTITIONS_TO_TXN);

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

            this.data = new AddPartitionsToTxnRequestData()
                            .setTransactionalId(transactionalId)
                            .setProducerId(producerId)
                            .setProducerEpoch(producerEpoch)
                            .setTopics(topics);
        }

        @Override
        public AddPartitionsToTxnRequest build(short version) {
            return new AddPartitionsToTxnRequest(data, version);
        }

        static List<TopicPartition> getPartitions(AddPartitionsToTxnRequestData data) {
            List<TopicPartition> partitions = new ArrayList<>();
            for (AddPartitionsToTxnTopic topicCollection : data.topics()) {
                for (Integer partition : topicCollection.partitions()) {
                    partitions.add(new TopicPartition(topicCollection.name(), partition));
                }
            }
            return partitions;
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

    public List<TopicPartition> partitions() {
        if (cachedPartitions != null) {
            return cachedPartitions;
        }
        cachedPartitions = Builder.getPartitions(data);
        return cachedPartitions;
    }

    @Override
    public AddPartitionsToTxnRequestData data() {
        return data;
    }

    @Override
    public AddPartitionsToTxnResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        final HashMap<TopicPartition, Errors> errors = new HashMap<>();
        for (TopicPartition partition : partitions()) {
            errors.put(partition, Errors.forException(e));
        }
        return new AddPartitionsToTxnResponse(throttleTimeMs, errors);
    }

    public static AddPartitionsToTxnRequest parse(ByteBuffer buffer, short version) {
        return new AddPartitionsToTxnRequest(new AddPartitionsToTxnRequestData(new ByteBufferAccessor(buffer), version), version);
    }
}
