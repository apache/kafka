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
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Possible error codes:
 *
 *   - {@link Errors#INVALID_PRODUCER_EPOCH}
 *   - {@link Errors#NOT_COORDINATOR}
 *   - {@link Errors#COORDINATOR_NOT_AVAILABLE}
 *   - {@link Errors#COORDINATOR_LOAD_IN_PROGRESS}
 *   - {@link Errors#OFFSET_METADATA_TOO_LARGE}
 *   - {@link Errors#GROUP_AUTHORIZATION_FAILED}
 *   - {@link Errors#INVALID_COMMIT_OFFSET_SIZE}
 *   - {@link Errors#TRANSACTIONAL_ID_AUTHORIZATION_FAILED}
 *   - {@link Errors#REQUEST_TIMED_OUT}
 *   - {@link Errors#UNKNOWN_MEMBER_ID}
 *   - {@link Errors#FENCED_INSTANCE_ID}
 *   - {@link Errors#ILLEGAL_GENERATION}
 */
public class TxnOffsetCommitResponse extends AbstractResponse {

    public final TxnOffsetCommitResponseData data;

    public TxnOffsetCommitResponse(TxnOffsetCommitResponseData data) {
        this.data = data;
    }

    public TxnOffsetCommitResponse(Struct struct, short version) {
        this.data = new TxnOffsetCommitResponseData(struct, version);
    }

    public TxnOffsetCommitResponse(int requestThrottleMs, Map<TopicPartition, Errors> responseData) {
        Map<String, TxnOffsetCommitResponseTopic> responseTopicDataMap = new HashMap<>();

        for (Map.Entry<TopicPartition, Errors> entry : responseData.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            String topicName = topicPartition.topic();

            TxnOffsetCommitResponseTopic topic = responseTopicDataMap.getOrDefault(
                topicName, new TxnOffsetCommitResponseTopic().setName(topicName));

            topic.partitions().add(new TxnOffsetCommitResponsePartition()
                                       .setErrorCode(entry.getValue().code())
                                       .setPartitionIndex(topicPartition.partition())
            );
            responseTopicDataMap.put(topicName, topic);
        }

        data = new TxnOffsetCommitResponseData()
                   .setTopics(new ArrayList<>(responseTopicDataMap.values()))
                   .setThrottleTimeMs(requestThrottleMs);
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(data.topics().stream()
                .flatMap(topic -> topic.partitions().stream())
                .map(partition -> Errors.forCode(partition.errorCode())));

//        return errorCounts(errors().values());

//        Map<Errors, Integer> errorMap = new HashMap<>();
//        data.topics().forEach(topic ->
//            topic.partitions().forEach(partition ->
//                updateErrorCounts(errorMap, Errors.forCode(partition.errorCode()))
//            )
//        );
//        return errorMap;

//        Map<Errors, Integer> errorMap = new HashMap<>();
//        for (TxnOffsetCommitResponseTopic topic : data.topics()) {
//            for (TxnOffsetCommitResponsePartition partition : topic.partitions()) {
//                updateErrorCounts(errorMap, Errors.forCode(partition.errorCode()));
//            }
//        }
//        return errorMap;
    }

    public Map<TopicPartition, Errors> errors() {
        Map<TopicPartition, Errors> errorMap = new HashMap<>();
        for (TxnOffsetCommitResponseTopic topic : data.topics()) {
            for (TxnOffsetCommitResponsePartition partition : topic.partitions()) {
                errorMap.put(new TopicPartition(topic.name(), partition.partitionIndex()),
                             Errors.forCode(partition.errorCode()));
            }
        }
        return errorMap;
    }

    public static TxnOffsetCommitResponse parse(ByteBuffer buffer, short version) {
        return new TxnOffsetCommitResponse(ApiKeys.TXN_OFFSET_COMMIT.parseResponse(version, buffer), version);
    }

    @Override
    public String toString() {
        return data.toString();
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }

    public static void main(String[] args) {
        int numTopics = 100;
        int numPartitions = 100;
        List<TxnOffsetCommitResponseTopic> topics = new ArrayList<>(numTopics);
        for (int i = 0; i < numTopics; i++) {
            List<TxnOffsetCommitResponsePartition> partitions = new ArrayList<>(numPartitions);
            for (int j = 0; j < numPartitions; j++) {
                partitions.add(new TxnOffsetCommitResponsePartition().setErrorCode(Errors.NONE.code()).setPartitionIndex(j));
            }
            topics.add(new TxnOffsetCommitResponseTopic().setName("topic-" + i).setPartitions(partitions));
        }
        TxnOffsetCommitResponse resp = new TxnOffsetCommitResponse(new TxnOffsetCommitResponseData().setTopics(topics));

        int times = 20_000;
        for (int i = 0; i < 20; i++) {
            // timed run
            long t0 = System.nanoTime();
            Map<Errors, Integer> map = c(resp, times);
            long t1 = System.nanoTime();
            System.out.println(map);
            long nanos = t1 - t0;
            System.out.println("run " + i + ", times=" + times + " took " + nanos + "ns, " + ((times * 1.0E9) / nanos) + "ops/s");
        }
    }

    private static Map<Errors, Integer> c(TxnOffsetCommitResponse resp, int times) {
        Map<Errors, Integer> map = new HashMap<>();
        for (int i = 0; i < times; i++) {
            Map<Errors, Integer> errorsIntegerMap = resp.errorCounts();
            for (Map.Entry<Errors, Integer> entry : errorsIntegerMap.entrySet()) {
                map.compute(entry.getKey(), (key, value) -> value != null ? value + entry.getValue() : entry.getValue());
            }
        }
        return map;
    }
}
