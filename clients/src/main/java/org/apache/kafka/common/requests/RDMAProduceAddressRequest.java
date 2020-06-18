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
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Collection;
import java.util.Collections;

import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.types.Type.INT32;
import static org.apache.kafka.common.protocol.types.Type.INT16;

public class RDMAProduceAddressRequest extends AbstractRequest {

    private static final Field.ComplexArray TOPICS = new Field.ComplexArray("topics",
            "Topics to fetch offsets. If the topic array is null fetch offsets for all topics.");

    private static final Field.ComplexArray TOPICS_TO_UPDATE = new Field.ComplexArray("topics to update",
            "Topics to fetch offsets. If the topic array is null fetch offsets for all topics.");

    private static final Field.Bool IS_FROM_LEADER = new Field.Bool("is_freom_leader",
            "Indicates that request comes from broker leader");

    // topic level fields
    private static final Field.Array PARTITIONS = new Field.Array("partitions", INT32,
            "Partitions to remove from the fetch session.");

    private static final Field TOPICS_V0 = TOPICS.withFields(
            TOPIC_NAME,
            PARTITIONS);


    private static final Field.Array PARTITIONS_TO_UPDATE = new Field.Array("partitions to update", INT32,
            "Partitions to remove from the fetch session.");

    private static final Field TOPIC_DATA_TO_UPDATE_V0 = TOPICS_TO_UPDATE.withFields(
            TOPIC_NAME,
            PARTITIONS_TO_UPDATE);


    private static final String ACKS_KEY_NAME = "acks";

    private static final String TIMEOUT_KEY_NAME = "timeout";




    private static final Schema RDMA_PRODUCE_ADDRESS_REQUEST_V0 =  new Schema(
            IS_FROM_LEADER,
            TOPICS_V0,
            TOPIC_DATA_TO_UPDATE_V0,
            new Field(TIMEOUT_KEY_NAME, INT32, "The time to await a response in ms."),
            new Field(ACKS_KEY_NAME, INT16, "The time to await a response in ms.")
            );


    public static Schema[] schemaVersions() {
        return new Schema[] {RDMA_PRODUCE_ADDRESS_REQUEST_V0};
    }


    public static class Builder extends AbstractRequest.Builder<RDMAProduceAddressRequest> {
        private final List<TopicPartition> topicPartitions;
        private final List<TopicPartition> toUpdate;
        private final int timeout;
        private final short acks;
        private final boolean fromLeader;



        public Builder(short acks,
                       List<TopicPartition> topicPartitions,
                       List<TopicPartition> toUpdate, int timeout) {
            this(acks, topicPartitions, toUpdate, timeout, false);
        }

        public Builder(short acks,
                       List<TopicPartition> topicPartitions,
                       List<TopicPartition> toUpdate, int timeout, boolean fromLeader) {
            super(ApiKeys.PRODUCER_RDMA_REGISTER);
            this.acks = acks;
            this.topicPartitions = topicPartitions;
            this.toUpdate = toUpdate;
            this.timeout = timeout;
            this.fromLeader = fromLeader;
        }

        @Override
        public RDMAProduceAddressRequest build(short version) {

            return new RDMAProduceAddressRequest(version, acks, topicPartitions, toUpdate, timeout, fromLeader);
        }


        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=RDMAProduceAddressRequest")
                    .append(", acks=").append(acks)
                    .append("'");
            return bld.toString();
        }
    }


    private final List<TopicPartition> topicPartitions;
    private final List<TopicPartition> toUpdate;
    private final int timeout;
    private final short acks;
    private final boolean isFromLeader;

    private RDMAProduceAddressRequest(short version, short acks,
                                      List<TopicPartition> topicPartitions,
                                      List<TopicPartition> toUpdate, int timeout) {
        this(version, acks, topicPartitions, toUpdate, timeout, false);
    }


    private RDMAProduceAddressRequest(short version, short acks,
                                      List<TopicPartition> topicPartitions,
                                      List<TopicPartition> toUpdate, int timeout, boolean isFromLeader) {
        super(ApiKeys.PRODUCER_RDMA_REGISTER, version);
        this.acks = acks;
        this.topicPartitions = topicPartitions;
        this.toUpdate = toUpdate;
        this.timeout = timeout;
        this.isFromLeader = isFromLeader;
    }



    public RDMAProduceAddressRequest(Struct struct, short version) {
        super(ApiKeys.PRODUCER_RDMA_REGISTER, version);
        this.isFromLeader = struct.get(IS_FROM_LEADER);
        Object[] topicArray = struct.get(TOPICS);
        if (topicArray != null) {
            topicPartitions = new ArrayList<>();
            for (Object topicResponseObj : topicArray) {
                Struct topicResponse = (Struct) topicResponseObj;
                String topic = topicResponse.get(TOPIC_NAME);
                for (Object partObj : topicResponse.get(PARTITIONS)) {
                    Integer part = (Integer) partObj;

                    topicPartitions.add(new TopicPartition(topic, part));
                }
            }
        } else
            topicPartitions = null;


        toUpdate = new ArrayList<>(0);
        for (Object forgottenTopicObj : struct.get(TOPICS_TO_UPDATE)) {
            Struct forgottenTopic = (Struct) forgottenTopicObj;
            String topicName = forgottenTopic.get(TOPIC_NAME);
            for (Object partObj : forgottenTopic.get(PARTITIONS_TO_UPDATE)) {
                Integer part = (Integer) partObj;
                toUpdate.add(new TopicPartition(topicName, part));
            }
        }


        this.timeout = struct.getInt(TIMEOUT_KEY_NAME);
        this.acks = struct.getShort(ACKS_KEY_NAME);
    }

    @Override
    public Struct toStruct() {
        // Store it in a local variable to protect against concurrent updates
        List<TopicPartition> topicPartitions = this.topicPartitions;
        Struct struct = new Struct(ApiKeys.PRODUCER_RDMA_REGISTER.requestSchema(version()));

        struct.set(IS_FROM_LEADER, isFromLeader);

        Map<String, List<Integer>> topicsData = CollectionUtils.groupPartitionsByTopic(topicPartitions);

        List<Struct> topicArray = new ArrayList<>();
        for (Map.Entry<String, List<Integer>> entries : topicsData.entrySet()) {
            Struct topicData = struct.instance(TOPICS);
            topicData.set(TOPIC_NAME, entries.getKey());
          /*  List<Struct> partitionArray = new ArrayList<>();
            for (Integer partitionId : entries.getValue()) {
                Struct partitionData = topicData.instance(PARTITIONS);
                partitionData.set(PARTITION_ID, partitionId);
                partitionArray.add(partitionData);
            }*/
            topicData.set(PARTITIONS,  entries.getValue().toArray());
            topicArray.add(topicData);
        }
        struct.set(TOPICS, topicArray.toArray());


        Map<String, List<Integer>> topicsToPartitions = new HashMap<>();
        for (TopicPartition part : toUpdate) {
            List<Integer> partitions = topicsToPartitions.computeIfAbsent(part.topic(), topic -> new ArrayList<>());
            partitions.add(part.partition());
        }
        List<Struct> toForgetStructs = new ArrayList<>();
        for (Map.Entry<String, List<Integer>> entry : topicsToPartitions.entrySet()) {
            Struct toForgetStruct = struct.instance(TOPICS_TO_UPDATE);
            toForgetStruct.set(TOPIC_NAME, entry.getKey());
            toForgetStruct.set(PARTITIONS_TO_UPDATE, entry.getValue().toArray());
            toForgetStructs.add(toForgetStruct);
        }
        struct.set(TOPICS_TO_UPDATE, toForgetStructs.toArray());

        struct.set(TIMEOUT_KEY_NAME, timeout);
        struct.set(ACKS_KEY_NAME, acks);

        return struct;
    }

    @Override
    public String toString(boolean verbose) {
        // Use the same format as `Struct.toString()`
        StringBuilder bld = new StringBuilder();
        bld.append("{acks=").append(acks);

        bld.append("}");
        return bld.toString();
    }

    @Override
    public RDMAProduceAddressResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        /* In case the producer doesn't actually want any response */
        if (acks == 0)
            return null;

        Errors error = Errors.forException(e);
        Map<TopicPartition, RDMAProduceAddressResponse.PartitionResponse> responseMap = new HashMap<>();
        RDMAProduceAddressResponse.PartitionResponse partitionResponse = new RDMAProduceAddressResponse.PartitionResponse(error);

        for (TopicPartition tp : partitions())
            responseMap.put(tp, partitionResponse);

        short versionId = version();
        switch (versionId) {
            case 0:
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
                return new RDMAProduceAddressResponse("", 0, responseMap);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.PRODUCER_RDMA_REGISTER.latestVersion()));
        }
    }

    @Override
    public Map<Errors, Integer> errorCounts(Throwable e) {
        Errors error = Errors.forException(e);
        return Collections.singletonMap(error, partitions().size());
    }

    public Collection<TopicPartition> partitions() {
        return topicPartitions;
    }

    public Collection<TopicPartition> partitionsToUpdate() {
        return toUpdate;
    }

    public short acks() {
        return acks;
    }

    public boolean isFromLeader() {
        return isFromLeader;
    }

    public int timeout() {
        return timeout;
    }


    public static ProduceRequest parse(ByteBuffer buffer, short version) {
        return new ProduceRequest(ApiKeys.PRODUCER_RDMA_REGISTER.parseRequest(version, buffer), version);
    }

}
