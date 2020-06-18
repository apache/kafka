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
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//import static org.apache.kafka.common.protocol.CommonFields.*;
import static org.apache.kafka.common.protocol.CommonFields.HOST;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.types.Type.INT64;

/**
 * This wrapper supports both v0 and v1 of ProduceResponse.
 */
public class RDMAProduceAddressResponse extends AbstractResponse {
    public static final long UNKNOWN_OFFSET = -1L;
    public static final long UNKNOWN_ADDRESS = -1L;
    public static final int UNKNOWN_RKEY = -1;
    public static final int UNKNOWN_IMMDATA = -1;
    public static final int UNKNOWN_LENGTH = -1;


    private static final Field.Int32 PORT = new Field.Int32("port",
            "rdmaport");

    private static final String RESPONSES_KEY_NAME = "responses";

    // topic level field names
    private static final String PARTITION_RESPONSES_KEY_NAME = "partition_responses";




    private static final String BASE_OFFSET_KEY_NAME = "base_offset";

    private static final Field.Int64 OFFSET = new Field.Int64("offset",
            "The address associated with the returned offset");

    private static final Field.Int64 ADDRESS = new Field.Int64("address",
            "The address associated with the returned offset");
    private static final Field.Int32 RKEY = new Field.Int32("rkey",
            "The address associated with the returned offset");
    private static final Field.Int32 IMMDATA = new Field.Int32("immdata",
            "The address associated with the returned offset");
    private static final Field.Int32 LENGTH = new Field.Int32("length",
            "Length of the address");

    private static final Schema RDMA_PRODUCE_ADDRESS_RESPONSE_V0 = new Schema(
            HOST, PORT,
            new Field(RESPONSES_KEY_NAME, new ArrayOf(new Schema(
                    TOPIC_NAME,
                    new Field(PARTITION_RESPONSES_KEY_NAME, new ArrayOf(new Schema(
                            PARTITION_ID,
                            ERROR_CODE,
                            new Field(BASE_OFFSET_KEY_NAME, INT64), OFFSET, ADDRESS, RKEY, IMMDATA, LENGTH
                    )))))));


    public static Schema[] schemaVersions() {
        return new Schema[]{RDMA_PRODUCE_ADDRESS_RESPONSE_V0};
    }


    private final String hostname;
    private final int rdmaport;
    private final Map<TopicPartition, PartitionResponse> responses;


    /**
     * Constructor for the latest version
     * @param responses Produced data grouped by topic-partition
     */
    public RDMAProduceAddressResponse(String hostname, int rdmaport, Map<TopicPartition, PartitionResponse> responses) {
        this.hostname = hostname;
        this.rdmaport = rdmaport;
        this.responses = responses;
    }

    /**
     * Constructor from a {@link Struct}.
     */
    public RDMAProduceAddressResponse(Struct struct) {
        this.hostname = struct.get(HOST);
        this.rdmaport = struct.get(PORT);
        responses = new HashMap<>();
        for (Object topicResponse : struct.getArray(RESPONSES_KEY_NAME)) {
            Struct topicRespStruct = (Struct) topicResponse;
            String topic = topicRespStruct.get(TOPIC_NAME);
            for (Object partResponse : topicRespStruct.getArray(PARTITION_RESPONSES_KEY_NAME)) {
                Struct partRespStruct = (Struct) partResponse;
                int partition = partRespStruct.get(PARTITION_ID);
                Errors error = Errors.forCode(partRespStruct.get(ERROR_CODE));
                long baseOffset = partRespStruct.getLong(BASE_OFFSET_KEY_NAME);
                long offset = partRespStruct.get(OFFSET);
                TopicPartition tp = new TopicPartition(topic, partition);
                long address = partRespStruct.get(ADDRESS);
                int rkey = partRespStruct.get(RKEY);
                int immdata = partRespStruct.get(IMMDATA);
                int length = partRespStruct.get(LENGTH);
                responses.put(tp, new PartitionResponse(error, baseOffset, offset, address, rkey, immdata, length));
            }
        }
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.PRODUCER_RDMA_REGISTER.responseSchema(version));
        struct.set(HOST, hostname);
        struct.set(PORT, rdmaport);
        Map<String, Map<Integer, PartitionResponse>> responseByTopic = CollectionUtils.groupPartitionDataByTopic(responses);
        List<Struct> topicDatas = new ArrayList<>(responseByTopic.size());
        for (Map.Entry<String, Map<Integer, PartitionResponse>> entry : responseByTopic.entrySet()) {
            Struct topicData = struct.instance(RESPONSES_KEY_NAME);
            topicData.set(TOPIC_NAME, entry.getKey());
            List<Struct> partitionArray = new ArrayList<>();
            for (Map.Entry<Integer, PartitionResponse> partitionEntry : entry.getValue().entrySet()) {
                PartitionResponse part = partitionEntry.getValue();
                short errorCode = part.error.code();
                // If producer sends ProduceRequest V3 or earlier, the client library is not guaranteed to recognize the error code
                // for KafkaStorageException. In this case the client library will translate KafkaStorageException to
                // UnknownServerException which is not retriable. We can ensure that producer will update metadata and retry
                // by converting the KafkaStorageException to NotLeaderForPartitionException in the response if ProduceRequest version <= 3
                if (errorCode == Errors.KAFKA_STORAGE_ERROR.code() && version <= 3)
                    errorCode = Errors.NOT_LEADER_FOR_PARTITION.code();
                Struct partStruct = topicData.instance(PARTITION_RESPONSES_KEY_NAME)
                        .set(PARTITION_ID, partitionEntry.getKey())
                        .set(ERROR_CODE, errorCode)
                        .set(BASE_OFFSET_KEY_NAME, part.baseOffset)
                        .set(OFFSET, part.offset)
                        .set(ADDRESS, part.address)
                        .set(RKEY, part.rkey)
                        .set(IMMDATA, part.immdata)
                        .set(LENGTH, part.length);

                partitionArray.add(partStruct);
            }
            topicData.set(PARTITION_RESPONSES_KEY_NAME, partitionArray.toArray());
            topicDatas.add(topicData);
        }
        struct.set(RESPONSES_KEY_NAME, topicDatas.toArray());

        return struct;
    }

    public Map<TopicPartition, PartitionResponse> responses() {
        return this.responses;
    }

    public String rdmaHost() {
        return this.hostname;
    }
    public int rdmaPort() {
        return this.rdmaport;
    }


    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        for (PartitionResponse response : responses.values())
            updateErrorCounts(errorCounts, response.error);
        return errorCounts;
    }

    public static final class PartitionResponse {
        final public Errors error;
        final public long baseOffset;

        final public long offset;
        final public Long address;
        final public Integer rkey;
        final public Integer immdata;
        final public Integer length;


        public PartitionResponse(Errors error) {
            this(error, UNKNOWN_OFFSET, UNKNOWN_OFFSET, UNKNOWN_ADDRESS, UNKNOWN_RKEY, UNKNOWN_IMMDATA, UNKNOWN_LENGTH);
        }

        public PartitionResponse(Errors error, long baseOffset, long offset, long address, int rkey,  int immdata, int length) {
            this.error = error;
            this.offset = offset;
            this.baseOffset = baseOffset;
            this.address = address;
            this.rkey = rkey;
            this.immdata = immdata;
            this.length = length;
        }

        @Override
        public String toString() {
            StringBuilder b = new StringBuilder();
            b.append('{');
            b.append("error: ");
            b.append(error);
            b.append(",offset: ");
            b.append(baseOffset);
            b.append('}');
            b.append(", address: ").append(address).
                    append(", rkey: ").append(rkey).
                    append(", length: ").append(length);
            return b.toString();
        }
    }

    public static RDMAProduceAddressResponse parse(ByteBuffer buffer, short version) {
        return new RDMAProduceAddressResponse(ApiKeys.PRODUCER_RDMA_REGISTER.responseSchema(version).read(buffer));
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 6;
    }
}
